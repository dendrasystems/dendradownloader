#!/usr/bin/env python3

import argparse
import configparser
import mimetypes
import re
from collections import namedtuple
from collections.abc import Generator
from functools import wraps
from pathlib import Path
from urllib.parse import ParseResult, urlencode, urlparse

try:
    import arcpy
except ModuleNotFoundError:
    arcpy = None
import requests

Parameters = namedtuple("Parameters", ["config", "hosts", "collections"])


class SettingsError(Exception):
    pass


app_integrations_url = "/internal/account/app-integrations"

REQUIRED_SETTINGS = {
    "auth_token": f"auth_token is required. Copy it from {app_integrations_url}",
    "catalogue_url": f"catalogue_url is requred. See STAC project URLs at {app_integrations_url}",
    "data_dir": "data_dir is required",
}


class Settings:
    config = None
    host = None

    settings = [
        "auth_token",
        "catalogue_url",
        "data_dir",
        "redownload",
        "add_to_active_map",
    ]

    auth_token: str = None
    catalogue_url: str = None
    data_dir: Path = None
    redownload: bool = False
    add_to_active_map: bool = False

    def __init__(self, config_path: str | Path, host: str):
        self.config = get_config(config_path)
        self.host = host

        for setting in self.settings:
            setattr(self, setting, self._get_setting(setting))

    def _get_setting(self, setting_name: str) -> bool | str:
        """
        Retrieve the setting from the config file.
        """
        match setting_name:
            case "redownload" | "add_to_active_map":
                setting_value = self.config[self.host].getboolean(setting_name)
            case "data_dir":
                setting_value = Path(self.config[self.host].get(setting_name))
            case _:
                setting_value = self.config[self.host].get(setting_name)

        if setting_value is None and setting_name in REQUIRED_SETTINGS:
            raise SettingsError(REQUIRED_SETTINGS[setting_name])

        return setting_value

    def show_settings(self) -> None:
        for attr in dir(self):
            if attr not in self.settings:
                continue

            value = getattr(self, attr)

            if attr == "auth_token":
                print(f"{attr}: {value[:5]}{'*' * (len(value) - 5)}")  # noqa: T201
                continue
            print(f"{attr}: {value}")  # noqa: T201


def params(fn):
    @wraps(fn)
    def with_params(self, parameters, *args, **kwargs):
        params = Parameters(*parameters)
        return fn(self, params, *args, **kwargs)

    return with_params


def format_mb(size: int) -> str:
    return f"{size / 1024 / 1024:.2f}"


def format_bytes(size: int) -> str:
    """
    Format bytes into KB, MB, or GB depending on the size.

    Args:
        size (int): The size in bytes.

    Returns:
        str: The formatted size as a string.
    """
    if size < 1024:
        return f"{size} B"
    elif size < 1024**2:
        return f"{size / 1024:.2f} KB"
    elif size < 1024**3:
        return f"{size / (1024**2):.2f} MB"
    else:
        return f"{size / (1024**3):.2f} GB"


def guess_suffix(mimetype: str) -> str:
    """
    Guess the file extension based on the MIME type.

    Args:
        mimetype (str): The MIME type of the file.

    Returns:
        str: The guessed file extension.
    """
    match mimetype:
        case "application/geo+json":
            return ".geojson"
        case x if "image/tif" in x:
            return ".tif"
        case _:
            return mimetypes.guess_extension(mimetype)


def progress_bar(done: int, total: int, progress: int) -> str:
    return f"\r[{'=' * done}{' ' * (50 - done)}] {format_mb(progress)}/{format_mb(total)} MiB"


def download_file(
    *,
    data_dir: str | Path,
    replace_existing: bool,
    parsed_url: ParseResult,
    local_filename: Path,
    argis_progress_msg: str,
) -> str:
    local_file_path = data_dir / local_filename

    if not local_file_path.exists() or replace_existing:
        with requests.get(parsed_url.geturl(), stream=True) as response:  # noqa: S113
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            downloaded_size = 0

            if arcpy:
                arcpy.SetProgressor("step", f"{argis_progress_msg}  [{format_bytes(total_size)}]", 0, 100, 1)
                current_step = 0

            with open(str(local_file_path), "wb") as f:
                for chunk in response.iter_content(chunk_size=100 * 1024):
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    if total_size == 0:
                        done = 0
                    else:
                        done = int(50 * downloaded_size / total_size)
                    if arcpy:
                        try:
                            progress = (downloaded_size / total_size) // 0.01
                        except ZeroDivisionError:
                            progress = 0
                        if progress > current_step:
                            current_step = progress
                            arcpy.SetProgressorPosition(int(current_step))
                    print(  # noqa: T201
                        progress_bar(done, total_size, downloaded_size),
                        end="",
                    )
            print()  # noqa: T201

            if arcpy:
                arcpy.SetProgressorPosition(100)

    return local_file_path


def get_config(config_path: str | Path):
    config = configparser.ConfigParser()
    config.read([config_path])
    return config


def get_next_link(response_data: dict) -> str | None:
    """
    Retrieve the next link from the response data.

    Returns the next link if it exists, otherwise None.
    """
    try:
        return next(link["href"] for link in response_data["links"] if link["rel"] == "next")
    except StopIteration:
        pass


def get_search_url(catalogue_url: str, query: dict) -> str:
    """
    Construct the search URL for the STAC API.

    Returns the search URL.
    """
    if collections := query.get("collections"):
        if isinstance(collections, list):
            query["collections"] = ",".join(str(x) for x in collections)

    parsed_url = urlparse(catalogue_url + "/search")
    parsed_url = parsed_url._replace(query=urlencode(query, safe=","))
    return parsed_url.geturl()


def get_result_count(auth_token: str, catalogue_url: str, collection_ids: list[str] | None = None) -> int:
    """
    Use STAC search API to retrieve the number of items.

    ConformsTo: https://api.stacspec.org/v1.0.0/item-search
    """
    query = {"collections": collection_ids} if collection_ids else {}
    query["limit"] = 0
    catalogue_search_url = get_search_url(catalogue_url, query=query)

    response = requests.get(
        catalogue_search_url,
        headers={"Authorization": f"Token {auth_token}"},
        timeout=60,
    )
    response.raise_for_status()
    response_data = response.json()

    return response_data["numberMatched"]


def search(
    auth_token: str, catalogue_url: str, collection_ids: list[str] | None = None
) -> Generator[list[dict], None, None]:
    """
    Use STAC search API to retrieve matching items.

    ConformsTo: https://api.stacspec.org/v1.0.0/item-search
    """
    query = {"collections": collection_ids} if collection_ids else {}
    catalogue_search_url = get_search_url(catalogue_url, query=query)

    while catalogue_search_url:
        response = requests.get(
            catalogue_search_url,
            headers={"Authorization": f"Token {auth_token}"},
            timeout=60,
        )
        response.raise_for_status()
        response_data = response.json()

        yield from response_data["features"]

        catalogue_search_url = get_next_link(response_data)


def get_available_collections(auth_token: str, catalogue_url: str) -> list[str]:
    """
    Use the collections endpoint to get a list of collections for each catalogue.

    ConformanceClass: https://api.stacspec.org/v1.0.0/collections
    """

    collection_list_url = catalogue_url + "/collections"
    response = requests.get(
        collection_list_url,
        headers={"Authorization": f"Token {auth_token}"},
        timeout=60,
    )
    response.raise_for_status()

    collections = response.json()["collections"]

    return [f"{collection['id']} {collection['title']}" for collection in collections]


def get_collection_title(item: dict) -> str | None:
    """
    Retrieve the collection name from the item's links.

    Returns the title of the collection if it exists, otherwise None.
    """
    try:
        return next(link["title"] for link in item["links"] if link["rel"] == "collection")
    except StopIteration:
        pass


def format_for_filename(filename: str) -> str:
    """
    Format the filename to be compatible with Windows.
    """
    # Replace invalid characters with an underscore
    filename = re.sub(r"[:\"/\|?*]", "", filename)
    filename = re.sub(r"\s+", " ", filename)

    # Special case for <> as they convey meaning
    filename = filename.replace("<", "under")
    filename = filename.replace(">", "over")
    return filename


def prepare_download(asset: dict) -> tuple[ParseResult, Path]:
    """
    Parse the asset URL and prepare the filename for download.
    """
    parsed_download_href = urlparse(asset["href"])

    filename = Path(parsed_download_href.path.split("/")[-1])

    suffix = filename.suffix
    if not suffix:
        suffix = guess_suffix(asset["type"]) or ""

    # Override the downloaded filename with the asset title if available
    if asset.get("title"):
        new_filename = filename.with_name(format_for_filename(asset["title"]))
        if not new_filename.suffix:
            new_filename = new_filename.with_suffix(suffix)
        filename = new_filename

    return parsed_download_href, filename


def download_files_in_collections(
    settings: Settings, collection_ids: list[str], on_downloaded=lambda x: x
) -> list[str]:
    data_dir = settings.data_dir

    http_error_400s = []

    result_count = get_result_count(settings.auth_token, settings.catalogue_url, collection_ids)

    if arcpy:
        arcpy.SetProgressorLabel(f"Found {result_count} items...")

    search_results = search(settings.auth_token, settings.catalogue_url, collection_ids)
    items_processed = 0

    for feature in search_results:
        base_progress_msg = f"({items_processed + 1}/{result_count}) STAC Items. Downloading assets:"

        collection_id = feature["collection"]
        collection_dir = data_dir / get_collection_title(feature)

        # Organize by survey date if available
        if datetime := feature["properties"].get("datetime"):
            collection_dir /= datetime[:7]

        if not collection_dir.exists():
            collection_dir.mkdir(parents=True)

        for i, asset in enumerate(feature["assets"].values()):
            parsed_download_href, filename = prepare_download(asset)

            try:
                on_downloaded(
                    download_file(
                        data_dir=collection_dir,
                        replace_existing=settings.redownload,
                        parsed_url=parsed_download_href,
                        local_filename=filename,
                        argis_progress_msg=f"{base_progress_msg} ({i + 1}/{len(feature['assets'])}) {filename.name}",  # noqa: T201
                    )
                )
            except requests.HTTPError as e:
                # This happens when an S3 token expires
                if e.response.status_code == 400:
                    http_error_400s.append(collection_id)
                else:
                    raise e

        items_processed += 1

    return http_error_400s


class Toolbox:
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Toolbox"
        self.alias = "toolbox"

        # List of tool classes associated with this toolbox
        self.tools = [DendraDownloader]


class DendraDownloader:
    def __init__(self):
        """
        Define the tool (tool name is the name of the class).
        """
        self.label = "Dendra Downloader"
        self.description = ""

    def getParameterInfo(self):
        config = arcpy.Parameter(
            displayName="Configuration File",
            name="config",
            datatype="DEFile",
            parameterType="Required",
            direction="Input",
        )
        config.filter.list = ["ini"]
        hosts = arcpy.Parameter(
            displayName="Hosts",
            name="hosts",
            datatype="GPValueTable",
            parameterType="Optional",
            direction="Input",
        )
        hosts.columns = [["GPString", "Title"]]
        hosts.filters[0].type = "ValueList"
        collections = arcpy.Parameter(
            displayName="Collections",
            name="collections",
            datatype="GPValueTable",
            parameterType="Optional",
            direction="Input",
            multiValue=True,
        )
        collections.columns = [["GPString", "Title"]]
        collections.filters[0].type = "ValueList"

        return [config, hosts, collections]

    def isLicensed(self):
        """
        Set whether the tool is licensed to execute.
        """
        return True

    @params
    def updateParameters(self, parameters):
        """
        Modify the values and properties of parameters before internal
        validation is performed.

        This method is called whenever a parameter has been changed.
        """
        return

    @params
    def updateMessages(self, parameters):
        """
        Modify the messages created by internal validation for each tool
        parameter.

        This method is called after internal validation.
        """
        if parameters.config.altered:
            config_path = Path(parameters.config.valueAsText)
            config = get_config(config_path)
            parameters.hosts.filters[0].list = config.sections()

        if parameters.hosts.altered:
            config_path = Path(parameters.config.valueAsText)
            settings = Settings(config_path, parameters.hosts.valueAsText)
            parameters.collections.filters[0].list = get_available_collections(
                settings.auth_token, settings.catalogue_url
            )

    @params
    def execute(self, parameters, messages):
        """
        Download the requested resources.
        """
        host = parameters.hosts.valueAsText
        config_path = Path(parameters.config.valueAsText)
        settings = Settings(config_path, host)
        active_map = None
        if settings.add_to_active_map:
            project = arcpy.mp.ArcGISProject("current")

            if project.activeMap:
                active_map = project.activeMap

        def update_arcgis(local_file_path):
            local_file_path_name = str(local_file_path)
            messages.addMessage(local_file_path_name)
            if active_map and (local_file_path.suffix.lower() == ".tif" or local_file_path.suffix == ".tiff"):
                active_map.addDataFromPath(local_file_path_name)

        collection_ids = [value[0].split()[0] for value in parameters.collections.values]
        http_error_400s = download_files_in_collections(settings, collection_ids, update_arcgis)

        # 400 happens when s3 token expires, retry once
        if http_error_400s:
            download_files_in_collections(settings, http_error_400s, update_arcgis)

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return


def command_line():
    actions = ["show-settings", "show-collection-ids", "download-files"]

    parser = argparse.ArgumentParser(
        prog="Dendra Downloader",
        description="Download collections of files via Dendra's Stac API",
    )
    parser.add_argument("action", choices=actions)
    parser.add_argument("--config-path", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--collection-ids", nargs="*")

    args = parser.parse_args()

    if args.action not in actions:
        raise Exception(f"Unknown action: {args.action}")

    settings = Settings(args.config_path, args.host)

    if args.action == "show-settings":
        settings.show_settings()

    elif args.action == "show-collection-ids":
        print("\n".join(get_available_collections(settings.auth_token, settings.catalogue_url)))  # noqa: T201

    elif args.action == "download-files":
        http_error_400s = download_files_in_collections(settings, args.collection_ids, print)

        if http_error_400s:
            print(f"The following collections returned an HTTP 400 (S3 token may have expired): {http_error_400s}")  # noqa: T201


if __name__ == "__main__":
    command_line()
