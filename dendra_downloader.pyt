#!/usr/bin/env python3

import argparse
import configparser
from collections import namedtuple
from functools import wraps
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

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

    def __init__(self, config_path, host):
        self.config = get_config(config_path)
        self.host = host
        self.auth_token = self._get_setting("auth_token")
        self.catalogue_url = self._get_setting("catalogue_url")
        self.data_dir = Path(self._get_setting("data_dir"))

    def _get_setting(self, setting_name):
        """
        Retrieve the setting from the config file.
        """
        if setting_name == "redownload" or setting_name == "add_to_active_map":
            setting_value = self.config[self.host].getboolean(setting_name)
        else:
            setting_value = self.config[self.host].get(setting_name)

        if setting_value is None and setting_name in REQUIRED_SETTINGS:
            raise SettingsError(REQUIRED_SETTINGS[setting_name])

        return setting_value

    def show_settings(self):
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


def format_mb(size):
    return f"{size / 1024 / 1024:.2f}"


def progress_bar(done, total, progress):
    return f"\r[{'=' * done}{' ' * (50 - done)}] {format_mb(progress)}/{format_mb(total)} MiB"


def download_file(data_dir, replace_existing, parsed_url):
    local_filename = parsed_url.path.split("/")[-1]
    local_file_path = data_dir / local_filename

    if not local_file_path.exists() or replace_existing:
        with requests.get(parsed_url.geturl(), stream=True) as response:  # noqa: S113
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            downloaded_size = 0

            with open(str(local_file_path), "wb") as f:
                for chunk in response.iter_content(chunk_size=100 * 1024):
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    if total_size == 0:
                        done = 0
                    else:
                        done = int(50 * downloaded_size / total_size)
                    print(  # noqa: T201
                        progress_bar(done, total_size, downloaded_size),
                        end="",
                    )
            print()  # noqa: T201

    return local_file_path


def get_config(config_path):
    config = configparser.ConfigParser()
    config.read([config_path])
    return config


def search(auth_token, catalogue_url, collection_ids=None):
    catalogue_search_url = catalogue_url + "/search"

    if collection_ids:
        catalogue_search_url += f"?collections={','.join(collection_ids)}"

    response = requests.get(
        catalogue_search_url,
        headers={"Authorization": f"Token {auth_token}"},
        timeout=60,
    )
    response.raise_for_status()

    return response.json()


def get_available_collections(auth_token, catalogue_url) -> list[str]:
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


def get_collection_title(item) -> Optional[str]:
    """
    Retrieve the collection name from the item's links.

    Returns the title of the collection if it exists, otherwise None.
    """
    try:
        return next(link["title"] for link in item["links"] if link["rel"] == "collection")
    except StopIteration:
        pass


def download_files_in_collections(settings: Settings, collection_ids: list[str], on_downloaded=lambda x: x):
    data_dir = settings.data_dir

    http_error_400s = []

    search_results = search(settings.auth_token, settings.catalogue_url, collection_ids)
    for feature in search_results["features"]:
        collection_id = feature["collection"]
        collection_dir = data_dir / get_collection_title(feature)
        parsed_download_href = urlparse(feature["assets"]["download"]["href"])

        if not collection_dir.exists():
            collection_dir.mkdir()

        try:
            on_downloaded(
                download_file(
                    collection_dir,
                    settings.redownload,
                    parsed_download_href,
                )
            )
        except requests.HTTPError as e:
            # This happens when an S3 token expires
            if e.response.status_code == 400:
                http_error_400s.append(collection_id)
            else:
                raise e

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
    parser.add_argument("--config-path")
    parser.add_argument("--host")
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
