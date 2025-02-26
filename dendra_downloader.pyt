#!/usr/bin/env python3

from collections import namedtuple
from datetime import datetime
from functools import wraps
from pathlib import Path
from urllib.parse import urlparse

import argparse
import configparser
import json

try:
    import arcpy
except ModuleNotFoundError:
    arcpy = None
import requests


Parameters = namedtuple("Parameters", ["config", "hosts", "collections"])


class SettingsError(Exception):
    pass


app_integrations_url = "https://aus.dendradev.io/internal/account/app-integrations"

auth_token_help_text = f"auth_token is required. Copy it from {app_integrations_url}"
catalogue_urls_help_text = (
    f"catalogue_urls is requred. See STAC project URLs at {app_integrations_url}"
)
data_dir_help_text = "data_dir is required"

# Anything that returns a SettingsError as default value is a required setting
SETTINGS = {
    "auth_token": lambda obj, name: obj.get(name, SettingsError(auth_token_help_text)),
    "catalogue_urls": lambda obj, name: obj.get(name).split("|")
    if obj.get(name)
    else SettingsError(catalogue_urls_help_text),
    "data_dir": lambda obj, name: obj.get(name, SettingsError(data_dir_help_text)),
    "redownload": lambda obj, name: obj.getboolean(name, False),
    "add_to_active_map": lambda obj, name: obj.getboolean(name, False),
    "cache_duration_mins": lambda obj, name: obj.getint(name, 10),
}


def get_setting(config, host, setting_name):
    """Try to get a setting from the config file, fall back to SETTINGS"""
    if setting_name not in SETTINGS:
        raise Exception(f"Unknown setting name: {setting_name}")

    setting_value = SETTINGS[setting_name](config[host], setting_name)

    if isinstance(setting_value, SettingsError):
        raise setting_value

    return setting_value


def params(fn):
    @wraps(fn)
    def with_params(self, parameters, *args, **kwargs):
        params = Parameters(*parameters)
        return fn(self, params, *args, **kwargs)

    return with_params


def download_file(data_dir, replace_existing, parsed_url):
    local_filename = parsed_url.path.split("/")[-1]
    local_file_path = data_dir / local_filename

    if not local_file_path.exists() or replace_existing:
        with requests.get(parsed_url.geturl(), stream=True) as response:
            response.raise_for_status()
            with open(str(local_file_path), "wb") as f:
                for chunk in response.iter_content(chunk_size=100 * 1024):
                    f.write(chunk)

    return local_file_path


def get_config(config_path):
    config = configparser.ConfigParser()
    config.read([config_path])
    return config


def fetch_catalogues(auth_token, state, catalogue_urls):
    state_copy = dict(state)

    for catalogue_url in catalogue_urls:
        catalogue_search_url = catalogue_url + "/search"
        response = requests.get(
            catalogue_search_url,
            headers={"Authorization": f"Token {auth_token}"},
        )
        response.raise_for_status()

        state_copy["search_results"][catalogue_search_url] = response.json()

    return state_copy


def get_collections(state):
    collections = {}
    for catalogue in state["search_results"].values():
        for feature in catalogue["features"]:
            collection_link = [
                link for link in feature["links"] if link["rel"] == "collection"
            ][0]
            collections[feature["collection"]] = collection_link
    return collections


def get_collection_titles(state):
    collections = get_collections(state)
    return {k: v["title"] for k, v in collections.items()}


def get_collection_hrefs(state):
    collections = get_collections(state)
    return {k: v["href"] for k, v in collections.items()}


def has_expired(cache_duration_mins, state):
    cache_duration_secs = cache_duration_mins * 60
    expiry_time = int(state["last_accessed"]) + cache_duration_secs
    return expiry_time < int(datetime.now().timestamp())


def sync_state(config, host, force_refresh=False):
    data_dir = Path(get_setting(config, host, "data_dir"))
    auth_token = get_setting(config, host, "auth_token")
    catalogue_urls = get_setting(config, host, "catalogue_urls")
    cache_duration_mins = get_setting(config, host, "cache_duration_mins")

    if not data_dir.exists():
        data_dir.mkdir()

    state_path = data_dir / "state.json"
    state = {
        "search_results": {},
        "collections": {},
        "last_accessed": 0,
    }

    if state_path.exists():
        state = json.loads(state_path.read_text())

    if force_refresh or has_expired(cache_duration_mins, state):
        state = fetch_catalogues(auth_token, state, catalogue_urls)
        state["last_accessed"] = int(datetime.now().timestamp())
        state_path.write_text(json.dumps(state))

    return state


def download_files_in_collections(
    config, host, state, collection_ids, on_downloaded=lambda x: x
):
    data_dir = Path(get_setting(config, host, "data_dir"))

    http_error_400s = []

    for catalogue in state["search_results"].values():
        for feature in catalogue["features"]:
            collection_id = feature["collection"]
            if collection_id in collection_ids:
                parsed_download_href = urlparse(feature["assets"]["download"]["href"])

                collection_dir = data_dir / get_collection_titles(state)[collection_id]

                if not collection_dir.exists():
                    collection_dir.mkdir()

                try:
                    on_downloaded(
                        download_file(
                            collection_dir,
                            get_setting(config, host, "redownload"),
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
        """Define the tool (tool name is the name of the class)."""
        self.label = "Dendra Downloader"
        self.description = ""

    def getParameterInfo(self):
        """Define the tool parameters."""
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
        """Set whether the tool is licensed to execute."""
        return True

    @params
    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    @params
    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        if parameters.config.altered:
            config_path = Path(parameters.config.valueAsText)
            config = get_config(config_path)
            parameters.hosts.filters[0].list = config.sections()

        if parameters.hosts.altered:
            config_path = Path(parameters.config.valueAsText)
            config = get_config(config_path)
            host = parameters.hosts.valueAsText
            state = sync_state(config, host)

            parameters.collections.filters[0].list = [
                f"{collection_id} {collection_title}"
                for collection_id, collection_title in get_collection_titles(
                    state
                ).items()
            ]

    @params
    def execute(self, parameters, messages):
        """The source code of the tool."""
        # messages.addMessage(response.json())
        host = parameters.hosts.valueAsText
        config_path = Path(parameters.config.valueAsText)
        config = get_config(config_path)
        data_dir = Path(get_setting(config, host, "data_dir"))
        state_path = data_dir / "state.json"
        state = json.loads(state_path.read_text())
        active_map = None

        if get_setting(config, host, "add_to_active_map"):
            project = arcpy.mp.ArcGISProject("current")

            if project.activeMap:
                active_map = project.activeMap

        def update_arcgis(local_file_path):
            local_file_path_name = str(local_file_path)
            messages.addMessage(local_file_path_name)
            if active_map and (
                local_file_path.suffix.lower() == ".tif"
                or local_file_path.suffix == ".tiff"
            ):
                active_map.addDataFromPath(local_file_path_name)

        collection_ids = [
            value[0].split()[0] for value in parameters.collections.values
        ]
        http_error_400s = download_files_in_collections(
            config, host, state, collection_ids, update_arcgis
        )

        # 400 happens when s3 token expires, retry once
        if http_error_400s:
            state = sync_state(config, host, force_refresh=True)
            download_files_in_collections(
                config, host, state, http_error_400s, update_arcgis
            )

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Dendra Downloader",
        description="Download collections of files via Dendra's Stac API",
    )
    parser.add_argument("action")
    parser.add_argument("--config-path")
    parser.add_argument("--host")
    parser.add_argument("--collection-ids", nargs="*")

    args = parser.parse_args()

    if args.action == "show-settings":
        config = get_config(args.config_path)
        for setting_name in SETTINGS:
            print(f"{setting_name}: {get_setting(config, args.host, setting_name)}")
    elif args.action == "show-collection-ids":
        config = get_config(args.config_path)
        state = sync_state(config, args.host)
        print(get_collection_titles(state))
    elif args.action == "download-files":
        config = get_config(args.config_path)
        state = sync_state(config, args.host)
        http_error_400s = download_files_in_collections(
            config, args.host, state, args.collection_ids, print
        )
        print(
            f"The following collections returned an HTTP 400 (S3 token may have expired): {http_error_400s}"
        )
    else:
        raise Exception(f"Unknown action: {args.action}")
