#!/usr/bin/env python3

from collections import namedtuple
from datetime import datetime
from functools import wraps
from pathlib import Path
from urllib.parse import urlparse
from unittest.mock import patch, Mock

import argparse
import configparser
import json
import shutil
import unittest

if __name__ == "__main__":
    arcpy = Mock()
else:
    import arcpy
import requests


Parameters = namedtuple("Parameters", ["config", "hosts", "collections"])


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


def fetch_catalogues(host_config, state):
    state_copy = dict(state)
    catalogue_urls = host_config["catalogue_urls"].split("|")

    for catalogue_url in catalogue_urls:
        catalogue_search_url = catalogue_url + "/search"
        response = requests.get(
            catalogue_search_url,
            headers={"Authorization": f"Token {host_config['auth_token']}"},
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


def has_expired(host_config, state):
    cache_duration = host_config.getint("cache_duration_mins") * 60
    expiry_time = int(state["last_accessed"]) + cache_duration
    return expiry_time < int(datetime.now().timestamp())


def sync_state(config, host, force_refresh=False):
    host_config = config[host]
    data_dir = Path(config[host]["data_dir"])

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

    if force_refresh or has_expired(host_config, state):
        state = fetch_catalogues(host_config, state)
        state["last_accessed"] = int(datetime.now().timestamp())
        state_path.write_text(json.dumps(state))

    return state


def download_files_in_collections(
    config, host, state, collection_ids, on_downloaded=lambda x: x
):
    host_config = config[host]
    data_dir = Path(host_config["data_dir"])

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
                            config.getboolean(host, "redownload"),
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
        host_config = config[host]
        data_dir = Path(host_config["data_dir"])
        state_path = data_dir / "state.json"
        state = json.loads(state_path.read_text())
        active_map = None

        if config.getboolean(host, "add_to_active_map"):
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


class TestDendraDownloader(unittest.TestCase):
    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return "fake value"

    maxDiff = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tmp = Path("/tmp/dendra_downloader_test/")
        self.config_path = self.tmp / "config.ini"
        self.state = {
            "search_results": {
                "http://www.example.com/foobar": {
                    "features": [
                        {
                            "collection": 1,
                            "links": [
                                {
                                    "rel": "collection",
                                    "title": "collection 1",
                                    "href": "http://www.example.com/collection_1",
                                }
                            ],
                            "assets": {
                                "download": {
                                    "href": "https://www.example.com/foobar/bazquux1?one=1&two=2&three=3"
                                }
                            },
                        },
                        {
                            "collection": 2,
                            "links": [
                                {
                                    "rel": "collection",
                                    "title": "collection 2",
                                    "href": "http://www.example.com/collection_2",
                                }
                            ],
                            "assets": {
                                "download": {
                                    "href": "https://www.example.com/foobar/bazquux2?one=1&two=2&three=3"
                                }
                            },
                        },
                        {
                            "collection": 3,
                            "links": [
                                {
                                    "rel": "collection",
                                    "title": "collection 3",
                                    "href": "http://www.example.com/collection_3",
                                }
                            ],
                            "assets": {
                                "download": {
                                    "href": "https://www.example.com/foobar/bazquux3?one=1&two=2&three=3"
                                }
                            },
                        },
                    ]
                }
            },
            "collections": {},
            "last_accessed": 0,
        }

    def setUp(self):
        self.tmp.mkdir()
        fake_config = "[unit.test]\nauth_token: foo\ncache_duration_mins: 1\ncatalogue_urls: http://www.example.com/catalogue_1|http://www.example.com/catalogue_2\ndata_dir: /tmp/dendra_downloader_test/"
        self.config_path.write_text(fake_config)

    @patch.object(requests, "get")
    def test_download_file(self, _):
        expected_file = self.tmp / "fake_file"
        fake_parsed_url = urlparse("http://www.example.com/fake_file")
        downloaded_file = download_file(self.tmp, False, fake_parsed_url)
        self.assertEqual(downloaded_file, expected_file)

    def test_get_config(self):
        config = get_config(self.config_path)
        self.assertEqual(
            dict(config["unit.test"]),
            {
                "auth_token": "foo",
                "cache_duration_mins": "1",
                "catalogue_urls": "http://www.example.com/catalogue_1|http://www.example.com/catalogue_2",
                "data_dir": "/tmp/dendra_downloader_test/",
            },
        )

    @patch.object(requests, "get")
    def test_fetch_catalogues(self, mock_get):
        config = get_config(self.config_path)
        mock_get.return_value = self.FakeResponse()
        catalogues = fetch_catalogues(config["unit.test"], self.state)
        self.assertEqual(
            catalogues["search_results"]["http://www.example.com/catalogue_1/search"],
            "fake value",
        )
        self.assertEqual(
            catalogues["search_results"]["http://www.example.com/catalogue_2/search"],
            "fake value",
        )

    def test_get_collections(self):
        collections = get_collections(self.state)
        self.assertEqual(
            collections,
            {
                1: {
                    "rel": "collection",
                    "title": "collection 1",
                    "href": "http://www.example.com/collection_1",
                },
                2: {
                    "rel": "collection",
                    "title": "collection 2",
                    "href": "http://www.example.com/collection_2",
                },
                3: {
                    "rel": "collection",
                    "title": "collection 3",
                    "href": "http://www.example.com/collection_3",
                },
            },
        )

    def test_get_collection_titles(self):
        collection_titles = get_collection_titles(self.state)
        self.assertEqual(
            collection_titles, {1: "collection 1", 2: "collection 2", 3: "collection 3"}
        )

    def test_get_collection_hrefs(self):
        collection_titles = get_collection_hrefs(self.state)
        self.assertEqual(
            collection_titles,
            {
                1: "http://www.example.com/collection_1",
                2: "http://www.example.com/collection_2",
                3: "http://www.example.com/collection_3",
            },
        )

    def test_has_expired_returns_true_if_expired_in_past(self):
        config = get_config(self.config_path)
        self.assertTrue(
            has_expired(config["unit.test"], {**self.state, "last_accessed": 1})
        )

    def test_has_expired_returns_true_if_expired_in_future(self):
        config = get_config(self.config_path)
        self.assertFalse(
            has_expired(config["unit.test"], {**self.state, "last_accessed": 9**99})
        )

    @patch.object(requests, "get")
    def test_sync_state(self, mock_get):
        config = get_config(self.config_path)
        mock_get.return_value = self.FakeResponse()
        state = sync_state(config, "unit.test")
        self.assertEqual(state["search_results"]["http://www.example.com/catalogue_1/search"], "fake value")
        self.assertEqual(state["search_results"]["http://www.example.com/catalogue_2/search"], "fake value")

    @patch.object(requests, "get")
    def test_download_files_in_collections(self, mock_get):
        class FakeResponse:
            def raise_for_status(self):
                pass

            def json(self):
                return {"features": []}

        config = get_config(self.config_path)
        mock_get.return_value = FakeResponse()
        state = sync_state(config, "unit.test")
        http_error_400s = download_files_in_collections(
            config, "unit.test", state, [1, 2]
        )
        self.assertEqual(http_error_400s, [])

    def tearDown(self):
        shutil.rmtree(str(self.tmp))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Dendra Downloader",
        description="Download collections of files via Dendra's Stac API",
    )
    parser.add_argument("--config-path")
    parser.add_argument("--host")
    parser.add_argument("--collection_ids", nargs="*")

    args = parser.parse_args()

    if args.config_path and args.host:
        if args.collection_ids:
            # download files for these collections
            config = get_config(args.config_path)
            state = sync_state(config, args.host)
            http_error_400s = download_files_in_collections(
                config, args.host, state, args.collection_ids, print
            )
            print(
                f"The following collections returned an HTTP 400 (S3 token may have expired): {http_error_400s}"
            )
        else:
            # just show collection ids
            config = get_config(args.config_path)
            state = sync_state(config, args.host)
            print(get_collection_titles(state))
    else:
        unittest.main()
