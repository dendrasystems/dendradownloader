import importlib.util
from importlib.machinery import SourceFileLoader
from pathlib import Path
from unittest.mock import patch
import shutil
import unittest
from urllib.parse import urlparse
import requests
import tempfile


def import_from_file(module_name, file_path):
    loader = SourceFileLoader(module_name, file_path)
    spec = importlib.util.spec_from_loader(module_name, loader)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


dd = import_from_file("./dendra_downloader.pyt", "dendra_downloader.pyt")


class TestDendraDownloader(unittest.TestCase):
    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return "fake value"

    maxDiff = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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
        self.tmp = Path(tempfile.mkdtemp())
        self.config_path = self.tmp / "config.ini"
        fake_config = f"[unit.test]\nauth_token: foo\ncache_duration_mins: 1\ncatalogue_urls: http://www.example.com/catalogue_1|http://www.example.com/catalogue_2\ndata_dir: {self.tmp}"
        self.config_path.write_text(fake_config)

    @patch.object(requests, "get")
    def test_download_file(self, _):
        expected_file = self.tmp / "fake_file"
        fake_parsed_url = urlparse("http://www.example.com/fake_file")
        downloaded_file = dd.download_file(self.tmp, False, fake_parsed_url)
        self.assertEqual(downloaded_file, expected_file)

    def test_get_config(self):
        config = dd.get_config(self.config_path)
        self.assertDictEqual(
            dict(config["unit.test"]),
            # setting types aren't correct because we haven't used getboolean, getint etc
            {
                "auth_token": "foo",
                "cache_duration_mins": "1",
                "catalogue_urls": "http://www.example.com/catalogue_1|http://www.example.com/catalogue_2",
                "data_dir": str(self.tmp),
                "redownload": "False",
                "add_to_active_map": "False",
            },
        )

    @patch.object(requests, "get")
    def test_fetch_catalogues(self, mock_get):
        config = dd.get_config(self.config_path)
        auth_token = dd.get_setting(config, "unit.test", "auth_token")
        catalogue_urls = dd.get_setting(config, "unit.test", "catalogue_urls")
        mock_get.return_value = self.FakeResponse()
        catalogues = dd.fetch_catalogues(auth_token, self.state, catalogue_urls)
        self.assertEqual(
            catalogues["search_results"]["http://www.example.com/catalogue_1/search"],
            "fake value",
        )
        self.assertEqual(
            catalogues["search_results"]["http://www.example.com/catalogue_2/search"],
            "fake value",
        )

    def test_get_collections(self):
        collections = dd.get_collections(self.state)
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
        collection_titles = dd.get_collection_titles(self.state)
        self.assertEqual(
            collection_titles, {1: "collection 1", 2: "collection 2", 3: "collection 3"}
        )

    def test_get_collection_hrefs(self):
        collection_titles = dd.get_collection_hrefs(self.state)
        self.assertEqual(
            collection_titles,
            {
                1: "http://www.example.com/collection_1",
                2: "http://www.example.com/collection_2",
                3: "http://www.example.com/collection_3",
            },
        )

    def test_has_expired_returns_true_if_expired_in_past(self):
        config = dd.get_config(self.config_path)
        cache_duration_mins = dd.get_setting(config, "unit.test", "cache_duration_mins")
        self.assertTrue(
            dd.has_expired(cache_duration_mins, {**self.state, "last_accessed": 1})
        )

    def test_has_expired_returns_false_if_expired_in_future(self):
        config = dd.get_config(self.config_path)
        cache_duration_mins = dd.get_setting(config, "unit.test", "cache_duration_mins")
        self.assertFalse(
            dd.has_expired(cache_duration_mins, {**self.state, "last_accessed": 9**99})
        )

    @patch.object(requests, "get")
    def test_sync_state(self, mock_get):
        config = dd.get_config(self.config_path)
        mock_get.return_value = self.FakeResponse()
        state = dd.sync_state(config, "unit.test")
        self.assertEqual(
            state["search_results"]["http://www.example.com/catalogue_1/search"],
            "fake value",
        )
        self.assertEqual(
            state["search_results"]["http://www.example.com/catalogue_2/search"],
            "fake value",
        )

    @patch.object(requests, "get")
    def test_download_files_in_collections(self, mock_get):
        class FakeResponse:
            def raise_for_status(self):
                pass

            def json(self):
                return {"features": []}

        config = dd.get_config(self.config_path)
        mock_get.return_value = FakeResponse()
        state = dd.sync_state(config, "unit.test")
        http_error_400s = dd.download_files_in_collections(
            config, "unit.test", state, [1, 2]
        )
        self.assertEqual(http_error_400s, [])

    def tearDown(self):
        shutil.rmtree(str(self.tmp))


if __name__ == "__main__":
    unittest.main()
