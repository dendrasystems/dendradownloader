import importlib.util
from importlib.machinery import SourceFileLoader
from unittest.mock import ANY, MagicMock, patch
from urllib.parse import urlparse

import pytest
import requests


def import_from_file(module_name, file_path):
    loader = SourceFileLoader(module_name, file_path)
    spec = importlib.util.spec_from_loader(module_name, loader)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


dd = import_from_file("./dendra_downloader.pyt", "dendra_downloader.pyt")


@pytest.fixture
def config_file(tmpdir):
    config_path = tmpdir / "config.ini"
    fake_config = (
        f"[unit.test]\nauth_token: foobar\ncatalogue_url: http://www.example.com/catalogue_1\ndata_dir: {tmpdir}"
    )
    config_path.write_text(fake_config, encoding="utf-8")
    return tmpdir, config_path


@pytest.fixture
def collections_response():
    return {
        "links": [],
        "collections": [
            {
                "id": "1",
                "title": "Collection1",
                "type": "Collection",
                "license": "proprietary",
            },
        ],
    }


@pytest.fixture
def item_response():
    return {
        "id": "1",
        "assets": {
            "download": {
                "href": "https://fake.com/rgbdownload.tif",
                "type": "image/tiff; application=geotiff",
                "title": "RGB",
                "roles": ["data"],
            }
        },
        "links": [
            {
                "href": "https://fake.io/api/stac/v1/1/2/collections/1/items/1",
                "rel": "self",
            },
            {"href": "https://fake.io/api/stac/v1/", "rel": "root"},
            {
                "href": "https://fake.io/api/stac/v1/1/2/collections/1",
                "rel": "collection",
                "title": "Collection 1",
            },
        ],
        "collection": "581",
    }


@pytest.fixture
def search_response(item_response):
    return {
        "type": "FeatureCollection",
        "features": [item_response],
        "links": [],
        "numberMatched": 1,
        "numberReturned": 1,
    }


def test_load_settings(config_file):
    """
    Test loads settings and defaults
    """
    data_dir, config_path = config_file
    settings = dd.Settings(config_path, "unit.test")

    assert settings.auth_token == "foobar"
    assert settings.catalogue_url == "http://www.example.com/catalogue_1"
    assert settings.data_dir == data_dir
    assert not settings.redownload
    assert not settings.add_to_active_map


def test_show_settings(config_file, capsys):
    data_dir, config_path = config_file
    settings = dd.Settings(config_path, "unit.test")
    settings.show_settings()

    expected = [
        "add_to_active_map: False",
        "auth_token: fooba*",
        "catalogue_url: http://www.example.com/catalogue_1",
        f"data_dir: {data_dir}",
        "redownload: False",
        "",
    ]

    actual = capsys.readouterr().out.split("\n")
    assert actual == expected


def test_format_mb():
    assert dd.format_mb(1024 * 1024) == "1.00"
    assert dd.format_mb(1024 * 1024 * 1024) == "1024.00"


@patch.object(requests, "get")
def test_download_file(mock_requests, tmpdir):
    expected_file = tmpdir / "fake_file"
    fake_parsed_url = urlparse("http://www.example.com/fake_file")
    downloaded_file = dd.download_file(tmpdir, False, fake_parsed_url)
    assert downloaded_file == expected_file


@patch.object(requests, "get")
def test_search(mock_request):
    dd.search("foobar", "http://www.example.com/catalogue_1")
    mock_request.assert_called_with(
        "http://www.example.com/catalogue_1/search",
        headers={"Authorization": "Token foobar"},
        timeout=60,
    )

    dd.search("foobar", "http://www.example.com/catalogue_1", ["1", "2"])
    mock_request.assert_called_with(
        "http://www.example.com/catalogue_1/search?collections=1,2",
        headers={"Authorization": "Token foobar"},
        timeout=60,
    )


@patch.object(requests, "get")
def test_get_available_collections(mock_request, collections_response):
    mock_get = mock_request.return_value = MagicMock()
    mock_get.json.return_value = collections_response

    response = dd.get_available_collections("foobar", "http://www.example.com/catalogue_1")
    mock_request.assert_called_with(
        "http://www.example.com/catalogue_1/collections",
        headers={"Authorization": "Token foobar"},
        timeout=60,
    )
    assert response == ["1 Collection1"]


def test_get_collection_title(item_response):
    assert dd.get_collection_title(item_response) == "Collection 1"
    assert dd.get_collection_title({"links": []}) is None


def test_download_files_in_collections(config_file, search_response):
    data_dir, config_path = config_file
    settings = dd.Settings(config_path, "unit.test")

    with (
        patch.object(dd, "search") as mock_search,
        patch.object(dd, "download_file") as mock_download,
    ):
        mock_search.return_value = search_response
        http_error_400s = dd.download_files_in_collections(settings, ["1"], print)

        mock_search.assert_called_with("foobar", "http://www.example.com/catalogue_1", ["1"])
        mock_download.assert_called_with(
            data_dir / "Collection 1",
            False,
            urlparse("https://fake.com/rgbdownload.tif"),
        )

    assert http_error_400s == []


class TestCommandLine:
    def test_command_line_show_settings(self, config_file, capsys):
        data_dir, config_path = config_file

        with patch(
            "sys.argv",
            [
                "dendra_downloader.pyt",
                "show-settings",
                "--config",
                str(config_path),
                "--host",
                "unit.test",
            ],
        ):
            dd.command_line()

        expected = [
            "add_to_active_map: False",
            "auth_token: fooba*",
            "catalogue_url: http://www.example.com/catalogue_1",
            f"data_dir: {data_dir}",
            "redownload: False",
            "",
        ]

        assert capsys.readouterr().out.split("\n") == expected

    @patch.object(requests, "get")
    def test_command_line_show_collection_ids(self, mock_request, config_file, collections_response, capsys):
        _, config_path = config_file

        mock_get = mock_request.return_value = MagicMock()
        mock_get.json.return_value = collections_response

        with patch(
            "sys.argv",
            [
                "dendra_downloader.pyt",
                "show-collection-ids",
                "--config",
                str(config_path),
                "--host",
                "unit.test",
            ],
        ):
            dd.command_line()

        assert capsys.readouterr().out == "1 Collection1\n"

    @patch.object(dd, "download_files_in_collections", return_value=[])
    def test_command_line_download_files(self, mock_download_files, config_file):
        _, config_path = config_file

        with patch(
            "sys.argv",
            [
                "dendra_downloader.pyt",
                "download-files",
                "--config",
                str(config_path),
                "--host",
                "unit.test",
                "--collection-ids",
                "1",
            ],
        ):
            dd.command_line()

        mock_download_files.assert_called_with(ANY, ["1"], print)
