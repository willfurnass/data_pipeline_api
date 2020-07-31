import os
import socket
from pathlib import Path
from unittest.mock import patch, Mock, call
from datetime import datetime as dt
import pytest

from data_pipeline_api.registry.common import (
    get_on_end_point,
    get_end_point,
    get_headers,
    get_remote_filesystem_and_path,
    build_query_string,
    DataRegistryField,
    sort_by_semver,
    DataRegistryTarget,
    unique_dicts,
    upload_to_storage,
    get_filter_fields,
)

DATA_REGISTRY_URL = "data/"
TOKEN = "token"


class MockResponse:
    def __init__(self, json, raise_for_status=False, status_code="200"):
        self._json = json
        self._raise_for_status = raise_for_status
        self._status_code = status_code

    def json(self):
        return self._json

    def raise_for_status(self):
        if self._raise_for_status:
            raise ValueError("Raise")

    @property
    def status_code(self):
        return self._status_code


@pytest.fixture(autouse=True)
def clear_cache():
    get_on_end_point.cache_clear()


def test_get_end_point():
    assert get_end_point("https://someurl", "target") == "https://someurl/target/"
    assert get_end_point("https://someurl/test", "target") == "https://someurl/test/target/"
    assert get_end_point("https://someurl/test/", "target") == "https://someurl/test/target/"
    assert get_end_point("https://someurl/test/", "target/") == "https://someurl/test/target/"


def test_get_headers():
    assert get_headers("abcde") == {"Authorization": "token abcde"}


def test_get_on_end_point():
    with patch("requests.get") as get:
        json_data_1 = [{"url": "mock_url_v", "version": "1", "model": "mock_url_b"}]
        get.return_value = MockResponse(json_data_1)
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == json_data_1
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == json_data_1
        get.assert_called_once_with(get_end_point(DATA_REGISTRY_URL, "target1"), headers=get_headers(TOKEN))
        json_data_2 = [{"a": 1}, {"b": 2}]
        get.return_value = MockResponse(json_data_2)
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target2"), TOKEN) == json_data_2
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == json_data_1


@pytest.mark.parametrize(
    ["patch_fs", "protocol", "uri", "path", "kwargs", "expected_path", "expected_call"],
    [
        [
            "GithubFileSystem",
            "github",
            "github://someorg:somerepo@somesha/",
            "data/data.csv",
            dict(token=TOKEN),
            "/data/data.csv",
            dict(org="someorg", repo="somerepo", sha="somesha", token=TOKEN),
        ],
        [
            "GithubFileSystem",
            "github",
            "someorg/somerepo",
            "data/data.csv",
            dict(token=TOKEN),
            "/data/data.csv",
            dict(org="someorg", repo="somerepo", sha="master", token=TOKEN),
        ],
        [
            "LocalFileSystem",
            "file",
            "file://C:\\test",
            "data/data.csv",
            {},
            "C:/test/data/data.csv" if os.name == "nt" else "C:\\test/data/data.csv",
            dict(auto_mkdir=True),
        ],
        [
            "LocalFileSystem",
            "file",
            "file:///test",
            "data/data.csv",
            dict(auto_mkdir=False),
            "/test/data/data.csv",
            dict(auto_mkdir=False),
        ],
        [
            "LocalFileSystem",
            "file",
            "/test",
            "data/data.csv",
            dict(auto_mkdir=False),
            "/test/data/data.csv",
            dict(auto_mkdir=False),
        ],
        [
            "HTTPFileSystem",
            "http",
            "http://test/",
            "data/data.csv",
            dict(arg=1),
            "http://test/data/data.csv",
            dict(arg=1),
        ],
        [
            "HTTPFileSystem",
            "https",
            "https://test/",
            "data/data.csv",
            dict(arg=2),
            "https://test/data/data.csv",
            dict(arg=2),
        ],
        [
            "HTTPFileSystem",
            "https",
            "https://test",
            "data/data.csv",
            dict(arg=3),
            "https://test/data/data.csv",
            dict(arg=3),
        ],
        [
            "FTPFileSystem",
            "ftp",
            "ftp://test/",
            "data/data.csv",
            dict(),
            "/data/data.csv",
            dict(host="test", username=None, password=None),
        ],
        [
            "FTPFileSystem",
            "ftp",
            "ftp://test/",
            "data/data.csv",
            dict(username="uname", password="pword"),
            "/data/data.csv",
            dict(host="test", username="uname", password="pword"),
        ],
        [
            "FTPFileSystem",
            "ftp",
            "ftp://uname:pword@test/",
            "data/data.csv",
            dict(),
            "/data/data.csv",
            dict(host="test", username="uname", password="pword"),
        ],
        [
            "FTPFileSystem",
            "ftp",
            "ftp://uname:pword@test/",
            "data/data.csv",
            dict(username="over_uname", password="over_pword"),
            "/data/data.csv",
            dict(host="test", username="over_uname", password="over_pword"),
        ],
        [
            "SFTPFileSystem",
            "sftp",
            "sftp://test/",
            "data/data.csv",
            dict(),
            "/data/data.csv",
            dict(host="test", username=None, password=None),
        ],
        [
            "SFTPFileSystem",
            "sftp",
            "sftp://test/",
            "data/data.csv",
            dict(username="uname", password="pword"),
            "/data/data.csv",
            dict(host="test", username="uname", password="pword"),
        ],
        [
            "SFTPFileSystem",
            "sftp",
            "sftp://uname:pword@test/",
            "data/data.csv",
            dict(),
            "/data/data.csv",
            dict(host="test", username="uname", password="pword"),
        ],
        [
            "SFTPFileSystem",
            "sftp",
            "sftp://uname:pword@test/",
            "data/data.csv",
            dict(username="over_uname", password="over_pword"),
            "/data/data.csv",
            dict(host="test", username="over_uname", password="over_pword"),
        ],
        [
            "SFTPFileSystem",
            "ssh",
            "ssh://test/",
            "data/data.csv",
            dict(),
            "/data/data.csv",
            dict(host="test", username=None, password=None),
        ],
        [
            "SFTPFileSystem",
            "ssh",
            "ssh://test/",
            "data/data.csv",
            dict(username="uname", password="pword"),
            "/data/data.csv",
            dict(host="test", username="uname", password="pword"),
        ],
        [
            "SFTPFileSystem",
            "ssh",
            "ssh://uname:pword@test/",
            "data/data.csv",
            dict(),
            "/data/data.csv",
            dict(host="test", username="uname", password="pword"),
        ],
        [
            "SFTPFileSystem",
            "ssh",
            "ssh://uname:pword@test/",
            "data/data.csv",
            dict(username="over_uname", password="over_pword"),
            "/data/data.csv",
            dict(host="test", username="over_uname", password="over_pword"),
        ],
        ["S3FileSystem", "s3", "s3://test/", "data/data.csv", dict(arg=1), "s3://test/data/data.csv", dict(arg=1),],
    ],
)
def test_get_remote_filesystem_and_path(patch_fs, protocol, uri, path, kwargs, expected_path, expected_call):
    with patch(f"data_pipeline_api.registry.common.{patch_fs}") as rfs:
        fs, path = get_remote_filesystem_and_path(protocol, uri, path, **kwargs)
    assert path == expected_path
    assert rfs._mock_name == patch_fs
    rfs.assert_called_once_with(**expected_call)


@pytest.mark.parametrize(["error"], [[TimeoutError], [socket.timeout]])
def test_get_remote_filesystem_and_path_active_ftp(error):
    def timeout():
        raise error

    with patch(f"data_pipeline_api.registry.common.FTPFileSystem") as rfs:
        new_fs = Mock()
        rfs.return_value = new_fs
        new_fs.ftp.dir.side_effect = timeout
        get_remote_filesystem_and_path("ftp", "ftp://test/", "data/data.csv")
    new_fs.ftp.dir.assert_called_once()
    new_fs.ftp.set_pasv.assert_called_once_with(False)


def test_build_query_string():
    with patch("data_pipeline_api.registry.common.get_filter_fields") as fields:
        fields.return_value = {DataRegistryField.name}
        assert build_query_string({}, DataRegistryTarget.issue, DATA_REGISTRY_URL, TOKEN) == ""
        assert (
            build_query_string({DataRegistryField.name: "name"}, DataRegistryTarget.issue, DATA_REGISTRY_URL, TOKEN) == "name=name"
        )
        assert build_query_string({"not_a_filter": "not_a_filter"}, DataRegistryTarget.issue, DATA_REGISTRY_URL, TOKEN) == ""
        assert (
            build_query_string(
                {"not_a_filter": "not_a_filter", DataRegistryField.name: "name"},
                DataRegistryTarget.issue,
                DATA_REGISTRY_URL,
                TOKEN
            )
            == "name=name"
        )
        assert (
            build_query_string({DataRegistryField.name: '!"£$%^&*()[]{}'}, DataRegistryTarget.issue, DATA_REGISTRY_URL, TOKEN)
            == "name=%21%22%C2%A3%24%25%5E%26%2A%28%29%5B%5D%7B%7D"
        )
        assert (
            build_query_string(
                {DataRegistryField.name: f"{DATA_REGISTRY_URL}/1/"}, DataRegistryTarget.issue, DATA_REGISTRY_URL, TOKEN
            )
            == "name=1"
        )
        assert build_query_string({DataRegistryField.name: ["blah"]}, DataRegistryTarget.issue, DATA_REGISTRY_URL, TOKEN) == ""
        assert build_query_string({DataRegistryField.name: dt(2020, 7, 24, 12, 1, 2)}, DataRegistryTarget.issue, DATA_REGISTRY_URL, TOKEN) == "name=2020-07-24T12%3A01%3A02Z"
        assert build_query_string({DataRegistryField.name: f"{DATA_REGISTRY_URL}/text_file/"}, DataRegistryTarget.issue, DATA_REGISTRY_URL, TOKEN) == "name=data%2F%2Ftext_file%2F"


def test_get_filter_fields():
    with patch("requests.options") as options:
        options.return_value = MockResponse({})
        assert get_filter_fields("target", DATA_REGISTRY_URL, TOKEN) == set()

        options.return_value = MockResponse({"filter_fields": []})
        assert get_filter_fields("target", DATA_REGISTRY_URL, TOKEN) == set()

        options.return_value = MockResponse({"filter_fields": ["field1", "field2", "field3"]})
        assert get_filter_fields("target", DATA_REGISTRY_URL, TOKEN) == {"field1", "field2", "field3"}


def test_sort_by_semver():
    def make_versions(items):
        return [{"version": i} for i in items]

    def get_versions(items):
        return [i["version"] for i in items]

    assert get_versions(sort_by_semver(make_versions(["0.1.0", "0.1.1", "0.0.1", "1.0.0"]))) == [
        "1.0.0",
        "0.1.1",
        "0.1.0",
        "0.0.1",
    ]
    assert get_versions(sort_by_semver(make_versions(["0.1.0", "0.1.1", "0.0.1", "1.0.0"]), descending=False)) == [
        "0.0.1",
        "0.1.0",
        "0.1.1",
        "1.0.0",
    ]


def test_unique_dicts():
    assert unique_dicts([{"a": 1}, {"b": 2}, {"a": 1}]) == [{"a": 1}, {"b": 2}]
    assert unique_dicts([{"a": 1}, {"b": 2}, {"a": 3}]) == [{"a": 1}, {"b": 2}, {"a": 3}]
    now = dt.now()
    assert unique_dicts([{"a": now}, {"a": 1}, {"a": now}]) == [{"a": now}, {"a": 1}]
    assert unique_dicts([{"a": {"a": {"a": 1}}}, {"b": {"b": {"b": 1}}}, {"a": {"a": {"a": 1}}}]) == [
        {"a": {"a": {"a": 1}}},
        {"b": {"b": {"b": 1}}},
    ]


@pytest.mark.parametrize(["remote_uri", "filename", "return_path", "prefix", "expected_path"], [
    ["ssh://server/srv/ftp/", "test.txt", "/srv/ftp/test.txt", None, "test_abc.txt"],
    ["ssh://server/srv/ftp/", "some/path/test.txt", "/srv/ftp/some/path/test.txt", None, "some/path/test_abc.txt"],
    ["http://somewebsite.com/", "some/path/test.txt", "http://somewebsite.com/some/path/test.txt", None, "some/path/test_abc.txt"],
    ["ssh://server/", "some/path/test.txt", "some/path/test.txt", None, "some/path/test_abc.txt"],
    ["ssh://server/", "some/path/test.txt", "prefix/some/path/test.txt", "prefix", "prefix/some/path/test_abc.txt"],
])
def test_upload_to_storage(remote_uri, filename, return_path, prefix, expected_path):
    with patch("data_pipeline_api.registry.common.get_remote_filesystem_and_path") as rfs_and_path:
        with patch("data_pipeline_api.registry.common.FileAPI") as file_api:
            file_api.calculate_hash.return_value = "abc"
            fs = Mock()
            rfs_and_path.return_value = [fs, return_path]
            path = upload_to_storage(remote_uri, {}, Path("."), Path(filename), path_prefix=prefix)
            assert path == expected_path
            rfs_and_path.assert_called_once_with(remote_uri.split(":")[0], remote_uri, "/".join(filter(None, [prefix, filename])))


def test_get_on_end_point_paginated_single_page():
    with patch("requests.get") as get:
        results = [{"url": "mock_url_v", "version": "1", "model": "mock_url_b"}]
        json_data_1 = {"count": 1, "next": None, "results": results}
        get.return_value = MockResponse(json_data_1)
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == results
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == results
        get.assert_called_once_with(get_end_point(DATA_REGISTRY_URL, "target1"), headers=get_headers(TOKEN))
        results2 = [{"a": 1}, {"b": 2}]
        json_data_2 = {"count": 1, "next": None, "results": results2}
        get.return_value = MockResponse(json_data_2)
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target2"), TOKEN) == results2
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == results


def test_get_on_end_point_paginated_multiple_pages():
    with patch("requests.get") as get:
        results = [{"url": "mock_url_v", "version": "1", "model": "mock_url_b"}]
        json_data_1 = {"count": 2, "next": "target2", "results": results}
        json_data_2 = {"count": 2, "next": None, "results": results}
        get.side_effect = [MockResponse(json_data_1), MockResponse(json_data_2)]
        expected = (results + results).copy()
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == expected
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == expected
        get.assert_has_calls([call(get_end_point(DATA_REGISTRY_URL, "target1"), headers=get_headers(TOKEN)),
                              call("target2", headers=get_headers(TOKEN))])


def test_get_on_end_point_paginated_no_count():
    with patch("requests.get") as get:
        results = [{"url": "mock_url_v", "version": "1", "model": "mock_url_b"}]
        json_data_1 = {"next": None, "results": results}
        get.return_value = MockResponse(json_data_1)
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == results
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == results
        get.assert_called_once_with(get_end_point(DATA_REGISTRY_URL, "target1"), headers=get_headers(TOKEN))


def test_get_on_end_point_unpaginated_dict_result():
    with patch("requests.get") as get:
        json_data_1 = {"url": "mock_url_v", "version": "1", "model": "mock_url_b"}
        get.return_value = MockResponse(json_data_1)
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == json_data_1
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == json_data_1
        get.assert_called_once_with(get_end_point(DATA_REGISTRY_URL, "target1"), headers=get_headers(TOKEN))
