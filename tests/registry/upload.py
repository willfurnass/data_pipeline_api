# contents of test_app.py, a simple test for our API retrieval
from unittest.mock import patch, call

import pytest
import yaml

from data_pipeline_api.registry.upload import (
    get_end_point,
    get_headers,
    resolve_references,
    get,
    upload_from_config,
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


def test_get_end_point():
    assert get_end_point("https://someurl", "target") == "https://someurl/target/"
    assert get_end_point("https://someurl/test", "target") == "https://someurl/test/target/"
    assert get_end_point("https://someurl/test/", "target") == "https://someurl/test/target/"
    assert get_end_point("https://someurl/test/", "target/") == "https://someurl/test/target/"


def test_get_headers():
    assert get_headers("abcde") == {"Authorization": "token abcde"}


def test_resolve_references_no_resolution():
    data = {"A": "1", "B": "2"}
    assert resolve_references(data, DATA_REGISTRY_URL, TOKEN) == data


def test_resolve_references_name():
    with patch("requests.get") as get:
        get.return_value = MockResponse([{"url": "mock_url_b", "name": "B"}])
        data = {"name": "A", "o": {"target": "nref", "data": {"name": "B", "o": "2"}}}
        assert resolve_references(data, DATA_REGISTRY_URL, TOKEN) == {"name": "A", "o": "mock_url_b"}
        data = {
            "name": "A",
            "o": {"target": "nref", "data": {"name": "B", "o": {"target": "aref", "data": {"name": "C"}}}},
        }
        assert resolve_references(data, DATA_REGISTRY_URL, TOKEN) == {"name": "A", "o": "mock_url_b"}


def test_resolve_references_version():
    data = {"name": "A", "o": {"target": "vref", "data": {"version_identifier": "1", "model": "mock_url_b"}}}
    with patch("requests.get") as get:
        get.return_value = MockResponse([{"url": "mock_url_v", "version_identifier": "1", "model": "mock_url_b"}])
        assert resolve_references(data, DATA_REGISTRY_URL, TOKEN) == {"name": "A", "o": "mock_url_v"}


def test_get_data_cache():
    with patch("requests.get") as get:
        json_data_1 = [{"url": "mock_url_v", "version_identifier": "1", "model": "mock_url_b"}]
        get.return_value = MockResponse(json_data_1)
        assert get("target1", DATA_REGISTRY_URL, TOKEN) == json_data_1
        assert get("target1", DATA_REGISTRY_URL, TOKEN) == json_data_1
        get.assert_called_once_with(get_end_point(DATA_REGISTRY_URL, "target1"), headers=get_headers(TOKEN))
        json_data_2 = [{"a": 1}, {"b": 2}]
        get.return_value = MockResponse(json_data_2)
        assert get("target2", DATA_REGISTRY_URL, TOKEN) == json_data_2
        assert get("target1", DATA_REGISTRY_URL, TOKEN) == json_data_1


def test_upload_from_config_with_patch_present():
    config = yaml.safe_load("""
patch:
    - 
        target: 'end_point_1'
        data:
            name: 'A'
            description: 'patched A'
    """)
    with patch("requests.get") as get:
        with patch("requests.patch") as rpatch:
            get.return_value = MockResponse([{"name": "A", "description": "initial A", "url": "mock_url_a"}])
            upload_from_config(config, DATA_REGISTRY_URL, TOKEN)
            rpatch.assert_called_once_with(
                "mock_url_a", data={"name": "A", "description": "patched A"}, headers=get_headers(TOKEN)
            )


def test_upload_from_config_with_post_not_present():
    config = yaml.safe_load("""
post:
    -
        target: 'end_point_1'
        data:
            name: 'B'
            description: 'posted B'
    """)
    with patch("requests.get") as get:
        with patch("requests.post") as post:
            get.return_value = MockResponse([])
            upload_from_config(config, DATA_REGISTRY_URL, TOKEN)
            post.assert_called_once_with(
                get_end_point(DATA_REGISTRY_URL, "end_point_1"),
                data={"name": "B", "description": "posted B"},
                headers=get_headers(TOKEN),
            )


def test_upload_from_config_with_post_present():
    config = yaml.safe_load("""
post:
    -
        target: 'end_point_1'
        data:
            name: 'B'
            description: 'posted B'
    """)
    with patch("requests.get") as get:
        with patch("requests.post") as post:
            get.return_value = MockResponse([{"name": "B", "description": "initial B", "url": "mock_url_b"}])
            upload_from_config(config, DATA_REGISTRY_URL, TOKEN)
            post.assert_not_called()


def test_upload_from_config_with_patch_not_present():
    config = yaml.safe_load("""
patch:
    - 
        target: 'end_point_1'
        data:
            name: 'A'
            description: 'patched A'
""")
    with patch("requests.get") as get:
        with patch("requests.patch") as rpatch:
            get.return_value = MockResponse([])
            upload_from_config(config, DATA_REGISTRY_URL, TOKEN)
            rpatch.assert_not_called()


def test_upload_from_config_good_version():
    config = yaml.safe_load("""
post:
    - 
        target: 'end_point_1'
        data:
            version_identifier: '1.1.1'
    """)
    with patch("requests.get") as get:
        with patch("requests.post") as post:
            get.return_value = MockResponse([])
            upload_from_config(config, DATA_REGISTRY_URL, TOKEN)
            post.assert_called_once_with(
                get_end_point(DATA_REGISTRY_URL, "end_point_1"),
                data={"version_identifier": "1.1.1"},
                headers=get_headers(TOKEN),
            )


def test_upload_from_config_bad_version():
    config = yaml.safe_load("""
post:
    - 
        target: 'end_point_1'
        data:
            version_identifier: '1'
    """)
    with patch("requests.get") as get:
        get.return_value = MockResponse([])
        with pytest.raises(ValueError):
            upload_from_config(config, DATA_REGISTRY_URL, TOKEN)


def test_resolve_references_recurse():
    data = {
        "name": "A",
        "a": {
            "target": "t",
            "data": {
                "name": "B",
                "b": {"target": "t", "data": {"name": "C", "c": {"target": "t", "data": {"name": "D"}}}},
            },
        },
    }
    with patch("data_pipeline_api.registry.upload.get_reference") as ref:

        def side_effect(ndata, ntarget, url, token):
            return ndata["name"]

        ref.side_effect = side_effect
        resolved = resolve_references(data, DATA_REGISTRY_URL, TOKEN)
        assert resolved == {"name": "A", "a": "B"}
        ref.assert_has_calls(
            [
                call({"name": "D"}, "t", DATA_REGISTRY_URL, TOKEN),
                call({"name": "C", "c": "D"}, "t", DATA_REGISTRY_URL, TOKEN),
                call({"name": "B", "b": "C"}, "t", DATA_REGISTRY_URL, TOKEN),
            ]
        )
