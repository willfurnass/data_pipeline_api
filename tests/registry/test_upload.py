from unittest.mock import patch, call

import pytest
import yaml

from data_pipeline_api.registry.upload import resolve_references, upload_from_config
from data_pipeline_api.registry.common import get_end_point, get_headers, get_on_end_point
from tests.registry.test_common import DATA_REGISTRY_URL, TOKEN, MockResponse


@pytest.fixture(autouse=True)
def clear_cache():
    get_on_end_point.cache_clear()


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
    data = {"name": "A", "o": {"target": "vref", "data": {"version": "1", "model": "mock_url_b"}}}
    with patch("requests.get") as get:
        get.return_value = MockResponse([{"url": "mock_url_v", "version": "1", "model": "mock_url_b"}])
        assert resolve_references(data, DATA_REGISTRY_URL, TOKEN) == {"name": "A", "o": "mock_url_v"}


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
            version: '1.1.1'
    """)
    with patch("requests.get") as get:
        with patch("requests.post") as post:
            get.return_value = MockResponse([])
            upload_from_config(config, DATA_REGISTRY_URL, TOKEN)
            post.assert_called_once_with(
                get_end_point(DATA_REGISTRY_URL, "end_point_1"),
                data={"version": "1.1.1"},
                headers=get_headers(TOKEN),
            )


def test_upload_from_config_bad_version():
    config = yaml.safe_load("""
post:
    - 
        target: 'end_point_1'
        data:
            version: '1'
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
