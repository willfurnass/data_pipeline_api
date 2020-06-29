# contents of test_app.py, a simple test for our API retrieval
from unittest.mock import patch

from data_pipeline_api.registry.common import get_on_end_point, get_end_point, get_headers

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


def test_get_on_end_point():
    with patch("requests.get") as get:
        json_data_1 = [{"url": "mock_url_v", "version_identifier": "1", "model": "mock_url_b"}]
        get.return_value = MockResponse(json_data_1)
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == json_data_1
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == json_data_1
        get.assert_called_once_with(get_end_point(DATA_REGISTRY_URL, "target1"), headers=get_headers(TOKEN))
        json_data_2 = [{"a": 1}, {"b": 2}]
        get.return_value = MockResponse(json_data_2)
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target2"), TOKEN) == json_data_2
        assert get_on_end_point(get_end_point(DATA_REGISTRY_URL, "target1"), TOKEN) == json_data_1
