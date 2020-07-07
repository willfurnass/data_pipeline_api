from hashlib import sha1
from datetime import datetime as dt
import pytest

from data_pipeline_api.registry.access_upload import _verify_hash, _create_target_data_dict, unique_posts


@pytest.fixture()
def tmp_file_calculated_hash(tmp_path):
    d = tmp_path / "sub"
    d.mkdir()
    p = d / "test.txt"
    p.write_text("some content in a file")
    return p


@pytest.fixture()
def calculated_hash(tmp_file_calculated_hash):
    with open(tmp_file_calculated_hash, "rb") as f:
        return sha1(f.read()).hexdigest()


def test_verify_hash(tmp_file_calculated_hash, calculated_hash):
    _verify_hash(tmp_file_calculated_hash, calculated_hash)
    with pytest.raises(ValueError):
        _verify_hash(tmp_file_calculated_hash, "somemadeuphash")


def test_create_target_data_dict():
    assert _create_target_data_dict("a_target", {"key": "value"}) == {"target": "a_target", "data": {"key": "value"}}


def test_unique_posts():
    assert unique_posts([{"a": 1}, {"b": 2}, {"a": 1}]) == [{"a": 1}, {"b": 2}]
    assert unique_posts([{"a": 1}, {"b": 2}, {"a": 3}]) == [{"a": 1}, {"b": 2}, {"a": 3}]
    now = dt.now()
    assert unique_posts([{"a": now}, {"a": 1}, {"a": now}]) == [{"a": now}, {"a": 1}]
    assert unique_posts([{"a": {"a": {"a": 1}}}, {"b": {"b": {"b": 1}}}, {"a": {"a": {"a": 1}}}]) == [
        {"a": {"a": {"a": 1}}},
        {"b": {"b": {"b": 1}}},
    ]
