import pytest
from data_pipeline_api.metadata_store import MetadataStore


def test_find_superset():
    assert MetadataStore([{"key": "value", "version": "1.0.0"}]).find(
        {"key": "value"}
    ) == {"key": "value", "version": "1.0.0"}


def test_do_not_find_subset():
    assert (
        MetadataStore([{"key": "value", "version": "1.0.0"}]).find(
            {"key": "value", "hello": "world"}
        )
        == None
    )


def test_find_highest_version():
    assert MetadataStore([{"version": "1.0.0"}, {"version": "2.0.0"}]).find({}) == {
        "version": "2.0.0"
    }


def test_versions_must_be_semver():
    with pytest.raises(ValueError):
        MetadataStore([{"version": 1}])


def test_missing_version_is_ok():
    MetadataStore([{"key": "world"}])
