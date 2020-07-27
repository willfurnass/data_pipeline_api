import pytest

from data_pipeline_api.registry.download import _parse_read_config


NAMESPACE = "namespace"


def test_parse_read_config_no_where_raises():
    with pytest.raises(ValueError):
        _parse_read_config({}, NAMESPACE)


def test_parse_read_config_no_data_product_in_where_raises():
    with pytest.raises(ValueError):
        _parse_read_config({"where": {}}, NAMESPACE)


def test_parse_read_config_raises_no_namespace():
    with pytest.raises(ValueError):
        _parse_read_config({"where": {"data_product": "name"}}, None)


def test_parse_read_config():
    result = _parse_read_config({"where": {"data_product": "data_product_name"}}, NAMESPACE)
    assert result.data_product == "data_product_name"
    assert result.namespace == NAMESPACE
    assert result.component is None
    assert result.version is None

    result = _parse_read_config(
        {
            "where": {"data_product": "data_product_name", "component": "component1", "version": "1.0.0"},
            "use": {"data_product": "override"},
        },
        NAMESPACE,
    )
    assert result.data_product == "override"
    assert result.namespace == NAMESPACE
    assert result.component is "component1"
    assert result.version is "1.0.0"

    result = _parse_read_config(
        {
            "where": {"data_product": "data_product_name", "component": "component1", "version": "1.0.0"},
            "use": {"component": "overidec", "version": "1.0.0+override"},
        },
        NAMESPACE,
    )
    assert result.data_product == "data_product_name"
    assert result.namespace == NAMESPACE
    assert result.component is "overidec"
    assert result.version is "1.0.0+override"

    result = _parse_read_config({"where": {"data_product": "data_product_name", "namespace": "otherns"}}, NAMESPACE)
    assert result.data_product == "data_product_name"
    assert result.namespace == "otherns"
    assert result.component is None
    assert result.version is None
