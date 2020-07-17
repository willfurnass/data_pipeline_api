import io
from pathlib import Path
from unittest.mock import patch, Mock

import pytest

from data_pipeline_api.registry.download import (
    _parse_read_config,
    _get_data_product_version_and_components,
    ParsedReadConfig,
    _write_metadata,
    _get_output_info,
    _download_data,
    OutputInfo,
)
from data_pipeline_api.registry.common import DataRegistryField
from tests.registry.test_common import DATA_REGISTRY_URL, TOKEN


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


def test_get_data_product_version_and_components():
    with patch("data_pipeline_api.registry.download.get_data") as get_data:
        with patch("data_pipeline_api.registry.download.get_on_end_point") as get_on_end_point:
            get_data.side_effect = [
                [{"url": "namespaceurl"}],
                [
                    {"name": "d1", "object": "o1", "version": "0.1.0", "namespace": "namespaceurl"},
                    {"name": "d1", "object": "o2", "version": "0.2.0", "namespace": "namespaceurl"},
                ],
            ]
            get_on_end_point.side_effect = [{"components": ["c1", "c2"]}, {"name": "c1"}, {"name": "c2"}]
            prc = ParsedReadConfig("namespace", "data_product", None, None)
            data_product_component_pairs = _get_data_product_version_and_components(prc, DATA_REGISTRY_URL, TOKEN)
            data_product, data_product_components = data_product_component_pairs[0]
            assert data_product["object"] == "o2"
            assert data_product["version"] == "0.2.0"
            assert data_product_components == ["c1", "c2"]

            get_data.side_effect = [
                [{"url": "namespaceurl"}],
                [
                    {"name": "d1", "object": "o1", "version": "0.1.0", "namespace": "namespaceurl"},
                    {"name": "d1", "object": "o2", "version": "0.2.0", "namespace": "namespaceurl"},
                ],
            ]
            get_on_end_point.side_effect = [{"components": ["c1", "c2"]}, {"name": "c1"}, {"name": "c2"}]
            prc = ParsedReadConfig("namespace", "data_product", "c2", None)
            data_product_component_pairs = _get_data_product_version_and_components(prc, DATA_REGISTRY_URL, TOKEN)
            data_product, data_product_components = data_product_component_pairs[0]
            assert data_product["object"] == "o2"
            assert data_product["version"] == "0.2.0"
            assert data_product_components == ["c2"]


def test_write_metadata():
    stream = io.StringIO()
    _write_metadata(
        "data_product_name", "1.0.0", "namespace1", 0, "somehash", ["c1", "c2"], Path("/filename.ext"), stream
    )
    assert (
        stream.getvalue().strip()
        == """
- accessibility: 0
  component: c1
  data_product: data_product_name
  extension: ext
  filename: /filename.ext
  namespace: namespace1
  verified_hash: somehash
  version: 1.0.0
- accessibility: 0
  component: c2
  data_product: data_product_name
  extension: ext
  filename: /filename.ext
  namespace: namespace1
  verified_hash: somehash
  version: 1.0.0
""".strip()
    )
    stream = io.StringIO()
    _write_metadata("data_product_name", "1.0.0", "namespace2", 1, "somehash", [], Path("/filename.ext"), stream)
    assert (
        stream.getvalue().strip()
        == """
- accessibility: 1
  data_product: data_product_name
  extension: ext
  filename: /filename.ext
  namespace: namespace2
  verified_hash: somehash
  version: 1.0.0
    """.strip()
    )


def test_get_output_info():
    with patch("data_pipeline_api.registry.download.get_on_end_point") as get_on_end_point:
        with patch("pathlib.Path.mkdir"):
            get_on_end_point.side_effect = [
                {DataRegistryField.storage_location: "url"},
                {
                    DataRegistryField.path: "path/to/file/path.csv",
                    DataRegistryField.storage_root: "store_root",
                    DataRegistryField.hash: "some_hash",
                },
                {DataRegistryField.root: "file://root_uri/", DataRegistryField.accessibility: 0},
            ]
            oi = _get_output_info(
                "data_product_name",
                {DataRegistryField.object: "obj", DataRegistryField.version: "1.0.0"},
                Path("data/"),
                TOKEN,
            )
            assert oi.source_uri == "file://root_uri/"
            assert oi.source_path == "path/to/file/path.csv"
            assert oi.output_filename.as_posix() == "data_product_name/1.0.0/path.csv"
            assert oi.output_path.as_posix() == "data/data_product_name/1.0.0/path.csv"
            assert oi.hash == "some_hash"
            assert oi.accessibility == 0


def test_download_data_public_file():
    with patch("data_pipeline_api.registry.download.get_remote_filesystem_and_path") as fs_path:
        fs = Mock()
        fs_path.return_value = fs, "path"
        fs.isdir.return_value = False
        _download_data(OutputInfo("http://source_uri", "source_path", "output_filename", "output_path", "hash", 0))
        fs_path.assert_called_once_with("http", "http://source_uri", "source_path")
        fs.get.assert_called_once_with("path", "output_path", block_size=0)


def test_download_data_public_dir():
    with patch("data_pipeline_api.registry.download.get_remote_filesystem_and_path") as fs_path:
        fs = Mock()
        fs_path.return_value = fs, "path"
        fs.isdir.return_value = True
        _download_data(OutputInfo("http://source_uri", "source_path", "output_filename", "output_path", "hash", 0))
        fs_path.assert_called_once_with("http", "http://source_uri", "source_path")
        fs.get.assert_called_once_with("path", "output_path", recursive=True, block_size=0)


def test_download_data_not_public():
    with patch("data_pipeline_api.registry.download.get_remote_filesystem_and_path") as fs_path:
        fs = Mock()
        fs_path.return_value = fs, "path"
        _download_data(OutputInfo("source_uri", "source_path", "output_filename", "output_path", "hash", 1))
        fs_path.assert_not_called()
        fs.assert_not_called()
