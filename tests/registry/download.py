import io
from pathlib import Path
from unittest.mock import patch, call, Mock

import pytest

from data_pipeline_api.registry.download import _parse_read_config, _get_data_product_version_and_components, \
    ParsedReadConfig, _write_metadata, _get_output_info, _download_data, OutputInfo
from tests.registry.common import DATA_REGISTRY_URL, TOKEN


def test_parse_read_config_no_where_raises():
    with pytest.raises(ValueError):
        _parse_read_config({}, DATA_REGISTRY_URL, TOKEN)


def test_parse_read_config_no_data_product_in_where_raises():
    with pytest.raises(ValueError):
        _parse_read_config({"where": {}}, DATA_REGISTRY_URL, TOKEN)


def test_parse_read_config_raises_no_data_product_ref():
    with patch("data_pipeline_api.registry.download.get_reference") as ref:
        ref.return_value = None
        with pytest.raises(ValueError):
            _parse_read_config({"where": {"data_product": "data_product_name"}}, DATA_REGISTRY_URL, TOKEN)
        ref.assert_called_once_with({"name": "data_product_name"}, "data_product", DATA_REGISTRY_URL, TOKEN)


def test_parse_read_config():
    with patch("data_pipeline_api.registry.download.get_reference") as get_reference:
        get_reference.return_value = f"{DATA_REGISTRY_URL}/1/"
        result = _parse_read_config({"where": {"data_product": "data_product_name"}}, DATA_REGISTRY_URL, TOKEN)
        assert result.data_product == f"{DATA_REGISTRY_URL}/1/"
        assert result.data_product_name == "data_product_name"
        assert result.component_name is None
        assert result.version_identifier is None
        assert get_reference.call_args_list[0] == call({"name": "data_product_name"}, "data_product", DATA_REGISTRY_URL, TOKEN)

        result = _parse_read_config({"where": {"data_product": "data_product_name", "component": "component1", "version": "1.0.0"}, "use": {"data_product": "override"}}, DATA_REGISTRY_URL, TOKEN)
        assert result.data_product_name == "override"
        assert result.component_name is "component1"
        assert result.version_identifier is "1.0.0"
        assert get_reference.call_args_list[1] == call({"name": "override"}, "data_product", DATA_REGISTRY_URL, TOKEN)

        result = _parse_read_config({"where": {"data_product": "data_product_name", "component": "component1", "version": "1.0.0"}, "use": {"component": "overidec", "version": "1.0.0+override"}}, DATA_REGISTRY_URL, TOKEN)
        assert result.data_product_name == "data_product_name"
        assert result.component_name is "componentc"
        assert result.version_identifier is "1.0.0+override"
        assert get_reference.call_args_list[2] == call({"name": "data_product_name"}, "data_product", DATA_REGISTRY_URL, TOKEN)


def test_get_data_product_version_and_components():
    with patch("data_pipeline_api.registry.download.get_data") as get_data:
        with patch("data_pipeline_api.registry.download.get_on_end_point") as get_on_end_point:
            get_data.return_value = [{"components": ["c1", "c2"], "version_identifier": "0.1.0"}, {"components": ["c3", "c4"], "version_identifier": "0.2.0"}]
            get_on_end_point.return_value = {"name": "component"}
            prc = ParsedReadConfig("data_product", "data_product_name", None, None)
            data_product_version, data_product_version_components = _get_data_product_version_and_components(prc, DATA_REGISTRY_URL, TOKEN)
            assert data_product_version["components"] == ["c3", "c4"]
            assert data_product_version["version_identifier"] == "0.2.0"
            assert data_product_version_components == ["component", "component"]
            assert get_data.call_args_list[0] == call({'data_product': 'data_product'}, 'data_product_version', DATA_REGISTRY_URL, TOKEN, exact=False)

            get_on_end_point.side_effect = [{"name": "c3"}, {"name": "c4"}, {"name": "c2"}, {"name": "c1"}]
            prc = ParsedReadConfig("data_product", "data_product_name", "c2", None)
            data_product_version, data_product_version_components = _get_data_product_version_and_components(prc, DATA_REGISTRY_URL, TOKEN)
            assert data_product_version["components"] == ["c1", "c2"]
            assert data_product_version["version_identifier"] == "0.1.0"
            assert data_product_version_components == ["c2"]
            assert get_data.call_args_list[1] == call({'data_product': 'data_product'}, 'data_product_version', DATA_REGISTRY_URL, TOKEN, exact=False)

            get_on_end_point.side_effect = None
            prc = ParsedReadConfig("data_product", "data_product_name", None, "0.1.0")
            _get_data_product_version_and_components(prc, DATA_REGISTRY_URL, TOKEN)
            assert get_data.call_args_list[2] == call({'data_product': 'data_product', 'version_identifier': '0.1.0'}, 'data_product_version', DATA_REGISTRY_URL, TOKEN, exact=False)


def test_write_metadata():
    stream = io.StringIO()
    _write_metadata("data_product_name", "1.0.0", "somehash", ["c1", "c2"], Path("/filename.ext"), stream)
    assert stream.getvalue().strip() == """
- component: c1
  data_product: data_product_name
  extension: ext
  filename: /filename.ext
  verified_hash: somehash
  version: 1.0.0
- component: c2
  data_product: data_product_name
  extension: ext
  filename: /filename.ext
  verified_hash: somehash
  version: 1.0.0
""".strip()
    stream = io.StringIO()
    _write_metadata("data_product_name", "1.0.0", "somehash", [], Path("/filename.ext"), stream)
    assert stream.getvalue().strip() == """
- data_product: data_product_name
  extension: ext
  filename: /filename.ext
  verified_hash: somehash
  version: 1.0.0
    """.strip()


def test_get_output_info():
    with patch("data_pipeline_api.registry.download.get_on_end_point") as get_on_end_point:
        with patch("pathlib.Path.mkdir"):
            get_on_end_point.side_effect = [{"path": "path/to/file/path.csv", "store_root": "store_root", "hash": "some_hash"}, {"uri": "file://root_uri/", "type": "type"}, {"name": "storage_type"}, {"name": "public"}]
            oi = _get_output_info("data_product_name", {"store": "store", "version_identifier": "1.0.0", "accessibility": "public"}, Path("data/"), TOKEN)
            assert oi.source_uri == "file://root_uri/"
            assert oi.source_path == "path/to/file/path.csv"
            assert oi.output_filename.as_posix() == "data_product_name/1.0.0/path.csv"
            assert oi.output_path.as_posix() == "data/data_product_name/1.0.0/path.csv"
            assert oi.hash == "some_hash"
            assert oi.accessibility == "public"


def test_download_data_public_not_file():
    with patch("data_pipeline_api.registry.download.get_remote_filesystem_and_path") as fs_path:
        with patch("pathlib.Path.mkdir") as mdir:
            fs = Mock()
            fs_path.return_value = fs, "path"
            _download_data(OutputInfo("source_uri", "source_path", "source_protocol", "output_filename", "output_path", "hash", "public"))
            fs_path.assert_called_once_with("source_protocol", "source_uri", "source_path")
            mdir.assert_not_called()
            fs.get.assert_called_once_with("path", "output_path")


def test_download_data_public_file():
    with patch("data_pipeline_api.registry.download.get_remote_filesystem_and_path") as fs_path:
        with patch("pathlib.Path.mkdir") as mdir:
            fs = Mock()
            fs_path.return_value = fs, "path"
            _download_data(OutputInfo("source_uri", "source_path", "file", "output_filename", "output_path", "hash", "public"))
            fs_path.assert_called_once_with("file", "source_uri", "source_path")
            mdir.assert_called_once_with(parents=True, exist_ok=True)
            fs.get.assert_called_once_with("path", "output_path")


def test_download_data_not_public():
    with patch("data_pipeline_api.registry.download.get_remote_filesystem_and_path") as fs_path:
        with patch("pathlib.Path.mkdir") as mdir:
            fs = Mock()
            fs_path.return_value = fs, "path"
            _download_data(OutputInfo("source_uri", "source_path", "file", "output_filename", "output_path", "hash", "private"))
            fs_path.assert_not_called()
            mdir.assert_not_called()
            fs.assert_not_called()
