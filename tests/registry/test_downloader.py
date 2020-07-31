import io
import itertools
from pathlib import Path
from typing import List
from unittest.mock import patch, Mock

import pytest

from data_pipeline_api.registry.downloader import Downloader
from data_pipeline_api.registry.common import DataRegistryTarget, DataRegistryField
from tests.registry.test_common import TOKEN, DATA_REGISTRY_URL


@pytest.fixture
def downloader(tmp_path):
    return Downloader(tmp_path, DATA_REGISTRY_URL, TOKEN)


def test_downloader_add_data_product(downloader):
    downloader.add_data_product("ns", "dp")
    downloader.add_data_product("ns", "dp", "c")
    downloader.add_data_product("ns", "dp", version="v")
    downloader.add_data_product("ns", "dp", "c", "v")
    assert downloader._data_products == [
        {
            (DataRegistryTarget.namespace, DataRegistryField.name): "ns",
            (DataRegistryTarget.data_product, DataRegistryField.name): "dp",
            (DataRegistryTarget.object_component, DataRegistryField.name): None,
            (DataRegistryTarget.data_product, DataRegistryField.version): None,
        },
        {
            (DataRegistryTarget.namespace, DataRegistryField.name): "ns",
            (DataRegistryTarget.data_product, DataRegistryField.name): "dp",
            (DataRegistryTarget.object_component, DataRegistryField.name): "c",
            (DataRegistryTarget.data_product, DataRegistryField.version): None,
        },
        {
            (DataRegistryTarget.namespace, DataRegistryField.name): "ns",
            (DataRegistryTarget.data_product, DataRegistryField.name): "dp",
            (DataRegistryTarget.object_component, DataRegistryField.name): None,
            (DataRegistryTarget.data_product, DataRegistryField.version): "v",
        },
        {
            (DataRegistryTarget.namespace, DataRegistryField.name): "ns",
            (DataRegistryTarget.data_product, DataRegistryField.name): "dp",
            (DataRegistryTarget.object_component, DataRegistryField.name): "c",
            (DataRegistryTarget.data_product, DataRegistryField.version): "v",
        },
    ]


def test_downloader_add_external_object(downloader):
    downloader.add_external_object("name")
    downloader.add_external_object("name", title="title")
    downloader.add_external_object("name", version="v")
    downloader.add_external_object("name", component="component")
    assert downloader._external_objects == [
        {
            (DataRegistryTarget.external_object, DataRegistryField.doi_or_unique_name): "name",
            (DataRegistryTarget.external_object, DataRegistryField.title): None,
            (DataRegistryTarget.external_object, DataRegistryField.version): None,
            (DataRegistryTarget.object_component, DataRegistryField.name): None,
        },
        {
            (DataRegistryTarget.external_object, DataRegistryField.doi_or_unique_name): "name",
            (DataRegistryTarget.external_object, DataRegistryField.title): "title",
            (DataRegistryTarget.external_object, DataRegistryField.version): None,
            (DataRegistryTarget.object_component, DataRegistryField.name): None,
        },
        {
            (DataRegistryTarget.external_object, DataRegistryField.doi_or_unique_name): "name",
            (DataRegistryTarget.external_object, DataRegistryField.title): None,
            (DataRegistryTarget.external_object, DataRegistryField.version): "v",
            (DataRegistryTarget.object_component, DataRegistryField.name): None,
        },
        {
            (DataRegistryTarget.external_object, DataRegistryField.doi_or_unique_name): "name",
            (DataRegistryTarget.external_object, DataRegistryField.title): None,
            (DataRegistryTarget.external_object, DataRegistryField.version): None,
            (DataRegistryTarget.object_component, DataRegistryField.name): "component",
        },
    ]


@pytest.mark.parametrize(
    ["return_value", "expected"],
    [
        [None, []],
        [[], []],
        [
            [{"a": 1, "b": 2}],
            [
                {
                    (DataRegistryTarget.namespace, DataRegistryField.name): "namespace",
                    (DataRegistryTarget.namespace, "a"): 1,
                    (DataRegistryTarget.namespace, "b"): 2,
                }
            ],
        ],
        [
            [{"a": 1, "b": 2}, {}],
            [
                {
                    (DataRegistryTarget.namespace, DataRegistryField.name): "namespace",
                    (DataRegistryTarget.namespace, "a"): 1,
                    (DataRegistryTarget.namespace, "b"): 2,
                },
                {(DataRegistryTarget.namespace, DataRegistryField.name): "namespace"},
            ],
        ],
    ],
)
def test_downloader_resolve_namespaces(downloader, return_value, expected):
    input_block = [{(DataRegistryTarget.namespace, DataRegistryField.name): "namespace"}]
    with patch("data_pipeline_api.registry.downloader.get_data") as get_data:
        get_data.return_value = return_value
        result = downloader._resolve_namespaces(input_block)
        assert result == expected
        get_data.assert_called_once_with(
            {DataRegistryField.name: "namespace"}, DataRegistryTarget.namespace, DATA_REGISTRY_URL, TOKEN, exact=False
        )


def _data_product_dict(name, url, component, version, **kwargs):
    d = {
        (DataRegistryTarget.data_product, DataRegistryField.name): name,
        (DataRegistryTarget.namespace, DataRegistryField.url): url,
        (DataRegistryTarget.object_component, DataRegistryField.name): component,
        (DataRegistryTarget.data_product, DataRegistryField.version): version,
    }
    for k, v in kwargs.items():
        d[DataRegistryTarget.data_product, k] = v
    return d


@pytest.mark.parametrize(
    ["input_block", "return_value", "expected"],
    [
        [[_data_product_dict("name", "url", None, "1.0.0")], None, []],
        [[_data_product_dict("name", "url", None, "1.0.0")], [], []],
        [
            [_data_product_dict("name", "url", None, "1.0.0")],
            [{DataRegistryField.version: "1.0.0", "a": 1, "name": "name"}],
            [_data_product_dict("name", "url", None, "1.0.0", a=1)],
        ],
        [[], [{DataRegistryField.version: "1.0.0", "a": 1}], []],
        [
            [_data_product_dict("name", "url", None, "1.0.0")],
            [{DataRegistryField.version: "1.0.0", "a": 1, "name": "name"},
             {DataRegistryField.version: "2.0.0", "b": 2, "name": "name"}],
            [_data_product_dict("name", "url", None, "2.0.0", b=2)],
        ],
        [
            [_data_product_dict("name", "url", "comp", "1.0.0")],
            [{DataRegistryField.version: "1.0.0", "a": 1, "name": "name"},
             {DataRegistryField.version: "2.0.0", "b": 2, "name": "name"}],
            [
                _data_product_dict("name", "url", "comp", "2.0.0", b=2),
                _data_product_dict("name", "url", "comp", "1.0.0", a=1),
            ],
        ],
        [
            [_data_product_dict("name", "url", None, None)],
            [{DataRegistryField.version: "1.0.0", "name": "name"},
             {DataRegistryField.version: "2.0.0", "name": "name"},
             {DataRegistryField.version: "3.0.0", "name": "name2"},
             {DataRegistryField.version: "1.0.0", "name": "name3"},
             ],
            [
                _data_product_dict("name2", "url", None, "3.0.0"),
                _data_product_dict("name", "url", None, "2.0.0"),
                _data_product_dict("name3", "url", None, "1.0.0"),
            ],
        ],
    ],
)
def test_downloader_resolve_data_products(downloader, input_block, return_value, expected):
    with patch("data_pipeline_api.registry.downloader.get_data") as get_data:
        get_data.return_value = return_value
        result = downloader._resolve_data_products(input_block)
        assert result == expected


def _object_dict(component, object_url, external=False, **kwargs):
    d = {
        (DataRegistryTarget.object_component, DataRegistryField.name): component,
        (
            DataRegistryTarget.external_object if external else DataRegistryTarget.data_product,
            DataRegistryField.object,
        ): object_url,
    }
    for k, v in kwargs.items():
        d[DataRegistryTarget.object, k] = v
    return d


@pytest.mark.parametrize(
    ["input_block", "return_value_data", "return_value_end_point", "external", "expected"],
    [
        [[_object_dict("c", "url")], None, None, False, []],
        [[_object_dict("c", "url")], [], None, False, []],
        [[_object_dict(None, "url")], None, {DataRegistryField.components: None}, False, []],
        [[_object_dict(None, "url")], None, {DataRegistryField.components: []}, False, []],
        [[_object_dict(None, "url", external=True)], None, {DataRegistryField.components: None}, True, []],
        [[_object_dict(None, "url", external=True)], None, {DataRegistryField.components: []}, True, []],
        [[_object_dict("c", "url")], ["component"], {"a": 1}, False, [_object_dict("c", "url", a=1)]],
        [
            [_object_dict("c", "url")],
            ["component", "another_component"],
            {"a": 1},
            False,
            [_object_dict("c", "url", a=1)],
        ],
        [
            [_object_dict(None, "url")],
            None,
            {"a": 1, "components": [1, 2, 3]},
            False,
            [_object_dict(None, "url", a=1, components=[1, 2, 3])],
        ],
    ],
)
def test_downloader_resolve_objects(
    downloader, input_block, return_value_data, return_value_end_point, external, expected
):
    with patch("data_pipeline_api.registry.downloader.get_data") as get_data:
        with patch("data_pipeline_api.registry.downloader.get_on_end_point") as get_on_end_point:
            get_data.return_value = return_value_data
            get_on_end_point.return_value = return_value_end_point
            result = downloader._resolve_objects(input_block, external=external)
            assert result == expected


def _component_dict(namespace, name, component, version, components, doi, title, **kwargs):
    d = {
        (DataRegistryTarget.object_component, DataRegistryField.name): component,
        (DataRegistryTarget.object, DataRegistryField.components): components,
    }
    external = bool(doi)
    if external:
        d[DataRegistryTarget.external_object, DataRegistryField.doi_or_unique_name] = doi
        d[DataRegistryTarget.external_object, DataRegistryField.title] = title
        d[DataRegistryTarget.external_object, DataRegistryField.version] = version
    else:
        d[DataRegistryTarget.namespace, DataRegistryField.name] = namespace
        d[DataRegistryTarget.data_product, DataRegistryField.name] = name
        d[DataRegistryTarget.data_product, DataRegistryField.version] = version

    for k, v in kwargs.items():
        d[DataRegistryTarget.object_component, k] = v
    return d


@pytest.mark.parametrize(
    ["input_block", "return_value", "external", "expected"],
    [
        [[_component_dict(None, None, "c", "1.0.0", [], "doi", "title")], None, True, []],
        [[_component_dict("ns", "name", "c", "1.0.0", [], None, None)], None, False, []],
        [
            [_component_dict(None, None, "c", "1.0.0", [1], "doi", "title")],
            {DataRegistryField.name: "c"},
            True,
            [_component_dict(None, None, "c", "1.0.0", [1], "doi", "title")],
        ],
        [
            [_component_dict("ns", "name", "c", "1.0.0", [1], None, None)],
            {DataRegistryField.name: "c"},
            False,
            [_component_dict("ns", "name", "c", "1.0.0", [1], None, None)],
        ],
        [
            [_component_dict(None, None, "c", "1.0.0", [1], "doi", "title")],
            {"a": 1, DataRegistryField.name: "c"},
            True,
            [_component_dict(None, None, "c", "1.0.0", [1], "doi", "title", a=1)],
        ],
        [
            [_component_dict("ns", "name", "c", "1.0.0", [1], None, None)],
            {"a": 1, DataRegistryField.name: "c"},
            False,
            [_component_dict("ns", "name", "c", "1.0.0", [1], None, None, a=1)],
        ],
        [
            [_component_dict("ns", "name", "c", "1.0.0", [1], None, None), _component_dict("ns", "name", "c", "2.0.0", [1], None, None)],
            {"a": 1, DataRegistryField.name: "c"},
            False,
            [_component_dict("ns", "name", "c", "2.0.0", [1], None, None, a=1)],
        ],
        [
            [_component_dict(None, None, "c", "1.0.0", [1], "doi", "title"), _component_dict(None, None, "c", "2.0.0", [1], "doi", "title")],
            {"a": 1, DataRegistryField.name: "c"},
            True,
            [_component_dict(None, None, "c", "2.0.0", [1], "doi", "title", a=1)],
        ],
        [
            [_component_dict("ns", "name", "c", "2.0.0", [1], None, None), _component_dict("ns", "name", "c", "1.0.0", [1], None, None)],
            {"a": 1, DataRegistryField.name: "c"},
            False,
            [_component_dict("ns", "name", "c", "2.0.0", [1], None, None, a=1)],
        ],
        [
            [
                _component_dict("ns", "name", "c", "2.0.0", [1], None, None),
                _component_dict("ns", "name", "c", "1.0.0", [1], None, None),
                _component_dict("ns", "name", "c2", "0.1.0", [1], None, None),
                _component_dict("ns", "name", "c2", "0.0.1", [1], None, None),
                _component_dict("ns", "othername", "c", "0.0.1", [1], None, None),
            ],
            [{"a": 1, DataRegistryField.name: "c"}, {"a": 1, DataRegistryField.name: "c2"}, {"a": 1, DataRegistryField.name: "c"}],
            False,
            [
                _component_dict("ns", "name", "c", "2.0.0", [1], None, None, a=1),
                _component_dict("ns", "name", "c2", "0.1.0", [1], None, None, a=1),
                _component_dict("ns", "othername", "c", "0.0.1", [1], None, None, a=1),
            ],
        ],
        [
            [
                _component_dict(None, None, "c", "2.0.0", [1], "doi", "title"),
                _component_dict(None, None, "c", "1.0.0", [1], "doi", "title"),
                _component_dict(None, None, "c2", "0.1.0", [1], "doi", "title"),
                _component_dict(None, None, "c2", "0.0.1", [1], "doi", "title"),
                _component_dict(None, None, "c", "0.0.1", [1], "doi", "othertitle"),
            ],
            [{"a": 1, DataRegistryField.name: "c"}, {"a": 1, DataRegistryField.name: "c2"}, {"a": 1, DataRegistryField.name: "c"}],
            True,
            [
                _component_dict(None, None, "c", "2.0.0", [1], "doi", "title", a=1),
                _component_dict(None, None, "c2", "0.1.0", [1], "doi", "title", a=1),
                _component_dict(None, None, "c", "0.0.1", [1], "doi", "othertitle", a=1),
            ],
        ],
        [
            [_component_dict("ns", "name", "c*", "2.0.0", [1], None, None), _component_dict("ns", "name", "c*", "1.0.0", [1], None, None)],
            {"a": 1, DataRegistryField.name: "c123"},
            False,
            [_component_dict("ns", "name", "c123", "2.0.0", [1], None, None, a=1)],
        ],
        [
            [_component_dict(None, None, "c*", "2.0.0", [1], "doi", "title"), _component_dict(None, None, "c*", "1.0.0", [1], "doi", "title")],
            {"a": 1, DataRegistryField.name: "c123"},
            True,
            [_component_dict(None, None, "c123", "2.0.0", [1], "doi", "title", a=1)],
        ],
    ],
)
def test_downloader_resolve_components(downloader, input_block, return_value, external, expected):
    with patch("data_pipeline_api.registry.downloader.get_on_end_point") as get_on_end_point:
        if not isinstance(return_value, List):
            return_value = [return_value]
        get_on_end_point.side_effect = itertools.cycle(return_value)
        result = downloader._resolve_components(input_block, external=external)
        assert result == expected


def _storage_location_dict(storage_location, name, version, external=False, **kwargs):
    d = {(DataRegistryTarget.object, DataRegistryField.storage_location): storage_location}
    target = DataRegistryTarget.external_object if external else DataRegistryTarget.data_product
    name_field = DataRegistryField.doi_or_unique_name if external else DataRegistryField.name
    d[target, name_field] = name
    d[target, DataRegistryField.version] = version
    for k, v in kwargs.items():
        d[DataRegistryTarget.storage_location, k] = v
    return d


@pytest.mark.parametrize(
    ["input_block", "return_value", "external", "expected"],
    [
        [
            [_storage_location_dict("loc", "name", "v")],
            {DataRegistryField.path: "path"},
            False,
            [_storage_location_dict("loc", "name", "v", path="path")],
        ],
        [
            [_storage_location_dict("loc", "name", "v", external=True)],
            {DataRegistryField.path: "path"},
            True,
            [_storage_location_dict("loc", "name", "v", external=True, path="path")],
        ],
    ],
)
def test_downloader_storage_locations(downloader, input_block, return_value, external, expected):
    with patch("data_pipeline_api.registry.downloader.get_on_end_point") as get_on_end_point:
        get_on_end_point.return_value = return_value
        result = downloader._resolve_storage_locations(input_block, external=external)
        expected[0]["output_filename"] = "name/v/path"
        expected[0]["full_output_filename"] = (Path(downloader._data_directory) / "name/v/path").as_posix()
        assert result == expected


@pytest.mark.parametrize(
    ["input_block", "return_value", "expected"],
    [
        [
            [{(DataRegistryTarget.storage_location, DataRegistryField.storage_root): "root"}],
            dict(a=1),
            [
                {
                    (DataRegistryTarget.storage_location, DataRegistryField.storage_root): "root",
                    (DataRegistryTarget.storage_root, "a"): 1,
                }
            ],
        ],
    ],
)
def test_downloader_storage_roots(downloader, input_block, return_value, expected):
    with patch("data_pipeline_api.registry.downloader.get_on_end_point") as get_on_end_point:
        get_on_end_point.return_value = return_value
        result = downloader._resolve_storage_roots(input_block)
        assert result == expected


def _external_object_dict(name, title, version, **kwargs):
    d = {
        (DataRegistryTarget.external_object, DataRegistryField.doi_or_unique_name): name,
        (DataRegistryTarget.external_object, DataRegistryField.title): title,
        (DataRegistryTarget.external_object, DataRegistryField.version): version,
    }
    for k, v in kwargs.items():
        d[DataRegistryTarget.external_object, k] = v
    return d


@pytest.mark.parametrize(
    ["input_block", "return_value", "expected"],
    [
        [[_external_object_dict("name", "title", "1.0.0")], None, []],
        [[_external_object_dict("name", "title", "1.0.0")], [], []],
        [
            [_external_object_dict("name", "title", "1.0.0")],
            [{DataRegistryField.version: "1.0.0", "a": 1, "doi_or_unique_name": "name", "title": "title"}],
            [_external_object_dict("name", "title", "1.0.0", a=1)],
        ],
        [[], [{DataRegistryField.version: "1.0.0", "a": 1, "doi_or_unique_name": "name", "title": "title"}], []],
        [
            [_external_object_dict("name", "title", "1.0.0")],
            [{DataRegistryField.version: "1.0.0", "a": 1, "doi_or_unique_name": "name", "title": "title"},
             {DataRegistryField.version: "2.0.0", "b": 2, "doi_or_unique_name": "name", "title": "title"}],
            [_external_object_dict("name", "title", "2.0.0", b=2)],
        ],
        [
            [_external_object_dict("name", "title", "1.0.0")],
            [{DataRegistryField.version: "1.0.0", "a": 1, "doi_or_unique_name": "name", "title": "title"},
             {DataRegistryField.version: "2.0.0", "b": 2, "doi_or_unique_name": "name", "title": "title"}],
            [_external_object_dict("name", "title", "2.0.0", b=2),],
        ],
        [
            [_external_object_dict("name", "title", "1.0.0")],
            [{DataRegistryField.version: "1.0.0", "doi_or_unique_name": "name", "title": "title"},
             {DataRegistryField.version: "2.0.0", "doi_or_unique_name": "name", "title": "title"},
             {DataRegistryField.version: "3.0.0", "doi_or_unique_name": "name2", "title": "title"},
             {DataRegistryField.version: "1.0.0", "doi_or_unique_name": "name3", "title": "title"}],
            [_external_object_dict("name2", "title", "3.0.0"),
             _external_object_dict("name", "title", "2.0.0"),
             _external_object_dict("name3", "title", "1.0.0")],
        ],
        [
            [_external_object_dict("name", "title", "1.0.0")],
            [{DataRegistryField.version: "1.0.0", "doi_or_unique_name": "name", "title": "title"},
             {DataRegistryField.version: "2.0.0", "doi_or_unique_name": "name", "title": "title2"},
             {DataRegistryField.version: "3.0.0", "doi_or_unique_name": "name", "title": "title"},
             {DataRegistryField.version: "1.0.0", "doi_or_unique_name": "name", "title": "title3"}],
            [_external_object_dict("name", "title", "3.0.0"),
             _external_object_dict("name", "title2", "2.0.0"),
             _external_object_dict("name", "title3", "1.0.0")],
        ],
    ],
)
def test_downloader_resolve_external_objects(downloader, input_block, return_value, expected):
    with patch("data_pipeline_api.registry.downloader.get_data") as get_data:
        get_data.return_value = return_value
        result = downloader._resolve_external_objects(input_block)
        assert result == expected


def test_write_metadata_data_product(downloader):
    stream = io.StringIO()
    downloader._resolved_data_products = [
        {
            (DataRegistryTarget.data_product, DataRegistryField.name): "name",
            (DataRegistryTarget.namespace, DataRegistryField.name): "namespace",
            (DataRegistryTarget.storage_root, DataRegistryField.accessibility): 0,
            (DataRegistryTarget.data_product, DataRegistryField.version): "1.0.0",
            (DataRegistryTarget.storage_location, DataRegistryField.hash): "somehash",
            "output_filename": "filename.ext",
            "full_output_filename": "/filename.ext",
            (DataRegistryTarget.object_component, DataRegistryField.name): "component",
        },
        {
            (DataRegistryTarget.data_product, DataRegistryField.name): "name1",
            (DataRegistryTarget.namespace, DataRegistryField.name): "namespace1",
            (DataRegistryTarget.storage_root, DataRegistryField.accessibility): 1,
            (DataRegistryTarget.data_product, DataRegistryField.version): "1.0.1",
            (DataRegistryTarget.storage_location, DataRegistryField.hash): "somehash1",
            "output_filename": "filename1.ext",
            "full_output_filename": "/filename1.ext",
            (DataRegistryTarget.object_component, DataRegistryField.name): "component1",
        },
    ]
    downloader._write_metadata_data_product(stream)
    assert (
        stream.getvalue().strip()
        == """
- accessibility: 0
  component: component
  data_product: name
  extension: ext
  filename: filename.ext
  namespace: namespace
  verified_hash: somehash
  version: 1.0.0
- accessibility: 1
  component: component1
  data_product: name1
  extension: ext
  filename: filename1.ext
  namespace: namespace1
  verified_hash: somehash1
  version: 1.0.1
    """.strip()
    )


def test_write_metadata_external_object(downloader):
    stream = io.StringIO()
    downloader._resolved_external_objects = [
        {
            (DataRegistryTarget.external_object, DataRegistryField.doi_or_unique_name): "name",
            (DataRegistryTarget.storage_root, DataRegistryField.accessibility): 0,
            (DataRegistryTarget.external_object, DataRegistryField.version): "1.0.0",
            (DataRegistryTarget.external_object, DataRegistryField.title): "title",
            (DataRegistryTarget.storage_location, DataRegistryField.hash): "somehash",
            "output_filename": "filename.ext",
            "full_output_filename": "/filename.ext",
            (DataRegistryTarget.object_component, DataRegistryField.name): "component",
        },
        {
            (DataRegistryTarget.external_object, DataRegistryField.doi_or_unique_name): "name1",
            (DataRegistryTarget.storage_root, DataRegistryField.accessibility): 1,
            (DataRegistryTarget.external_object, DataRegistryField.version): "1.0.1",
            (DataRegistryTarget.external_object, DataRegistryField.title): "title2",
            (DataRegistryTarget.storage_location, DataRegistryField.hash): "somehash1",
            "output_filename": "filename1.ext",
            "full_output_filename": "/filename1.ext",
            (DataRegistryTarget.object_component, DataRegistryField.name): "component1",
        },
    ]
    downloader._write_metadata_external_object(stream)
    assert (
        stream.getvalue().strip()
        == """
- accessibility: 0
  component: component
  doi_or_unique_name: name
  extension: ext
  filename: filename.ext
  title: title
  verified_hash: somehash
  version: 1.0.0
- accessibility: 1
  component: component1
  doi_or_unique_name: name1
  extension: ext
  filename: filename1.ext
  title: title2
  verified_hash: somehash1
  version: 1.0.1
    """.strip()
    )


def test_download_data_public_file(downloader):
    with patch("data_pipeline_api.registry.downloader.get_remote_filesystem_and_path") as fs_path:
        with patch.object(Path, "mkdir"):
            downloader._resolved_data_products = [
                {
                    (DataRegistryTarget.storage_root, DataRegistryField.accessibility): 0,
                    (DataRegistryTarget.storage_root, DataRegistryField.root): "http://source_uri",
                    (DataRegistryTarget.storage_location, DataRegistryField.path): "source_path",
                    (DataRegistryTarget.storage_location, DataRegistryField.hash): "some_hash",
                    "full_output_filename": "output_path",
                }
            ]
            fs = Mock()
            fs_path.return_value = fs, "path"
            fs.isdir.return_value = False
            downloader._download()
            fs_path.assert_called_once_with("http", "http://source_uri", "source_path")
            fs.get.assert_called_once_with("path", "output_path", block_size=0)


def test_download_data_public_dir(downloader):
    with patch("data_pipeline_api.registry.downloader.get_remote_filesystem_and_path") as fs_path:
        with patch.object(Path, "mkdir"):
            downloader._resolved_data_products = [
                {
                    (DataRegistryTarget.storage_root, DataRegistryField.accessibility): 0,
                    (DataRegistryTarget.storage_root, DataRegistryField.root): "http://source_uri",
                    (DataRegistryTarget.storage_location, DataRegistryField.path): "source_path",
                    (DataRegistryTarget.storage_location, DataRegistryField.hash): "some_hash",
                    "full_output_filename": "output_path",
                }
            ]
            fs = Mock()
            fs_path.return_value = fs, "path"
            fs.isdir.return_value = True
            downloader._download()
            fs_path.assert_called_once_with("http", "http://source_uri", "source_path")
            fs.get.assert_called_once_with("path", "output_path", recursive=True, block_size=0)


def test_download_data_not_public(downloader):
    with patch("data_pipeline_api.registry.downloader.get_remote_filesystem_and_path") as fs_path:
        with patch.object(Path, "mkdir"):
            downloader._resolved_data_products = [
                {(DataRegistryTarget.storage_root, DataRegistryField.accessibility): 1,
                 (DataRegistryTarget.storage_location, DataRegistryField.hash): "some_hash",}
            ]
            fs = Mock()
            fs_path.return_value = fs, "path"
            downloader._download()
            fs_path.assert_not_called()
            fs.assert_not_called()


def test_download_data_twice(downloader):
    with patch("data_pipeline_api.registry.downloader.get_remote_filesystem_and_path") as fs_path:
        with patch.object(Path, "mkdir"):
            downloader._resolved_data_products = [
                {
                    (DataRegistryTarget.storage_root, DataRegistryField.accessibility): 0,
                    (DataRegistryTarget.storage_root, DataRegistryField.root): "http://source_uri",
                    (DataRegistryTarget.storage_location, DataRegistryField.path): "source_path",
                    (DataRegistryTarget.storage_location, DataRegistryField.hash): "some_hash",
                    "full_output_filename": "output_path",
                },
                {
                    (DataRegistryTarget.storage_root, DataRegistryField.accessibility): 0,
                    (DataRegistryTarget.storage_location, DataRegistryField.hash): "some_hash",
                }
            ]
            fs = Mock()
            fs_path.return_value = fs, "path"
            fs.isdir.return_value = False
            downloader._download()
            fs_path.assert_called_once_with("http", "http://source_uri", "source_path")
            fs.get.assert_called_once_with("path", "output_path", block_size=0)
