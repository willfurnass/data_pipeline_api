from io import IOBase
import h5py
import pandas as pd
import numpy as np
from functools import reduce
from operator import getitem
from typing import Union, List, NamedTuple, Optional, Any


Table = pd.DataFrame


class Dimension(NamedTuple):
    title: Optional[str] = None
    names: Optional[List[str]] = None
    values: Optional[List[Any]] = None
    units: Optional[str] = None

    def __eq__(self, other):
        if isinstance(other, Dimension):
            return (
                (self.title == other.title)
                and (self.names == other.names)
                and (self.values == other.values)
                and (self.units == other.units)
            )
        else:
            return False


class Array(NamedTuple):
    data: np.ndarray
    dimensions: Optional[List[Dimension]] = None
    units: Optional[str] = None

    def __eq__(self, other):
        if isinstance(other, Array):
            return (
                np.array_equal(self.data, other.data)
                and (self.dimensions == other.dimensions)
                and (self.units == other.units)
            )
        else:
            return False


def get_components(file: IOBase) -> List[str]:
    components = []

    def add_dataset_parent(name, obj):
        if isinstance(obj, h5py.Dataset):
            components.append(obj.parent.name)

    h5py.File(file, mode="r").visititems(add_dataset_parent)
    return components


def get_read_group(file: IOBase, component: str) -> h5py.Group:
    return h5py.File(file, mode="r").get(component)


def get_write_group(file: IOBase, component: str) -> h5py.Group:
    return h5py.File(file, mode="a").require_group(component)


def read_table(file: IOBase, component: str) -> Table:
    return pd.DataFrame(get_read_group(file, component)["table"][()]).apply(
        lambda s: s.str.decode("utf-8")
        if pd.api.types.is_object_dtype(s.infer_objects())
        else s
    )


def write_table(file: IOBase, component: str, table: Table):
    # Assumes all object columns are strings.
    records = table.to_records(
        index=False,
        column_dtypes={
            column: (
                table[column].values.astype(np.string_).dtype
                if dtype == "O"
                else dtype
            )
            for column, dtype in table.dtypes.items()
        },
    )
    get_write_group(file, component).require_dataset(
        "table", shape=records.shape, dtype=records.dtype, data=records
    )


DIMENSION_PREFIX = "Dimension_"
TITLE_SUFFIX = "_title"
NAMES_SUFFIX = "_names"
VALUES_SUFFIX = "_values"
UNITS_SUFFIX = "_units"


def get_string_array(array) -> np.ndarray:
    """Attempt to coerce an arbitrary numpy array into a decoded string-type array."""
    return np.char.decode(np.array(array, dtype=np.string_))


def get_string_list(array) -> List[str]:
    """Attempt to coerce an arbitrary numpy array into a string list."""
    return list(get_string_array(array))


def get_single_string(array) -> str:
    """Attempt to coerce an arbitrary numpy array into a single string."""
    string_array = get_string_array(array)
    if string_array.shape == ():
        return string_array[()]
    if string_array.shape == (1,):
        return string_array[0]
    raise ValueError(f"Cannot get a single string from a {string_array.shape} array")


def read_array(file: IOBase, component: str) -> Array:
    group = get_read_group(file, component)
    data = group["array"][()]
    dimension_title = {}
    dimension_names = {}
    dimension_values = {}
    dimension_units = {}
    for name in group:
        if name.startswith(DIMENSION_PREFIX):
            rest = name[len(DIMENSION_PREFIX) :]
            if rest.endswith(TITLE_SUFFIX):
                dimension_title[int(rest[: -len(TITLE_SUFFIX)])] = get_single_string(
                    group[name][()]
                )
            elif rest.endswith(NAMES_SUFFIX):
                dimension_names[int(rest[: -len(NAMES_SUFFIX)])] = get_string_list(
                    group[name][()]
                )
            elif rest.endswith(VALUES_SUFFIX):
                dimension_values[int(rest[: -len(VALUES_SUFFIX)])] = list(
                    group[name][()]
                )
            elif rest.endswith(UNITS_SUFFIX):
                dimension_units[int(rest[: -len(UNITS_SUFFIX)])] = get_single_string(
                    group[name][()]
                )
    max_dimension = max(
        set(dimension_title)
        | set(dimension_names)
        | set(dimension_values)
        | set(dimension_units),
        default=None,
    )
    if max_dimension is None:
        dimensions = None
    else:
        dimensions = [
            Dimension(
                title=dimension_title.get(dimension),
                names=dimension_names.get(dimension),
                values=dimension_values.get(dimension),
                units=dimension_units.get(dimension),
            )
            for dimension in range(1, max_dimension + 1)
        ]
    if "units" in group:
        units = get_single_string(group["units"][()])
    else:
        units = None
    # TODO : More validation on the outputs?
    return Array(data=data, dimensions=dimensions, units=units)


def write_array(file: IOBase, component: str, array: Array):
    # TODO : More validation on the inputs?
    group = get_write_group(file, component)
    group.require_dataset(
        "array", shape=array.data.shape, dtype=array.data.dtype, data=array.data
    )
    if array.dimensions is not None:
        for i, dimension in enumerate(array.dimensions, start=1):
            if dimension.title is not None:
                group.require_dataset(
                    f"Dimension_{i}_title",
                    dtype=h5py.string_dtype(),
                    shape=(),
                    data=dimension.title,
                )
            if dimension.names is not None:
                encoded_names = np.char.encode(dimension.names)
                group.require_dataset(
                    f"Dimension_{i}_names",
                    dtype=h5py.string_dtype(),
                    shape=encoded_names.shape,
                    data=encoded_names,
                )
            if dimension.values is not None:
                values = np.array(dimension.values)
                group.require_dataset(
                    f"Dimension_{i}_values",
                    dtype=values.dtype,
                    shape=values.shape,
                    data=values,
                )
            if dimension.units is not None:
                group.require_dataset(
                    f"Dimension_{i}_units",
                    dtype=h5py.string_dtype(),
                    shape=(),
                    data=dimension.units,
                )
    if array.units is not None:
        group.require_dataset(
            "units", dtype=h5py.string_dtype(), shape=(), data=array.units,
        )
