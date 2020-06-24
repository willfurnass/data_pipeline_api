from io import IOBase
import h5py
import pandas as pd
import numpy as np
from functools import reduce
from operator import getitem
from typing import Union, List


Table = pd.DataFrame
Array = np.ndarray


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
    records = table.to_records(index=False)
    get_write_group(file, component).require_dataset(
        "table", shape=records.shape, dtype=records.dtype, data=records
    )


def read_array(file: IOBase, component: str) -> Array:
    return get_read_group(file, component)["array"][()]


def write_array(file: IOBase, component: str, array: Array):
    get_write_group(file, component).require_dataset(
        "array", shape=array.shape, dtype=array.dtype, data=array
    )
