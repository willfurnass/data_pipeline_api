# pylint: disable=missing-function-docstring,import-error
from pathlib import Path
import pandas as pd
import numpy as np
from data_pipeline_api.file_formats import object_file


def test_table_roundtrip(tmp_path):
    df = pd.DataFrame({"a": [1, 2.0], "b": ["hello", "world"]})
    with open(tmp_path / "test.h5", "wb") as file:
        object_file.write_table(file, "test", df)
    with open(tmp_path / "test.h5", "rb") as file:
        pd.testing.assert_frame_equal(object_file.read_table(file, "test"), df)


def test_array_roundtrip(tmp_path):
    array = object_file.Array(
        data=np.array([1, 2, 3]),
        dimensions=[
            object_file.Dimension(
                title="dimension 1",
                names=["column 1"],
                values=[1],
                units="dimension 1 units",
            )
        ],
        units="array units",
    )
    with open(tmp_path / "test.h5", "w+b") as file:
        object_file.write_array(file, "test", array)
    with open(tmp_path / "test.h5", "r+b") as file:
        output_array = object_file.read_array(file, "test")
        assert output_array == array


def test_can_read_R_hdf5_file():
    with open(Path(__file__).parent.parent / "data" / "demographics.h5", "rb") as file:
        array = object_file.read_array(file, "hb/1year/persons")
        assert array.dimensions[0].title == 'health board'
