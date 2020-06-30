import pandas as pd
import numpy as np
from data_pipeline_api.file_formats import object_file


def test_table_roundtrip(tmp_path):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    with open(tmp_path / "test.h5", "wb") as file:
        object_file.write_table(file, "test", df)
    with open(tmp_path / "test.h5", "rb") as file:
        pd.testing.assert_frame_equal(object_file.read_table(file, "test"), df)


def test_array_roundtrip(tmp_path):
    array = np.array([1, 2, 3])
    with open(tmp_path / "test.h5", "wb") as file:
        object_file.write_array(file, "test", array)
    with open(tmp_path / "test.h5", "rb") as file:
        np.testing.assert_array_equal(object_file.read_array(file, "test"), array)
