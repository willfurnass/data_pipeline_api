import pytest
import numpy as np
import pandas as pd
from scipy import stats
from pathlib import Path
from data_pipeline_api.standard_api import StandardAPI, Array

CONFIG_PATH = Path(__file__).parent / "data" / "config.yaml"


def test_write_estimate():
    StandardAPI(CONFIG_PATH).write_estimate("parameter", "example-estimate", 1.0)


def test_read_estimate_as_estimate():
    assert (
        StandardAPI(CONFIG_PATH).read_estimate("parameter", "example-estimate") == 1.0
    )


def test_read_estimate_as_distribution():
    with pytest.raises(ValueError):
        StandardAPI(CONFIG_PATH).read_distribution("parameter", "example-estimate")


def test_read_estimate_as_sample():
    assert StandardAPI(CONFIG_PATH).read_sample("parameter", "example-estimate") == 1.0


def test_write_distribution():
    StandardAPI(CONFIG_PATH).write_distribution(
        "parameter", "example-distribution", stats.gamma(1, scale=2)
    )


def test_read_distribution_as_estimate():
    assert (
        StandardAPI(CONFIG_PATH).read_estimate("parameter", "example-distribution")
        == 2.0
    )


def test_read_distribution_as_distribution():
    distribution = StandardAPI(CONFIG_PATH).read_distribution(
        "parameter", "example-distribution"
    )
    assert distribution.dist._parse_args(*distribution.args, **distribution.kwds) == (
        (1.0,),
        0,
        2.0,
    )


def test_read_distribution_as_sample():
    np.random.seed(0)
    assert (
        StandardAPI(CONFIG_PATH).read_sample("parameter", "example-distribution")
        == 1.59174901632622
    )


def test_write_samples():
    StandardAPI(CONFIG_PATH).write_samples(
        "parameter", "example-samples", np.array([1, 2, 3])
    )


def test_read_samples_as_estimate():
    assert StandardAPI(CONFIG_PATH).read_estimate("parameter", "example-samples") == 2.0


def test_read_samples_as_distribution():
    with pytest.raises(ValueError):
        StandardAPI(CONFIG_PATH).read_distribution("parameter", "example-samples")


def test_read_samples_as_sample():
    np.random.seed(0)
    assert StandardAPI(CONFIG_PATH).read_sample("parameter", "example-samples") == 1.0


def test_read_table():
    pd.testing.assert_frame_equal(
        StandardAPI(CONFIG_PATH).read_table("object", "example-table"),
        pd.DataFrame({"a": [1, 2], "b": [3, 4]}),
    )


def test_read_array():
    assert StandardAPI(CONFIG_PATH).read_array("object", "example-array") == Array(
        np.array([1, 2, 3])
    )


def test_write_table():
    StandardAPI(CONFIG_PATH).write_table(
        "object", "example-table", pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    )


def test_write_array():
    StandardAPI(CONFIG_PATH).write_array(
        "object", "example-array", Array(np.array([1, 2, 3]))
    )
