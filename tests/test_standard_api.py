# pylint: disable=redefined-outer-name,missing-function-docstring,import-error
from pathlib import Path
import pytest
import yaml
import numpy as np
import pandas as pd
from scipy import stats
from data_pipeline_api.standard_api import StandardAPI, Array, Issue


CONFIG_PATH = Path(__file__).parent / "data" / "config.yaml"


@pytest.fixture
def standard_api():
    return StandardAPI(CONFIG_PATH, "test_uri", "test_git_sha")


def test_write_estimate(standard_api):
    with standard_api as api:
        api.write_estimate("parameter", "example-estimate", 1.0)


def test_read_estimate_as_estimate(standard_api):
    with standard_api as api:
        assert api.read_estimate("parameter", "example-estimate") == 1.0


def test_read_estimate_as_distribution(standard_api):
    with pytest.raises(ValueError):
        with standard_api as api:
            api.read_distribution("parameter", "example-estimate")


def test_read_estimate_as_sample(standard_api):
    with standard_api as api:
        assert api.read_sample("parameter", "example-estimate") == 1.0


def test_write_distribution(standard_api):
    with standard_api as api:
        api.write_distribution(
            "parameter", "example-distribution", stats.gamma(1, scale=2)
        )


def test_read_distribution_as_estimate(standard_api):
    with standard_api as api:
        assert api.read_estimate("parameter", "example-distribution") == 2.0


def test_read_distribution_as_distribution(standard_api):
    # pylint: disable=protected-access
    distribution = standard_api.read_distribution("parameter", "example-distribution")
    assert distribution.dist._parse_args(*distribution.args, **distribution.kwds) == (
        (1.0,),
        0,
        2.0,
    )


def test_read_distribution_as_sample(standard_api):
    np.random.seed(0)
    with standard_api as api:
        assert api.read_sample("parameter", "example-distribution") == 1.59174901632622


def test_write_samples(standard_api):
    with standard_api as api:
        api.write_samples("parameter", "example-samples", np.array([1, 2, 3]))


def test_read_samples_as_estimate(standard_api):
    with standard_api as api:
        assert api.read_estimate("parameter", "example-samples") == 2.0


def test_read_samples_as_distribution(standard_api):
    with pytest.raises(ValueError):
        with standard_api as api:
            api.read_distribution("parameter", "example-samples")


def test_read_samples_as_sample(standard_api):
    np.random.seed(0)
    with standard_api as api:
        assert api.read_sample("parameter", "example-samples") == 1.0


def test_read_table(standard_api):
    with standard_api as api:
        pd.testing.assert_frame_equal(
            api.read_table("object", "example-table"),
            pd.DataFrame({"a": [1, 2], "b": [3, 4]}),
        )


def test_read_array(standard_api):
    with standard_api as api:
        assert api.read_array("object", "example-array") == Array(np.array([1, 2, 3]))


def test_write_table(standard_api):
    with standard_api as api:
        api.write_table(
            "object", "example-table", pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        )


def test_write_array(standard_api):
    with standard_api as api:
        api.write_array("object", "example-array", Array(np.array([1, 2, 3])))


def test_access_log_contains_uri_and_git_sha(standard_api):
    with standard_api:
        pass
    with open(CONFIG_PATH.parent / "access-example.yaml") as access_file:
        assert yaml.safe_load(access_file)["metadata"] == {
            "uri": "test_uri",
            "git_sha": "test_git_sha",
        }


def test_issue_logging(standard_api):
    with standard_api as api:
        api.write_estimate(
            "parameter", "example-estimate", 1.0, issues=[Issue("test issue 1", 1), Issue("test issue 2", 2)]
        )
    with open(CONFIG_PATH.parent / "access-example.yaml") as access_file:
        access_yaml = yaml.safe_load(access_file)
        assert access_yaml["io"][0]["call_metadata"]["issues"] == [
            {"description": "test issue 1", "severity": 1},
            {"description": "test issue 2", "severity": 2},
        ]
        assert access_yaml["io"][0]["access_metadata"]["issues"] == [
            {"description": "test issue 1", "severity": 1},
            {"description": "test issue 2", "severity": 2},
        ]


def test_description(standard_api):
    with standard_api as api:
        api.write_estimate(
            "parameter", "example-estimate", 1.0, description="test description"
        )
    with open(CONFIG_PATH.parent / "access-example.yaml") as access_file:
        access_yaml = yaml.safe_load(access_file)
        assert access_yaml["io"][0]["call_metadata"]["description"] == "test description"
        assert access_yaml["io"][0]["access_metadata"]["description"] == "test description"
