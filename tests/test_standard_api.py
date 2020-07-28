# pylint: disable=redefined-outer-name,missing-function-docstring,import-error
import os
from pathlib import Path
import pytest
import yaml
import numpy as np
import pandas as pd
from scipy import stats
from data_pipeline_api.file_api import RunMetadata
from data_pipeline_api.standard_api import StandardAPI, Array, Issue

DATA_ROOT = Path(__file__).parent / "data"


@pytest.fixture
def standard_api(tmp_path):
    for filename in ("config.yaml", "metadata.yaml", "object", "parameter"):
        os.symlink(DATA_ROOT / filename, tmp_path / filename)
    return StandardAPI.from_config(tmp_path / "config.yaml", "test_git_repo", "test_git_sha")


def test_write_estimate(standard_api):
    with standard_api as api:
        api.write_estimate("output-parameter", "example-estimate", 1.0)


def test_read_estimate_as_estimate(standard_api):
    with standard_api as api:
        assert api.read_estimate("parameter", "example-estimate") == 1.0


def test_read_estimate_as_distribution(standard_api):
    with pytest.raises(ValueError):
        with standard_api as api:
            api.read_distribution("parameter", "example-estimate")


def test_read_estimate_as_samples(standard_api):
    with pytest.raises(ValueError):
        with standard_api as api:
            api.read_samples("parameter", "example-estimate")


def test_write_distribution(standard_api):
    with standard_api as api:
        api.write_distribution(
            "output-parameter", "example-distribution", stats.gamma(1, scale=2)
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


def test_read_distribution_as_samples(standard_api):
    with pytest.raises(ValueError):
        with standard_api as api:
            api.read_samples("parameter", "example-distribution")


def test_write_samples(standard_api):
    with standard_api as api:
        api.write_samples("output-parameter", "example-samples", np.array([1, 2, 3]))


def test_read_samples_as_estimate(standard_api):
    with standard_api as api:
        assert api.read_estimate("parameter", "example-samples") == 2.0


def test_read_samples_as_distribution(standard_api):
    with pytest.raises(ValueError):
        with standard_api as api:
            api.read_distribution("parameter", "example-samples")


def test_read_samples_as_samples(standard_api):
    with standard_api as api:
        np.array_equal(
            api.read_samples("parameter", "example-samples"), np.array([1, 2, 3])
        )


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
            "output-object", "example-table", pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        )


def test_write_array(standard_api):
    with standard_api as api:
        api.write_array("output-object", "example-array", Array(np.array([1, 2, 3])))


def test_access_log_contains_uri_and_git_sha(tmp_path, standard_api):
    with standard_api:
        pass
    with open(tmp_path / "access-example.yaml") as access_file:
        run_metadata = yaml.safe_load(access_file)["run_metadata"]
        assert run_metadata[RunMetadata.git_repo] == "test_git_repo"
        assert run_metadata[RunMetadata.git_sha] == "test_git_sha"


def test_issue_logging(tmp_path, standard_api):
    with standard_api as api:
        api.write_estimate(
            "parameter",
            "example-estimate",
            1.0,
            issues=[Issue("test issue 1", 1), Issue("test issue 2", 2)],
        )
    with open(tmp_path / "access-example.yaml") as access_file:
        access_yaml = yaml.safe_load(access_file)
        assert access_yaml["io"][0]["call_metadata"]["issues"] == [
            {"description": "test issue 1", "severity": 1},
            {"description": "test issue 2", "severity": 2},
        ]
        assert access_yaml["io"][0]["access_metadata"]["issues"] == [
            {"description": "test issue 1", "severity": 1},
            {"description": "test issue 2", "severity": 2},
        ]


def test_description(tmp_path, standard_api):
    with standard_api as api:
        api.write_estimate(
            "parameter", "example-estimate", 1.0, description="test description"
        )
    with open(tmp_path / "access-example.yaml") as access_file:
        access_yaml = yaml.safe_load(access_file)
        assert (
            access_yaml["io"][0]["call_metadata"]["description"] == "test description"
        )
        assert (
            access_yaml["io"][0]["access_metadata"]["description"] == "test description"
        )
