import pytest
from data_pipeline_api.standard_api import Estimate, Distribution


def test_estimate_read_estimate():
    assert Estimate.read_parameter({"point-estimate": {"value": 2.0}}) == 2.0


def test_estimate_read_distribution():
    assert (
        Estimate.read_parameter(
            {"distribution": {"distribution": "gamma", "shape": 3.0, "scale": 2.0}}
        )
        == 6.0
    )


def test_distribution_read_estimate():
    with pytest.raises(ValueError):
        Distribution.read_parameter({"point-estimate": {"value": 2.0}})


def test_distribution_read_distribution():
    distribution = Distribution.read_parameter(
        {"distribution": {"distribution": "gamma", "shape": 3.0, "scale": 2.0}}
    )
    assert distribution.dist.name == "gamma"
    assert distribution.dist._parse_args(*distribution.args, **distribution.kwds) == (
        (3.0,),
        0,
        2.0,
    )
