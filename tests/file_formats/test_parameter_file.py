import pytest
import numpy as np
from io import TextIOWrapper
from data_pipeline_api.file_formats import parameter_file


def test_write_parameter_to_empty_file(tmp_path):
    with open(tmp_path / "test.toml", "w+b") as file:
        parameter_file.write_parameter(TextIOWrapper(file), "test", "test")


def test_write_parameter_to_nonempty_file(tmp_path):
    with open(tmp_path / "test.toml", "w+b") as file:
        parameter_file.write_parameter(TextIOWrapper(file), "test", "test")
    with open(tmp_path / "test.toml", "r+b") as file:
        parameter_file.write_parameter(TextIOWrapper(file), "test2", "test2")


def test_overwrite_parameter(tmp_path):
    with open(tmp_path / "test.toml", "w+b") as file:
        parameter_file.write_parameter(TextIOWrapper(file), "test", "test")
    with open(tmp_path / "test.toml", "r+b") as file:
        parameter_file.write_parameter(TextIOWrapper(file), "test", "test")


def test_parameter_roundtrip(tmp_path):
    with open(tmp_path / "test.toml", "w+b") as file:
        parameter_file.write_parameter(TextIOWrapper(file), "test", "test")
    with open(tmp_path / "test.toml", "r+b") as file:
        assert parameter_file.read_parameter(TextIOWrapper(file), "test") == "test"


def test_estimate_roundtrip(tmp_path):
    estimate = 3
    with open(tmp_path / "test.toml", "w+b") as file:
        parameter_file.write_estimate(TextIOWrapper(file), "test", estimate)
    with open(tmp_path / "test.toml", "r+b") as file:
        assert parameter_file.read_estimate(TextIOWrapper(file), "test") == estimate


def test_write_np_float_estimate(tmp_path):
    estimate = np.float64(3)
    with open(tmp_path / "test.toml", "w+b") as file:
        parameter_file.write_estimate(TextIOWrapper(file), "test", estimate)
    with open(tmp_path / "test.toml", "r+b") as file:
        assert parameter_file.read_estimate(TextIOWrapper(file), "test") == estimate


@pytest.mark.parametrize(
    ("name", "parameters"),
    [
        ("categorical", {"bins": ["a", "b"], "weights": [0.2, 0.8]}),
        ("gamma", {"k": 2, "theta": 3}),
        ("normal", {"mu": 2, "sigma": 3}),
        ("uniform", {"a": 2, "b": 3}),
        ("poisson", {"lambda": 2}),
        ("exponential", {"lambda": 2}),
        ("beta", {"alpha": 2, "beta": 3}),
        ("binomial", {"n": 100, "p": 0.2}),
        ("multinomial", {"n": 100, "p": [0.2, 0.8]}),
    ],
)
def test_distribution_roundtrip(name, parameters):
    encoded_distribution = dict(type="distribution", distribution=name, **parameters)
    assert (
        parameter_file.encode_distribution(
            parameter_file.decode_distribution(encoded_distribution)
        )
        == encoded_distribution
    )


@pytest.mark.parametrize(
    ("name", "parameters", "mean"),
    [
        ("gamma", {"k": 2, "theta": 3}, 2 * 3),  # k theta
        ("normal", {"mu": 2, "sigma": 3}, 2),  # mu
        ("uniform", {"a": 2, "b": 3}, (2 + 3) / 2),  # (a + b) / 2
        ("poisson", {"lambda": 2}, 2),  # lambda
        ("exponential", {"lambda": 2}, 1 / 2),  # 1 / lambda
        ("beta", {"alpha": 2, "beta": 3}, 2 / (2 + 3)),  # alpha / (alpha + beta)
        ("binomial", {"n": 100, "p": 0.2}, 100 * 0.2),  # n p
        ("multinomial", {"n": 100, "p": [0.2, 0.8]}, 100 * np.array([0.2, 0.8])),  # n p
    ],
)
def test_distribution_mean(name, parameters, mean):
    assert np.array_equal(
        parameter_file.decode_distribution(
            dict(type="distribution", distribution=name, **parameters)
        ).mean(),
        mean,
    )


@pytest.mark.parametrize(
    ("name", "parameters", "variance"),
    [
        (
            "categorical",
            {"bins": ["a", "b"], "weights": [0.2, 0.8]},
            np.array([0.2, 0.8]) * (1 - np.array([0.2, 0.8])),
        ),  # p (1 - p)
        ("gamma", {"k": 2, "theta": 3}, 2 * 3 ** 2),  # k theta^2
        ("normal", {"mu": 2, "sigma": 3}, 3 ** 2),  # sigma^2
        ("uniform", {"a": 2, "b": 3}, (3 - 2) ** 2 / 12),  # (b - a)^2 / 12
        ("poisson", {"lambda": 2}, 2),  # lambda
        ("exponential", {"lambda": 2}, 1 / 2 ** 2),  # 1 / lambda ^ 2
        (
            "beta",
            {"alpha": 2, "beta": 3},
            (2 * 3) / ((2 + 3) ** 2 * (2 + 3 + 1)),
        ),  # (alpha * beta) / ((alpha + beta)^2 * (alpha + beta + 1))
        ("binomial", {"n": 100, "p": 0.2}, 100 * 0.2 * (1 - 0.2),),  # n p (1 - p)
        (
            "multinomial",
            {"n": 100, "p": [0.2, 0.8]},
            100 * np.array([0.2, 0.8]) * (1 - np.array([0.2, 0.8])),
        ),  # n p (1 - p)
    ],
)
def test_distribution_variance(name, parameters, variance):
    distribution = parameter_file.decode_distribution(
        dict(type="distribution", distribution=name, **parameters)
    )
    try:
        assert distribution.var() == variance
    except AttributeError:
        # Covariance calculations are not exact.
        assert np.allclose(distribution.cov().diagonal(), variance)


def test_samples_roundtrip(tmp_path):
    samples = np.array([1, 2, 3])
    with open(tmp_path / "test.toml", "w+b") as file:
        parameter_file.write_samples(TextIOWrapper(file), "test", samples)
    with open(tmp_path / "test.toml", "r+b") as file:
        np.testing.assert_array_equal(
            parameter_file.read_samples(TextIOWrapper(file), "test"), samples
        )

