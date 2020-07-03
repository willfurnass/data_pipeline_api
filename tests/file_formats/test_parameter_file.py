import numpy as np
from scipy import stats
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



def assert_distribution_roundtrip(tmp_path, distribution):
    with open(tmp_path / "test.toml", "w+b") as file:
        parameter_file.write_distribution(TextIOWrapper(file), f"test-{distribution.dist.name}", distribution)

    def representation(distribution):
        return distribution.dist._parse_args(*distribution.args, **distribution.kwds)

    with open(tmp_path / "test.toml", "r+b") as file:
        assert representation(
            parameter_file.read_distribution(TextIOWrapper(file), f"test-{distribution.dist.name}")
        ) == representation(distribution)


def test_gamma_distribution_roundtrip(tmp_path):
    assert_distribution_roundtrip(tmp_path, stats.gamma(a=2, scale=3))


def test_normal_distribution_roundtrip(tmp_path):
    assert_distribution_roundtrip(tmp_path, stats.norm(loc=2, scale=3))


def test_samples_roundtrip(tmp_path):
    samples = np.array([1, 2, 3])
    with open(tmp_path / "test.toml", "w+b") as file:
        parameter_file.write_samples(TextIOWrapper(file), "test", samples)
    with open(tmp_path / "test.toml", "r+b") as file:
        np.testing.assert_array_equal(
            parameter_file.read_samples(TextIOWrapper(file), "test"), samples
        )

