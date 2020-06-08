import pathlib
from data_pipeline_api.file_system_data_access import (
    FileSystemDataAccess,
    FileSystemParameterVersion,
)


DATA_PATH = pathlib.Path(__file__).parent.parent / "example_data"


def test_get_parameter_version():
    parameter = "human/infection/SARS-CoV-2/latent-period"
    version = 2
    path = DATA_PATH / parameter / str(version)
    verified_hexdigest = (
        "9ca8c7131633de54c26af6fd8c6e4e940cb96bb672945e82b1f7cdf1ba8f758bc"
    )
    parameter_version = FileSystemDataAccess(DATA_PATH, None).get_parameter_version(
        parameter
    )
    assert parameter_version.parameter == parameter
    assert parameter_version.version == version
    assert parameter_version.path == path
    assert parameter_version.verified_hexdigest == verified_hexdigest


def test_get_verified_hexdigest():
    data_access = FileSystemDataAccess(DATA_PATH, None)
    parameter_version = data_access.get_parameter_version(
        "human/infection/SARS-CoV-2/latent-period"
    )
    assert (
        parameter_version.get_verified_hexdigest()
        == "9ca8c7131633de54c26af6fd8c6e4e940cb96bb672945e82b1f7cdf1ba8f758bc"
    )


def test_read_bytes():
    data_access = FileSystemDataAccess(DATA_PATH, None)
    parameter_version = data_access.get_parameter_version(
        "human/infection/SARS-CoV-2/latent-period"
    )
    assert (
        parameter_version.read_bytes("data.toml").decode()
        == """[distribution]
distribution = "gamma"
shape = 3.0
scale = 2.0
"""
    )
