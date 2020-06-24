import pytest
from pathlib import Path
from data_pipeline_api.file_api import FileAPI

@pytest.fixture
def configuration_file(tmp_path: Path) -> Path:
    with open(tmp_path / "version1.txt", "w") as file:
        file.write("contents1")

    with open(tmp_path / "version2.txt", "w") as file:
        file.write("contents2")

    metadata_file = tmp_path / "metadata.yaml"
    with open(metadata_file, "w") as file:
        file.write(
            """
-
  data_product: test
  version: 1.0.0
  filename: version1.txt
-
  data_product: test
  version: 2.0.0
  filename: version2.txt
"""
        )

    configuration_file = tmp_path / "config.yaml"
    with open(configuration_file, "w") as file:
        file.write(
            """
data_directory: .
access_log: access.yaml
fail_on_hash_mismatch: False
"""
        )
    return configuration_file


def test_read_latest_version(configuration_file: Path):
    file_api = FileAPI(configuration_file)
    with file_api.open_for_read(data_product="test") as file:
        assert file.read().decode() == "contents2"


def test_read_specific_version(configuration_file: Path):
    file_api = FileAPI(configuration_file)
    with file_api.open_for_read(data_product="test", version="1.0.0") as file:
        assert file.read().decode() == "contents1"


def test_write(tmp_path: Path, configuration_file: Path):
    with FileAPI(configuration_file) as api:
        with api.open_for_write(data_product="test", extension="txt") as file:
            file.write("contents3".encode())
        with open(tmp_path / "test" / f"{api.run_id}.txt") as file:
            assert file.read() == "contents3"


# TODO : test access logging.
# TODO : test configuration loading.
# TODO : test metadata loading.
