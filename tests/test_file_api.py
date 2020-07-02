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
# TODO : test for behaviour when we can't find a filename.
# TODO : test for behaviour when we can't find a file.


def test_access_log_written_if_not_set(tmp_path):
    configuration_file = tmp_path / "config.yaml"
    with open(configuration_file, "w") as file:
        file.write(
            """
data_directory: .
run_id: test
fail_on_hash_mismatch: False
"""
        )
    with FileAPI(configuration_file):
        pass
    assert (tmp_path / "access-test.yaml").exists()


def test_access_log_written_if_set_to_path(tmp_path):
    configuration_file = tmp_path / "config.yaml"
    with open(configuration_file, "w") as file:
        file.write(
            """
data_directory: .
access_log: access.yaml
fail_on_hash_mismatch: False
"""
        )
    with FileAPI(configuration_file):
        pass
    assert (tmp_path / "access.yaml").exists()


def test_access_log_not_written_if_set_to_False(tmp_path):
    configuration_file = tmp_path / "config.yaml"
    with open(configuration_file, "w") as file:
        file.write(
            """
data_directory: .
access_log: False
fail_on_hash_mismatch: False
"""
        )
    with FileAPI(configuration_file):
        pass
    assert not (tmp_path / "access.yaml").exists()


def test_access_log_written_if_set_to_absolute_path(tmp_path):
    configuration_file = tmp_path / "config.yaml"
    access_file_path = tmp_path / "access.yaml"
    with open(configuration_file, "w") as file:
        file.write(
            f"""
data_directory: .
access_log: {str(access_file_path.resolve())}
fail_on_hash_mismatch: False
"""
        )
    with FileAPI(configuration_file):
        pass
    assert access_file_path.exists()
