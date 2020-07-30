import logging
from pathlib import Path
from unittest.mock import Mock, patch
import pytest
from data_pipeline_api.file_api import FileAPI, FileAccess, ReadAccess, WriteAccess

logging.basicConfig(level="DEBUG")


@pytest.fixture
def configuration_file(tmp_path: Path) -> Path:
    with open(tmp_path / "version1.txt", "w") as file:
        file.write("contents1")

    with open(tmp_path / "version2.txt", "w") as file:
        file.write("contents2")

    hash1 = FileAPI.calculate_hash(tmp_path / "version1.txt")
    hash2 = FileAPI.calculate_hash(tmp_path / "version2.txt")
    metadata_file = tmp_path / "metadata.yaml"
    with open(metadata_file, "w") as file:
        file.write(
            """
-
  data_product: test
  version: 1.0.0
  filename: version1.txt
  verified_hash: {hash1}
-
  data_product: test
  version: 2.0.0
  filename: version2.txt
  verified_hash: {hash2}
""".format(hash1=hash1, hash2=hash2)
        )

    configuration_file = tmp_path / "config.yaml"
    with open(configuration_file, "w") as file:
        file.write(
            """
data_directory: .
run_id: test_run
access_log: access.yaml
fail_on_hash_mismatch: True
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
        with open(tmp_path / "test" / "test_run.txt") as file:
            assert file.read() == "contents3"


def test_read_hash_mismatch(configuration_file: Path):
    file_api = FileAPI(configuration_file)

    with pytest.raises(ValueError):
        with patch("data_pipeline_api.file_api.FileAPI.calculate_hash") as mock_hash:
            mock_hash.return_value = "some_random_hash"
            with file_api.open_for_read(data_product="test", version="1.0.0") as file:
                pass


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


def test_set_get_run_metadata():
    with FileAPI() as api:
        api.set_run_metadata("key", "value")
        assert api.get_run_metadata("key") == "value"


@pytest.mark.parametrize(
    ("key"), FileAPI.RESERVED_RUN_METADATA_KEYS,
)
def test_cannot_set_reserved_run_metadata_keys(key):
    with pytest.raises(ValueError):
        FileAPI().set_run_metadata(key, "value")


def test_access_file_contains_run_metadata(tmp_path):
    configuration_file = tmp_path / "config.yaml"
    with open(configuration_file, "w") as file:
        file.write("access_log: access.yaml")
    with FileAPI(configuration_file):
        pass
    with FileAPI():
        pass
    assert (tmp_path / "access.yaml").exists()


def test_file_access_to_access_log_record():
    assert FileAccess(
        "test", {"call": "call"}, {"calculated_hash": "access"}, "test"
    ).to_access_log_record() == {
        "timestamp": "test",
        "call_metadata": {"call": "call"},
        "access_metadata": {"calculated_hash": "access"},
    }


def test_file_access_to_access_log_record_prefers_access_metadata(tmp_path):
    assert FileAccess(
        1, {"call": "call"}, {"calculated_hash": "access"}, tmp_path
    ).to_access_log_record({tmp_path.resolve(): "test"}) == {
        "timestamp": 1,
        "call_metadata": {"call": "call"},
        "access_metadata": {"calculated_hash": "access"},
    }


def test_file_access_to_access_log_record_uses_cache_if_not_in_metadata(tmp_path):
    assert FileAccess(1, {"call": "call"}, {}, tmp_path).to_access_log_record(
        {tmp_path.resolve(): "test"}
    ) == {
        "timestamp": 1,
        "call_metadata": {"call": "call"},
        "access_metadata": {"calculated_hash": "test"},
    }


def test_file_access_to_access_log_record_calculates_hash_if_not_found(tmp_path):
    with open(tmp_path / "test.txt", "w") as file:
        file.write("hello")
    assert FileAccess(
        1, {"call": "call"}, {}, tmp_path / "test.txt"
    ).to_access_log_record({}) == {
        "timestamp": 1,
        "call_metadata": {"call": "call"},
        "access_metadata": {
            "calculated_hash": "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"
        },
    }


def test_file_access_to_access_log_record_raises_on_invalid_path(tmp_path):
    with pytest.raises(FileNotFoundError):
        assert FileAccess(
            1, {"call": "call"}, {}, tmp_path / "test.txt"
        ).to_access_log_record({})


def test_read_access():
    assert ReadAccess(
        "test", {"call": "call"}, {"calculated_hash": "access"}, "test"
    ).to_access_log_record() == {
        "type": "read",
        "timestamp": "test",
        "call_metadata": {"call": "call"},
        "access_metadata": {"calculated_hash": "access"},
    }


def test_write_access(tmp_path):
    assert WriteAccess(
        "test",
        {"call": "call"},
        {"calculated_hash": "access"},
        "test",
        open(tmp_path / "test.txt", "w"),
    ).to_access_log_record() == {
        "type": "write",
        "timestamp": "test",
        "call_metadata": {"call": "call"},
        "access_metadata": {"calculated_hash": "access"},
    }


def test_write_access_flushes_open_file(tmp_path):
    file = open(tmp_path / "test.txt", "w")
    file.flush = Mock()
    WriteAccess(
        "test", {"call": "call"}, {"calculated_hash": "access"}, "test", file
    ).to_access_log_record()
    file.flush.assert_called_once()
    file.close()


def test_generate_access_log(configuration_file):
    file_api = FileAPI(configuration_file)
    file_api.open_for_read(data_product="test", version="1.0.0").close()
    file_api.open_for_write(data_product="test2", extension="txt").close()
    assert len(file_api._generate_access_log()["io"]) == 2


def test_multiple_writes_to_same_file_record_same_hash(configuration_file):
    file_api = FileAPI(configuration_file)
    with file_api.open_for_write(data_product="test", extension="txt") as file:
        file.write("foo".encode())
    with file_api.open_for_write(data_product="test", extension="txt") as file:
        file.write("bar".encode())
    access_log = file_api._generate_access_log()
    assert (
        access_log["io"][0]["access_metadata"]["calculated_hash"]
        == access_log["io"][1]["access_metadata"]["calculated_hash"]
    )
