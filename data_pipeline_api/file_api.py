from io import IOBase
from uuid import uuid4
from datetime import datetime
from pathlib import Path
from typing import Union, Optional, Any, Iterable, Dict, List
from dataclasses import dataclass
from hashlib import sha1
from logging import getLogger, WARNING, DEBUG
import yaml

from data_pipeline_api.metadata import Metadata, MetadataKey, log_format_metadata
from data_pipeline_api.metadata_store import MetadataStore
from data_pipeline_api.overrides import Overrides

logger = getLogger(__name__)


@dataclass(frozen=True)
class FileAccess:
    """Represents a generic file access.
    """

    timestamp: datetime
    call_metadata: Metadata
    access_metadata: Metadata
    path: Path

    def to_access_log_record(
        self, hash_cache: Optional[Dict[Path, str]] = None
    ) -> Dict[str, Any]:
        if hash_cache is None:
            hash_cache = {}
        access_metadata = self.access_metadata.copy()
        if MetadataKey.calculated_hash not in access_metadata:
            absolute_path = self.path.resolve()
            try:
                access_metadata[MetadataKey.calculated_hash] = hash_cache[absolute_path]
            except KeyError:
                access_metadata[MetadataKey.calculated_hash] = hash_cache[
                    absolute_path
                ] = FileAPI.calculate_hash(absolute_path)
        return {
            "timestamp": self.timestamp,
            "call_metadata": self.call_metadata,
            "access_metadata": access_metadata,
        }


class ReadAccess(FileAccess):
    """Represents a file read.
    """

    def to_access_log_record(
        self, hash_cache: Optional[Dict[Path, str]] = None
    ) -> Dict[str, Any]:
        return dict(type="read", **super().to_access_log_record(hash_cache))


@dataclass(frozen=True)
class WriteAccess(FileAccess):
    """Represents a file write.
    """

    file_handle: IOBase

    def to_access_log_record(
        self, hash_cache: Optional[Dict[Path, str]] = None
    ) -> Dict[str, Any]:
        if not self.file_handle.closed:
            logger.warning(
                "the file handle to write to %s is still open, attempting to flush",
                self.path,
            )
            self.file_handle.flush()
        return dict(type="write", **super().to_access_log_record(hash_cache))


class RunMetadata:
    run_id = "run_id"
    open_timestamp = "open_timestamp"
    close_timestamp = "close_timestamp"
    data_directory = "data_directory"

    git_repo = "git_repo"
    git_sha = "git_sha"
    default_input_namespace = "default_input_namespace"
    default_output_namespace = "default_output_namespace"
    data_registry_url = "data_registry_url"
    remote_uri = "remote_uri"
    remote_uri_override = "remote_uri_override"
    model_version = "model_version"
    model_name = "model_name"
    description = "description"
    submission_script = "submission_script"


class FileAPI:
    RESERVED_RUN_METADATA_KEYS = {
        RunMetadata.run_id,
        RunMetadata.open_timestamp,
        RunMetadata.close_timestamp,
        RunMetadata.data_directory,
    }

    @staticmethod
    def normalise_path(root: Path, path: Path) -> Path:
        """Normalise a path by prepending a known root if it is not absolute.
        """
        return path if path.is_absolute() else root / path

    @staticmethod
    def calculate_hash(filename: Path, extra_bytes: Optional[bytes] = None) -> str:
        """Calculate the SHA1 hash of a file, optionally appending extra bytes.
        """
        with open(filename, "rb") as file:
            message = sha1(file.read())
        if extra_bytes:
            message.update(extra_bytes)
        hexdigest = message.hexdigest()
        logger.debug(
            "calculated hash of %s as %s", filename, hexdigest,
        )
        return hexdigest

    @staticmethod
    def construct_overrides(overrides: Iterable[Dict[str, Dict[str, Any]]]):
        """Construct an Overrides given the structure found in the FileAPI config.
        """
        return Overrides(
            (override.get("where", {}), override.get("use", {}))
            for override in overrides
        )

    @staticmethod
    def construct_metadata_store(metadata_filename: Path) -> MetadataStore:
        """Construct a MetadataStore corresponding to a file.
        """
        metadata_store = None
        if metadata_filename.exists():
            logger.debug("loading metadata from %s", metadata_filename)
            with open(metadata_filename) as metadata_store_file:
                metadata_store = yaml.safe_load(metadata_store_file)
        else:
            logger.warning("could not find metadata.yaml")
        return MetadataStore(metadata_store)

    def __init__(self, config_filename: Optional[Union[Path, str]] = None):
        """The FileAPI class provides tracked interaction with the filesystem.

        Files are located through a metadata lookup system based on two files: a
        metadata database file, which lists all known files and their associated
        metadata, and a configuration file, which provides mechanisms for influencing
        the metadata lookup process.
        """
        self._accesses: List[FileAccess] = []
        self._run_metadata = {}

        self._open_timestamp = datetime.now()
        logger.debug("open_timestamp = %s", self._open_timestamp)

        if config_filename is None:
            self._config_filename = None
            self._config = {}
            self._root = Path()
        else:
            self._config_filename = Path(config_filename)
            with open(self._config_filename) as config_file:
                self._config = yaml.safe_load(config_file)
            self._root = self._config_filename.parent
        logger.debug("config_filename = %s", self._config_filename)
        logger.debug("root = %s", self._root.resolve())

        self._run_id = self._config.get("run_id", uuid4().hex)
        logger.debug("run_id = %s", self._run_id)

        self._fail_on_hash_mismatch = self._config.get("fail_on_hash_mismatch", True)
        logger.log(
            DEBUG if self._fail_on_hash_mismatch else WARNING,
            "fail_on_hash_mismatch = %s",
            self._fail_on_hash_mismatch,
        )

        data_directory = self._config.get("data_directory", self._root)
        self._unnormalised_data_directory = Path(data_directory)

        self._data_directory = FileAPI.normalise_path(
            self._root, self._unnormalised_data_directory
        )
        logger.debug("data_directory = %s", self._data_directory)

        access_log = self._config.get("access_log", "access-{run_id}.yaml")
        if access_log is False:
            self._access_log_path = None
            logger.warning("disabled access log")
        else:
            self._access_log_path = FileAPI.normalise_path(
                self._root, Path(access_log.format(run_id=self._run_id)),
            )
        logger.debug("access_log_path = %s", self._access_log_path)

        # Carefully set up the run metadata, preferring overrides from config.
        self._run_metadata["run_id"] = self._run_id
        self._run_metadata["data_directory"] = str(self._unnormalised_data_directory)
        self._run_metadata["open_timestamp"] = self._open_timestamp
        run_metadata = self._config.get("run_metadata", {})
        if any(key in run_metadata for key in FileAPI.RESERVED_RUN_METADATA_KEYS):
            raise ValueError(f"reserved key {key} is set in run_metadata")
        self._run_metadata.update(run_metadata)

        self._read_overrides = FileAPI.construct_overrides(self._config.get("read", ()))
        self._write_overrides = FileAPI.construct_overrides(
            self._config.get("write", ())
        )

        self.load_metadata_store()

    def load_metadata_store(self):
        metadata_store_filename = self._data_directory / "metadata.yaml"
        logger.debug("loading metadata store from %s", metadata_store_filename)
        self._metadata_store = FileAPI.construct_metadata_store(metadata_store_filename)

    def get_read_metadata(self, metadata: Metadata) -> Metadata:
        read_metadata = metadata.copy()
        self._read_overrides.apply(read_metadata)
        return self._metadata_store.find(read_metadata) or read_metadata

    def open_for_read(self, **call_metadata) -> IOBase:
        """Return a file open for reading corresponding to the given metadata.

        The file contents are hashed, and a record is made of the read.
        """
        logger.debug("starting open_for_read(%s)", log_format_metadata(call_metadata))
        read_metadata = self.get_read_metadata(call_metadata)
        path = self._data_directory / read_metadata[MetadataKey.filename]
        read_metadata[MetadataKey.calculated_hash] = FileAPI.calculate_hash(path)

        if self._fail_on_hash_mismatch:
            if (
                read_metadata[MetadataKey.calculated_hash]
                == read_metadata[MetadataKey.verified_hash]
            ):
                logger.debug("verified hash")
            else:
                raise ValueError(
                    (
                        "calculated hash {calculated_hash} != "
                        "verified hash {verified_hash}"
                    ).format(**read_metadata)
                )

        logger.debug("open('%s', mode='rb')", path)
        file = open(path, mode="rb")
        self._accesses.append(
            ReadAccess(
                timestamp=datetime.now(),
                call_metadata=call_metadata,
                access_metadata=read_metadata,
                path=path,
            )
        )
        logger.info("recorded read(%s)", log_format_metadata(call_metadata))
        return file

    def get_write_metadata(self, metadata: Metadata) -> Metadata:
        write_metadata = metadata.copy()
        self._write_overrides.apply(write_metadata)
        if MetadataKey.filename not in write_metadata:
            write_metadata[MetadataKey.filename] = str(
                Path(write_metadata[MetadataKey.data_product])
                / "{}.{}".format(self._run_id, write_metadata[MetadataKey.extension])
            )
            logger.debug("generated filename %s", write_metadata[MetadataKey.filename])
        return write_metadata

    def open_for_write(self, **call_metadata) -> IOBase:
        """Return a file open for update corresponding to the given metadata.

        When the file is closed the file contents are hashed, and a record is made of
        the write.
        """
        logger.debug("starting open_for_write(%s)", log_format_metadata(call_metadata))
        write_metadata = self.get_write_metadata(call_metadata)

        path = self._data_directory / write_metadata[MetadataKey.filename]
        if path.exists():
            mode = "r+b"
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            mode = "w+b"
        logger.debug("open('%s', mode='%s')", path, mode)
        file = open(path, mode=mode)
        self._accesses.append(
            WriteAccess(
                timestamp=datetime.now(),
                call_metadata=call_metadata,
                access_metadata=write_metadata,
                path=path,
                file_handle=file,
            )
        )
        logger.info("recorded write(%s)", log_format_metadata(call_metadata))
        return file

    def set_run_metadata(self, key: str, value: Any):
        """Set the value for a run-level metadata key.
        """
        if key in FileAPI.RESERVED_RUN_METADATA_KEYS:
            raise ValueError(f"{key} is reserved")
        self._run_metadata[key] = value

    def get_run_metadata(self, key: str) -> Any:
        """Get the value for a run-level metadata key.
        """
        return self._run_metadata[key]

    def _generate_access_log(self) -> Dict[str, Any]:
        calculated_path_hashes = {}
        return {
            "run_metadata": dict(close_timestamp=datetime.now(), **self._run_metadata),
            "config": self._config,
            "io": [
                access.to_access_log_record(calculated_path_hashes)
                for access in self._accesses
            ],
        }

    def close(self):
        """Close the session and write the access log.
        """
        if self._access_log_path:
            with open(self._access_log_path, "w") as output_file:
                yaml.dump(
                    self._generate_access_log(), output_file, sort_keys=False,
                )
            logger.info("wrote access log")
        else:
            logger.warning("did not write access log")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.close()
