from io import IOBase
from uuid import uuid4
from datetime import datetime
from pathlib import Path
from typing import Union, Optional, Any, Iterable, Dict
from functools import wraps
from hashlib import sha1
from logging import getLogger, WARNING, DEBUG
import yaml

from data_pipeline_api.metadata import Metadata, MetadataKey, log_format_metadata
from data_pipeline_api.metadata_store import MetadataStore
from data_pipeline_api.overrides import Overrides

logger = getLogger(__name__)


class AccessType:
    """File access type constants.
    """

    READ = "read"
    WRITE = "write"


class FileAPI:
    RESERVED_RUN_METADATA_KEYS = {
        "run_id",
        "open_timestamp",
        "close_timestamp",
        "data_directory",
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
        return message.hexdigest()

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
        self._accesses = []
        self._run_metadata = {}

        self._open_timestamp = datetime.now()
        self._run_metadata["open_timestamp"] = self._open_timestamp
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
        self._run_metadata["run_id"] = self._run_id
        logger.debug("run_id = %s", self._run_id)

        self._fail_on_hash_mismatch = self._config.get("fail_on_hash_mismatch", True)
        logger.log(
            DEBUG if self._fail_on_hash_mismatch else WARNING,
            "fail_on_hash_mismatch = %s",
            self._fail_on_hash_mismatch,
        )

        data_directory = self._config.get("data_directory", self._root)
        self._unnormalised_data_directory = Path(data_directory)
        self._run_metadata["data_directory"] = str(self._unnormalised_data_directory)
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

        self._run_metadata.update(self._config.get("metadata", {}))
        self._read_overrides = FileAPI.construct_overrides(self._config.get("read", ()))
        self._write_overrides = FileAPI.construct_overrides(
            self._config.get("write", ())
        )

        self.load_metadata_store()

    def load_metadata_store(self):
        metadata_store_filename = self._data_directory / "metadata.yaml"
        logger.debug("loading metadata store from %s", metadata_store_filename)
        self._metadata_store = FileAPI.construct_metadata_store(metadata_store_filename)

    def _record_access(
        self,
        access_type: str,
        call_metadata: Metadata,
        access_metadata: Metadata,
        path: Path,
    ):
        logger.debug("recording a %s", access_type)
        access_time = datetime.now()
        access_metadata = access_metadata.copy()
        access_metadata[MetadataKey.calculated_hash] = FileAPI.calculate_hash(path)
        logger.debug(
            "calculated hash of %s as %s",
            path,
            access_metadata[MetadataKey.calculated_hash],
        )
        if access_type == AccessType.READ and self._fail_on_hash_mismatch:
            if (
                access_metadata[MetadataKey.calculated_hash]
                == access_metadata[MetadataKey.verified_hash]
            ):
                logger.debug("verified hash")
            else:
                logger.critical(
                    "found hash mismatch %s != %s",
                    access_metadata[MetadataKey.calculated_hash],
                    access_metadata[MetadataKey.verified_hash],
                )
                raise ValueError(
                    (
                        "calculated hash {calculated_hash} != "
                        "verified hash {verified_hash}"
                    ).format(**access_metadata)
                )
        self._accesses.append(
            {
                "type": access_type,
                "timestamp": access_time,
                "call_metadata": call_metadata,
                "access_metadata": access_metadata,
            }
        )
        logger.info("recorded %s(%s)", access_type, log_format_metadata(call_metadata))

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
        try:
            filename = Path(read_metadata[MetadataKey.filename])
        except KeyError:
            raise KeyError(f"could not find {MetadataKey.filename} in {read_metadata}")
        path = self._data_directory / filename
        logger.debug("open('%s', mode='rb')", path)
        file = open(path, mode="rb")
        self._record_access(AccessType.READ, call_metadata, read_metadata, path)
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
        # Wrap the file close method with something to record the file access.
        close_file = file.close

        @wraps(close_file)
        def close():
            file.flush()
            self._record_access(AccessType.WRITE, call_metadata, write_metadata, path)
            return close_file()

        file.close = close
        return file

    def set_run_metadata(self, key: str, value: Any):
        """Set run-level metadata.
        """
        if key in FileAPI.RESERVED_RUN_METADATA_KEYS:
            raise ValueError(f"{key} is reserved")
        self._run_metadata[key] = value

    def get_run_metadata(self, key: str) -> Any:
        """Get run-level metadata.
        """
        return self._run_metadata[key]

    def close(self):
        """Close the session and write the access log.
        """

        if self._access_log_path:
            self._run_metadata["close_timestamp"] = datetime.now()
            with open(self._access_log_path, "w") as output_file:
                yaml.dump(
                    {
                        "run_metadata": self._run_metadata,
                        "config": self._config,
                        "io": self._accesses,
                    },
                    output_file,
                    sort_keys=False,
                )
            logger.info("wrote %s accesses to access log", len(self._accesses))
        else:
            logger.warning("did not write access log")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.close()
