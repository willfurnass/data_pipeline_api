from io import IOBase
from datetime import datetime
from pathlib import Path
from typing import Union, Optional
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
    @staticmethod
    def normalise_path(root: Path, path: Path):
        """Normalise a path by prepending a known root if it is not absolute.
        """
        return path if path.is_absolute() else root / path

    @staticmethod
    def calculate_hash(filename: Path, extra_bytes: Optional[bytes] = None):
        """Calculate the SHA1 hash of a file, optionally appending extra bytes.
        """
        with open(filename, "rb") as file:
            message = sha1(file.read())
        if extra_bytes:
            message.update(extra_bytes)
        return message.hexdigest()

    @staticmethod
    def construct_overrides(overrides):
        """Construct an Overrides given the structure found in the FileAPI config.
        """
        return Overrides(
            (override.get("where", {}), override.get("use", {}))
            for override in overrides
        )

    @staticmethod
    def construct_metadata_store(metadata_filename: Path):
        """Construct a MetadataStore corresponding to a file.
        """
        if metadata_filename.exists():
            logger.debug("loading metadata from %s", metadata_filename)
            with open(metadata_filename) as metadata_store_file:
                metadata_store = yaml.safe_load(metadata_store_file)
        else:
            logger.warning("could not find metadata.yaml")
            metadata_store = {}
        return MetadataStore(metadata_store)

    def __init__(self, config_filename: Union[Path, str]):
        """The FileAPI class provides tracked interaction with the filesystem.

        Files are located through a metadata lookup system based on two files: a
        metadata database file, which lists all known files and their associated
        metadata, and a configuration file, which provides mechanisms for influencing
        the metadata lookup process.
        """
        logger.debug("config_filename = %s", config_filename)
        self._open_timestamp = datetime.now()
        logger.debug("open_timestamp = %s", self._open_timestamp)
        self._accesses = []
        self._run_metadata = {}

        config_filename = Path(config_filename)
        config_root = config_filename.parent

        try:
            with open(config_filename) as config_file:
                self._config = yaml.safe_load(config_file)
                if "run_id" in self._config:
                    self.run_id = self._config["run_id"]
                    logger.debug("run_id = %s (from config)", self.run_id)
                else:
                    # Compute a run_id.
                    self.run_id = FileAPI.calculate_hash(
                        config_filename, str(self._open_timestamp).encode()
                    )
                    logger.debug("run_id = %s (computed)", self.run_id)

                self.data_directory = Path(self._config.get("data_directory", "."))
                logger.debug("data_directory = %s", self.data_directory)

                self.fail_on_hash_mismatch = self._config.get(
                    "fail_on_hash_mismatch", True
                )
                logger.log(
                    DEBUG if self.fail_on_hash_mismatch else WARNING,
                    "fail_on_hash_mismatch = %s",
                    self.fail_on_hash_mismatch,
                )

                self._read_overrides = FileAPI.construct_overrides(
                    self._config.get("read", ())
                )
                self._write_overrides = FileAPI.construct_overrides(
                    self._config.get("write", ())
                )

                access_log = self._config.get("access_log", "access-{run_id}.yaml")
                if access_log is False:
                    self.access_log_path = None
                    logger.warning("disabled access log")
                else:
                    self.access_log_path = FileAPI.normalise_path(
                        config_root, Path(access_log.format(run_id=self.run_id)),
                    )
                logger.debug("access_log_path = %s", self.access_log_path)

        except Exception as exception:
            raise ValueError("could not parse config file") from exception

        self.normalised_data_directory = FileAPI.normalise_path(
            config_root, self.data_directory
        )
        logger.debug("normalised_data_directory = %s", self.normalised_data_directory)

        self._metadata_store = FileAPI.construct_metadata_store(
            self.normalised_data_directory / "metadata.yaml"
        )

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
        if access_type == AccessType.READ and self.fail_on_hash_mismatch:
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

    def open_for_read(self, **call_metadata) -> IOBase:
        """Return a file open for reading corresponding to the given metadata.

        The file contents are hashed, and a record is made of the read.
        """
        logger.debug("starting open_for_read(%s)", log_format_metadata(call_metadata))
        read_metadata = call_metadata.copy()
        self._read_overrides.apply(read_metadata)
        read_metadata = self._metadata_store.find(read_metadata) or read_metadata
        try:
            filename = Path(read_metadata[MetadataKey.filename])
        except KeyError:
            raise KeyError(f"could not find {MetadataKey.filename} in {read_metadata}")
        path = self.normalised_data_directory / filename
        logger.debug("open('%s', mode='rb')", path)
        file = open(path, mode="rb")
        self._record_access(AccessType.READ, call_metadata, read_metadata, path)
        return file

    def open_for_write(self, **call_metadata) -> IOBase:
        """Return a file open for update corresponding to the given metadata.

        When the file is closed the file contents are hashed, and a record is made of
        the write.
        """
        logger.debug("starting open_for_write(%s)", log_format_metadata(call_metadata))
        write_metadata = call_metadata.copy()
        self._write_overrides.apply(write_metadata)
        if MetadataKey.filename not in write_metadata:
            write_metadata[MetadataKey.filename] = str(
                Path(write_metadata[MetadataKey.data_product])
                / "{}.{}".format(self.run_id, write_metadata[MetadataKey.extension],)
            )
            logger.debug(
                "generated filename %s", write_metadata[MetadataKey.filename],
            )

        path = self.normalised_data_directory / write_metadata[MetadataKey.filename]
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

    def set_metadata(self, key, value):
        """Set run-level metadata.
        """
        self._run_metadata[key] = value

    def close(self):
        """Close the session and write the access log.
        """
        if self.access_log_path:
            with open(self.access_log_path, "w") as output_file:
                yaml.dump(
                    {
                        "data_directory": str(self.data_directory),
                        "open_timestamp": self._open_timestamp,
                        "close_timestamp": datetime.now(),
                        "run_id": self.run_id,
                        "config": self._config,
                        "metadata": self._run_metadata,
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
