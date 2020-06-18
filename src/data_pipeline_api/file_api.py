from io import IOBase
from operator import itemgetter
from datetime import datetime
from pathlib import Path
from typing import Union, Mapping
from functools import wraps
from hashlib import sha1
import yaml
import toml

METADATA_FILENAME = ".metadata"
DATA_DIRECTORY_CONFIG_KEY = "data_directory"
ACCESS_LOG_CONFIG_KEY = "access_log"
OVERRIDES_CONFIG_KEY = "overrides"
OPEN_TIMESTAMP_CONFIG_KEY = "open_timestamp"
CLOSE_TIMESTAMP_CONFIG_KEY = "close_timestamp"
IO_CONFIG_KEY = "io"


class AccessKey:
    type = "type"
    path = "path"
    calculated_hash = "calculated_hash"
    timestamp = "timestamp"


Metadata = Mapping[str, str]
PathMetadata = Mapping[Path, Metadata]


def standard_hash(path: Path) -> str:
    """Return a standardised hash given a path.
    """
    with open(path, "rb") as file:
        return sha1(file.read()).hexdigest()


def standard_path(metadata: Metadata) -> Path:
    """Return a standardised path given some metadata.
    """
    return Path(metadata[MetadataKey.quantity]) / "{run_id}.{extension}".format(
        **metadata
    )


class MetadataKey:
    filename = "filename"
    quantity = "quantity"
    extension = "extension"
    format = "format"
    run_id = "run_id"
    version = "version"
    hash = "hash"


class FileAPI:
    def __init__(
        self,
        config_filename: Union[Path, str],
        do_not_overwrite: bool = True,
        require_matching_hashes: bool = True,
    ):
        self._open_timestamp = datetime.now()
        self._config_filename = Path(config_filename)
        self._do_not_overwrite = do_not_overwrite
        self._require_matching_hashes = require_matching_hashes
        self._accesses = []
        # Config file defines the base root
        # If data directory is relative then it is relative to the config root
        # If the log path is relative then it is relative to the config root

        if not self._config_filename.is_file():
            raise ValueError("f{self._config_filename} is not a file")
        else:
            with open(self._config_filename) as config_file:
                self._config = yaml.load(config_file)

        self._config_root = self._config_filename.parent

        data_directory = Path(self._config[DATA_DIRECTORY_CONFIG_KEY])
        if data_directory.is_absolute():
            self._data_directory = data_directory
        else:
            self._data_directory = self._config_root / data_directory

        with open(self._data_directory / METADATA_FILENAME) as path_metadata_file:
            self._metadata = yaml.load(path_metadata_file)

        log_path = Path(self._config[ACCESS_LOG_CONFIG_KEY])
        if log_path.is_absolute():
            self._log_path = log_path
        else:
            self._log_path = self._config_root / log_path

    def read(self, **metadata) -> IOBase:
        """Return a file open for reading corresponding to the given metadata.

        The file contents are hashed, and a record is made of the read.
        """
        normalised_metadata = self.normalise_metadata(metadata)
        matching_paths = tuple(
            filter(
                lambda metadata: all(
                    key in metadata and metadata[key] == value
                    for key, value in normalised_metadata.items()
                ),
                self._metadata,
            )
        )
        if not matching_paths:
            raise ValueError(f"no matching paths for {normalised_metadata}")

        read_metadata = max(matching_paths, key=itemgetter(MetadataKey.version))
        filename = Path(read_metadata[MetadataKey.filename])
        filepath = self._data_directory / filename
        file_hash = standard_hash(filepath)
        if self._require_matching_hashes:
            try:
                metadata_hash = read_metadata[MetadataKey.hash]
            except KeyError:
                # TODO : consider logging a warning here?
                pass
            else:
                if file_hash != metadata_hash:
                    raise ValueError(
                        f"file hash {file_hash} != metadata hash {metadata_hash}"
                    )
        self._accesses.append(
            {
                AccessKey.type: "read",
                AccessKey.path: str(filename),
                AccessKey.calculated_hash: file_hash,
                AccessKey.timestamp: datetime.now(),
                "call_metadata": metadata,
                "normalised_metadata": normalised_metadata,
                "read_metadata": read_metadata,
            }
        )
        return open(filepath, mode="rb")

    def write(self, **metadata) -> IOBase:
        """Return a file open for writing corresponding to the given metadata.

        When the file is closed the file contents are hashed, and a record is made of
        the write.
        """
        normalised_metadata = self.normalise_metadata(metadata)
        path = standard_path(normalised_metadata)
        full_path = self._data_directory / path
        if not full_path.parent.exists():
            full_path.parent.mkdir(parents=True)
        if self._do_not_overwrite and full_path.exists():
            raise ValueError(f"{path} already exists")
        file = open(self._data_directory / path, mode="wb")
        # Wrap the file close method with something to record the file access.
        close_file = file.close

        @wraps(close_file)
        def close():
            file.flush()
            self._accesses.append(
                {
                    AccessKey.type: "write",
                    AccessKey.path: str(path),
                    AccessKey.calculated_hash: standard_hash(
                        self._data_directory / path
                    ),
                    AccessKey.timestamp: datetime.now(),
                    "call_metadata": metadata,
                    "write_metadata": normalised_metadata,
                }
            )
            return close_file()

        file.close = close
        return file

    def close(self):
        """Write 
        """
        with open(self._log_path, "w") as output_file:
            yaml.dump(
                {
                    DATA_DIRECTORY_CONFIG_KEY: str(
                        self._data_directory
                        if self._data_directory.is_absolute()
                        else self._data_directory.relative_to(self._config_root)
                    ),
                    OPEN_TIMESTAMP_CONFIG_KEY: self._open_timestamp,
                    CLOSE_TIMESTAMP_CONFIG_KEY: datetime.now(),
                    OVERRIDES_CONFIG_KEY: self._config[OVERRIDES_CONFIG_KEY],
                    IO_CONFIG_KEY: self._accesses,
                },
                output_file,
                sort_keys=False,
            )

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ==================================================================================
    # Metadata transformation
    # ==================================================================================

    format_extensions = {
        "csv": "csv",
        "parameter": "toml",
    }

    def normalise_metadata(self, metadata: Metadata) -> Metadata:
        """Normalise metadata.
        """
        # Augment with metadata from the config.
        try:
            config_metadata = self._config[OVERRIDES_CONFIG_KEY][
                metadata[MetadataKey.quantity]
            ]
        except KeyError:
            pass
        else:
            metadata = metadata.copy()
            metadata.update(config_metadata)

        # Attempt to infer the extension.
        try:
            extension = FileAPI.format_extensions[metadata[MetadataKey.format]]
        except KeyError:
            pass
        else:
            metadata = metadata.copy()
            metadata[MetadataKey.extension] = extension

        return metadata
