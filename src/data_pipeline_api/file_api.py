from io import IOBase
from operator import itemgetter
from datetime import datetime
from pathlib import Path
from typing import Union, Mapping, Iterable
from functools import wraps
from hashlib import sha1
import yaml


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
    return Path(metadata[MetadataKey.quantity]) / "{}.{}".format(
        metadata[MetadataKey.run_id], metadata[MetadataKey.extension]
    )


def is_superset(metadataA: Metadata, metadataB: Metadata) -> bool:
    """Return True if metadataA is a superset of metadataB.
    """
    return all(
        key in metadataA and metadataA[key] == value for key, value in metadataB.items()
    )


class MetadataKey:
    filename = "filename"
    quantity = "quantity"
    extension = "extension"
    run_id = "run_id"
    version = "version"
    verified_hash = "verified_hash"
    calculated_hash = "calculated_hash"


class RunConfig:
    def __init__(self, filename: Path):
        # TODO : compute the run id.
        self._run_id = 12345

        with open(filename, "rb") as file:
            m = sha1(file.read())
        m.update(str(datetime.now()).encode())
        self._run_id = m.hexdigest()

        with open(filename) as config_file:
            self._config = yaml.load(config_file)

        self._root = filename.parent

        data_directory = Path(self._config.get("data_directory", "."))
        if data_directory.is_absolute():
            self._data_directory = data_directory
        else:
            self._data_directory = self._root / data_directory

        log_path = Path(
            self._config.get("access_log", "access-{run_id}.yaml").format(
                run_id=self._run_id
            )
        )
        if log_path.is_absolute():
            self._access_log_path = log_path
        else:
            self._access_log_path = self._root / log_path

        self.fail_on_overwrite = self._config.get("fail_on_overwrite", True)
        self.fail_on_hash_mismatch = self._config.get("fail_on_hash_mismatch", True)

        self._read_overrides = self._config.get("read", ())
        self._write_overrides = self._config.get("write", ())

    @property
    def run_id(self):
        return self._run_id

    @property
    def data_directory(self):
        return self._data_directory

    @property
    def relative_data_directory(self):
        return str(
            self.data_directory
            if self.data_directory.is_absolute()
            else self.data_directory.relative_to(self._root)
        )

    @property
    def metadata_filename(self):
        return self.data_directory / "metadata.yaml"

    @property
    def access_log_path(self):
        return self._access_log_path

    def find(self, metadata: Metadata, write: bool = False) -> Iterable[Metadata]:
        # Find all overrides which match metadata.
        return (
            match["use"]
            for match in filter(
                lambda entry: is_superset(metadata, entry["where"]),
                self._write_overrides if write else self._read_overrides,
            )
        )

    def apply(self, metadata: Metadata, write: bool = False) -> Metadata:
        overrides = tuple(self.find(metadata, write=write))
        if overrides:
            metadata = metadata.copy()
            for override in overrides:
                metadata.update(override)
        return metadata


class FileMetadata:
    def __init__(self, path: Path):
        with open(path) as path_metadata_file:
            self._metadata = yaml.load(path_metadata_file)

    def find(self, metadata: Metadata) -> Iterable[Metadata]:
        matching_metadata = tuple(
            filter(
                lambda match_metadata: is_superset(match_metadata, metadata),
                self._metadata,
            )
        )
        if matching_metadata:
            return max(matching_metadata, key=itemgetter(MetadataKey.version))
        else:
            return metadata


class FileAPI:
    def __init__(
        self, config_filename: Union[Path, str],
    ):
        self._open_timestamp = datetime.now()
        self._accesses = []

        self._config = RunConfig(Path(config_filename))
        if self._config.fail_on_overwrite and self._config.access_log_path.exists():
            raise ValueError(f"{self._config.access_log_path} already exists")

        self._file_metadata = FileMetadata(self._config.metadata_filename)

    def _record_access(
        self, access_type: str, call_metadata: Metadata, access_metadata: Metadata
    ):
        self._accesses.append(
            {
                "type": access_type,
                "timestamp": datetime.now(),
                "call_metadata": call_metadata,
                "access_metadata": access_metadata,
            }
        )

    def read(self, **metadata) -> IOBase:
        """Return a file open for reading corresponding to the given metadata.

        The file contents are hashed, and a record is made of the read.
        """
        read_metadata = metadata.copy()
        read_metadata = self._config.apply(read_metadata, write=False)
        read_metadata = self._file_metadata.find(read_metadata)
        try:
            filename = Path(read_metadata[MetadataKey.filename])
        except KeyError:
            raise KeyError(f"could not find {MetadataKey.filename} in {read_metadata}")
        filepath = self._config.data_directory / filename
        read_metadata[MetadataKey.calculated_hash] = standard_hash(filepath)
        if self._config.fail_on_hash_mismatch:
            if (
                read_metadata[MetadataKey.calculated_hash]
                != read_metadata[MetadataKey.verified_hash]
            ):
                raise ValueError(
                    (
                        "calculated hash {calculated_hash} != "
                        "verified hash {verified_hash}"
                    ).format(**read_metadata)
                )
        read_metadata[MetadataKey.run_id] = self._config.run_id
        self._record_access("read", metadata, read_metadata)
        return open(filepath, mode="rb")

    def write(self, **metadata) -> IOBase:
        """Return a file open for writing corresponding to the given metadata.

        When the file is closed the file contents are hashed, and a record is made of
        the write.
        """
        write_metadata = metadata.copy()
        write_metadata = self._config.apply(metadata, write=True)
        write_metadata[MetadataKey.run_id] = self._config.run_id
        if MetadataKey.filename not in write_metadata:
            write_metadata[MetadataKey.filename] = str(standard_path(write_metadata))

        full_path = self._config.data_directory / write_metadata[MetadataKey.filename]
        if not full_path.parent.exists():
            full_path.parent.mkdir(parents=True)
        if self._config.fail_on_overwrite and full_path.exists():
            raise ValueError(f"{full_path} already exists")
        file = open(full_path, mode="wb")
        # Wrap the file close method with something to record the file access.
        close_file = file.close

        @wraps(close_file)
        def close():
            file.flush()
            write_metadata[MetadataKey.calculated_hash] = standard_hash(full_path)
            self._record_access("write", metadata, write_metadata)
            return close_file()

        file.close = close
        return file

    def close(self):
        """Close the session and write the access log.
        """
        with open(self._config.access_log_path, "w") as output_file:
            yaml.dump(
                {
                    "data_directory": self._config.relative_data_directory,
                    "open_timestamp": self._open_timestamp,
                    "close_timestamp": datetime.now(),
                    "io": self._accesses,
                },
                output_file,
                sort_keys=False,
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.close()
