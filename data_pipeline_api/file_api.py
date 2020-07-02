from io import IOBase
from datetime import datetime
from pathlib import Path
from typing import Union
from functools import wraps
from hashlib import sha1
import yaml

from data_pipeline_api.metadata import Metadata, MetadataKey
from data_pipeline_api.metadata_store import MetadataStore
from data_pipeline_api.overrides import Overrides


class FileAPI:
    def __init__(
        self, config_filename: Union[Path, str],
    ):
        self._open_timestamp = datetime.now()
        self._accesses = []
        config_filename = Path(config_filename)

        try:
            with open(config_filename) as config_file:
                self._config = yaml.safe_load(config_file)
                if "run_id" in self._config:
                    self.run_id = self._config["run_id"]
                else:
                    # Compute a run_id.
                    with open(config_filename, "rb") as file:
                        m = sha1(file.read())
                    m.update(str(self._open_timestamp).encode())
                    self.run_id = m.hexdigest()
                self.data_directory = Path(self._config.get("data_directory", "."))
                self.fail_on_hash_mismatch = self._config.get(
                    "fail_on_hash_mismatch", True
                )
                self._read_overrides = Overrides(
                    (override.get("where", {}), override.get("use", {}))
                    for override in self._config.get("read", ())
                )
                self._write_overrides = Overrides(
                    (override.get("where", {}), override.get("use", {}))
                    for override in self._config.get("write", ())
                )
                access_log = self._config.get("access_log", "access-{run_id}.yaml")
                if access_log is False:
                    self.access_log_path = None
                else:
                    self.access_log_path = Path(access_log.format(run_id=self.run_id))
                    if not self.access_log_path.is_absolute():
                        self.access_log_path = (
                            config_filename.parent / self.access_log_path
                        )

        except Exception as exception:
            raise ValueError("could not parse config file") from exception

        if self.data_directory.is_absolute():
            self.normalised_data_directory = self.data_directory
        else:
            self.normalised_data_directory = (
                config_filename.parent / self.data_directory
            )

        metadata_filename = self.normalised_data_directory / "metadata.yaml"
        if metadata_filename.exists():
            with open(metadata_filename) as metadata_store_file:
                metadata_store = yaml.safe_load(metadata_store_file)
        else:
            metadata_store = {}
        self._metadata_store = MetadataStore(metadata_store)

    def _record_access(
        self,
        access_type: str,
        call_metadata: Metadata,
        access_metadata: Metadata,
        path: Path,
        verify_hash: bool,
    ):
        access_metadata = access_metadata.copy()
        with open(path, "rb") as file:
            access_metadata[MetadataKey.calculated_hash] = sha1(file.read()).hexdigest()
        if verify_hash:
            if (
                access_metadata[MetadataKey.calculated_hash]
                != access_metadata[MetadataKey.verified_hash]
            ):
                raise ValueError(
                    (
                        "calculated hash {calculated_hash} != "
                        "verified hash {verified_hash}"
                    ).format(**access_metadata)
                )
        self._accesses.append(
            {
                "type": access_type,
                "timestamp": datetime.now(),
                "call_metadata": call_metadata,
                "access_metadata": access_metadata,
            }
        )

    def open_for_read(self, **metadata) -> IOBase:
        """Return a file open for reading corresponding to the given metadata.

        The file contents are hashed, and a record is made of the read.
        """
        read_metadata = metadata.copy()
        self._read_overrides.apply(read_metadata)
        read_metadata = self._metadata_store.find(read_metadata) or read_metadata
        try:
            filename = Path(read_metadata[MetadataKey.filename])
        except KeyError:
            raise KeyError(f"could not find {MetadataKey.filename} in {read_metadata}")
        path = self.normalised_data_directory / filename
        self._record_access(
            "read",
            metadata,
            read_metadata,
            path,
            verify_hash=self.fail_on_hash_mismatch,
        )
        return open(path, mode="rb")

    def open_for_write(self, **metadata) -> IOBase:
        """Return a file open for update corresponding to the given metadata.

        When the file is closed the file contents are hashed, and a record is made of
        the write.
        """
        write_metadata = metadata.copy()
        self._write_overrides.apply(write_metadata)
        if MetadataKey.filename not in write_metadata:
            write_metadata[MetadataKey.filename] = str(
                Path(write_metadata[MetadataKey.data_product])
                / "{}.{}".format(self.run_id, write_metadata[MetadataKey.extension],)
            )

        path = self.normalised_data_directory / write_metadata[MetadataKey.filename]
        if path.exists():
            file = open(path, mode="r+b")
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            file = open(path, mode="w+b")
        # Wrap the file close method with something to record the file access.
        close_file = file.close

        @wraps(close_file)
        def close():
            file.flush()
            self._record_access("write", metadata, write_metadata, path, False)
            return close_file()

        file.close = close
        return file

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
