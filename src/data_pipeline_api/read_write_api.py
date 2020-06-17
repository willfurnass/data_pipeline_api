from io import IOBase
from pathlib import Path
from typing import Union
from functools import wraps
from hashlib import sha256
import yaml
from .filename_api import FilenameAPI, Metadata


def hexdigest(path: Path) -> str:
    with open(path, "rb") as file:
        return sha256(file.read()).hexdigest()


class ReadWriteAPI:
    def __init__(self, path: Path, filename_api: FilenameAPI):
        self._path = Path(path)
        self._filename_api = filename_api
        self._accesses = []

    def read(self, **metadata) -> IOBase:
        results = self._filename_api.find(metadata)
        if len(results) != 1:
            raise ValueError(f"{len(results)} results ({results}) for {metadata}")
        filename, matched_metadata = results.popitem()
        self._accesses.append(
            {
                "type": "read",
                "filename": str(filename),
                "hexdigest": hexdigest(filename),
                "requested_metadata": metadata,
                "read_metadata": matched_metadata,
            }
        )
        return open(filename, mode="rb")

    def write(self, **metadata) -> IOBase:
        filename = self._filename_api.path(metadata)
        if not filename.parent.exists():
            filename.parent.mkdir(parents=True)
        file = open(filename, mode="wb")
        # Wrap the file close method with something to record the file access.
        close_file = file.close

        @wraps(close_file)
        def close():
            file.flush()
            self._accesses.append(
                {
                    "type": "write",
                    "filename": str(filename),
                    "hexdigest": hexdigest(filename),
                    "write_metadata": metadata,
                }
            )
            return close_file()

        file.close = close
        return file

    def write_access_file(self, access_filename="access.yaml"):
        with open(self._path / access_filename, "w") as access_file:
            yaml.dump(
                {"path": str(self._path), "accesses": self._accesses},
                access_file,
                sort_keys=False,
            )
