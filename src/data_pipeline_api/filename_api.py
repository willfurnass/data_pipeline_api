"""
call + criteria + config + metadata -> filename

For writing, we can choose whether we want it to be overwritten or not.
"""

import toml
from typing import Iterator, NamedTuple, Mapping, Iterable, Dict, Optional, Callable
from pathlib import Path


Metadata = Mapping[str, str]
PathMetadata = Mapping[Path, Metadata]


class FilenameAPI:
    """Load a toml-formatted file containing path metadata.

    The format is of the toml file is expected to be sections of the form:

        [path]
        key = value
        ...
    
    Where the metadata is defined by the (key, value) pairs.
    """

    def __init__(self, data_path: Path):
        self._data_path = data_path
        with open(self._data_path / ".metadata") as path_metadata_file:
            self._path_metadata = {
                Path(path): metadata
                for path, metadata in toml.load(path_metadata_file).items()
            }

    def find(self, metadata: Metadata) -> PathMetadata:
        """Find PathMetadata corresponding to metadata.
        """
        transformed_metadata = self.transform(metadata)
        return self.filter(
            {
                self._data_path / path: metadata
                for path, metadata in self._path_metadata.items()
                if all(
                    key in metadata and metadata[key] == value
                    for key, value in transformed_metadata.items()
                )
            }
        )

    def path(self, metadata: Metadata) -> Path:
        """Generate a standard Path for metadata.
        """
        raise NotImplementedError

    def transform(self, metadata: Metadata) -> Metadata:
        """Transform metadata.
        """
        # FilenameAPI does no metadata transformation.
        return metadata

    def filter(self, path_metadata: PathMetadata) -> PathMetadata:
        """Filter path_metadata.
        """
        # FilenameAPI does no path_metadata filtering.
        return path_metadata

