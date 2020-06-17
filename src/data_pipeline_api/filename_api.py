"""
call + criteria + config + metadata -> filename

For writing, we can choose whether we want it to be overwritten or not.
"""

import toml
from typing import Iterator, NamedTuple, Mapping, Iterable, Dict, Optional, Callable
from pathlib import Path

Identifier = Mapping[str, str]
Metadata = Mapping[str, str]
Paths = Mapping[Path, Metadata]


class FilenameAPI:
    def __init__(self, path: Path):
        self._path = path
        with open(self._path / ".metadata") as metadata_file:
            self._metadata = {
                Path(path): metadata
                for path, metadata in toml.load(metadata_file).items()
            }
        with open(self._path / "config.toml") as config_file:
            self._config = toml.load(config_file)

    def find_matching(self, identifier: Identifier) -> Paths:
        return {
            path: metadata
            for path, metadata in self._metadata.items()
            if all(metadata.get(key) == value for key, value in identifier.items())
        }

    def match(self, identifier: Identifier) -> Paths:
        # 0. begin with the input
        # 1. use the config to augment our info
        identifier = self.augment(identifier)
        # 2. infer any other attributes we can (?)
        # 3. look up everything that matches (?)
        matching_filenames = self.find_matching(identifier)
        # 4. filter using additional criteria (?)
        # 5. if there is exactly one result, return it, otherwise fail
        return matching_filenames

    def augment(self, identifier: Identifier) -> Identifier:
        """Augment an identifier using the configuration file using the "quantity"
        attribute. Has no effect if "quantity" is not specified.
        """
        if "quantity" in identifier:
            quantity = identifier["quantity"]
            if quantity in self._config:
                augmented_identifier = identifier.copy()
                augmented_identifier.update(self._config[quantity])
                return augmented_identifier
        return identifier

    def standard_filename(self, identifier: Identifier) -> Path:
        if "quantity" not in identifier:
            raise ValueError(f"{identifier} has no quantity")
        return Path(identifier["quantity"]) / "{run_id}.{extension}".format(
            **identifier
        )

