from io import IOBase
from pathlib import Path
from typing import Union, Mapping
from functools import wraps
from hashlib import sha256
import yaml
import toml


Metadata = Mapping[str, str]
PathMetadata = Mapping[Path, Metadata]


class Key:
    quantity = "quantity"
    extension = "extension"
    format = "format"
    run_id = "run_id"


class FileAPI:
    def __init__(
        self,
        root: Union[Path, str],
        config_path: Union[Path, str],
        access_path: Union[Path, str],
    ):
        self._root = Path(root)
        self._config_path = config_path
        self._access_path = access_path
        self._accesses = []
        with open(self._root / ".metadata") as path_metadata_file:
            self._path_metadata = {
                Path(path): metadata
                for path, metadata in toml.load(path_metadata_file).items()
            }
        with open(self._config_path) as config_file:
            self._config = toml.load(config_file)

    @staticmethod
    def hexdigest(path: Path) -> str:
        with open(path, "rb") as file:
            return sha256(file.read()).hexdigest()

    def read(self, **metadata) -> IOBase:
        """Return a file open for reading corresponding to the given metadata.

        The file contents are hashed, and a record is made of the read.
        """
        results = self.find(metadata)
        if len(results) != 1:
            raise ValueError(f"{len(results)} results ({results}) for {metadata}")
        filename, matched_metadata = results.popitem()
        self._accesses.append(
            {
                "type": "read",
                "filename": str(filename),
                "hexdigest": self.hexdigest(filename),
                "requested_metadata": metadata,
                "read_metadata": matched_metadata,
            }
        )
        return open(filename, mode="rb")

    def write(self, **metadata) -> IOBase:
        """Return a file open for writing corresponding to the given metadata.

        When the file is closed the file contents are hashed, and a record is made of
        the write.
        """
        filename = self.path(metadata)
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
                    "hexdigest": self.hexdigest(filename),
                    "write_metadata": metadata,
                }
            )
            return close_file()

        file.close = close
        return file

    def write_access_file(self):
        with open(self._access_path, "w") as access_file:
            yaml.dump(
                {"path": str(self._root), "accesses": self._accesses},
                access_file,
                sort_keys=False,
            )

    def find(self, metadata: Metadata) -> PathMetadata:
        """Find PathMetadata corresponding to metadata.
        """
        transformed_metadata = self.transform(metadata)
        return self.filter(
            {
                self._root / path: metadata
                for path, metadata in self._path_metadata.items()
                if all(
                    key in metadata and metadata[key] == value
                    for key, value in transformed_metadata.items()
                )
            }
        )

    # ==================================================================================
    # Metadata transformation
    # ==================================================================================

    def augment_metadata_from_config(self, metadata: Metadata) -> Metadata:
        """Augment metadata by looking up the quantity key in our config file.
        """
        if Key.quantity in metadata:
            quantity = metadata[Key.quantity]
            if quantity in self._config:
                augmented_metadata = metadata.copy()
                augmented_metadata.update(self._config[quantity])
                return augmented_metadata
        return metadata

    format_extensions = {
        "csv": "csv",
        "parameter": "toml",
    }

    @staticmethod
    def add_extension(metadata: Metadata) -> Metadata:
        """Add an extension corresponding to the format, if present.
        """
        try:
            extension = FileAPI.format_extensions[metadata[Key.format]]
        except KeyError:
            return metadata
        else:
            augmented_metadata = metadata.copy()
            augmented_metadata[Key.extension] = extension
            return augmented_metadata

    def transform(self, metadata: Metadata) -> Metadata:
        metadata = self.augment_metadata_from_config(metadata)
        metadata = self.add_extension(metadata)
        return metadata

    # ==================================================================================
    # PathMetadata filtering
    # ==================================================================================

    def filter(self, path_metadata: PathMetadata) -> PathMetadata:
        # TODO : version handling
        return path_metadata

    # ==================================================================================
    # Path generation
    # ==================================================================================

    def path(self, metadata: Metadata) -> Path:
        metadata = self.transform(metadata)
        missing_keys = {
            Key.quantity,
            Key.run_id,
            Key.extension,
        } - metadata.keys()
        if missing_keys:
            raise ValueError(f"{metadata} is missing {missing_keys}")
        return (
            self._root
            / Path(metadata[Key.quantity])
            / "{run_id}.{extension}".format(**metadata)
        )
