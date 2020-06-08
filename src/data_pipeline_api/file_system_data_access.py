from dataclasses import dataclass
from typing import Optional, BinaryIO, Any, Sequence, NoReturn
from pathlib import Path
import toml
import pickle
from data_pipeline_api.api import (
    ParameterVersion,
    Hexdigest,
    Parameter,
    Version,
    ParameterRead,
    ParameterWrite,
    DataAccess,
)


@dataclass
class FileSystemParameterVersion(ParameterVersion):
    path: Path
    verified_hexdigest: Optional[Hexdigest]

    def read_bytes(self, filename: str) -> bytes:
        if not self.path.exists():
            raise ValueError(f"Cannot find {self.path}")
        with open(self.path / filename, "rb") as f:
            return f.read()

    def write_bytes(self, filename: str, b: bytes) -> NoReturn:
        if not self.path.exists():
            self.path.mkdir(parents=True)
        with open(self.path / filename, "wb") as f:
            f.write(b)

    def get_verified_hexdigest(self) -> Optional[Hexdigest]:
        return self.verified_hexdigest


class FileSystemDataAccess(DataAccess):
    """Access data directly from the filesystem.

    Assumes that we can find a directory structure at `data_root` like the following:
    {data_root}/.known_hashes.toml
        Containing known hashes and their corresponding parameter/version
    {data_root}/{parameter parts}/.../{version}/data.[toml|h5]
    """

    def __init__(self, data_root: str, metadata_filename: str):
        super().__init__()
        self._data_root = Path(data_root)
        self._metadata_filename = metadata_filename

        self._parameter_version_hashes = {}
        known_hashes_path = self._data_root / ".known_hashes.toml"
        if known_hashes_path.exists() and known_hashes_path.is_file():
            with open(known_hashes_path) as known_hashes_file:
                known_hashes = toml.load(known_hashes_file)
                self._parameter_version_hashes.update(
                    {
                        (value["parameter"], value["version"]): key
                        for key, value in known_hashes.items()
                    }
                )

    def get_parameter_version(
        self, parameter: Parameter, version: Optional[Version] = None
    ) -> FileSystemParameterVersion:
        parameter_path = self._data_root / parameter
        if version is None:
            # FIXME : This is terrible.
            versions = []
            for p in parameter_path.iterdir():
                try:
                    versions.append(int(p.name))
                except ValueError:
                    pass
            version = max(versions)

        known_hash = self._parameter_version_hashes.get((parameter, version))
        version_path = parameter_path / str(version)
        return FileSystemParameterVersion(parameter, version, version_path, known_hash)

    def write_output_metadata(
        self, reads: Sequence[ParameterRead], writes: Sequence[ParameterWrite]
    ) -> NoReturn:
        with open(self._metadata_filename, mode="w") as metadata_file:
            toml.dump(
                {
                    "read": {str(i): read._asdict() for i, read in enumerate(reads)},
                    "write": {
                        str(i): write._asdict() for i, write in enumerate(writes)
                    },
                },
                metadata_file,
            )
