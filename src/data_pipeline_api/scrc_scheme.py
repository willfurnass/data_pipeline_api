import toml
from pathlib import Path
from typing import Mapping
from data_pipeline_api.filename_api import FilenameAPI, Metadata, PathMetadata

Quantity = str
QuantityMetadata = Mapping[Quantity, Metadata]


class SCRCKey:
    quantity = "quantity"
    extension = "extension"
    format = "format"
    run_id = "run_id"


def metadata_path(metadata: Metadata) -> Path:
    """Generate a standard Path from the given metadata.
    """
    if SCRCKey.quantity not in metadata:
        raise ValueError(f"{metadata} has no quantity")
    return Path(metadata[SCRCKey.quantity]) / "{run_id}.{extension}".format(**metadata)


class SCRCFilenameAPI(FilenameAPI):
    def __init__(self, data_path: Path, config_path: Path):
        super().__init__(data_path)
        with open(config_path) as config_file:
            self._config = toml.load(config_file)

    # ==================================================================================
    # Metadata transformation
    # ==================================================================================

    def augment_metadata_from_config(self, metadata: Metadata) -> Metadata:
        """Augment metadata by looking up the quantity key in our config file.
        """
        if SCRCKey.quantity in metadata:
            quantity = metadata[SCRCKey.quantity]
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
            extension = SCRCFilenameAPI.format_extensions[metadata[SCRCKey.format]]
        except KeyError:
            return metadata
        else:
            augmented_metadata = metadata.copy()
            augmented_metadata[SCRCKey.extension] = extension
            return augmented_metadata

    def transform(self, metadata: Metadata) -> Metadata:
        metadata = self.augment_metadata_from_config(metadata)
        metadata = self.add_extension(metadata)
        return metadata

    # ==================================================================================
    # PathMetadata filtering
    # ==================================================================================

    def filter(self, path_metadata: PathMetadata) -> PathMetadata:
        return super().filter(path_metadata)

    # ==================================================================================
    # Path generation
    # ==================================================================================

    def path(self, metadata: Metadata) -> Path:
        metadata = self.transform(metadata)
        missing_keys = {
            SCRCKey.quantity,
            SCRCKey.run_id,
            SCRCKey.extension,
        } - metadata.keys()
        if missing_keys:
            raise ValueError(f"{metadata} is missing {missing_keys}")
        return (
            self._data_path
            / Path(metadata[SCRCKey.quantity])
            / "{run_id}.{extension}".format(**metadata)
        )
