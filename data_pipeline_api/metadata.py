from typing import Mapping, Optional
from pathlib import Path

Metadata = Mapping[str, str]

METADATA_FILENAME = "metadata.yaml"

class MetadataKey:
    filename = "filename"
    data_product = "data_product"
    component = "component"
    extension = "extension"
    run_id = "run_id"
    version = "version"
    verified_hash = "verified_hash"
    calculated_hash = "calculated_hash"


def is_superset(metadataA: Metadata, metadataB: Metadata) -> bool:
    """Return True if metadataA is a superset of metadataB.
    """
    return all(
        key in metadataA and metadataA[key] == value for key, value in metadataB.items()
    )
