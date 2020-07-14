from typing import Mapping

Metadata = Mapping[str, str]

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


def log_format_metadata(metadata):
    """Return a string representation of the metadata formatted for log output.
    """
    return ", ".join("{}={}".format(k, v) for k, v in metadata.items())
