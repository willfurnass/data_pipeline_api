from fnmatch import fnmatch
from typing import Mapping, Any

Metadata = Mapping[str, Any]



class MetadataKey:
    """Metadata key constants.
    """

    filename = "filename"
    data_product = "data_product"
    namespace = "namespace"
    component = "component"
    extension = "extension"
    run_id = "run_id"
    version = "version"
    verified_hash = "verified_hash"
    calculated_hash = "calculated_hash"
    accessibility = "accessibility"


def value_matches(value_a: Any, value_b: Any) -> bool:
    try:
        return fnmatch(value_a, value_b)
    except TypeError:
        return value_a == value_b


def matches(metadata: Metadata, pattern: Metadata) -> bool:
    """Return True if metadata matches pattern.

    A match occurs if metadata contains matching values for all keys in pattern. The
    values in pattern may be globs, in which case the corresponding key would be
    considered to match if the value in metadata matches the glob.
    """
    return all(
        key in metadata and value_matches(metadata[key], value)
        for key, value in pattern.items()
    )


def log_format_metadata(metadata):
    """Return a string representation of the metadata formatted for log output.
    """
    return ", ".join("{}={}".format(k, v) for k, v in metadata.items())
