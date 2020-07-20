from logging import getLogger
from typing import Sequence, Optional, NamedTuple
from operator import attrgetter
from semver import VersionInfo
from data_pipeline_api.metadata import (
    Metadata,
    MetadataKey,
    matches,
    log_format_metadata,
)

logger = getLogger(__name__)


class MetadataRecord(NamedTuple):
    """A versioned Metadata object.
    """
    metadata: Metadata
    version: Optional[VersionInfo]


class MetadataStore:
    def __init__(self, metadata_sequence: Sequence[Metadata]):
        """The MetadataStore class provides a simple metadata lookup mechanism.

        A MetadataStore is initialised with a Sequence of Metadata objects, which can
        then be searched by matching against Metadata fragments.
        """
        try:
            self._metadata_records = tuple(
                MetadataRecord(
                    metadata=metadata,
                    version=VersionInfo.parse(metadata[MetadataKey.version])
                    if MetadataKey.version in metadata
                    else None,
                )
                for metadata in metadata_sequence
            )
        except Exception as exception:
            raise ValueError("invalid metadata") from exception

    def find(self, metadata: Metadata) -> Optional[Metadata]:
        try:
            results = tuple(
                filter(
                    lambda record: matches(record.metadata, metadata),
                    self._metadata_records,
                )
            )
            for result in results:
                logger.debug(
                    "found matching metadata %s", log_format_metadata(result.metadata)
                )
            selected = max(results, key=attrgetter("version"),).metadata
            logger.debug("selected metadata %s", log_format_metadata(selected))
            return selected
        except ValueError:
            logger.debug("could not find any matching metadata")
            return None
