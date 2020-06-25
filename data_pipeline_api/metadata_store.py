from typing import Sequence, Optional, NamedTuple
from operator import attrgetter
from semver import VersionInfo
from data_pipeline_api.metadata import Metadata, MetadataKey, is_superset


class MetadataRecord(NamedTuple):
    metadata: Metadata
    version: Optional[VersionInfo]


class MetadataStore:
    def __init__(self, metadata_sequence: Sequence[Metadata]):
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
            return max(
                filter(
                    lambda record: is_superset(record.metadata, metadata),
                    self._metadata_records,
                ),
                key=attrgetter("version"),
            ).metadata
        except ValueError:
            return None
