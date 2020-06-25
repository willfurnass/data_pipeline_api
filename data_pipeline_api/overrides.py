from typing import Iterable, NamedTuple, Sequence, Tuple
from data_pipeline_api.metadata import Metadata, is_superset


class Override(NamedTuple):
    where: Metadata
    use: Metadata


class Overrides:
    def __init__(self, overrides: Sequence[Tuple[Metadata, Metadata]]):
        self._overrides = tuple(Override(*override) for override in overrides)

    def find(self, metadata: Metadata) -> Iterable[Override]:
        return filter(
            lambda override: is_superset(metadata, override.where), self._overrides,
        )

    def apply(self, metadata: Metadata):
        for override in self.find(metadata):
            metadata.update(override.use)
