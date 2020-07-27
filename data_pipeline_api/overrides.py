from logging import getLogger
from typing import Iterable, NamedTuple, Sequence, Tuple
from data_pipeline_api.metadata import Metadata, matches, log_format_metadata

logger = getLogger(__name__)


class Override(NamedTuple):
    """A metadata override.
    """
    where: Metadata
    use: Metadata


class Overrides:
    def __init__(self, overrides: Sequence[Tuple[Metadata, Metadata]]):
        """The Overrides class provides a simple metadata override mechanism.

        An Overrides object is initialised with a list of Metadata pairs, the first of
        each pair being a metadata template to match, and the second being some
        additional metadata to use as an override once a match has been found.
        """
        self._overrides = tuple(Override(*override) for override in overrides)

    def find(self, metadata: Metadata) -> Iterable[Override]:
        return filter(
            lambda override: matches(metadata, override.where), self._overrides,
        )

    def apply(self, metadata: Metadata):
        for override in self.find(metadata):
            logger.debug("applying override %s", log_format_metadata(override.use))
            metadata.update(override.use)
