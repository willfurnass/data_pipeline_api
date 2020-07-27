from io import IOBase
from data_pipeline_api.standard_api import StandardAPI


class DataProcessingAPI(StandardAPI):
    """The DataProcessingAPI extends the StandardAPI with access to external objects.
    """

    def read_external_object(self, doi_or_unique_name: str, component: str) -> IOBase:
        """Return an open file handle to read the given external object.
        """
        return self.file_api.open_for_read(
            doi_or_unique_name=doi_or_unique_name, component=component
        )
