from io import IOBase
from typing import Optional
from data_pipeline_api.standard_api import StandardAPI


class DataProcessingAPI(StandardAPI):
    """The DataProcessingAPI extends the StandardAPI with access to external objects.
    """

    def read_external_object(
        self, doi_or_unique_name: str, title: str, component: Optional[str] = None,
    ) -> IOBase:
        """Return an open file handle to read the given external object.
        """
        kwds = dict(doi_or_unique_name=doi_or_unique_name, title=title)
        if component is not None:
            kwds["component"] = component
        return self.file_api.open_for_read(**kwds)
