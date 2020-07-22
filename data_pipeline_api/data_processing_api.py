"""
If we are working with data products then in principle we should restrict ourselves to
the standard API.
Build a description of what we want.

inputs = {"input1": {"type": "samples", "data_product": "foo", "component": "bar"}}

Download what we want.
Execute our script.
Generate outputs that we want.

The overall framework should remain fairly constant. Whatever happens we want to:
1. download stuff
2. execute the script
3. upload stuff

It is possible that the download will be with non-standard API things. The upload should
always be with standard API things?

Input could be a mapping of keys metadata dicts, which we then turn into a dict of
appropriate objects.
"""

from io import IOBase
from data_pipeline_api.standard_api import StandardAPI


class DataProcessingAPI(StandardAPI):
    """The DataProcessingAPI extends the StandardAPI with access to external objects.
    """
    def read_external_object(self, doi_or_unique_name: str) -> IOBase:
        return self.file_api.open_for_read(doi_or_unique_name=doi_or_unique_name)
