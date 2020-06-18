import pandas as pd
from pathlib import Path
from data_pipeline_api.csv_api import CsvAPI

"""
TODO

[X] Make things work just off the config file?
[X] Consider changing the config file to yaml.
[X] Implement version handling.
[ ] Work out the set of metadata keys.
[ ] Flesh out the standard interface.
"""

with CsvAPI(
    "repos/data_pipeline_api/examples/test_data_2/config.yaml",
    do_not_overwrite=False,
) as api:

    print(api.read_csv("human/estimate"))
    api.write_csv("human/estimatec", pd.DataFrame({"a": [1, 2], "b": [3, 4]}))
