import pandas as pd
from pathlib import Path
from data_pipeline_api.simple_network_sim_api import SimpleNetworkSimAPI

"""
TODO

[X] Make things work just off the config file?
[X] Consider changing the config file to yaml.
[X] Implement version handling.
[ ] Add logging describing the process, to make it easier to debug failures.
[ ] Work out the set of metadata keys.
[ ] Flesh out the standard interface.
[ ] Add up-front verification, to make sure we are not about to start a run that we
    cannot complete.
"""

with SimpleNetworkSimAPI(
    "repos/data_pipeline_api/examples/test_data_2/config.yaml",
) as api:

    print(api.read_table("human/mixing-matrix"))
    api.write_table("human/estimatec", pd.DataFrame({"a": [1, 2], "b": [3, 4]}))
