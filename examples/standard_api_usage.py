import pandas as pd
from scipy.stats import gamma
from pathlib import Path
from data_pipeline_api.standard_api import StandardAPI

CONFIG_PATH = Path(__file__).parent.parent / "tests" / "data" / "config.yaml"

with StandardAPI(CONFIG_PATH) as api:
    print(api.read_estimate("parameter", "example-estimate"))
    print(api.read_estimate("parameter", "example-distribution"))
    print(api.read_estimate("parameter", "example-samples"))

    # print(api.read_distribution("parameter", "example-estimate"))  # expected to fail
    print(api.read_distribution("parameter", "example-distribution"))
    # print(api.read_distribution("parameter", "example-samples"))  # expected to fail
    
    print(api.read_sample("parameter", "example-estimate"))
    print(api.read_sample("parameter", "example-distribution"))
    print(api.read_sample("parameter", "example-samples"))
    
    print(api.read_table("object", "example-table"))
    print(api.read_array("object", "example-array"))
