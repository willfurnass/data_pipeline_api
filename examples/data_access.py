import pandas as pd
from pathlib import Path
from data_pipeline_api.file_api import FileAPI
from data_pipeline_api.standard_api import StandardAPI
from data_pipeline_api.csv_api import CsvAPI

data_path = Path("repos/data_pipeline_api/examples/test_data_2")
file_api = FileAPI(data_path, data_path / "config.toml", data_path / "access.yaml")

api = CsvAPI(file_api)
print(api.read_csv("human/estimate"))
api.write_csv("human/estimatec", pd.DataFrame({"a": [1, 2], "b": [3, 4]}))

api = StandardAPI(file_api)
print(api.read_estimate("human/estimate"))
api.write_estimate("human/estimateb", 0.5)

file_api.write_access_file()
