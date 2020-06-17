import pandas as pd
from pathlib import Path
from data_pipeline_api.scrc_scheme import SCRCFilenameAPI
from data_pipeline_api.read_write_api import ReadWriteAPI
from data_pipeline_api.standard_api import StandardAPI
from data_pipeline_api.csv_api import CsvAPI

data_path = Path("repos/data_pipeline_api/examples/test_data_2")
filename_api = SCRCFilenameAPI(data_path, data_path / "config.toml")
read_write_api = ReadWriteAPI(data_path, filename_api)

api = CsvAPI(read_write_api)
print(api.read_csv("human/estimate"))
api.write_csv("human/estimatec", pd.DataFrame({"a": [1, 2], "b": [3, 4]}))

api = StandardAPI(read_write_api)
print(api.read_estimate("human/estimate"))
api.write_estimate("human/estimateb", 0.5)

read_write_api.write_access_file()
