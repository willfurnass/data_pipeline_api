import pandas as pd
from data_pipeline_api.read_write_api import ReadWriteAPI
from data_pipeline_api.standard_api import StandardAPI
from data_pipeline_api.csv_api import CsvAPI

read_write_api = ReadWriteAPI("test_data_2")

api = CsvAPI(read_write_api)
print(api.read_csv("human/estimate"))
api.write_csv("human/estimatec", pd.DataFrame({"a": [1, 2], "b": [3, 4]}))

api = StandardAPI(read_write_api)
print(api.read_estimate("human/estimate"))
# api.write_estimate("human/estimateb", 0.3)

read_write_api.write_access_file()
