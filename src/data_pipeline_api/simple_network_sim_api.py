from io import TextIOWrapper
from pathlib import Path
from pandas import read_csv, DataFrame
from data_pipeline_api.file_api import FileAPI

class SimpleNetworkSimAPI(FileAPI):
    def read_table(self, data_product: str) -> DataFrame:
        with TextIOWrapper(self.read(data_product=data_product, extension="csv")) as csv_file:
            return read_csv(csv_file)

    def write_table(self, data_product: str, value: DataFrame):
        with TextIOWrapper(self.write(data_product=data_product, extension="csv")) as csv_file:
            value.to_csv(csv_file, index=False)
