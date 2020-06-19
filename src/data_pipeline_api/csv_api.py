from io import TextIOWrapper
from pathlib import Path
from pandas import read_csv, DataFrame
from data_pipeline_api.file_api import FileAPI

class CsvAPI(FileAPI):
    def read_csv(self, quantity: str) -> DataFrame:
        with TextIOWrapper(self.read(quantity=quantity, extension="csv")) as csv_file:
            return read_csv(csv_file)

    def write_csv(self, quantity: str, value: DataFrame):
        with TextIOWrapper(self.write(quantity=quantity, extension="csv")) as csv_file:
            value.to_csv(csv_file, index=False)
