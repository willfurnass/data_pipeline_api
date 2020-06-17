from io import TextIOWrapper
from pathlib import Path
from pandas import read_csv, DataFrame
from data_pipeline_api.file_api import FileAPI

class CsvAPI:
    def __init__(self, file_api: FileAPI):
        self._file_api = file_api

    def read_csv(self, quantity: str) -> DataFrame:
        with TextIOWrapper(self._file_api.read(quantity=quantity, format="csv")) as csv_file:
            return read_csv(csv_file)

    def write_csv(self, quantity: str, value: DataFrame):
        with TextIOWrapper(self._file_api.write(quantity=quantity, run_id=1, extension="csv")) as csv_file:
            value.to_csv(csv_file, index=False)
