from io import TextIOWrapper
from pathlib import Path
import toml
from .read_write_api import ReadWriteAPI
from .parameter_file import Estimate, Distribution

class StandardAPI:
    def __init__(self, read_write_api: ReadWriteAPI):
        self._read_write_api = read_write_api

    def read_estimate(self, quantity: str) -> float:
        with TextIOWrapper(self._read_write_api.read(quantity=quantity, format="parameter")) as toml_file:
            return Estimate.read_parameter(toml.load(toml_file))

    def write_estimate(self, quantity: str, value: float):
        with TextIOWrapper(self._read_write_api.write(quantity=quantity, format="parameter", run_id=1, extension="toml")) as toml_file:
            toml.dump(Estimate.write_parameter(value), toml_file)
