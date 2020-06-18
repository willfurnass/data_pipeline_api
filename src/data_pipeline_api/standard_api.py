from io import TextIOWrapper
from pathlib import Path
import toml
from data_pipeline_api.file_api import FileAPI

from typing import Union, TypeVar, Generic, Dict
from scipy import stats


class StandardAPI(FileAPI):
    def read_estimate(self, quantity: str) -> float:
        with TextIOWrapper(
            self.read(quantity=quantity, format="parameter")
        ) as toml_file:
            return Estimate.read_parameter(toml.load(toml_file))

    def write_estimate(self, quantity: str, value: float):
        with TextIOWrapper(
            self.write(
                quantity=quantity, format="parameter", run_id=1, extension="toml"
            )
        ) as toml_file:
            toml.dump(Estimate.write_parameter(value), toml_file)


T = TypeVar("T")


class ParameterFile(Generic[T]):
    @classmethod
    def read_parameter(cls, data: Dict) -> T:
        if len(data.keys()) != 1:
            raise ValueError(
                f"parameter data has more than one key: {tuple(data.keys())}"
            )
        parameter_type, parameter_data = next(iter(data.items()))
        if parameter_type == "point-estimate":
            return cls._read_point_estimate(parameter_data)
        elif parameter_type == "distribution":
            return cls._read_distribution(parameter_data)
        else:
            raise ValueError(f"don't know how to parse a {parameter_type} parameter")

    @classmethod
    def write_parameter(cls, value: T) -> Dict:
        raise NotImplementedError

    @staticmethod
    def _parse_point_estimate(data) -> float:
        return float(data["value"])

    distribution_parsers = {
        "gamma": lambda data: stats.gamma(a=data["shape"], scale=data["scale"]),
    }

    @staticmethod
    def _parse_distribution(data) -> Union[stats.rv_discrete, stats.rv_continuous]:
        try:
            return ParameterFile.distribution_parsers[data["distribution"]](data)
        except KeyError:
            raise ValueError(
                f"don't know how to parse a {data['distribution']} distribution"
            )

    @classmethod
    def _read_point_estimate(cls, parameter_data) -> T:
        raise NotImplementedError

    @classmethod
    def _read_distribution(cls, parameter_data) -> T:
        raise NotImplementedError


class Estimate(ParameterFile[float]):
    @classmethod
    def write_parameter(cls, value: float) -> Dict:
        return {"point-estimate": {"value": value}}

    @classmethod
    def _read_point_estimate(cls, parameter_data):
        return cls._parse_point_estimate(parameter_data)

    @classmethod
    def _read_distribution(cls, parameter_data):
        return cls._parse_distribution(parameter_data).mean()


class Distribution(ParameterFile[Union[stats.rv_discrete, stats.rv_continuous]]):
    @classmethod
    def write_parameter(
        cls, value: Union[stats.rv_discrete, stats.rv_continuous]
    ) -> Dict:
        shape, loc, scale = value.dist._parse_args(*value.args, **value.kwds)
        return {
            "distribution": {
                "distribution": value.dist.name,
                "shape": shape[0],
                "scale": scale,
            }
        }

    @classmethod
    def _read_point_estimate(cls, parameter_data):
        raise ValueError("Don't know how to build a distribution from a point estimate")

    @classmethod
    def _read_distribution(cls, parameter_data):
        return cls._parse_distribution(parameter_data)
