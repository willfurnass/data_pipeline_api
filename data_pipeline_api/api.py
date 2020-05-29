import toml
import h5py
import numpy as np
from operator import getitem
from io import IOBase, TextIOWrapper
from dataclasses import dataclass
from hashlib import sha256
from enum import Enum
from pathlib import Path
from functools import reduce
import scipy.stats as stats
from typing import (
    NoReturn,
    Optional,
    Any,
    Dict,
    List,
    Sequence,
    NamedTuple,
    Union,
    Mapping,
    BinaryIO,
)


class DataType(Enum):
    parameter = "parameter"
    dataset = "dataset"

    # TODO : Can't decide whether I like this here or not.
    def parse_location(self, parameter: str, metadata: Dict[str, Any]) -> str:
        if self is DataType.parameter:
            return f"{parameter}/{metadata['location']}"
        elif self is DataType.dataset:
            return metadata["path"]
        else:
            raise ValueError(f"don't know how to parse a {self.value} data type")


class ParamType(Enum):
    point_estimate = "point-estimate"
    distribution = "distribution"

    def parse_param(
        self, data: Mapping[str, Any]
    ) -> Union[float, stats.rv_discrete, stats.rv_continuous]:
        """Extract a parameter value from data according to the value of self."""
        if self is ParamType.point_estimate:
            return float(data["value"])
        elif self is ParamType.distribution:
            if data["distribution"] == "gamma":
                return stats.gamma(a=data["shape"], scale=data["scale"])
            else:
                raise ValueError(f"unrecognised distribution {data['distribution']}")
        else:
            raise ValueError(f"don't know how to parse a {self.value} parameter type")


class ParameterMetadata(NamedTuple):
    data_type: DataType
    location: str
    hexdigest: str


class ParameterRead(NamedTuple):
    parameter: str
    component: str
    version: str
    verified: bool


class DataLayer:
    def __init__(self, path: str):
        self._path = Path(path).resolve()

    def get_parameter_metadata(self, parameter: str) -> ParameterMetadata:
        path = self._path / parameter / "metadata" / "public"
        metadata_filename = sorted(path.glob("*.toml"))[-1]
        with open(metadata_filename) as metadata_file:
            parameter_metadata = toml.load(metadata_file)["source"]
            data_type = DataType(parameter_metadata["type"])
            return ParameterMetadata(
                data_type=data_type,
                # FIXME : There are currently different schemes for locating metadata and parameter files.
                location=data_type.parse_location(parameter, parameter_metadata),
                hexdigest=parameter_metadata["hash"],
            )

    def get_data_file(self, metadata: ParameterMetadata) -> BinaryIO:
        return open(self._path / metadata.location, mode="rb")


class Session:
    def __init__(self, data_layer: DataLayer):
        self._data_layer = data_layer
        self._reads: List[ParameterRead] = []

    def get_hexdigest(self, metadata: ParameterMetadata) -> str:
        message = sha256()
        with self._data_layer.get_data_file(metadata) as data_file:
            message.update(data_file.read())
        return message.hexdigest()

    def is_data_verified(self, metadata: ParameterMetadata) -> bool:
        return self.get_hexdigest(metadata) == metadata.hexdigest

    def read_param(
        self,
        parameter: str,
        component: Optional[str] = None,
        units: Optional[str] = None,
        version: Optional[str] = None,
        access: Optional[str] = None,
    ) -> Union[float, stats.rv_discrete, stats.rv_continuous]:
        metadata: ParameterMetadata = self._data_layer.get_parameter_metadata(parameter)
        if metadata.data_type is not DataType.parameter:
            raise ValueError(f"{parameter} is a {metadata.type} not a param")

        param_data = toml.load(TextIOWrapper(self._data_layer.get_data_file(metadata)))
        # TODO : I think we only accept data with one key, but I'm not sure.
        if len(param_data.keys()) != 1:
            raise ValueError(
                f"parameter data has more than one key: {tuple(param_data.keys())}"
            )
        param_type = ParamType(next(iter(param_data.keys())))
        value = param_type.parse_param(param_data[param_type.value])
        # TODO : Caching?
        # TODO : Unit conversion.

        self._reads.append(
            ParameterRead(
                parameter=parameter,
                component=component,
                version=version,
                verified=self.is_data_verified(metadata),
            )
        )
        return value

    def read_matrix(
        self,
        parameter: str,
        component: Optional[str] = None,
        units: Optional[str] = None,
        version: Optional[str] = None,
        access: Optional[str] = None,
    ) -> np.ndarray:
        metadata: ParameterMetadata = self._data_layer.get_parameter_metadata(parameter)
        if metadata.data_type is not DataType.dataset:
            raise ValueError(f"{parameter} is a {metadata.data_type} not a dataset")
        dataset_root: h5py.File = h5py.File(
            self._data_layer.get_data_file(metadata), "r"
        )
        # FIXME : We should be getting the component metadata in a more structured way.
        dataset_data = reduce(getitem, component.split("/"), dataset_root)
        # TODO : Caching?
        # TODO : Unit conversion.

        self._reads.append(
            ParameterRead(
                parameter=parameter,
                component=component,
                version=version,
                verified=self.is_data_verified(metadata),
            )
        )
        return dataset_data[()]

    def log_reads(self, filename: Optional[str] = None) -> List[ParameterRead]:
        # TODO : This should actually write out a JSON file documenting all of our reads.
        return self._reads
