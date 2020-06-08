import h5py
import toml
import numpy as np
import pandas as pd
import pickle
from io import BytesIO
from hashlib import sha256
from scipy import stats
from pathlib import Path
from functools import reduce
from operator import getitem
from dataclasses import dataclass
from typing import (
    BinaryIO,
    NamedTuple,
    Optional,
    List,
    Union,
    NoReturn,
    Any,
    Sequence,
    Callable,
)
from .parameter_file import Estimate, Distribution


Parameter = str
Component = str
Version = str
Hexdigest = str
HexdigestFunction = Callable[[BinaryIO], Hexdigest]


class ParameterRead(NamedTuple):
    parameter: Parameter
    requested_version: Optional[Version]
    read_version: Version
    component: Optional[Component]
    file_hexdigest: Hexdigest
    verified_hexdigest: Optional[Hexdigest]


class ParameterWrite(NamedTuple):
    parameter: Parameter
    component: Optional[Component]
    version: Version
    file_hexdigest: Hexdigest


@dataclass
class ParameterVersion:
    parameter: Parameter
    version: Version

    def read_bytes(self, filename: str) -> bytes:
        raise NotImplementedError

    def write_bytes(self, filename: str, b: bytes) -> NoReturn:
        raise NotImplementedError

    def get_verified_hexdigest(self) -> Optional[Hexdigest]:
        raise NotImplementedError


class DataAccess:
    def get_parameter_version(
        self, parameter: Parameter, version: Optional[Version] = None
    ) -> ParameterVersion:
        raise NotImplementedError

    def write_output_metadata(
        self, reads: Sequence[ParameterRead], writes: Sequence[ParameterWrite]
    ) -> NoReturn:
        raise NotImplementedError


class API:
    def __init__(self, data_access: DataAccess, raise_on_hash_mismatch: bool = False):
        self._data_access = data_access
        self._raise_on_hash_mismatch = raise_on_hash_mismatch
        self._reads: List[ParameterRead] = []
        self._writes: List[ParameterWrite] = []

    def _read_parameter_version_bytes(
        self,
        filename: str,
        parameter: Parameter,
        component: Optional[Component] = None,
        version: Optional[Version] = None,
    ) -> bytes:
        parameter_version = self._data_access.get_parameter_version(parameter, version)
        file_hexdigest = self.get_hexdigest(parameter_version.read_bytes(filename))
        verified_hexdigest = parameter_version.get_verified_hexdigest()
        if self._raise_on_hash_mismatch and file_hexdigest != verified_hexdigest:
            raise ValueError(
                f"file hash for {parameter}.{version} does not match the "
                f"verified hash ({file_hexdigest} != {verified_hexdigest})"
            )
        self._reads.append(
            ParameterRead(
                parameter=parameter,
                requested_version=version,
                read_version=parameter_version.version,
                component=component,
                file_hexdigest=file_hexdigest,
                verified_hexdigest=verified_hexdigest,
            )
        )
        return parameter_version.read_bytes(filename)

    def read_estimate(
        self,
        parameter: Parameter,
        component: Optional[Component] = None,
        version: Optional[Version] = None,
    ) -> Union[float, stats.rv_discrete, stats.rv_continuous]:
        return Estimate.read_parameter(
            toml.loads(
                self._read_parameter_version_bytes(
                    "data.toml", parameter, component, version
                ).decode()
            )
        )

    def read_distribution(
        self,
        parameter: Parameter,
        component: Optional[Component] = None,
        version: Optional[Version] = None,
    ) -> Union[stats.rv_discrete, stats.rv_continuous]:
        return Distribution.read_parameter(
            toml.loads(
                self._read_parameter_version_bytes(
                    "data.toml", parameter, component, version
                ).decode()
            )
        )

    def read_matrix(
        self,
        parameter: Parameter,
        component: Optional[Component] = None,
        version: Optional[Version] = None,
    ) -> np.ndarray:
        dataset_root: h5py.File = h5py.File(
            BytesIO(
                self._read_parameter_version_bytes(
                    "data.h5", parameter, component, version
                )
            )
        )
        # FIXME : We should be getting the component metadata in a more structured way.
        dataset_data = reduce(getitem, component.split("/"), dataset_root)
        return dataset_data[()]

    def read_table(
        self,
        parameter: Parameter,
        component: Optional[Component] = None,
        version: Optional[Version] = None,
    ) -> pd.DataFrame:
        return pd.read_csv(
            BytesIO(
                self._read_parameter_version_bytes(
                    "data.csv", parameter, component, version
                )
            )
        )

    def _write_parameter_version_bytes(
        self,
        filename: str,
        data: bytes,
        parameter: Parameter,
        component: Optional[Component] = None,
        version: Optional[Version] = None,
    ) -> NoReturn:
        parameter_version = self._data_access.get_parameter_version(parameter, version)
        parameter_version.write_bytes(filename, data)
        file_hexdigest = self.get_hexdigest(parameter_version.read_bytes(filename))
        self._writes.append(
            ParameterWrite(
                parameter=parameter,
                component=component,
                version=version,
                file_hexdigest=file_hexdigest,
            )
        )

    def write_estimate(
        self,
        estimate: float,
        parameter: Parameter,
        component: Optional[Component] = None,
        version: Optional[Version] = None,
    ) -> NoReturn:
        self._write_parameter_version_bytes(
            "data.toml",
            toml.dumps(Estimate.write_parameter(estimate)).encode(),
            parameter,
            component,
            version,
        )

    def write_distribution(
        self,
        distribution: Union[stats.rv_discrete, stats.rv_continuous],
        parameter: Parameter,
        component: Optional[Component] = None,
        version: Optional[Version] = None,
    ) -> NoReturn:
        self._write_parameter_version_bytes(
            "data.toml",
            toml.dumps(Distribution.write_parameter(distribution)).encode(),
            parameter,
            component,
            version,
        )

    def write_array(
        self,
        array: np.ndarray,
        parameter: Parameter,
        component: Optional[Component] = None,
        version: Optional[Version] = None,
    ) -> NoReturn:
        raise NotImplementedError

    def write_table(
        self,
        table: pd.DataFrame,
        parameter: Parameter,
        component: Optional[Component] = None,
        version: Optional[Version] = None,
    ) -> NoReturn:
        self._write_parameter_version_bytes(
            "data.csv",
            table.to_csv(index=False).encode(),
            parameter,
            component,
            version,
        )

    def close(self) -> NoReturn:
        self._data_access.write_output_metadata(self._reads, self._writes)

    def get_hexdigest(self, b: bytes) -> Hexdigest:
        return sha256(b).hexdigest()
