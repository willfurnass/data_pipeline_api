import toml
from io import TextIOBase
from enum import Enum
from numbers import Real
from typing import Union, Sequence, Dict, Any
import numpy as np
from scipy import stats

# ======================================================================================
# Common
# ======================================================================================

ParameterComponent = Dict[str, Any]
Estimate = Real
Distribution = Union[stats.rv_discrete, stats.rv_continuous]
Samples = np.ndarray


class Type(Enum):
    POINT_ESTIMATE = "point-estimate"
    DISTRIBUTION = "distribution"
    SAMPLES = "samples"


def read_parameter(file: TextIOBase, component: str) -> ParameterComponent:
    file.seek(0)
    return toml.load(file)[component]


def write_parameter(file: TextIOBase, component: str, parameter: ParameterComponent):
    parameter_data = toml.load(file)
    parameter_data[component] = parameter
    file.seek(0)
    file.truncate()
    toml.dump(parameter_data, file)


def read_type(file: TextIOBase, component: str) -> Type:
    parameter = read_parameter(file, component)
    return Type(parameter["type"])


# ======================================================================================
# Estimate
# ======================================================================================


def read_estimate(file: TextIOBase, component: str) -> Estimate:
    parameter = read_parameter(file, component)
    if Type(parameter["type"]) is Type.POINT_ESTIMATE:
        # TODO : validate
        return parameter["value"]
    else:
        raise ValueError(f"{parameter['type']} != 'point-estimate'")


def write_estimate(file: TextIOBase, component: str, estimate: Estimate):
    write_parameter(
        file, component, {"type": "point-estimate", "value": float(estimate)}
    )


# ======================================================================================
# Distribution
# ======================================================================================

distribution_parsers = {
    "gamma": lambda data: stats.gamma(a=data["shape"], scale=data["scale"]),
    "norm": lambda data: stats.norm(loc=data["loc"], scale=data["scale"]),
}


def read_distribution(file: TextIOBase, component: str) -> Distribution:
    parameter = read_parameter(file, component)
    if Type(parameter["type"]) is Type.DISTRIBUTION:
        # TODO : validate
        return distribution_parsers[parameter["distribution"]](parameter)
    else:
        raise ValueError(f"{parameter['type']} != 'distribution'")


def write_distribution(file: TextIOBase, component: str, distribution: Distribution):
    shape, loc, scale = distribution.dist._parse_args(
        *distribution.args, **distribution.kwds
    )
    parameter = {
        "type": "distribution",
        "distribution": distribution.dist.name,
    }
    if loc:
        parameter["loc"] = loc
    if shape:
        parameter["shape"] = shape[0]
    if scale:
        parameter["scale"] = scale
    write_parameter(
        file, component, parameter,
    )


# ======================================================================================
# Samples
# ======================================================================================


def read_samples(file: TextIOBase, component: str) -> Samples:
    parameter = read_parameter(file, component)
    if Type(parameter["type"]) is Type.SAMPLES:
        return np.array(parameter["samples"])
    else:
        raise ValueError(f"{parameter['type']} != 'samples'")


def write_samples(file: TextIOBase, component: str, samples: Samples):
    write_parameter(file, component, {"type": "samples", "samples": samples.tolist()})
