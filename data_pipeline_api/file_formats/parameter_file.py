from io import TextIOBase
from enum import Enum
from numbers import Real
from typing import Union, Dict, Any, Tuple
import toml
import numpy as np
from scipy import stats

# ======================================================================================
# Common
# ======================================================================================

ParameterComponent = Dict[str, Any]
Estimate = Real
Distribution = Union[stats.rv_discrete, stats.rv_continuous]
Samples = np.ndarray


class ParameterType(Enum):
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


def read_type(file: TextIOBase, component: str) -> ParameterType:
    parameter = read_parameter(file, component)
    return ParameterType(parameter["type"])


# ======================================================================================
# Estimate
# ======================================================================================


def read_estimate(file: TextIOBase, component: str) -> Estimate:
    parameter = read_parameter(file, component)
    if ParameterType(parameter["type"]) is ParameterType.POINT_ESTIMATE:
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


class Categorical(stats._multivariate.multinomial_frozen):
    """A scipy-compatible categorical distribution, built on top of a multinomial.
    """

    def __init__(self, categories, p):
        super().__init__(n=1, p=p)
        self.categories = np.array(categories)

    def rvs(self, size=1, random_state=None):
        return self.categories[
            super().rvs(size=size, random_state=random_state).argmax(axis=-1)
        ]


def distribution_parameters(
    distribution: Distribution,
) -> Tuple[Tuple[float, ...], float, float]:
    """Extract the parameters from a scipy rv_frozen object, in the form
    ((shape, ...), loc, scale).
    """
    return distribution.dist._parse_args(*distribution.args, **distribution.kwds)


# Mapping between scipy and standard names.
distribution_name_mapping = {
    "gamma": "gamma",
    "norm": "normal",
    "uniform": "uniform",
    "poisson": "poisson",
    "expon": "exponential",
    "beta": "beta",
    "binom": "binomial",
}

# Functions to encode (scipy) distribution parameters.
distribution_parameter_encoders = {
    "gamma": lambda shapes, loc, scale: dict(k=shapes[0], theta=scale),
    "normal": lambda shapes, loc, scale: dict(mu=loc, sigma=scale),
    "uniform": lambda shapes, loc, scale: dict(a=loc, b=loc + scale),
    "poisson": lambda shapes, loc, scale: {"lambda": shapes[0]},
    "exponential": lambda shapes, loc, scale: {"lambda": 1 / scale},
    "beta": lambda shapes, loc, scale: dict(alpha=shapes[0], beta=shapes[1]),
    "binomial": lambda shapes, loc, scale: dict(n=shapes[0], p=shapes[1]),
}


def encode_distribution(distribution: Distribution) -> Dict[str, Any]:
    """Encode distribution into a serialisable format."""
    if isinstance(distribution, Categorical):
        name = "categorical"
        encoded_parameters = {
            "bins": [str(c) for c in distribution.categories],
            "weights": list(distribution.p),
        }
    elif isinstance(distribution, stats._multivariate.multinomial_frozen):
        name = "multinomial"
        encoded_parameters = {
            "n": distribution.n,
            "p": list(distribution.p),
        }
    elif isinstance(distribution, stats.distributions.rv_frozen):
        name = distribution_name_mapping[distribution.dist.name]
        encoded_parameters = distribution_parameter_encoders[name](
            *distribution_parameters(distribution)
        )
    else:
        raise ValueError(f"Do not have a codec for {distribution}")
    return dict(type="distribution", distribution=name, **encoded_parameters)


# Functions to decode serialised distributions.
distribution_decoders = {
    "categorical": lambda data: Categorical(data["bins"], data["weights"]),
    "gamma": lambda data: stats.gamma(data["k"], scale=data["theta"]),
    "normal": lambda data: stats.norm(data["mu"], data["sigma"]),
    "uniform": lambda data: stats.uniform(data["a"], data["b"] - data["a"]),
    "poisson": lambda data: stats.poisson(data["lambda"]),
    "exponential": lambda data: stats.expon(scale=1 / data["lambda"]),
    "beta": lambda data: stats.beta(data["alpha"], data["beta"]),
    "binomial": lambda data: stats.binom(data["n"], data["p"]),
    "multinomial": lambda data: stats.multinomial(data["n"], data["p"]),
}


def decode_distribution(encoded_distribution: Dict[str, Any]) -> Distribution:
    """Decode distribution from serialised format.
    """
    return distribution_decoders[encoded_distribution["distribution"]](
        encoded_distribution
    )


def read_distribution(file: TextIOBase, component: str) -> Distribution:
    parameter = read_parameter(file, component)
    if ParameterType(parameter["type"]) is ParameterType.DISTRIBUTION:
        return decode_distribution(parameter)
    raise ValueError(f"{parameter['type']} != 'distribution'")


def write_distribution(file: TextIOBase, component: str, distribution: Distribution):
    """Write distribution to file under component."""
    write_parameter(
        file, component, encode_distribution(distribution),
    )


# ======================================================================================
# Samples
# ======================================================================================


def read_samples(file: TextIOBase, component: str) -> Samples:
    parameter = read_parameter(file, component)
    if ParameterType(parameter["type"]) is ParameterType.SAMPLES:
        return np.array(parameter["samples"])
    else:
        raise ValueError(f"{parameter['type']} != 'samples'")


def write_samples(file: TextIOBase, component: str, samples: Samples):
    write_parameter(file, component, {"type": "samples", "samples": samples.tolist()})
