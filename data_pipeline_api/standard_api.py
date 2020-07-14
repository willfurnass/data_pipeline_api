from io import TextIOWrapper
from pathlib import Path
from contextlib import contextmanager
from typing import Union
import numpy as np
from data_pipeline_api.file_api import FileAPI
from data_pipeline_api.file_formats.parameter_file import (
    Type,
    Estimate,
    Distribution,
    Samples,
    read_type,
    read_estimate,
    read_distribution,
    read_samples,
    write_estimate,
    write_distribution,
    write_samples,
)
from data_pipeline_api.file_formats.object_file import (
    Array,
    Table,
    read_array,
    read_table,
    write_array,
    write_table,
)


class StandardAPI:
    """The StandardAPI class provides access to data products conforming to the Standard
    API specification.
    """
    def __init__(self, config_filename: Union[Path, str], uri: str, git_sha: str):
        self.file_api = FileAPI(config_filename)
        self.file_api.set_metadata("uri", uri)
        self.file_api.set_metadata("git_sha", git_sha)

    def __enter__(self):
        self.file_api.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.file_api.__exit__(exc_type, exc_value, traceback)

    # ==================================================================================
    # Parameter files
    # ==================================================================================

    @contextmanager
    def open_parameter_file_for_read(self, data_product: str, component: str):
        """Open a parameter file for reading.
        """
        with TextIOWrapper(
            self.file_api.open_for_read(data_product=data_product, component=component)
        ) as parameter_file:
            yield parameter_file

    @contextmanager
    def open_parameter_file_for_write(self, data_product: str, component: str):
        """Open a parameter file for writing.
        """
        with TextIOWrapper(
            self.file_api.open_for_write(
                data_product=data_product, component=component, extension="toml"
            )
        ) as parameter_file:
            yield parameter_file

    # ----------------------------------------------------------------------------------
    # Estimate
    # ----------------------------------------------------------------------------------

    def read_estimate(self, data_product: str, component: str) -> Estimate:
        """Read an estimate from the data product component.
        """
        with self.open_parameter_file_for_read(data_product, component) as file:
            parameter_type = read_type(file, component)
            if parameter_type is Type.POINT_ESTIMATE:
                return read_estimate(file, component)
            if parameter_type is Type.DISTRIBUTION:
                return read_distribution(file, component).mean()
            if parameter_type is Type.SAMPLES:
                return read_samples(file, component).mean()
            raise ValueError(f"unrecognised type {parameter_type}")

    def write_estimate(self, data_product: str, component: str, estimate: Estimate):
        """Write an estimate to the data product component.
        """
        with self.open_parameter_file_for_write(data_product, component) as file:
            write_estimate(file, component, estimate)

    # ----------------------------------------------------------------------------------
    # Distribution
    # ----------------------------------------------------------------------------------

    def read_distribution(self, data_product: str, component: str) -> Distribution:
        """Read a distribution from the data product component.
        """
        with self.open_parameter_file_for_read(data_product, component) as file:
            parameter_type = read_type(file, component)
            if parameter_type is Type.POINT_ESTIMATE:
                raise ValueError("point-estimate cannot be read as a distribution")
            if parameter_type is Type.DISTRIBUTION:
                return read_distribution(file, component)
            if parameter_type is Type.SAMPLES:
                raise ValueError("samples cannot be read as a distribution")
            raise ValueError(f"unrecognised type {parameter_type}")

    def write_distribution(
        self, data_product: str, component: str, distribution: Distribution,
    ):
        """Write a distribution to the data product component.
        """
        with self.open_parameter_file_for_write(data_product, component) as file:
            write_distribution(file, component, distribution)

    # ----------------------------------------------------------------------------------
    # Samples
    # ----------------------------------------------------------------------------------

    def read_sample(self, data_product: str, component: str) -> float:
        """Read a sample from the data product component.
        """
        with self.open_parameter_file_for_read(data_product, component) as file:
            parameter_type = read_type(file, component)
            if parameter_type is Type.POINT_ESTIMATE:
                return read_estimate(file, component)
            if parameter_type is Type.DISTRIBUTION:
                return read_distribution(file, component).rvs()
            if parameter_type is Type.SAMPLES:
                return np.random.choice(read_samples(file, component))
            raise ValueError(f"unrecognised type {parameter_type}")

    def write_samples(self, data_product: str, component: str, samples: Samples):
        """Write samples to the data product component.
        """
        with self.open_parameter_file_for_write(data_product, component) as file:
            write_samples(file, component, samples)

    # ==================================================================================
    # Object (hdf5) files
    # ==================================================================================

    @contextmanager
    def open_object_file_for_read(self, data_product: str, component: str):
        """Open an parameter file for reading.
        """
        with self.file_api.open_for_read(
            data_product=data_product, component=component
        ) as object_file:
            yield object_file

    @contextmanager
    def open_object_file_for_write(self, data_product: str, component: str):
        """Open an parameter file for writing.
        """
        with self.file_api.open_for_write(
            data_product=data_product, component=component, extension="h5"
        ) as object_file:
            yield object_file

    def read_table(self, data_product: str, component: str) -> Table:
        """Read a table from the data product component.
        """
        with self.open_object_file_for_read(data_product, component) as file:
            return read_table(file, component)

    def write_table(self, data_product: str, component: str, table: Table):
        """Write a table to the data product component.
        """
        with self.open_object_file_for_write(data_product, component) as file:
            write_table(file, component, table)

    def read_array(self, data_product: str, component: str) -> Array:
        """Read an array from the data product component.
        """
        with self.open_object_file_for_read(data_product, component) as file:
            return read_array(file, component)

    def write_array(self, data_product: str, component: str, array: Array):
        """Write an array to the data product component.
        """
        with self.open_object_file_for_write(data_product, component) as file:
            write_array(file, component, array)
