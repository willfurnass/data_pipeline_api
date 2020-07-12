from io import TextIOWrapper
from pathlib import Path
from contextlib import contextmanager
import numpy as np
import pandas as pd
from data_pipeline_api.file_api import FileAPI
from data_pipeline_api.file_formats import parameter_file
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
    Dimension,
    Table,
    read_array,
    read_table,
    write_array,
    write_table,
)


class StandardAPI(FileAPI):
    # ==================================================================================
    # Parameter files
    # ==================================================================================

    @contextmanager
    def open_parameter_file_for_read(self, data_product: str, component: str):
        with TextIOWrapper(
            self.open_for_read(data_product=data_product, component=component)
        ) as parameter_file:
            yield parameter_file

    @contextmanager
    def open_parameter_file_for_write(self, data_product: str, component: str):
        with TextIOWrapper(
            self.open_for_write(
                data_product=data_product, component=component, extension="toml"
            )
        ) as parameter_file:
            yield parameter_file

    # ----------------------------------------------------------------------------------
    # Estimate
    # ----------------------------------------------------------------------------------

    def read_estimate(
        self, data_product: str, component: str
    ) -> Estimate:
        with self.open_parameter_file_for_read(data_product, component) as file:
            parameter_type = read_type(file, component)
            if parameter_type is Type.POINT_ESTIMATE:
                return read_estimate(file, component)
            elif parameter_type is Type.DISTRIBUTION:
                return read_distribution(file, component).mean()
            elif parameter_type is Type.SAMPLES:
                return read_samples(file, component).mean()
            else:
                raise ValueError(f"unrecognised type {parameter_type}")

    def write_estimate(
        self, data_product: str, component: str, estimate: Estimate
    ):
        with self.open_parameter_file_for_write(data_product, component) as file:
            write_estimate(file, component, estimate)

    # ----------------------------------------------------------------------------------
    # Distribution
    # ----------------------------------------------------------------------------------

    def read_distribution(
        self, data_product: str, component: str
    ) -> Distribution:
        with self.open_parameter_file_for_read(data_product, component) as file:
            parameter_type = read_type(file, component)
            if parameter_type is Type.POINT_ESTIMATE:
                raise ValueError("point-estimate cannot be read as a distribution")
            elif parameter_type is Type.DISTRIBUTION:
                return read_distribution(file, component)
            elif parameter_type is Type.SAMPLES:
                raise ValueError("samples cannot be read as a distribution")
            else:
                raise ValueError(f"unrecognised type {parameter_type}")

    def write_distribution(
        self,
        data_product: str,
        component: str,
        distribution: Distribution,
    ):
        with self.open_parameter_file_for_write(data_product, component) as file:
            write_distribution(file, component, distribution)

    # ----------------------------------------------------------------------------------
    # Samples
    # ----------------------------------------------------------------------------------

    def read_sample(self, data_product: str, component: str) -> float:
        with self.open_parameter_file_for_read(data_product, component) as file:
            parameter_type = read_type(file, component)
            if parameter_type is Type.POINT_ESTIMATE:
                return read_estimate(file, component)
            elif parameter_type is Type.DISTRIBUTION:
                return read_distribution(file, component).rvs()
            elif parameter_type is Type.SAMPLES:
                return np.random.choice(read_samples(file, component))
            else:
                raise ValueError(f"unrecognised type {parameter_type}")

    def write_samples(
        self, data_product: str, component: str, samples: Samples
    ):
        with self.open_parameter_file_for_write(data_product, component) as file:
            write_samples(file, component, samples)

    # ==================================================================================
    # Object (hdf5) files
    # ==================================================================================

    @contextmanager
    def open_object_file_for_read(self, data_product: str, component: str):
        with self.open_for_read(
            data_product=data_product, component=component
        ) as object_file:
            yield object_file

    @contextmanager
    def open_object_file_for_write(self, data_product: str, component: str):
        with self.open_for_write(
            data_product=data_product, component=component, extension="h5"
        ) as object_file:
            yield object_file

    def read_table(self, data_product: str, component: str) -> Table:
        with self.open_object_file_for_read(data_product, component) as file:
            return read_table(file, component)

    def write_table(self, data_product: str, component: str, table: Table):
        with self.open_object_file_for_write(data_product, component) as file:
            write_table(file, component, table)

    def read_array(self, data_product: str, component: str) -> Array:
        with self.open_object_file_for_read(data_product, component) as file:
            return read_array(file, component)

    def write_array(self, data_product: str, component: str, array: Array):
        with self.open_object_file_for_write(data_product, component) as file:
            write_array(file, component, array)
