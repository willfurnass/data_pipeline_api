from io import TextIOWrapper
from pathlib import Path
from contextlib import contextmanager
import numpy as np
import pandas as pd
from data_pipeline_api.file_api import FileAPI
from data_pipeline_api.file_formats import parameter_file, object_file


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
    ) -> parameter_file.Estimate:
        with self.open_parameter_file_for_read(data_product, component) as file:
            parameter_type = parameter_file.read_type(file, component)
            if parameter_type is parameter_file.Type.POINT_ESTIMATE:
                return parameter_file.read_estimate(file, component)
            elif parameter_type is parameter_file.Type.DISTRIBUTION:
                return parameter_file.read_distribution(file, component).mean()
            elif parameter_type is parameter_file.Type.SAMPLES:
                return parameter_file.read_samples(file, component).mean()
            else:
                raise ValueError(f"unrecognised type {parameter_type}")

    def write_estimate(
        self, data_product: str, component: str, estimate: parameter_file.Estimate
    ):
        with self.open_parameter_file_for_write(data_product, component) as file:
            parameter_file.write_estimate(file, component, estimate)

    # ----------------------------------------------------------------------------------
    # Distribution
    # ----------------------------------------------------------------------------------

    def read_distribution(
        self, data_product: str, component: str
    ) -> parameter_file.Distribution:
        with self.open_parameter_file_for_read(data_product, component) as file:
            parameter_type = parameter_file.read_type(file, component)
            if parameter_type is parameter_file.Type.POINT_ESTIMATE:
                raise ValueError("point-estimate cannot be read as a distribution")
            elif parameter_type is parameter_file.Type.DISTRIBUTION:
                return parameter_file.read_distribution(file, component)
            elif parameter_type is parameter_file.Type.SAMPLES:
                raise ValueError("samples cannot be read as a distribution")
            else:
                raise ValueError(f"unrecognised type {parameter_type}")

    def write_distribution(
        self,
        data_product: str,
        component: str,
        distribution: parameter_file.Distribution,
    ):
        with self.open_parameter_file_for_write(data_product, component) as file:
            parameter_file.write_distribution(file, component, distribution)

    # ----------------------------------------------------------------------------------
    # Samples
    # ----------------------------------------------------------------------------------

    def read_sample(self, data_product: str, component: str) -> float:
        with self.open_parameter_file_for_read(data_product, component) as file:
            parameter_type = parameter_file.read_type(file, component)
            if parameter_type is parameter_file.Type.POINT_ESTIMATE:
                return parameter_file.read_estimate(file, component)
            elif parameter_type is parameter_file.Type.DISTRIBUTION:
                return parameter_file.read_distribution(file, component).rvs()
            elif parameter_type is parameter_file.Type.SAMPLES:
                return np.random.choice(parameter_file.read_samples(file, component))
            else:
                raise ValueError(f"unrecognised type {parameter_type}")

    def write_samples(
        self, data_product: str, component: str, samples: parameter_file.Samples
    ):
        with self.open_parameter_file_for_write(data_product, component) as file:
            parameter_file.write_samples(file, component, samples)

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

    def read_table(self, data_product: str, component: str) -> object_file.Table:
        with self.open_object_file_for_read(data_product, component) as file:
            return object_file.read_table(file, component)

    def write_table(self, data_product: str, component: str, table: object_file.Table):
        with self.open_object_file_for_write(data_product, component) as file:
            object_file.write_table(file, component, table)

    def read_array(self, data_product: str, component: str) -> object_file.Array:
        with self.open_object_file_for_read(data_product, component) as file:
            return object_file.read_array(file, component)

    def write_array(self, data_product: str, component: str, array: object_file.Array):
        with self.open_object_file_for_write(data_product, component) as file:
            object_file.write_array(file, component, array)