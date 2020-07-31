import logging
from collections import namedtuple
from pathlib import Path
from typing import Dict, List, Union, Optional, Any

import click
import yaml

from data_pipeline_api.metadata import MetadataKey
from data_pipeline_api.registry.common import (
    configure_cli_logging,
    DATA_REGISTRY_ACCESS_TOKEN,
)
from data_pipeline_api.registry.downloader import Downloader
from data_pipeline_api.file_api import RunMetadata, FileAPI

logger = logging.getLogger(__name__)


ReadConfig = Dict[str, Dict[str, str]]
ReadConfigs = List[ReadConfig]

ParsedReadConfig = namedtuple(
    "ParsedReadConfig", ("namespace", "data_product", "component", "version")
)


def _parse_read_config(
    read_config: ReadConfig, namespace: Optional[str]
) -> ParsedReadConfig:
    """
    Reads a single read config block, performs some basic validation and retrieves the required information from it for
    downloading data from the registry

    :param read_config: single read block read from the config
    :param namespace: the default namespace to use if no override is provided
    :return: validated and parsed read block
    """
    if "where" not in read_config:
        raise ValueError(f"No where specified in read_config {read_config}")
    if "data_product" not in read_config["where"]:
        raise ValueError(
            f"No data_product specified in where clause of read_config {read_config}"
        )
    config = read_config["where"].copy()
    config.update(
        read_config.get("use", {})
    )  # override the where block with the use block contents
    data_product = config["data_product"]
    component = config.get("component")
    namespace = config.get("namespace", namespace)
    if namespace is None:
        raise ValueError(f"No namespace specified for read_config {read_config}")
    version = config.get("version")
    logger.info(
        f"Read config for data_product: {data_product}, "
        f"namespace: {namespace}, "
        f"component: {component}, "
        f"version: {version}"
    )
    return ParsedReadConfig(namespace, data_product, component, version)


def download_from_configs(
    run_metadata: Dict[str, Any],
    read_configs: ReadConfigs,
    token: str,
    root_dir: Optional[Union[Path, str]] = None,
) -> None:
    """
    Iterates through the config read blocks and downloads the relevant data for each block

    :param run_metadata: dictionary of run metadata
    :param read_configs: list of read blocks
    :param token: personal access token
    :param root_dir: root directory to instantiate the data in, defaults to current working directory
    """
    unnormalised_data_directory = Path(run_metadata[RunMetadata.data_directory])
    root_dir = Path(root_dir) if root_dir is not None else Path.cwd()
    data_directory = FileAPI.normalise_path(root_dir, unnormalised_data_directory)
    downloader = Downloader(
        data_directory=data_directory,
        data_registry_url=run_metadata.get(RunMetadata.data_registry_url),
        token=token,
    )

    for read_config in read_configs:
        if "doi_or_unique_name" in read_config.get("where", {}):
            config = read_config["where"].copy()
            config.update(read_config.get("use", {}))
            downloader.add_external_object(
                config[MetadataKey.doi_or_unique_name],
                config.get(MetadataKey.title),
                config.get(MetadataKey.component),
                config.get(MetadataKey.version),
            )
        else:
            parsed_config = _parse_read_config(
                read_config, run_metadata.get(RunMetadata.default_input_namespace)
            )
            downloader.add_data_product(
                parsed_config.namespace,
                parsed_config.data_product,
                parsed_config.component,
                parsed_config.version,
            )

    downloader.download()


def download_from_config_file(config_filename: Union[Path, str], token: str) -> None:
    """
    Parses a config.yaml file and downloads the relevant data from the read block
     
    :param config_filename: filename (str or Path) of the config.yaml file
    :param token: personal access token
    """
    config_filename = Path(config_filename)
    root = config_filename.parent
    with open(config_filename, "r") as cf:
        config = yaml.safe_load(cf)
    run_metadata = config.get("run_metadata", {})
    # Copy the top level data_directory into the run_metadata.
    run_metadata[RunMetadata.data_directory] = config["data_directory"]
    read_configs = config.get("read")
    if not read_configs:
        raise ValueError("No read config specified in configuration file")

    download_from_configs(run_metadata, read_configs, token, root)


@click.command(context_settings=dict(max_content_width=200))
@click.option(
    "--config",
    required=True,
    type=click.Path(exists=True),
    help="Path to the yaml config file.",
)
@click.option(
    "--token",
    type=str,
    envvar=f"{DATA_REGISTRY_ACCESS_TOKEN}",
    help=f"data registry access token. Defaults to {DATA_REGISTRY_ACCESS_TOKEN} env if not passed."
    f" access tokens can be created from the data registry's get-token end point",
)
def download_cli(config, token):
    configure_cli_logging()
    download_from_config_file(config_filename=config, token=token)


if __name__ == "__main__":
    logger = logging.getLogger(f"{__package__}.{__name__}")
    # pylint: disable=no-value-for-parameter
    download_cli()
