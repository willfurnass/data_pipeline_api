import logging
from collections import namedtuple
from pathlib import Path
from typing import Dict, List, Union, Optional

import click
import yaml

from data_pipeline_api.registry.common import (
    configure_cli_logging,
    DATA_REGISTRY_ACCESS_TOKEN,
    DATA_REGISTRY_URL,
    DEFAULT_DATA_REGISTRY_URL,
)
from data_pipeline_api.registry.downloader import Downloader

logger = logging.getLogger(__name__)


ReadConfig = Dict[str, Dict[str, str]]
ReadConfigs = List[ReadConfig]

ParsedReadConfig = namedtuple(
    "ParsedReadConfig", ("namespace", "data_product", "component", "version")
)


def _parse_read_config(read_config: ReadConfig, namespace: Optional[str]) -> ParsedReadConfig:
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
        raise ValueError(f"No data_product specified in where clause of read_config {read_config}")
    config = read_config["where"].copy()
    config.update(read_config.get("use", {}))  # override the where block with the use block contents
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
    read_configs: ReadConfigs, namespace: str, data_directory: Union[Path, str], data_registry_url: str, token: str
) -> None:
    """
    Iterates through the config read blocks and downloads the relevant data for each block

    :param read_configs: list of read blocks
    :param namespace: the default namespace to use if no override is provided
    :param data_directory: base directory for downloading data to
    :param data_registry_url: base url of the data registry
    :param token: personal access token
    """
    downloader = Downloader(data_directory=data_directory, data_registry_url=data_registry_url, token=token)

    for read_config in read_configs:
        if "doi_or_unique_name" in read_config.get("where", {}):
            config = read_config["where"].copy()
            config.update(read_config.get("use", {}))
            downloader.add_external_object(config["doi_or_unique_name"], config.get("version"))
        else:
            parsed_config = _parse_read_config(read_config, namespace)
            downloader.add_data_product(parsed_config.namespace, parsed_config.data_product, parsed_config.component, parsed_config.version)

    downloader.download()


def download_from_config_file(config_filename: Union[Path, str], data_registry_url: str, token: str) -> None:
    """
    Parses a config.yaml file and downloads the relevant data from the read block
     
    :param config_filename: filename (str or Path) of the config.yaml file 
    :param data_registry_url: base url of the data registry
    :param token: personal access token
    """
    config_filename = Path(config_filename)
    with open(config_filename, "r") as cf:
        config = yaml.safe_load(cf)
    data_directory = Path(config.get("data_directory", "."))
    if not data_directory.is_absolute():
        data_directory = config_filename.parent / data_directory
    read_configs = config.get("read")
    namespace = config.get("namespace")
    if not read_configs:
        raise ValueError("No read config specified in configuration file")

    download_from_configs(read_configs, namespace, data_directory, data_registry_url, token)


@click.command(context_settings=dict(max_content_width=200))
@click.option(
    "--config", required=True, type=click.Path(exists=True), help="Path to the yaml config file.",
)
@click.option(
    "--data-registry",
    type=str,
    envvar=f"{DATA_REGISTRY_URL}",
    help=f"URL of the data registry API. Defaults to {DATA_REGISTRY_URL} env "
    f"variable followed by {DEFAULT_DATA_REGISTRY_URL}.",
)
@click.option(
    "--token",
    type=str,
    envvar=f"{DATA_REGISTRY_ACCESS_TOKEN}",
    help=f"data registry access token. Defaults to {DATA_REGISTRY_ACCESS_TOKEN} env if not passed."
    f" access tokens can be created from the data registry's get-token end point",
)
def download_cli(config, data_registry, token):
    configure_cli_logging()
    data_registry = data_registry or DEFAULT_DATA_REGISTRY_URL
    download_from_config_file(config_filename=config, data_registry_url=data_registry, token=token)


if __name__ == "__main__":
    logger = logging.getLogger(f"{__package__}.{__name__}")
    # pylint: disable=no-value-for-parameter
    download_cli()
