import logging
import logging.config
import os
from pathlib import Path
from typing import Dict, Union, List

import click
import requests
import semver
import yaml

from data_pipeline_api.registry.common import configure_cli_logging, YamlDict, get_reference, get_end_point, \
    get_headers, get_on_end_point, DATA_REGISTRY_ACCESS_TOKEN, DATA_REGISTRY_URL, DEFAULT_DATA_REGISTRY_URL, DataRegistryField

logger = logging.getLogger(__name__)


def resolve_references(data: YamlDict, data_registry_url: str, token: str) -> Dict[str, str]:
    """
    Iterates through the provided data object and resolves any nested data to reference urls where they exist.

    :param data: Dict of data, keys are str and values are either str or a nested dict referring to another data
    :param data_registry_url: base url of the data registry
    :param token: github personal access token
    :return: A new dict of data that will have been flatted to Dict[str, str] with references resolved to urls
    """

    def resolve(value):
        if isinstance(value, dict):
            nested_target = value["target"]
            nested_data = value["data"]
            nested_data = resolve_references(nested_data, data_registry_url, token)
            return get_reference(nested_data, nested_target, data_registry_url, token)
        else:
            return value.strip()

    return {k: resolve(v) for k, v in data.items()}


def upload_from_config(config: Dict[str, List[YamlDict]], data_registry_url: str, token: str) -> None:
    """
    Iterates over the provided input configuration and calls PATCH or POST with the data to the data registry as
    appropriate, resolving references to other data where required.

    :param config: loaded configuration
    :param data_registry_url: base url of the data registry 
    :param token: github personal access token
    """
    for method in ("PATCH", "POST"):
        data_list = config.get(method.lower(), [])
        for data in data_list:
            target = data["target"]
            data = data["data"]
            logger.info(f"Working on {method} for target '{target}'")
            data = resolve_references(data, data_registry_url, token)
            reference = get_reference(data, target, data_registry_url, token)

            if method == "POST":
                end_point = get_end_point(data_registry_url, target)
                requests_func = requests.post
                clear_cache = reference is None
                do_request = reference is None
            else:
                end_point = reference
                requests_func = requests.patch
                clear_cache = False
                do_request = reference is not None

            if do_request:

                if DataRegistryField.version_identifier in data:
                    try:
                        semver.parse_version_info(data[DataRegistryField.version_identifier])
                    except ValueError as e:
                        raise ValueError(
                            f"version_identifier must match the Semantic Versioning (SemVer) "
                            f"format but was '{data['version_identifier']}'"
                        ) from e

                logger.info(f"{method} {end_point}: {data}")
                result = requests_func(end_point, data=data, headers=get_headers(token))
                result.raise_for_status()
                logger.info(f"{method} successful: {result.status_code}")
            else:
                logger.info(f"Nothing to do for {method} for target '{target}'")

            if clear_cache:
                get_on_end_point.cache_clear()


def upload_from_config_file(config_filename: Union[Path, str], data_registry_url: str, token: str) -> None:
    """
    Reads the provided configuration files and then calls PATCH or POST with the data to the data registry as
    appropriate, resolving references to other data where required.
    
    :param config_filename: file path to the configuration file
    :param data_registry_url: base url of the data registry 
    :param token: github personal access token
    """
    config_filename = Path(config_filename)
    with open(config_filename, "r") as cf:
        config = yaml.safe_load(cf)
    upload_from_config(config, data_registry_url, token)


@click.command()
@click.option(
    "--config", required=True, type=str, help=f"Path to the yaml config file.",
)
@click.option(
    "--data-registry",
    type=str,
    help=f"URL of the data registry API. Defaults to {DATA_REGISTRY_URL} env "
    f"variable followed by {DEFAULT_DATA_REGISTRY_URL}.",
)
@click.option(
    "--token",
    type=str,
    help=f"data registry access token. Defaults to {DATA_REGISTRY_ACCESS_TOKEN} env if not passed."
    f" access tokens can be created from the data registry's get-token end point",
)
def upload_cli(config, data_registry, token):
    configure_cli_logging()
    data_registry = data_registry or os.environ.get(DATA_REGISTRY_URL, DEFAULT_DATA_REGISTRY_URL)
    token = token or os.environ.get(DATA_REGISTRY_ACCESS_TOKEN)
    if not token:
        raise ValueError(
            f"Personal Access Token must be provided through either --token cmd line arg "
            f"or environment variable {DATA_REGISTRY_ACCESS_TOKEN}"
        )
    upload_from_config_file(config_filename=config, data_registry_url=data_registry, token=token)


if __name__ == "__main__":
    logger = logging.getLogger(f"{__package__}.{__name__}")
    upload_cli()