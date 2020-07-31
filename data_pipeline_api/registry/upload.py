import logging
import logging.config
from hashlib import sha1
from pathlib import Path
from typing import Dict, Union, List

import click
import requests
import semver
import yaml

from data_pipeline_api.registry.common import (
    configure_cli_logging,
    YamlDict,
    get_reference,
    get_end_point,
    get_headers,
    get_on_end_point,
    DATA_REGISTRY_ACCESS_TOKEN,
    DATA_REGISTRY_URL,
    DEFAULT_DATA_REGISTRY_URL,
    DataRegistryField,
    DataRegistryTarget,
)

logger = logging.getLogger(__name__)


def upload_text_to_text_table(text: str, data_registry_url: str, token: str) -> str:
    """
    Uploads text to the text_file table and creates a storage_root and storage_location for it

    :param text: text to upload
    :param data_registry_url: the url of the data registry
    :param token: personal access token
    :return: storage_location url reference for this text
    """
    hash_ = sha1(text.encode("utf-8")).hexdigest()
    text_data = {DataRegistryField.text: text}
    storage_root_data = {
        DataRegistryField.root: get_end_point(data_registry_url, DataRegistryTarget.text_file),
        DataRegistryField.name: "text_file",
    }
    post_data = {
        "post": [
            {"target": DataRegistryTarget.text_file, "data": text_data},
            {"target": DataRegistryTarget.storage_root, "data": storage_root_data},
        ]
    }
    upload_from_config(post_data, data_registry_url, token)
    text_ref = get_reference(text_data, DataRegistryTarget.text_file, data_registry_url, token)
    storage_root_ref = get_reference(storage_root_data, DataRegistryTarget.storage_root, data_registry_url, token)
    storage_location_data = {
        DataRegistryField.storage_root: storage_root_ref,
        DataRegistryField.hash: hash_,
        DataRegistryField.path: f"{text_ref.split('/')[-2]}/?format=text",
    }
    post_data = {"post": [{"target": DataRegistryTarget.storage_location, "data": storage_location_data}]}
    upload_from_config(post_data, data_registry_url, token)
    return get_reference(storage_location_data, DataRegistryTarget.storage_location, data_registry_url, token)


def upload_to_text_table(filename: Path, data_registry_url: str, token: str) -> str:
    """
    Uploads text from a file to the text_file table and create a storage_root and storage_location for it

    :param filename: file to upload
    :param data_registry_url: the url of the data registry
    :param token: personal access token
    :return: storage_location url reference for this text
    """
    with open(filename, "r") as f:
        text = f.read()
    return upload_text_to_text_table(text, data_registry_url, token)


def resolve_references(data: YamlDict, data_registry_url: str, token: str) -> Dict[str, str]:
    """
    Iterates through the provided data object and resolves any nested data to reference urls where they exist.

    :param data: Dict of data, keys are str and values are either str or a nested dict referring to another data
    :param data_registry_url: base url of the data registry
    :param token: personal access token
    :return: A new dict of data that will have been flatted to Dict[str, str] with references resolved to urls
    """

    def resolve(value):
        if isinstance(value, dict):
            nested_target = value["target"]
            nested_data = value["data"]
            nested_data = resolve_references(nested_data, data_registry_url, token)
            return get_reference(nested_data, nested_target, data_registry_url, token)
        elif isinstance(value, List):
            return [resolve(inner_value) for inner_value in value]
        else:
            return value.strip() if isinstance(value, str) else value

    return {k: resolve(v) for k, v in data.items()}


def upload_from_config(config: Dict[str, List[YamlDict]], data_registry_url: str, token: str) -> None:
    """
    Iterates over the provided input configuration and calls PATCH or POST with the data to the data registry as
    appropriate, resolving references to other data where required.

    :param config: loaded configuration
    :param data_registry_url: base url of the data registry 
    :param token: personal access token
    """
    for method in ("PATCH", "POST"):
        data_list = config.get(method.lower(), [])
        post = method == "POST"
        for data in data_list:
            target = data["target"]
            data = data["data"]
            fail_fast = data.get("fail_fast", False)
            logger.info(f"Working on {method} for target '{target}'")
            data = resolve_references(data, data_registry_url, token)
            reference = get_reference(data, target, data_registry_url, token)

            if post:
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
                if DataRegistryField.version in data:
                    try:
                        semver.parse_version_info(data[DataRegistryField.version])
                    except ValueError as e:
                        raise ValueError(
                            f"version must match the Semantic Versioning (SemVer) "
                            f"format but was '{data['version']}'"
                        ) from e

                logger.info(f"{method} {end_point}: {data}")
                result = requests_func(end_point, data=data, headers=get_headers(token))
                result.raise_for_status()
                url = result.json().get(DataRegistryField.url)
                logger.info(f"{method} successful: {result.status_code}. URL: {url}")
            elif fail_fast and post:
                raise ValueError(f"fail_fast POST was attempted but data already existed at {end_point}: {data}")
            elif fail_fast:
                raise ValueError(f"fail_fast PATCH was attempted but no data existed at {end_point}: {data}")
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
    :param token: personal access token
    """
    config_filename = Path(config_filename)
    with open(config_filename, "r") as cf:
        config = yaml.safe_load(cf)
    upload_from_config(config, data_registry_url, token)


@click.command(context_settings=dict(max_content_width=200))
@click.option(
    "--config", required=True, type=str, help="Path to the yaml config file.",
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
def upload_cli(config, data_registry, token):
    configure_cli_logging()
    data_registry = data_registry or DEFAULT_DATA_REGISTRY_URL
    if not token:
        raise ValueError(
            f"Personal Access Token must be provided through either --token cmd line arg "
            f"or environment variable {DATA_REGISTRY_ACCESS_TOKEN}"
        )
    upload_from_config_file(config_filename=config, data_registry_url=data_registry, token=token)


if __name__ == "__main__":
    logger = logging.getLogger(f"{__package__}.{__name__}")
    # pylint: disable=no-value-for-parameter
    upload_cli()
