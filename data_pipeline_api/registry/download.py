import logging
import os
import urllib
from collections import namedtuple
from pathlib import Path
from typing import Dict, List, Union, IO, Tuple
from urllib.parse import urlparse

import click
import fsspec
import semver
import yaml

from data_pipeline_api.metadata import MetadataKey, METADATA_FILENAME
from data_pipeline_api.registry.common import (
    configure_cli_logging,
    get_data,
    get_reference,
    DATA_REGISTRY_ACCESS_TOKEN,
    get,
    DATA_REGISTRY_URL,
    DEFAULT_DATA_REGISTRY_URL,
    DataRegistryTarget,
    DataRegistryFilter,
    DataRegistryField,
)


logger = logging.getLogger(__name__)


ReadConfig = Dict[str, Dict[str, str]]
ReadConfigs = List[ReadConfig]

ParsedReadConfig = namedtuple(
    "ParsedReadConfig", ("data_product", "data_product_name", "component_name", "version_identifier")
)
OutputInfo = namedtuple(
    "OutputInfo",
    ("source_uri", "source_path", "source_protocol", "output_filename", "output_path", "hash", "accessibility"),
)


def _parse_read_config(read_config: ReadConfig, data_registry_url: str, token: str) -> ParsedReadConfig:
    if "where" not in read_config:
        raise ValueError(f"No where specified in read_config {read_config}")
    config = read_config.get("use", {}).copy()
    config.update(read_config["where"])
    if "data_product" not in read_config["where"]:
        raise ValueError(f"No data_product specified in where clause of read_config {read_config}")
    data_product_name = config["data_product"]
    component_name = config.get("component")
    data_product = get_reference({"name": data_product_name}, DataRegistryTarget.data_product, data_registry_url, token)
    if data_product is None:
        raise ValueError(f"No data_product found with name {data_product_name}")
    version_identifier = config.get("version")
    logger.info(f"Read config for data_product: {data_product_name}, component: {component_name}, version: {version_identifier}")
    return ParsedReadConfig(data_product, data_product_name, component_name, version_identifier)


def _get_data_product_version_and_components(parsed_config: ParsedReadConfig, data_registry_url: str, token: str) -> Tuple[Dict[str, str], List[str]]:
    query_data = {DataRegistryFilter.data_product: parsed_config.data_product}
    if parsed_config.version_identifier is not None:
        query_data[DataRegistryFilter.version_identifier] = parsed_config.version_identifier

    data_product_versions = get_data(
        query_data, DataRegistryTarget.data_product_version, data_registry_url, token, exact=False
    )
    data_product_versions = sorted(
        data_product_versions, key=lambda data: semver.parse_version_info(data[DataRegistryFilter.version_identifier])
    )
    if parsed_config.component_name:
        data_product_version = None
        data_product_version_components = []
        for dpv in data_product_versions:
            data_product_version_components = [
                cn
                for cn in [get(c, token)[DataRegistryField.name] for c in dpv[DataRegistryField.components]]
                if cn == parsed_config.component_name
            ]
            if data_product_version_components:
                data_product_version = dpv
                break
        if data_product_version is None:
            raise ValueError(
                f"No data_product_version found for data_product '{parsed_config.data_product_name}' with component '{parsed_config.component_name}'"
            )
    else:
        data_product_version = next(iter(data_product_versions))
        data_product_version_components = [
            get(c, token)[DataRegistryField.name] for c in data_product_version[DataRegistryField.components]
        ]
    logger.info(f"Found data_product_version: {data_product_version} and data_product_version_components: {data_product_version_components}")
    return data_product_version, data_product_version_components


def _write_metadata(
    data_product_name: str, version: str, sha1_hash: str, components: List[str], filename: Path, stream: IO
):
    base = {
        MetadataKey.data_product: data_product_name,
        MetadataKey.version: version,
        MetadataKey.verified_hash: sha1_hash,
        MetadataKey.extension: filename.suffix[1:],
        MetadataKey.filename: filename.as_posix(),
    }
    metadatas = []
    if components:
        for component in components:
            component_metadata = base.copy()
            component_metadata[MetadataKey.component] = component
            metadatas.append(component_metadata)
    else:
        metadatas.append(base)
    logger.info(f"Writing metadata for {data_product_name}")
    yaml.safe_dump(metadatas, stream)


def _get_output_info(
    data_product_name: str, data_product_version: Dict[str, str], data_directory: Path, token: str
) -> OutputInfo:
    store = get(data_product_version[DataRegistryField.store], token)
    path = store[DataRegistryField.path]

    store_root = get(store[DataRegistryField.store_root], token)
    uri = store_root[DataRegistryField.uri]
    storage_options = fsspec.utils.infer_storage_options(uri)
    root_path = storage_options.pop("path")

    storage_type = get(store_root[DataRegistryField.type], token)[DataRegistryField.name]

    path = urllib.parse.urljoin(root_path + "/", path)

    output_filename = (
        Path(data_product_name) / data_product_version[DataRegistryField.version_identifier] / Path(path).name
    )

    full_output_filename = Path(data_directory) / output_filename
    full_output_filename.parent.mkdir(parents=True, exist_ok=True)

    accessibility = get(data_product_version[DataRegistryField.accessibility], token)
    oi = OutputInfo(
        source_uri=uri,
        source_path=path,
        source_protocol=storage_type,
        output_filename=output_filename,
        output_path=full_output_filename,
        hash=store[DataRegistryField.hash],
        accessibility=accessibility[DataRegistryField.name],
    )
    logger.info(f"Constructed output information: {oi}")
    return oi


def _download_data(output_info):
    if output_info.accessibility == "public":
        logger.info(f"Downloading public data from uri: {output_info.source_uri}, path: {output_info.source_path} to "
                    f"{output_info.output_path}")
        fsspec.open(output_info.source_uri, protocol=output_info.source_protocol).fs.download(
            output_info.source_path, output_info.output_path
        )
    else:
        logger.info(f"Data is not public, skipping download")


def download_from_configs(
    read_configs: ReadConfigs, data_directory: Union[Path, str], data_registry_url: str, token: str
) -> None:
    data_directory = Path(data_directory)
    logger.info(f"Creating data directory if it does not exist: {data_directory}")
    data_directory.mkdir(parents=True, exist_ok=True)

    metadata_file = data_directory / METADATA_FILENAME

    with open(metadata_file, "w") as metadata_stream:
        logger.info(f"Opened stream to metadata file: {metadata_file}")
        for read_config in read_configs:
            parsed_config = _parse_read_config(read_config, data_registry_url, token)

            data_product_version, data_product_version_components = _get_data_product_version_and_components(
                parsed_config, data_registry_url, token
            )
            output_info = _get_output_info(parsed_config.data_product_name, data_product_version, data_directory, token)
            _write_metadata(
                parsed_config.data_product_name,
                data_product_version[DataRegistryField.version_identifier],
                output_info.hash,
                data_product_version_components,
                output_info.output_filename,
                metadata_stream,
            )

            _download_data(output_info)


def download_from_config_file(config_filename: Union[Path, str], data_registry_url: str, token: str) -> None:
    config_filename = Path(config_filename)
    with open(config_filename, "r") as cf:
        config = yaml.safe_load(cf)
    data_directory = Path(config.get("data_directory", "."))
    read_configs = config.get("read")
    if not read_configs:
        raise ValueError("No read config specified in configuration file")

    download_from_configs(read_configs, data_directory, data_registry_url, token)


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
    help=f"github personal access token. Defaults to {DATA_REGISTRY_ACCESS_TOKEN} env if not passed."
    f"Personal access tokens can be created from https://github.com/settings/tokens, only read:org "
    f"permissions are required.",
)
def download_cli(config, data_registry, token):
    configure_cli_logging()
    data_registry = data_registry or os.environ.get(DATA_REGISTRY_URL, DEFAULT_DATA_REGISTRY_URL)
    token = token or os.environ.get(DATA_REGISTRY_ACCESS_TOKEN)
    download_from_config_file(config_filename=config, data_registry_url=data_registry, token=token)


if __name__ == "__main__":
    logger = logging.getLogger(f"{__package__}.{__name__}")
    download_cli()
