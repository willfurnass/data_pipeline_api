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
    get_on_end_point,
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
    """
    Reads a single read config block, performs some basic validation (including checking that the data_product itself
    exists in the registry) and retrieves the required information from it for downloading data from the registry

    :param read_config: single read block read from the config
    :param data_registry_url: base url of the data registry
    :param token: github personal access token
    :return: validated and parsed read block
    """
    if "where" not in read_config:
        raise ValueError(f"No where specified in read_config {read_config}")
    config = read_config.get("use", {}).copy()
    config.update(read_config["where"])  # override the use block with the where block contents
    if "data_product" not in read_config["where"]:
        raise ValueError(f"No data_product specified in where clause of read_config {read_config}")
    data_product_name = config["data_product"]
    component_name = config.get("component")
    data_product = get_reference(
        {DataRegistryField.name: data_product_name}, DataRegistryTarget.data_product, data_registry_url, token
    )
    if data_product is None:
        raise ValueError(f"No data_product found with name {data_product_name}")
    version_identifier = config.get("version")
    logger.info(
        f"Read config for data_product: {data_product_name}, "
        f"component: {component_name}, "
        f"version: {version_identifier}"
    )
    return ParsedReadConfig(data_product, data_product_name, component_name, version_identifier)


def _get_data_product_version_and_components(
    parsed_config: ParsedReadConfig, data_registry_url: str, token: str
) -> Tuple[Dict[str, str], List[str]]:
    """
    For a provided parsed read config block gets the specified data_product_version (filtering on data_product name
    and version_identifier), if no version is specified all versions will be gotten. The data_product_versions are
    then sorted by their semver (descending). If a component name has been specified in the config, we iterate
    through the retrieved data_product_versions and find the first data_product_version in our sorted list that
    contains the component and retain that pair, if no component is specified we take the first data_product_version
    in our sorted list and return the pair of that and all of its components. If a component and version is specified
    but no component of that name is linked to that version, then an error is raised.

    :param parsed_config: a read config block parsed to the relevant data
    :param data_registry_url: base url of the data registry
    :param token: github personal access token
    :return: a pair of data_product_version data, List[data_product_version_component[name]]
    """
    query_data = {DataRegistryFilter.data_product: parsed_config.data_product}
    if parsed_config.version_identifier is not None:
        query_data[DataRegistryFilter.version_identifier] = parsed_config.version_identifier

    # if query_data contains a version_identifier this will return a list with a single element, else it will return
    # a list of all versions for the data_product
    data_product_versions = get_data(
        query_data, DataRegistryTarget.data_product_version, data_registry_url, token, exact=False
    )
    # sort descending by semver VersionInfo
    data_product_versions = sorted(
        data_product_versions, key=lambda data: semver.parse_version_info(data[DataRegistryFilter.version_identifier])
    )
    if parsed_config.component_name:
        data_product_version = None
        data_product_version_components = []
        # if a specific component has been requested then just get that component, iterate through the
        # data_product_versions, and for each get their component urls, get the data at that url and break on the
        # first data_product_version found that contains the requested component
        for dpv in data_product_versions:
            data_product_version_components = [
                cn
                for cn in [get_on_end_point(c, token)[DataRegistryField.name] for c in dpv[DataRegistryField.components]]
                if cn == parsed_config.component_name
            ]
            if data_product_version_components:
                data_product_version = dpv
                break
        if data_product_version is None:
            raise ValueError(
                f"No data_product_version found for data_product '{parsed_config.data_product_name}'"
                f" with component '{parsed_config.component_name}'"
            )
    else:
        data_product_version = next(iter(data_product_versions))
        data_product_version_components = [
            get_on_end_point(c, token)[DataRegistryField.name] for c in data_product_version[DataRegistryField.components]
        ]
    logger.info(
        f"Found data_product_version: {data_product_version} "
        f"and data_product_version_components: {data_product_version_components}"
    )
    return data_product_version, data_product_version_components


def _write_metadata(
    data_product_name: str, version: str, sha1_hash: str, components: List[str], filename: Path, stream: IO
) -> None:
    """
    For a given data product writes the associated metadata yaml, the base set of metadata is replicated for each
    component

    :param data_product_name:  name of the data_product
    :param version: version of the data_product_version
    :param sha1_hash: verified hash of the data as provided by the registry
    :param components: list of components that this data_product contains
    :param filename: relative path of the downloaded file location (relative to the data directory)
    :param stream: IO stream to write the metadata to
    """
    base = {
        MetadataKey.data_product: data_product_name,
        MetadataKey.version: version,
        MetadataKey.verified_hash: sha1_hash,
        MetadataKey.extension: filename.suffix[1:],
        MetadataKey.filename: filename.as_posix(),
    }
    metadatas = []
    if components:
        # if there are components present replicate the metadata across them
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
    """
    For a given data product generates the required set of information to download the data from the source
    to disk.

    :param data_product_name: name of the data_product, used to generate the download path
    :param data_product_version: data_product_version json data
    :param data_directory: base directory for downloading data to
    :param token: github personal access token
    :return: required information to download the data from source to disk
    """
    # lookup the data at the data_product_version's store url and retrieve the storage_location path
    store = get_on_end_point(data_product_version[DataRegistryField.store], token)
    path = store[DataRegistryField.path]

    # lookup the data at the store's store_root url and retrieve the uri
    store_root = get_on_end_point(store[DataRegistryField.store_root], token)
    uri = store_root[DataRegistryField.uri]
    # We need to find out what fsspec views as the path from the uri for this protocol, and combine it with our
    # storage_location path to find the 'true' path to our file to use with fsspec
    storage_options = fsspec.utils.infer_storage_options(uri)
    root_path = storage_options.pop("path")

    storage_type = get_on_end_point(store_root[DataRegistryField.type], token)[DataRegistryField.name]

    path = urllib.parse.urljoin(root_path + "/", path)

    output_filename = (
        Path(data_product_name) / data_product_version[DataRegistryField.version_identifier] / Path(path).name
    )

    full_output_filename = Path(data_directory) / output_filename
    full_output_filename.parent.mkdir(parents=True, exist_ok=True)

    accessibility = get_on_end_point(data_product_version[DataRegistryField.accessibility], token)
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


def _download_data(output_info: OutputInfo) -> None:
    """
    Reads the provided output information and downloads the data

    :param output_info: output information generated from the configuration and data registry data
    """
    if output_info.accessibility == "public":
        logger.info(
            f"Downloading public data from uri: {output_info.source_uri}, path: {output_info.source_path} to "
            f"{output_info.output_path}"
        )
        fsspec.open(output_info.source_uri, protocol=output_info.source_protocol).fs.download(
            output_info.source_path, output_info.output_path
        )
    else:
        # FIXME: Currently we only download data that's public, it's not clear what will happen in other cases
        # or what those other cases might be, some 100% imagined scenarios:
        # 1. private - expect the user to have provided the data in the right location, we still write metadata from registry for validation?
        # 2. credentials provided - somewhere in the data registry we provide credentials, if you can access the registry, you can access the data?
        # 3. credentials db? a separate database that's less public that provides credentials?
        logger.info(f"Data is not public, skipping download")


def download_from_configs(
    read_configs: ReadConfigs, data_directory: Union[Path, str], data_registry_url: str, token: str
) -> None:
    """
    Iterates through the config read blocks and downloads the relevant data for each block

    :param read_configs: list of read blocks
    :param data_directory: base directory for downloading data to
    :param data_registry_url: base url of the data registry
    :param token: github personal access token
    """
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
    """
    Parses a config.yaml file and downloads the relevant data from the read block
     
    :param config_filename: filename (str or Path) of the config.yaml file 
    :param data_registry_url: base url of the data registry
    :param token: github personal access token
    """
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
