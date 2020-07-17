import logging
import fnmatch
import re
import urllib
from collections import namedtuple, defaultdict
from pathlib import Path
from typing import Dict, List, Union, IO, Tuple, Optional

import click
import yaml
from fsspec.implementations.sftp import SFTPFileSystem

from data_pipeline_api.metadata import MetadataKey
from data_pipeline_api.registry.common import (
    configure_cli_logging,
    get_data,
    DATA_REGISTRY_ACCESS_TOKEN,
    get_on_end_point,
    DATA_REGISTRY_URL,
    DEFAULT_DATA_REGISTRY_URL,
    DataRegistryTarget,
    DataRegistryField,
    get_remote_filesystem_and_path,
    sort_by_semver,
)

logger = logging.getLogger(__name__)


ReadConfig = Dict[str, Dict[str, str]]
ReadConfigs = List[ReadConfig]

ParsedReadConfig = namedtuple(
    "ParsedReadConfig", ("namespace", "data_product", "component", "version")
)
OutputInfo = namedtuple(
    "OutputInfo",
    ("source_uri", "source_path", "output_filename", "output_path", "hash", "accessibility"),
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


def _get_data_product_version_and_components(
    parsed_config: ParsedReadConfig, data_registry_url: str, token: str
) -> List[Tuple[Dict[str, str], List[str]]]:
    """
    For a provided parsed read config block gets the specified data_product (filtering on namespace, name
    and version), if no version is specified all versions will be gotten. The data_products are
    then sorted by their semver (descending). If a component has been specified in the config, we iterate
    through the retrieved data_products and find the first data_product in our sorted list that
    contains the component and retain that pair, if no component is specified we take the first data_product
    in our sorted list and return the pair of that and all of its components. If a component and version is specified
    but no component of that name is linked to that version, then an error is raised.

    :param parsed_config: a read config block parsed to the relevant data
    :param data_registry_url: base url of the data registry
    :param token: personal access token
    :return: a list of pairs of data_product data, List[data_product_component[name]]
    """
    namespace = get_data({DataRegistryField.name: parsed_config.namespace}, DataRegistryTarget.namespace, data_registry_url, token, exact=False)
    if namespace is None:
        raise ValueError(f"No namespace found with name: {parsed_config.namespace}")
    data_products_list = []
    for ns in namespace:
        query_data = {DataRegistryField.name: parsed_config.data_product, DataRegistryField.namespace: ns[DataRegistryField.url]}
        if parsed_config.version is not None:
            query_data[DataRegistryField.version] = parsed_config.version

        # if query_data contains a version this will return a list with a single element, else it will return
        # a list of all versions for the data_product
        data_products = get_data(
            query_data, DataRegistryTarget.data_product, data_registry_url, token, exact=False
        )
        if data_products:
            data_products_list.extend(sort_by_semver(data_products))

    if not data_products_list:
        raise ValueError(f"No data_product found with namespace {parsed_config.namespace}, name {parsed_config.data_product} and version {parsed_config.version}")

    grouped_data_products = defaultdict(list)
    for data_product in data_products_list:
        grouped_data_products[data_product[DataRegistryField.namespace], data_product[DataRegistryField.name]].append(data_product)

    data_product_component_pairs = []
    for group in grouped_data_products.values():
        if parsed_config.component:
            data_product = None
            data_product_components = []
            # if a specific component has been requested then just get that component, iterate through the
            # data_products, and for each get their component urls, get the data at that url and break on the
            # first data_product found that contains the requested component
            for dp in group:
                obj = get_on_end_point(dp[DataRegistryField.object], token)
                data_product_components = [
                    cn
                    for cn in [
                        get_on_end_point(c, token)[DataRegistryField.name] for c in obj[DataRegistryField.components]
                    ]
                    if re.match(fnmatch.translate(parsed_config.component), cn)
                ]
                if data_product_components:
                    data_product = dp
                    break
        else:
            data_product = next(iter(group))
            obj = get_on_end_point(data_product[DataRegistryField.object], token)
            data_product_components = [
                get_on_end_point(c, token)[DataRegistryField.name]
                for c in obj[DataRegistryField.components]
            ]
        if data_product is not None:
            logger.info(
                f"Found data_product: {data_product} "
                f"and components: {data_product_components}"
            )
            data_product_component_pairs.append((data_product, data_product_components))
    if not data_product_component_pairs:
        raise ValueError(
            f"No data_product found for data_product '{parsed_config.data_product}'"
            f" with component '{parsed_config.component}'"
        )
    return data_product_component_pairs


def _write_metadata(
    data_product_name: str, version: str, namespace: str, accessibility: int, sha1_hash: str, components: List[str], filename: Path, stream: IO
) -> None:
    """
    For a given data product writes the associated metadata yaml, the base set of metadata is replicated for each
    component

    :param data_product_name:  name of the data_product
    :param version: version of the data_product
    :param namespace: the namespace of the data_product
    :param accessibility: the accessibility ordinal for this data product
    :param sha1_hash: verified hash of the data as provided by the registry
    :param components: list of components that this data_product contains
    :param filename: relative path of the downloaded file location (relative to the data directory)
    :param stream: IO stream to write the metadata to
    """
    base = {
        MetadataKey.data_product: data_product_name,
        MetadataKey.namespace: namespace,
        MetadataKey.accessibility: accessibility,
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
    data_product_name: str, data_product: Dict[str, str], data_directory: Path, token: str
) -> OutputInfo:
    """
    For a given data product generates the required set of information to download the data from the source
    to disk.

    :param data_product_name: name of the data_product, used to generate the download path
    :param data_product: data_product_version json data
    :param data_directory: base directory for downloading data to
    :param token: personal access token
    :return: required information to download the data from source to disk
    """
    # lookup the data at the data_product object url and retrieve the storage_location path
    storage_location_ref = get_on_end_point(data_product[DataRegistryField.object], token)[DataRegistryField.storage_location]
    storage_location = get_on_end_point(storage_location_ref, token)
    path = storage_location[DataRegistryField.path]

    # lookup the data at the store's store_root url and retrieve the uri
    store_root = get_on_end_point(storage_location[DataRegistryField.storage_root], token)
    uri = store_root[DataRegistryField.root]

    output_filename = (
            Path(data_product_name) / data_product[DataRegistryField.version] / Path(path).name
    )

    full_output_filename = Path(data_directory) / output_filename
    full_output_filename.parent.mkdir(parents=True, exist_ok=True)

    accessibility = store_root[DataRegistryField.accessibility]

    oi = OutputInfo(
        source_uri=uri,
        source_path=path,
        output_filename=output_filename,
        output_path=full_output_filename,
        hash=storage_location[DataRegistryField.hash],
        accessibility=accessibility,
    )
    logger.info(f"Constructed output information: {oi}")
    return oi


def _download_data(output_info: OutputInfo) -> None:
    """
    Reads the provided output information and downloads the data

    :param output_info: output information generated from the configuration and data registry data
    """
    if output_info.accessibility == 0:  # public
        logger.info(
            f"Downloading public data from uri: {output_info.source_uri}, path: {output_info.source_path} to "
            f"{output_info.output_path}"
        )
        protocol = urllib.parse.urlsplit(output_info.source_uri).scheme
        fs, source_path = get_remote_filesystem_and_path(
            protocol, output_info.source_uri, output_info.source_path
        )
        kwargs = {}
        if not isinstance(fs, SFTPFileSystem):
            if fs.isdir(source_path):
                kwargs["recursive"] = True
            kwargs["block_size"] = 0
        fs.get(source_path, output_info.output_path, **kwargs)
    else:
        logger.info(f"Data is not public, skipping download")


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
    data_directory = Path(data_directory)
    logger.info(f"Creating data directory if it does not exist: {data_directory}")
    data_directory.mkdir(parents=True, exist_ok=True)

    metadata_file = data_directory / "metadata.yaml"

    with open(metadata_file, "w") as metadata_stream:
        logger.info(f"Opened stream to metadata file: {metadata_file}")
        for read_config in read_configs:
            parsed_config = _parse_read_config(read_config, namespace)

            data_product_component_pairs = _get_data_product_version_and_components(
                parsed_config, data_registry_url, token
            )

            for data_product, data_product_components in data_product_component_pairs:
                output_info = _get_output_info(data_product[DataRegistryField.name], data_product, data_directory, token)
                ns = get_on_end_point(data_product[DataRegistryField.namespace], token)[DataRegistryField.name]
                _write_metadata(
                    data_product[DataRegistryField.name],
                    data_product[DataRegistryField.version],
                    ns,
                    output_info.accessibility,
                    output_info.hash,
                    data_product_components,
                    output_info.output_filename,
                    metadata_stream,
                )

                _download_data(output_info)


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
