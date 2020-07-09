import logging
import logging.config
import os
import urllib
from hashlib import sha1
from pathlib import Path
from typing import Dict, Union, List, Any, Tuple, Optional
from datetime import datetime as dt

import click
import yaml

from data_pipeline_api.registry.common import (
    configure_cli_logging,
    YamlDict,
    DATA_REGISTRY_ACCESS_TOKEN,
    DATA_REGISTRY_URL,
    DEFAULT_DATA_REGISTRY_URL,
    DataRegistryField,
    DataRegistryFilter,
    get_data,
    DataRegistryTarget,
    get_remote_filesystem_and_path,
)
from data_pipeline_api.registry.upload import upload_from_config

logger = logging.getLogger(__name__)


def _create_target_data_dict(target: str, data: YamlDict) -> YamlDict:
    return {"target": target, "data": data}


def _get_input_url(
    data_product_name: str, version: str, component_name: str, data_registry_url: str, token: str
) -> str:
    """
    Gets the url reference of an input parameter
    
    :param data_product_name: Name of the data product 
    :param version: version of the data product 
    :param component_name: name of the data product component used as input
    :param data_registry_url: base url of the data registry
    :param token: personal access token
    :return: url reference to the input parameter data product version component
    """
    query_data = {
        DataRegistryFilter.name: data_product_name,
    }

    data_product = get_data(query_data, DataRegistryTarget.data_product, data_registry_url, token)

    query_data = {
        DataRegistryFilter.data_product: data_product["url"],
        DataRegistryFilter.version_identifier: version,
    }

    data_product_version = get_data(query_data, DataRegistryTarget.data_product_version, data_registry_url, token)
    query_data = {
        DataRegistryFilter.data_product_version: data_product_version["url"],
        DataRegistryFilter.name: component_name,
    }
    data_product_version_component = get_data(
        query_data, DataRegistryTarget.data_product_version_component, data_registry_url, token
    )
    url = data_product_version_component["url"]
    logger.info(f"Retrieved {url} for {data_product_name}/{component_name}/{version}")
    return url


def _verify_hash(filename: Path, access_calculated_hash: str) -> None:
    """
    Verifies the hash of the file matches the calculated hash from the access log
    
    :param filename: file to verify the hash of 
    :param access_calculated_hash: hash read from the access log for this filename
    """
    with open(filename, "rb") as file:
        calculated_hash = sha1(file.read()).hexdigest()
    if access_calculated_hash != calculated_hash:
        raise ValueError(
            f"access log contains hash {access_calculated_hash} but calculated hash of {filename} is {calculated_hash}"
        )


def upload_to_storage(remote_uri: str, storage_options: Dict[str, Any], data_directory: Path, filename: Path) -> str:
    """
    Uploads a file to the remote uri
     
    :param remote_uri: URI to the root of the storage
    :param storage_options: (key, value) pairs that are passed to the remote storage, e.g. credentials 
    :param data_directory: root of the data directory read from the access log
    :param filename: file to upload
    :return: path of the file on the remote storage
    """
    protocol = urllib.parse.urlsplit(remote_uri).scheme
    upload_path = filename.absolute().relative_to(data_directory.absolute()).as_posix()
    fs, path = get_remote_filesystem_and_path(protocol, remote_uri, upload_path, **storage_options)
    if protocol == "file":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Uploading {filename.as_posix()} to {path} on {remote_uri}")
    fs.put(filename.as_posix(), path)
    # some remote filesystems expect the root uri in the path, others don't, but the registry path doesn't
    return path.replace(remote_uri, "")


def _add_storage_type_and_root(
    posts: List[YamlDict], remote_uri: str, data_registry_url: str, token: str
) -> Tuple[YamlDict, YamlDict]:
    """
    Gets the storage type and root, adds them to the list of objects to post to the data registry, and returns them 
    
    :param posts: List of posts to the data registry, will be modified
    :param remote_uri: URI to the root of the storage
    :param data_registry_url: base url of the data registry
    :param token: personal access token 
    :return: the storage type and storage root dicts
    """
    scheme = urllib.parse.urlsplit(remote_uri).scheme
    storage_type = get_data(
        {DataRegistryFilter.name: scheme}, DataRegistryTarget.storage_type, data_registry_url, token
    )
    storage_root = None
    if storage_type is None:
        logger.info(f"No storage_type found for name {scheme}, creating storage_type and storage_root to POST.")
        storage_type = _create_target_data_dict(
            DataRegistryTarget.storage_type, {DataRegistryField.name: scheme, DataRegistryField.description: scheme}
        )
        storage_root = _create_target_data_dict(
            DataRegistryTarget.storage_root,
            {
                DataRegistryField.name: remote_uri,
                DataRegistryField.uri: remote_uri,
                DataRegistryField.description: remote_uri,
                DataRegistryField.type: storage_type,
            },
        )
        posts.extend([storage_type, storage_root])
    else:
        storage_roots = get_data({}, DataRegistryTarget.storage_root, data_registry_url, token, exact=False)
        for root in storage_roots:
            if root["type"] == storage_type["url"] and root["uri"] == remote_uri:
                storage_root = root["url"]
                logger.info(f"Found existing storage_root with name {root['name']}")
                break
        if storage_root is None:
            logger.info(
                f"No storage_type found for type {storage_type['name']} and uri {remote_uri}, creating new storage_root"
            )
            storage_root = _create_target_data_dict(
                DataRegistryTarget.storage_root,
                {
                    DataRegistryField.name: remote_uri,
                    DataRegistryField.uri: remote_uri,
                    DataRegistryField.description: remote_uri,
                    DataRegistryField.type: storage_type["url"],
                },
            )
            posts.append(storage_root)
    return storage_type, storage_root


def _add_data_product_output_posts(
    posts: List[YamlDict],
    path: str,
    data_product_name: str,
    model_version_str: str,
    run_id: str,
    component_name: str,
    accessibility: YamlDict,
    calculated_hash: str,
    responsible_person: YamlDict,
    storage_root: YamlDict,
) -> YamlDict:
    """
    Collates the required data registry posts for this write block of the model run access yaml and returns the ultimate
    output DataProductVersionComponent

    :param posts: List of posts to the data registry, will be modified
    :param path: StorageLocation path
    :param data_product_name: Name of the output data product
    :param model_version_str: version number of the model
    :param run_id: id of the run
    :param component_name: name of the output component
    :param accessibility: accessibility level of the output data product
    :param calculated_hash: calculated hash of the output data product file
    :param responsible_person: individual responsible for this output, read from the access yaml
    :param storage_root: StorageRoot that the path refers to
    :return: YamlDict representation of the DataProductVersionComponent output
    """
    storage_location = _create_target_data_dict(
        DataRegistryTarget.storage_location,
        {
            DataRegistryField.name: path,
            DataRegistryField.path: path,
            DataRegistryField.hash: calculated_hash,
            DataRegistryField.responsible_person: responsible_person,
            DataRegistryField.store_root: storage_root,
        },
    )
    data_product_type = _create_target_data_dict(
        DataRegistryTarget.data_product_type, {DataRegistryField.name: "output"}
    )
    data_product = _create_target_data_dict(
        DataRegistryTarget.data_product,
        {
            DataRegistryField.name: data_product_name,
            DataRegistryField.type: data_product_type,
            DataRegistryField.responsible_person: responsible_person,
        },
    )
    data_product_version = _create_target_data_dict(
        DataRegistryTarget.data_product_version,
        {
            DataRegistryField.version_identifier: f"{model_version_str}+{run_id}",
            DataRegistryField.responsible_person: responsible_person,
            DataRegistryField.data_product: data_product,
            DataRegistryField.store: storage_location,
            DataRegistryField.accessibility: accessibility,
        },
    )
    data_product_version_component = _create_target_data_dict(
        DataRegistryTarget.data_product_version_component,
        {
            DataRegistryField.name: component_name if component_name else data_product_name,
            DataRegistryField.responsible_person: responsible_person,
            DataRegistryField.data_product_version: data_product_version,
        },
    )
    posts.extend(
        [storage_location, data_product_type, data_product, data_product_version, data_product_version_component]
    )
    logger.debug(f"Creating DataProductVersionComponent: {data_product_version_component}")
    return data_product_version_component


def _add_model_run(
    posts: List[YamlDict],
    model_version_str: str,
    model_name: str,
    run_id: str,
    open_timestamp: dt,
    responsible_person: YamlDict,
    inputs: List[str],
    outputs: List[YamlDict],
    data_registry_url: str,
    token: str,
) -> None:
    """
    Generates the post required for adding a model run and appends it to the list of posts.

    :param posts: List of posts to the data registry, will be modified
    :param model_version_str: version number of the model
    :param model_name: name of the model
    :param run_id: id of this run, read from the access log
    :param open_timestamp: timestamp that the access log was first opened as the run date
    :param responsible_person: individual responsible for this model run, read from the access yaml
    :param inputs: List of input data product version component reference urls
    :param outputs: List of output data product version component YamlDicts
    :param data_registry_url: base url of the data registry
    :param token: personal access token
    """
    model = get_data({DataRegistryFilter.name: model_name}, DataRegistryTarget.model, data_registry_url, token)
    model_version = get_data(
        {DataRegistryFilter.version_identifier: model_version_str, DataRegistryFilter.model: model["url"]},
        DataRegistryTarget.model_version,
        data_registry_url,
        token,
    )
    model_run = _create_target_data_dict(
        DataRegistryTarget.model_run,
        {
            DataRegistryField.run_id: run_id,
            DataRegistryField.run_date: open_timestamp,
            DataRegistryField.description: run_id,
            DataRegistryField.model_config: "",
            DataRegistryField.submission_script: "",
            DataRegistryField.responsible_person: responsible_person,
            DataRegistryField.model_version: model_version["url"],
            DataRegistryField.supersedes: "",
            DataRegistryField.inputs: inputs,
            DataRegistryField.outputs: outputs,
        },
    )
    logger.debug(f"Created ModelRun: {model_run}")
    posts.append(model_run)


def unique_posts(posts: List[YamlDict]) -> List[YamlDict]:
    """
    Removes duplicate posts from the list of posts while maintaining their ordering.

    :param posts: List of posts to the data registry, will be modified
    :return: Unique list of posts to the data registry
    """
    set_of_yamls = {yaml.safe_dump(d): None for d in posts}
    logger.info(f"Removed {len(posts) - len(set_of_yamls)} duplicate POSTs.")
    return [yaml.safe_load(t) for t in set_of_yamls.keys()]


def upload_model_run(
    config_filename: Union[Path, str],
    remote_uri: str,
    remote_uri_override: Optional[str],
    storage_options: Dict[str, str],
    accessibility_name: str,
    data_registry_url: str,
    token: str,
) -> None:
    """
    Reads the provided configuration files and then calls PATCH or POST with the data to the data registry as
    appropriate, resolving references to other data where required.

    :param config_filename: file path to the configuration file
    :param remote_uri: URI to the root of the storage for uploading
    :param remote_uri_override: URI to the root of the storage that gets put into the data registry as the URI
    :param storage_options: (key, value) pairs that are passed to the remote storage, e.g. credentials
    :param accessibility_name: name of the accessibility level the outputs of this model run have
    :param data_registry_url: base url of the data registry
    :param token: personal access token
    """
    remote_uri_override = remote_uri_override or remote_uri
    config_filename = Path(config_filename)
    with open(config_filename, "r") as cf:
        config = yaml.safe_load(cf)
    data_directory = Path(config["data_directory"])
    if not data_directory.is_absolute():
        data_directory = config_filename.parent / data_directory
    run_id = config["run_id"]
    config_yaml = config["config"]
    responsible_person = _create_target_data_dict(
        DataRegistryTarget.users, {DataRegistryField.username: config["responsible_person"]}
    )
    model_version_str = config_yaml["model_version"]
    model_name = config_yaml["model_name"]
    accessibility = _create_target_data_dict(
        DataRegistryTarget.accessibility, {DataRegistryField.name: accessibility_name}
    )
    open_timestamp = config_yaml["open_timestamp"]
    inputs = []
    outputs = []
    posts = []

    storage_type, storage_root = _add_storage_type_and_root(posts, remote_uri_override, data_registry_url, token)

    for event in config["io"]:
        read = event["type"] == "read"
        metadata = event["access_metadata"]
        data_product_name = metadata["data_product"]
        component = metadata.get("component")
        version = metadata.get("version", "")
        access_calculated_hash = metadata["calculated_hash"]
        filename = data_directory / Path(metadata["filename"])
        if read:
            inputs.append(_get_input_url(data_product_name, version, component, data_registry_url, token))
        else:
            _verify_hash(filename, access_calculated_hash)

            path = upload_to_storage(remote_uri, storage_options, data_directory, filename)

            data_product_component_version = _add_data_product_output_posts(
                posts,
                path,
                data_product_name,
                model_version_str,
                run_id,
                component,
                accessibility,
                access_calculated_hash,
                responsible_person,
                storage_root,
            )
            outputs.append(data_product_component_version)

    _add_model_run(
        posts,
        model_version_str,
        model_name,
        run_id,
        open_timestamp,
        responsible_person,
        inputs,
        outputs,
        data_registry_url,
        token,
    )

    posts = unique_posts(posts)

    upload_from_config({"post": posts}, data_registry_url, token)


@click.command()
@click.option(
    "--config", required=True, type=str, help=f"Path to the access yaml file.",
)
@click.option("--remote-uri", "-u", required=True, type=str, help=f"URI to the root of the storage")
@click.option(
    "--remote-option",
    "-o",
    nargs=2,
    multiple=True,
    type=click.Tuple([str, str]),
    help="(key, value) pairs that are passed to the remote storage, e.g. credentials",
)
@click.option("--remote-uri-override", type=str, help=f"URI to the root of the storage to post in the registry"
                                                            f" required if the uri to use for download from the registry"
                                                            f" is different from that used to upload the item")
@click.option("--accessibility", type=str, default="public", help=f"accessibility of the data, defaults to public")
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
def upload_model_run_cli(config, remote_uri, remote_option, remote_uri_override, accessibility, data_registry, token):
    configure_cli_logging()
    data_registry = data_registry or os.environ.get(DATA_REGISTRY_URL, DEFAULT_DATA_REGISTRY_URL)
    token = token or os.environ.get(DATA_REGISTRY_ACCESS_TOKEN)
    remote_options = dict(remote_option) if remote_option else {}
    remote_uri_override = remote_uri_override or remote_uri
    accessibility = accessibility or "public"
    if not token:
        raise ValueError(
            f"Personal Access Token must be provided through either --token cmd line arg "
            f"or environment variable {DATA_REGISTRY_ACCESS_TOKEN}"
        )
    upload_model_run(
        config_filename=config,
        remote_uri=remote_uri,
        remote_uri_override=remote_uri_override,
        storage_options=remote_options,
        accessibility_name=accessibility,
        data_registry_url=data_registry,
        token=token,
    )


if __name__ == "__main__":
    logger = logging.getLogger(f"{__package__}.{__name__}")
    upload_model_run_cli()
