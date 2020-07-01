import json
import logging
import logging.config
import os
import urllib
from hashlib import sha1
from pathlib import Path
from typing import Dict, Union, List, Any, Tuple
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
    return data_product_version_component["url"]


def _verify_hash(filename: Path, access_calculated_hash: str) -> None:
    with open(filename, "rb") as file:
        calculated_hash = sha1(file.read()).hexdigest()
    if access_calculated_hash != access_calculated_hash:
        raise ValueError(
            f"access log contains hash {access_calculated_hash} but calculated hash of {filename} is {calculated_hash}"
        )


def upload_to_storage(uri: str, storage_options: Dict[str, Any], data_directory: Path, filename: Path) -> str:
    protocol = urllib.parse.urlsplit(uri).scheme
    upload_path = filename.absolute().relative_to(data_directory.absolute()).as_posix()
    fs, path = get_remote_filesystem_and_path(protocol, uri, upload_path, storage_options)
    fs.upload(filename.as_posix(), path)
    return path


def _add_storage_type_and_root(
    posts: List[YamlDict], remote_uri: str, data_registry_url: str, token: str
) -> Tuple[YamlDict, YamlDict]:
    scheme = urllib.parse.urlsplit(remote_uri).scheme
    storage_type = get_data(
        {DataRegistryFilter.name: scheme}, DataRegistryTarget.storage_type, data_registry_url, token
    )
    storage_root = None
    if storage_type is None:
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
                storage_root = root
                break
    return storage_type, storage_root


def _add_data_registry_posts(
    posts: List[YamlDict],
    path: str,
    data_product_name: str,
    model_version_str: str,
    run_id: str,
    component_name: str,
    accessibility: str,
    calculated_hash: str,
    responsible_person: str,
    storage_root: YamlDict,
) -> YamlDict:
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
        [storage_location, data_product_type, data_product, data_product_version, data_product_version_component,]
    )
    return data_product_version_component


def _add_model_run(
    posts: List[YamlDict],
    model_version_str: str,
    model_name: str,
    run_id: str,
    responsible_person: str,
    inputs: List[str],
    outputs: List[YamlDict],
    data_registry_url: str,
    token: str,
) -> None:
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
            DataRegistryField.release_id: run_id,
            DataRegistryField.release_date: dt.now(),
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
    posts.append(model_run)


def unique_posts(posts: List[YamlDict]) -> List[YamlDict]:
    set_of_jsons = {json.dumps(d, sort_keys=True) for d in posts}
    return [json.loads(t) for t in set_of_jsons]


def upload_model_run(
    config_filename: Union[Path, str],
    remote_uri: str,
    storage_options: Dict[str, str],
    accessibility: str,
    data_registry_url: str,
    token: str,
) -> None:
    """
    Reads the provided configuration files and then calls PATCH or POST with the data to the data registry as
    appropriate, resolving references to other data where required.

    :param config_filename: file path to the configuration file
    :param remote_uri:
    :param storage_options:
    :param accessibility:
    :param data_registry_url: base url of the data registry
    :param token: github personal access token
    """
    config_filename = Path(config_filename)
    with open(config_filename, "r") as cf:
        config = yaml.safe_load(cf)
    data_directory = Path(config["data_directory"])
    if not data_directory.is_absolute():
        data_directory = config_filename.parent / data_directory
    run_id = config["run_id"]
    config_yaml = config["config"]
    responsible_person = config_yaml["responsible_person"]
    model_version_str = config_yaml["model_version"]
    model_name = config_yaml["model_name"]

    inputs = []
    outputs = []
    posts = []

    storage_type, storage_root = _add_storage_type_and_root(posts, remote_uri, data_registry_url, token)

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

            data_product_component_version = _add_data_registry_posts(
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
        posts, model_version_str, model_name, run_id, responsible_person, inputs, outputs, data_registry_url, token
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
def upload_model_run_cli(config, remote_uri, remote_option, accessibility, data_registry, token):
    configure_cli_logging()
    data_registry = data_registry or os.environ.get(DATA_REGISTRY_URL, DEFAULT_DATA_REGISTRY_URL)
    token = token or os.environ.get(DATA_REGISTRY_ACCESS_TOKEN)
    remote_options = dict(remote_option) if remote_option else {}
    accessibility = accessibility or "public"
    if not token:
        raise ValueError(
            f"Personal Access Token must be provided through either --token cmd line arg "
            f"or environment variable {DATA_REGISTRY_ACCESS_TOKEN}"
        )
    upload_model_run(
        config_filename=config,
        remote_uri=remote_uri,
        storage_options=remote_options,
        accessibility=accessibility,
        data_registry_url=data_registry,
        token=token,
    )


if __name__ == "__main__":
    logger = logging.getLogger(f"{__package__}.{__name__}")
    upload_model_run_cli()
