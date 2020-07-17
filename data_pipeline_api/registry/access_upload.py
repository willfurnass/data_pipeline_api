import logging
import logging.config
import urllib
from pathlib import Path
from typing import Dict, Union, List, Any, Optional
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
    get_data,
    DataRegistryTarget,
    get_remote_filesystem_and_path,
)
from data_pipeline_api.registry.upload import upload_from_config
from data_pipeline_api.file_api import FileAPI

logger = logging.getLogger(__name__)


def _create_target_data_dict(target: str, data: YamlDict) -> YamlDict:
    return {"target": target, "data": data}


def _get_input_url(
    data_product_name: str, namespace: str, version: str, component_name: str, data_registry_url: str, token: str
) -> str:
    """
    Gets the url reference of an input parameter
    
    :param data_product_name: Name of the data product
    :param namespace: namespace that the data product is a member of
    :param version: version of the data product 
    :param component_name: name of the data product component used as input
    :param data_registry_url: base url of the data registry
    :param token: personal access token
    :return: url reference to the input parameter data product version component
    """
    query_data = {
        DataRegistryField.namespace: namespace,
        DataRegistryField.name: data_product_name,
        DataRegistryField.version: version,
    }

    data_product = get_data(query_data, DataRegistryTarget.data_product, data_registry_url, token)
    obj = data_product["object"]

    query_data = {
        DataRegistryField.object: obj,
        DataRegistryField.name: component_name,
    }
    object_component = get_data(query_data, DataRegistryTarget.object_component, data_registry_url, token)
    url = object_component["url"]
    logger.info(f"Retrieved {url} for {namespace}/{data_product_name}/{component_name}/{version}")
    return url


def _verify_hash(filename: Path, access_calculated_hash: str) -> None:
    """
    Verifies the hash of the file matches the calculated hash from the access log
    
    :param filename: file to verify the hash of 
    :param access_calculated_hash: hash read from the access log for this filename
    """
    calculated_hash = FileAPI.calculate_hash(filename)
    if access_calculated_hash != calculated_hash:
        raise ValueError(
            f"access log contains hash {access_calculated_hash} but calculated hash of {filename} is {calculated_hash}"
        )


def upload_to_storage(
    remote_uri: str,
    storage_options: Dict[str, Any],
    data_directory: Path,
    filename: Path,
    upload_path: Optional[Union[str, Path]] = None,
) -> str:
    """
    Uploads a file to the remote uri
     
    :param remote_uri: URI to the root of the storage
    :param storage_options: (key, value) pairs that are passed to the remote storage, e.g. credentials 
    :param data_directory: root of the data directory read from the access log
    :param filename: file to upload
    :param upload_path: optional override to the upload path of the file
    :return: path of the file on the remote storage
    """
    protocol = urllib.parse.urlsplit(remote_uri).scheme
    upload_path = upload_path or filename.absolute().relative_to(data_directory.absolute()).as_posix()
    fs, path = get_remote_filesystem_and_path(protocol, remote_uri, upload_path, **storage_options)
    if protocol in {"file", "ssh", "sftp"}:
        fs.makedirs(Path(path).parent.as_posix(), exist_ok=True)
    logger.info(f"Uploading {filename.as_posix()} to {path} on {remote_uri}")
    fs.put(filename.as_posix(), path)
    # some remote filesystems expect the root uri in the path, others don't, but the registry path doesn't
    return path.replace(remote_uri, "")


def _add_storage_root(
    posts: List[YamlDict], remote_uri: str, accessibility: int, data_registry_url: str, token: str
) -> Union[YamlDict, str]:
    """
    Gets the storage root, adds it to the list of objects to post to the data registry, and returns them
    
    :param posts: List of posts to the data registry, will be modified
    :param remote_uri: URI to the root of the storage
    :param accessibility: accessibility level of the storage root
    :param data_registry_url: base url of the data registry
    :param token: personal access token 
    :return: the storage root dict or reference url
    """
    storage_root = get_data(
        {DataRegistryField.root: remote_uri}, DataRegistryTarget.storage_root, data_registry_url, token
    )
    if storage_root is None:
        logger.info(f"No storage_root found for {remote_uri}, creating new storage_root")
        storage_root = _create_target_data_dict(
            DataRegistryTarget.storage_root,
            {
                DataRegistryField.name: remote_uri,
                DataRegistryField.root: remote_uri,
                DataRegistryField.accessibility: accessibility,
            },
        )
        posts.append(storage_root)
    else:
        storage_root = storage_root["url"]
    return storage_root


def _add_data_product_output_posts(
    posts: List[YamlDict],
    path: str,
    data_product_name: str,
    namespace: str,
    model_version_str: str,
    run_id: str,
    component_name: str,
    calculated_hash: str,
    storage_root: YamlDict,
) -> YamlDict:
    """
    Collates the required data registry posts for this write block of the model run access yaml and returns the ultimate
    output ObjectComponent

    :param posts: List of posts to the data registry, will be modified
    :param path: StorageLocation path
    :param data_product_name: Name of the output data product
    :param namespace: namespace that the data product is a member of
    :param model_version_str: version number of the model
    :param run_id: id of the run
    :param component_name: name of the output component
    :param calculated_hash: calculated hash of the output data product file
    :param storage_root: StorageRoot that the path refers to
    :return: YamlDict representation of the ObjectComponent output
    """
    storage_location = _create_target_data_dict(
        DataRegistryTarget.storage_location,
        {
            DataRegistryField.path: path,
            DataRegistryField.hash: calculated_hash,
            DataRegistryField.storage_root: storage_root,
        },
    )
    obj = _create_target_data_dict(DataRegistryTarget.object, {DataRegistryField.storage_location: storage_location})
    data_product = _create_target_data_dict(
        DataRegistryTarget.data_product,
        {
            DataRegistryField.name: data_product_name,
            DataRegistryField.version: f"{model_version_str}+{run_id}",
            DataRegistryField.namespace: namespace,
            DataRegistryField.object: obj,
        },
    )
    object_component = _create_target_data_dict(
        DataRegistryTarget.object_component,
        {
            DataRegistryField.name: component_name if component_name else data_product_name,
            DataRegistryField.object: obj,
        },
    )
    posts.extend([storage_location, obj, data_product, object_component])
    logger.debug(f"Creating ObjectComponent: {object_component}")
    return object_component


def _add_model_run(
    posts: List[YamlDict],
    run_id: str,
    open_timestamp: dt,
    inputs: List[str],
    outputs: List[YamlDict],
    model_config: YamlDict,
    submission_script: YamlDict,
    code_repo: YamlDict,
) -> None:
    """
    Generates the post required for adding a model run and appends it to the list of posts.

    :param posts: List of posts to the data registry, will be modified
    :param run_id: id of this run, read from the access log
    :param open_timestamp: timestamp that the access log was first opened as the run date
    :param inputs: List of input data product version component reference urls
    :param outputs: List of output data product version component YamlDicts
    :param model_config: model config target for this code run
    :param submission_script: submission script target for this code run
    :param code_repo: code repo script target for this code run
    """
    code_run = _create_target_data_dict(
        DataRegistryTarget.code_run,
        {
            DataRegistryField.run_date: open_timestamp,
            DataRegistryField.run_identifier: run_id,
            DataRegistryField.code_repo: code_repo,
            DataRegistryField.model_config: model_config,
            DataRegistryField.submission_script: submission_script,
            DataRegistryField.inputs: inputs,
            DataRegistryField.outputs: outputs,
        },
    )
    logger.debug(f"Created ModelRun: {code_run}")
    posts.append(code_run)


def unique_posts(posts: List[YamlDict]) -> List[YamlDict]:
    """
    Removes duplicate posts from the list of posts while maintaining their ordering.

    :param posts: List of posts to the data registry, will be modified
    :return: Unique list of posts to the data registry
    """
    set_of_yamls = {yaml.safe_dump(d): None for d in posts}
    logger.info(f"Removed {len(posts) - len(set_of_yamls)} duplicate POSTs.")
    return [yaml.safe_load(t) for t in set_of_yamls.keys()]


def _get_accessibility(config):
    if all(int(event.get("accessibility", 0)) == 0 for event in config["io"]):
        return 0
    else:
        return max(int(event.get("accessibility", 0)) for event in config["io"])


def to_github_uri(input_uri: str, git_sha: str = "master") -> str:
    """
    A basic and crude function that attempts to convert an https://github or git@github.com uri to a github
    format uri of github://<org>:<repo>@<sha>/<path>. if conversion can't be done the original uri is returned

    :param input_uri: the input uri
    :param git_sha: the git sha of where this uri points, defaults to master
    :return: github format uri of github://<org>:<repo>@<sha>/<path>
    """
    if input_uri.startswith("https://github.com/"):
        try:
            split_uri = input_uri.split("/")
            org = split_uri[3]
            repo = split_uri[4]
            path = "/".join(split_uri[5:])
            github_uri = f"github://{org}:{repo}@{git_sha}/{path}"
        except IndexError:
            github_uri = input_uri
    elif input_uri.startswith("git@github.com:"):
        split_uri = input_uri.replace("git@github.com:", "").split("/")
        org = split_uri[0]
        repo = split_uri[1].replace(".git", "")
        github_uri = f"github://{org}:{repo}@{git_sha}/"
    else:
        github_uri = input_uri
    return github_uri


def _add_code_repo(
    posts: List[YamlDict], model_name: str, model_version: str, model_git_sha: str, model_uri: str
) -> YamlDict:
    """
    Creates a code_repo_release and storage_location for the model and adds them to the list of objects to post to the
    data registry

    :param posts: List of posts to the data registry, will be modified
    :param model_name: name of the model
    :param model_version: version of the model
    :param model_git_sha: git sha of the model
    :param model_uri: uri that the model is located at
    :return: the object reference to the code repo release
    """
    storage_root_uri = to_github_uri(model_uri, model_git_sha)

    code_repo_storage = _create_target_data_dict(
        DataRegistryTarget.storage_root, {DataRegistryField.name: model_uri, DataRegistryField.root: storage_root_uri,},
    )
    code_repo_location = _create_target_data_dict(
        DataRegistryTarget.storage_location,
        {
            DataRegistryField.path: "/",
            DataRegistryField.hash: model_git_sha,
            DataRegistryField.storage_root: code_repo_storage,
        },
    )
    code_repo_obj = _create_target_data_dict(
        DataRegistryTarget.object,
        {
            DataRegistryField.description: f"{model_name}+{model_version}",
            DataRegistryField.storage_location: code_repo_location,
        },
    )
    code_repo_release = _create_target_data_dict(
        DataRegistryTarget.code_repo_release,
        {
            DataRegistryField.name: model_name,
            DataRegistryField.version: model_version,
            DataRegistryField.website: model_uri,
            DataRegistryField.object: code_repo_obj,
        },
    )
    posts.extend([code_repo_storage, code_repo_location, code_repo_obj, code_repo_release])

    return code_repo_obj


def _upload_file_to_storage(
    posts: List[YamlDict],
    filename: Union[str, Path],
    remote_uri: str,
    storage_options: Dict[str, str],
    storage_root: Union[str, YamlDict],
) -> YamlDict:
    """
    for a given filename, uploads it to the remote uri and returns a reference to the object that will be posted

    :param posts: List of posts to the data registry, will be modified
    :param filename: path to the file to upload
    :param remote_uri: URI to the root of the storage for uploading
    :param storage_options: (key, value) pairs that are passed to the remote storage, e.g. credentials
    :param storage_root: existing reference to the storage_root that this was uploaded to
    :return: object reference to the uploaded file
    """
    filename = Path(filename)
    path = upload_to_storage(remote_uri, storage_options, filename.parent, filename)
    file_hash = FileAPI.calculate_hash(filename)
    location = _create_target_data_dict(
        DataRegistryTarget.storage_location,
        {
            DataRegistryField.path: path,
            DataRegistryField.hash: file_hash,
            DataRegistryField.storage_root: storage_root,
        },
    )
    posts.append(location)
    obj = _create_target_data_dict(DataRegistryTarget.object, {DataRegistryField.storage_location: location})
    posts.append(obj)
    return obj


def upload_model_run(
    config_filename: Union[Path, str],
    model_config_filename: Union[Path, str],
    submission_script_filename: Union[Path, str],
    remote_uri: str,
    remote_uri_override: Optional[str],
    storage_options: Dict[str, str],
    data_registry_url: str,
    token: str,
) -> None:
    """
    Reads the provided configuration files and then calls PATCH or POST with the data to the data registry as
    appropriate, resolving references to other data where required.

    :param config_filename: file path to the configuration file
    :param model_config_filename: file path to the model configuration file
    :param submission_script_filename: file path to the submission script file
    :param remote_uri: URI to the root of the storage for uploading
    :param remote_uri_override: URI to the root of the storage that gets put into the data registry as the URI
    :param storage_options: (key, value) pairs that are passed to the remote storage, e.g. credentials
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
    accessibility = _get_accessibility(config)
    namespace = config.get("namespace")
    model_version_str = config_yaml["model_version"]
    model_name = config_yaml["model_name"]
    open_timestamp = config_yaml["open_timestamp"]
    model_git_sha = config["metadata"]["git_sha"]
    model_uri = config["metadata"]["uri"]

    inputs = []
    outputs = []
    posts = []

    storage_root = _add_storage_root(posts, remote_uri_override, accessibility, data_registry_url, token)
    code_repo = _add_code_repo(posts, model_name, model_version_str, model_git_sha, model_uri)

    for event in config["io"]:
        read = event["type"] == "read"
        metadata = event["access_metadata"]
        data_product_name = metadata["data_product"]
        component = metadata.get("component")
        version = metadata.get("version", "")
        event_namespace = metadata.get("namespace", namespace)
        if event_namespace is None:
            raise ValueError(f"No namespace specified for {event}")
        access_calculated_hash = metadata["calculated_hash"]
        filename = data_directory / Path(metadata["filename"])
        if read:
            inputs.append(
                _get_input_url(data_product_name, event_namespace, version, component, data_registry_url, token)
            )
        else:
            _verify_hash(filename, access_calculated_hash)

            path = upload_to_storage(remote_uri, storage_options, data_directory, filename)

            object_component = _add_data_product_output_posts(
                posts,
                path,
                data_product_name,
                event_namespace,
                model_version_str,
                run_id,
                component,
                access_calculated_hash,
                storage_root,
            )
            outputs.append(object_component)

    model_config_obj = _upload_file_to_storage(posts, model_config_filename, remote_uri, storage_options, storage_root)
    submission_script_obj = _upload_file_to_storage(
        posts, submission_script_filename, remote_uri, storage_options, storage_root
    )

    _add_model_run(
        posts, run_id, open_timestamp, inputs, outputs, model_config_obj, submission_script_obj, code_repo,
    )

    posts = unique_posts(posts)

    upload_from_config({"post": posts}, data_registry_url, token)


@click.command(context_settings=dict(max_content_width=200))
@click.option(
    "--config", required=True, type=click.Path(exists=True), help="Path to the access yaml file.",
)
@click.option(
    "--model-config", required=True, type=click.Path(exists=True), help="Path to the model config yaml file.",
)
@click.option(
    "--submission-script", required=True, type=click.Path(exists=True), help="Path to the submission script file.",
)
@click.option("--remote-uri", "-u", required=True, type=str, help="URI to the root of the storage")
@click.option(
    "--remote-option",
    "-o",
    nargs=2,
    multiple=True,
    type=click.Tuple([str, str]),
    help="(key, value) pairs that are passed to the remote storage, e.g. credentials",
)
@click.option(
    "--remote-uri-override",
    type=str,
    help="URI to the root of the storage to post in the registry"
    " required if the URI to use for download from the registry"
    " is different from that used to upload the item",
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
def upload_model_run_cli(
    config, model_config, submission_script, remote_uri, remote_option, remote_uri_override, data_registry, token
):
    configure_cli_logging()
    data_registry = data_registry or DEFAULT_DATA_REGISTRY_URL
    remote_options = dict(remote_option) if remote_option else {}
    remote_uri_override = remote_uri_override or remote_uri
    if not token:
        raise ValueError(
            f"Personal Access Token must be provided through either --token cmd line arg "
            f"or environment variable {DATA_REGISTRY_ACCESS_TOKEN}"
        )
    upload_model_run(
        config_filename=config,
        model_config_filename=model_config,
        submission_script_filename=submission_script,
        remote_uri=remote_uri,
        remote_uri_override=remote_uri_override,
        storage_options=remote_options,
        data_registry_url=data_registry,
        token=token,
    )


if __name__ == "__main__":
    logger = logging.getLogger(f"{__package__}.{__name__}")
    # pylint: disable=no-value-for-parameter
    upload_model_run_cli()
