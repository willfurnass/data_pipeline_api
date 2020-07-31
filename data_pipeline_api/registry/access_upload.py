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
    unique_dicts,
    get_reference,
    upload_to_storage,
)
from data_pipeline_api.registry.upload import upload_from_config, upload_to_text_table
from data_pipeline_api.file_api import FileAPI, RunMetadata
from data_pipeline_api.metadata import MetadataKey
from data_pipeline_api.registry.utils import (
    get_remote_options,
    get_data_registry_url,
)

logger = logging.getLogger(__name__)


def _create_target_data_dict(target: str, data: YamlDict) -> YamlDict:
    return {"target": target, "data": data}


def _get_data_product_url(
    data_product_name: str,
    namespace: str,
    version: str,
    component_name: str,
    data_registry_url: str,
    token: str,
) -> str:
    """
    Gets the url reference of a data product
    
    :param data_product_name: Name of the data product
    :param namespace: namespace that the data product is a member of
    :param version: version of the data product 
    :param component_name: name of the data product component used as input
    :param data_registry_url: base url of the data registry
    :param token: personal access token
    :return: url reference to the data product version component
    """
    namespace_ref = get_reference(
        {DataRegistryField.name: namespace},
        DataRegistryTarget.namespace,
        data_registry_url,
        token,
    )
    if namespace_ref is None:
        raise ValueError(f"No namespace found for {namespace}")

    query_data = {
        DataRegistryField.namespace: namespace_ref,
        DataRegistryField.name: data_product_name,
        DataRegistryField.version: version,
    }

    data_product = get_data(
        query_data, DataRegistryTarget.data_product, data_registry_url, token
    )
    obj = data_product["object"]

    query_data = {
        DataRegistryField.object: obj,
        DataRegistryField.name: component_name,
    }
    object_component = get_data(
        query_data, DataRegistryTarget.object_component, data_registry_url, token
    )
    url = object_component["url"]
    logger.info(
        f"Retrieved {url} for {namespace}/{data_product_name}/{version}/{component_name}"
    )
    return url


def _get_external_object_url(
    doi_or_unique_name: str,
    version: str,
    component_name: str,
    data_registry_url: str,
    token: str,
) -> str:
    """
    Gets the url reference of an external object

    :param doi_or_unique_name: Identifier of the external object
    :param version: version of the external object
    :param component_name: name of the external object component used as input
    :param data_registry_url: base url of the data registry
    :param token: personal access token
    :return: url reference to the external object version component
    """
    query_data = {
        DataRegistryField.doi_or_unique_name: doi_or_unique_name,
        DataRegistryField.version: version,
    }

    data_product = get_data(
        query_data, DataRegistryTarget.external_object, data_registry_url, token
    )
    obj = data_product["object"]

    query_data = {
        DataRegistryField.object: obj,
        DataRegistryField.name: component_name,
    }
    object_component = get_data(
        query_data, DataRegistryTarget.object_component, data_registry_url, token
    )
    url = object_component["url"]
    logger.info(f"Retrieved {url} for {doi_or_unique_name}/{version}/{component_name}")
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


def _add_storage_root(
    posts: List[YamlDict],
    remote_uri: str,
    accessibility: int,
    data_registry_url: str,
    token: str,
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
        {DataRegistryField.root: remote_uri},
        DataRegistryTarget.storage_root,
        data_registry_url,
        token,
    )
    if storage_root is None:
        logger.info(
            f"No storage_root found for {remote_uri}, creating new storage_root"
        )
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
    namespace_str: str,
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
    :param namespace_str: namespace that the data product is a member of
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
    obj = _create_target_data_dict(
        DataRegistryTarget.object,
        {DataRegistryField.storage_location: storage_location},
    )
    namespace = _create_target_data_dict(
        DataRegistryTarget.namespace, {DataRegistryField.name: namespace_str}
    )
    data_product = _create_target_data_dict(
        DataRegistryTarget.data_product,
        {
            DataRegistryField.name: data_product_name,
            DataRegistryField.version: f"{run_id}",
            DataRegistryField.namespace: namespace,
            DataRegistryField.object: obj,
        },
    )
    object_component = _create_target_data_dict(
        DataRegistryTarget.object_component,
        {
            DataRegistryField.name: component_name
            if component_name
            else data_product_name,
            DataRegistryField.object: obj,
        },
    )
    posts.extend([storage_location, obj, namespace, data_product, object_component])
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
    description: str,
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
    :param description: description of this code run
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
            DataRegistryField.description: description,
        },
    )
    logger.debug(f"Created ModelRun: {code_run}")
    posts.append(code_run)


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
            repo = repo[:-len(".git")] if repo.endswith(".git") else repo
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
    posts: List[YamlDict],
    model_git_sha: str,
    model_uri: str,
    model_name: Optional[str],
    model_version: Optional[str],
) -> YamlDict:
    """
    Creates a code_repo and storage_location, and optionally a code_repo_release for the
    model and adds them to the list of objects to post to the data registry

    :param posts: List of posts to the data registry, will be modified
    :param model_git_sha: git sha of the model
    :param model_uri: uri that the model is located at
    :param model_name: name of the model
    :param model_version: version of the model
    :return: the object reference to the code repo
    """
    storage_root_uri = to_github_uri(model_uri, model_git_sha)

    if storage_root_uri != model_uri:
        # if it's changed, it's now a github uri, we know the split to the first @ is going to be the root to sha split
        root, sha_and_path = storage_root_uri.split("@", maxsplit=1)
        path = "@" + sha_and_path
    else:
        root = storage_root_uri
        path = "/"

    code_repo_storage = _create_target_data_dict(
        DataRegistryTarget.storage_root,
        {DataRegistryField.name: root, DataRegistryField.root: root,},
    )
    code_repo_location = _create_target_data_dict(
        DataRegistryTarget.storage_location,
        {
            DataRegistryField.path: path,
            DataRegistryField.hash: model_git_sha,
            DataRegistryField.storage_root: code_repo_storage,
        },
    )
    code_repo_obj = _create_target_data_dict(
        DataRegistryTarget.object,
        {
            DataRegistryField.description: f"{storage_root_uri}",
            DataRegistryField.storage_location: code_repo_location,
        },
    )
    posts.extend(
        [code_repo_storage, code_repo_location, code_repo_obj]
    )
    if model_name is not None and model_version is not None:
        code_repo_release = _create_target_data_dict(
            DataRegistryTarget.code_repo_release,
            {
                DataRegistryField.name: model_name,
                DataRegistryField.version: model_version,
                DataRegistryField.website: model_uri,
                DataRegistryField.object: code_repo_obj,
            },
        )
        posts.append(code_repo_release)

    return code_repo_obj


def _upload_file_to_text_table(
    posts: List[YamlDict],
    filename: Union[str, Path],
    data_registry_url: str,
    token: str,
) -> YamlDict:
    """
    for a given filename, uploads its contents to the text_file table and returns a reference to the object that will be
    posted

    :param posts: List of posts to the data registry, will be modified
    :param filename: path to the file to upload
    :param data_registry_url: base url of the data registry
    :param token: personal access token
    :return: object reference to the uploaded file
    """
    filename = Path(filename)
    location = upload_to_text_table(filename, data_registry_url, token)
    obj = _create_target_data_dict(
        DataRegistryTarget.object, {DataRegistryField.storage_location: location}
    )
    posts.append(obj)
    return obj


def _upload_file_to_storage(
    posts: List[YamlDict],
    filename: Union[str, Path],
    remote_uri: str,
    storage_options: Dict[str, str],
    storage_root: Union[str, YamlDict],
    namespace: Optional[str] = None,
) -> YamlDict:
    """
    for a given filename, uploads it to the remote uri and returns a reference to the object that will be posted

    :param posts: List of posts to the data registry, will be modified
    :param filename: path to the file to upload
    :param remote_uri: URI to the root of the storage for uploading
    :param storage_options: (key, value) pairs that are passed to the remote storage, e.g. credentials
    :param storage_root: existing reference to the storage_root that this was uploaded to
    :param namespace: namespace of the file being uploaded, if provided will be prefixed onto the upload path
    :return: object reference to the uploaded file
    """
    filename = Path(filename)
    path = upload_to_storage(remote_uri, storage_options, filename.parent, filename, path_prefix=namespace)
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
    obj = _create_target_data_dict(
        DataRegistryTarget.object, {DataRegistryField.storage_location: location}
    )
    posts.append(obj)
    return obj


def upload_model_run(
    config_filename: Union[Path, str],
    model_config_filename: Union[Path, str],
    submission_script_filename: Union[Path, str],
    remote_options: Dict[str, str],
    token: str,
    text_file_table: bool = True,
) -> None:
    """
    Reads the provided configuration files and then calls PATCH or POST with the data to the data registry as
    appropriate, resolving references to other data where required.

    :param config_filename: file path to the configuration file
    :param model_config_filename: file path to the model configuration file
    :param submission_script_filename: file path to the submission script file
    :param remote_options: (key, value) pairs that are passed to the remote storage, e.g. credentials
    :param token: personal access token
    :param text_file_table: If true, model_config and submission_script are uploaded to the text_file table in the data_registry
    """
    config_filename = Path(config_filename)
    with open(config_filename, "r") as cf:
        config = yaml.safe_load(cf)

    accessibility = _get_accessibility(config)
    run_metadata = config["run_metadata"]

    data_directory = Path(run_metadata[RunMetadata.data_directory])
    if not data_directory.is_absolute():
        data_directory = config_filename.parent / data_directory
    run_id = run_metadata[RunMetadata.run_id]
    namespace = run_metadata.get(RunMetadata.default_output_namespace)
    model_version_str = run_metadata.get(RunMetadata.model_version)
    model_name = run_metadata.get(RunMetadata.model_name)
    open_timestamp = run_metadata[RunMetadata.open_timestamp]
    model_git_sha = run_metadata[RunMetadata.git_sha]
    model_uri = run_metadata[RunMetadata.git_repo]
    remote_uri = run_metadata[RunMetadata.remote_uri]
    remote_uri_override = run_metadata.get(RunMetadata.remote_uri_override, remote_uri)
    data_registry_url = run_metadata.get(
        RunMetadata.data_registry_url, get_data_registry_url()
    )
    description = run_metadata[RunMetadata.description]

    inputs = []
    outputs = []
    posts = []

    storage_root = _add_storage_root(
        posts, remote_uri_override, accessibility, data_registry_url, token
    )
    code_repo = _add_code_repo(
        posts, model_git_sha, model_uri, model_name, model_version_str
    )

    for event in config["io"]:
        read = event["type"] == "read"
        metadata = event["access_metadata"]
        component = metadata.get(MetadataKey.component)
        version = metadata.get(MetadataKey.version, "")
        access_calculated_hash = metadata[MetadataKey.calculated_hash]
        filename = data_directory / Path(metadata[MetadataKey.filename])
        if MetadataKey.data_product in metadata:
            data_product_name = metadata[MetadataKey.data_product]
            event_namespace = metadata.get(MetadataKey.namespace, namespace)
            if event_namespace is None:
                raise ValueError(f"No namespace specified for {event}")
            if read:
                inputs.append(
                    _get_data_product_url(
                        data_product_name,
                        event_namespace,
                        version,
                        component,
                        data_registry_url,
                        token,
                    )
                )
            else:
                _verify_hash(filename, access_calculated_hash)

                path = upload_to_storage(
                    remote_uri, remote_options, data_directory, filename, path_prefix=namespace
                )

                object_component = _add_data_product_output_posts(
                    posts,
                    path,
                    data_product_name,
                    event_namespace,
                    run_id,
                    component,
                    access_calculated_hash,
                    storage_root,
                )
                outputs.append(object_component)
        elif MetadataKey.doi_or_unique_name in metadata:
            doi_or_unique_name = metadata[MetadataKey.doi_or_unique_name]
            if read:
                inputs.append(
                    _get_external_object_url(
                        doi_or_unique_name, version, component, data_registry_url, token
                    )
                )
            else:
                raise ValueError("can only read external objects")
        else:
            raise ValueError(
                "metadata did not contain a data product or an external object"
            )

    if text_file_table:
        model_config_obj = _upload_file_to_text_table(
            posts, model_config_filename, data_registry_url, token
        )
        submission_script_obj = _upload_file_to_text_table(
            posts, submission_script_filename, data_registry_url, token
        )
    else:
        model_config_obj = _upload_file_to_storage(
            posts, model_config_filename, remote_uri, remote_options, storage_root, namespace
        )
        submission_script_obj = _upload_file_to_storage(
            posts, submission_script_filename, remote_uri, remote_options, storage_root, namespace
        )

    _add_model_run(
        posts,
        run_id,
        open_timestamp,
        inputs,
        outputs,
        model_config_obj,
        submission_script_obj,
        code_repo,
        description,
    )

    posts = unique_dicts(posts)

    upload_from_config({"post": posts}, data_registry_url, token)


@click.command(context_settings=dict(max_content_width=200))
@click.option(
    "--config",
    required=True,
    type=click.Path(exists=True),
    help="Path to the access yaml file.",
)
@click.option(
    "--model-config",
    required=True,
    type=click.Path(exists=True),
    help="Path to the model config yaml file.",
)
@click.option(
    "--submission-script",
    required=True,
    type=click.Path(exists=True),
    help="Path to the submission script file.",
)
@click.option(
    "--remote-option",
    "-o",
    nargs=2,
    multiple=True,
    type=click.Tuple([str, str]),
    help="(key, value) pairs that are passed to the remote storage, e.g. credentials",
)
@click.option(
    "--token",
    type=str,
    envvar=f"{DATA_REGISTRY_ACCESS_TOKEN}",
    help=f"data registry access token. Defaults to {DATA_REGISTRY_ACCESS_TOKEN} env if not passed."
    f" access tokens can be created from the data registry's get-token end point",
)
@click.option(
    "--text-file-table/--no-text-file-table",
    default=True,
    help="Whether to upload the model-config and submission-script to the text_file table of the data registry "
    "(default), or to the remote-uri",
)
def upload_model_run_cli(
    config, model_config, submission_script, remote_option, token, text_file_table,
):
    configure_cli_logging()
    remote_options = get_remote_options()
    arg_remote_options = dict(remote_option) if remote_option else {}
    remote_options.update(arg_remote_options)
    if not token:
        raise ValueError(
            f"Personal Access Token must be provided through either --token cmd line arg "
            f"or environment variable {DATA_REGISTRY_ACCESS_TOKEN}"
        )
    upload_model_run(
        config_filename=config,
        model_config_filename=model_config,
        submission_script_filename=submission_script,
        text_file_table=text_file_table,
        remote_options=remote_options,
        token=token,
    )


if __name__ == "__main__":
    logger = logging.getLogger(f"{__package__}.{__name__}")
    # pylint: disable=no-value-for-parameter
    upload_model_run_cli()
