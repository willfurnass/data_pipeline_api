import math
import os
import re
import socket
import urllib
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Union, List, Any, Tuple, Set
import requests
import logging
import logging.config
from functools import lru_cache

import semver
import yaml
from fsspec import AbstractFileSystem
from fsspec.implementations.ftp import FTPFileSystem
from fsspec.implementations.github import GithubFileSystem
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.sftp import SFTPFileSystem
from fsspec.utils import infer_storage_options
from s3fs import S3FileSystem

from data_pipeline_api.file_api import FileAPI

logger = logging.getLogger(__name__)

DATA_REGISTRY_URL = "DATA_REGISTRY_URL"
DATA_REGISTRY_ACCESS_TOKEN = "DATA_REGISTRY_ACCESS_TOKEN"

DEFAULT_DATA_REGISTRY_URL = "https://data.scrc.uk/api/"


YamlDict = Dict[str, Union[str, "YamlDict"]]
JsonResult = Union[List[Dict[str, str]], Dict[str, str]]


class DataRegistryTarget:
    """
    Full set of Data Registry end points
    """

    users = "users"
    groups = "groups"
    issue = "issue"
    object = "object"
    object_component = "object_component"
    code_run = "code_run"
    storage_root = "storage_root"
    storage_location = "storage_location"
    source = "source"
    external_object = "external_object"
    quality_controlled = "quality_controlled"
    keyword = "keyword"
    author = "author"
    licence = "licence"
    namespace = "namespace"
    data_product = "data_product"
    code_repo_release = "code_repo_release"
    key_value = "key_value"
    text_file = "text_file"


class DataRegistryField:
    """
    Incomplete set of Data Registry Fields - only those that are being used as constants
    """

    abbreviation = "abbreviation"
    accessibility = "accessibility"
    authors = "authors"
    code_repo = "code_repo"
    code_repo_release = "code_repo_release"
    components = "components"
    data_product = "data_product"
    description = "description"
    doi_or_unique_name = "doi_or_unique_name"
    email = "email"
    external_object = "external_object"
    family_name = "family_name"
    full_name = "full_name"
    hash = "hash"
    inputs = "inputs"
    issues = "issues"
    key = "key"
    keyphrase = "keyphrase"
    keywords = "keywords"
    last_updated = "last_updated"
    licence_info = "licence_info"
    licences = "licences"
    model_config = "model_config"
    name = "name"
    namespace = "namespace"
    object = "object"
    orgs = "orgs"
    original_store = "original_store"
    outputs = "outputs"
    path = "path"
    personal_name = "personal_name"
    primary_not_supplement = "primary_not_supplement"
    quality_control = "quality_control"
    release_date = "release_date"
    root = "root"
    run_date = "run_date"
    run_identifier = "run_identifier"
    severity = "severity"
    source = "source"
    storage_location = "storage_location"
    storage_root = "storage_root"
    submission_script = "submission_script"
    text = "text"
    title = "title"
    updated_by = "updated_by"
    url = "url"
    username = "username"
    value = "value"
    version = "version"
    website = "website"


def sort_by_semver(items: List[Dict[str, Any]], descending: bool = True, key: Any = DataRegistryField.version) -> List[Dict[str, Any]]:
    """
    Sorts a list of dicts containing a version identifier by semver VersionInfo, defaults to descending

    :param items: list of items to sort
    :param descending: sort order
    :param key: the key to get the version field
    :return: Sorted list of items
    """
    return sorted(
        items, key=lambda data: semver.parse_version_info(data[key]), reverse=descending,
    )


def get_remote_filesystem_and_path(
    protocol: str, uri: str, path: str, **storage_options: Dict[str, Any]
) -> Tuple[AbstractFileSystem, str]:
    """
    For a given protocol, root uri, path and kwargs returns the appropriate FileSystem representation and the
    representation of path on this FileSystem required to access path.

    :param protocol: protocol of the target FileSystem, e.g. https, file, github
    :param uri: uri of the root of the FileSystem, from where path is expected to join
    :param path: path to the location in the context of the uri
    :param storage_options: arguments passed through to the FileSystem instantiation
    :return: The FileSystem and access path on this FileSystem
    """
    if protocol == "file":
        storage_options.setdefault("auto_mkdir", True)
        uri = Path(uri.replace("file://", "")) / Path(path)
        return LocalFileSystem(**storage_options), uri.as_posix()
    elif protocol in {"http", "https", "s3"}:
        # storage_options are parameters passed to request
        path = urllib.parse.quote(path)
        uri = "/".join(s.strip("/") for s in [uri, path])
        fs_class = S3FileSystem if protocol == "s3" else HTTPFileSystem
        return fs_class(**storage_options), uri
    elif protocol in {"sftp", "ssh", "ftp"}:
        inferred_options = infer_storage_options(uri)
        username = storage_options.pop("username", None) or inferred_options.get("username")
        password = storage_options.pop("password", None) or inferred_options.get("password")
        uri = (Path(inferred_options["path"]) / Path(path)).as_posix()
        fs_class = FTPFileSystem if protocol == "ftp" else SFTPFileSystem
        fs = fs_class(host=inferred_options["host"], username=username, password=password, **storage_options)
        if protocol == "ftp":
            try:
                fs.ftp.dir()
            except (TimeoutError, socket.timeout):
                fs.ftp.set_pasv(False)
        return fs, uri
    elif protocol == "github":
        if re.match(r"\w+/\w+", uri):
            uri = f"github://{uri.split('/')[0]}:{uri.split('/')[1]}@master/"
        # infer options on a github uri reads the org, repo and sha incorrectly (as if it were an ftp uri)
        # this is because it uses urllib.parse.urlsplit under the hood
        inferred_options = infer_storage_options(uri)
        org = inferred_options.get("username")
        repo = inferred_options.get("password")
        sha = inferred_options.get("host") or "master"
        path = (Path(inferred_options.get("path", "")) / Path(path)).as_posix()
        return GithubFileSystem(org=org, repo=repo, sha=sha, **storage_options), path
    else:
        raise NotImplementedError(f"Unsupported remote filesystem {protocol}:{uri}")


def get_end_point(data_registry_url: str, target: str) -> str:
    """
    Returns a data registry end point for the given data registry base url and target name

    :param data_registry_url: base url of the data registry
    :param target: target end point of the data registry
    :return: end point for this target on the data registry
    """
    return urllib.parse.urljoin(data_registry_url + "/", target + "/")


def get_headers(token: str) -> Dict[str, str]:
    """
    Returns authorization header for the data registry with the provided token.

    :param token: personal access token
    :return: Authorization headers
    """
    return {"Authorization": f"token {token}"} if token else {}


def get_filter_fields(target: str, data_registry_url: str, token: str) -> Set[str]:
    """
    Returns a list of filterable fields from a target end point by calling OPTIONS

    :param target: target end point of the data registry
    :param data_registry_url: the url of the data registry
    :param token: personal access token
    :return: the set of filterable fields on this target end point
    """
    end_point = get_end_point(data_registry_url, target)
    result = requests.options(end_point, headers=get_headers(token))
    result.raise_for_status()
    options = result.json()
    return set(options.get("filter_fields", []))


def build_query_string(query_data: YamlDict, target: str, data_registry_url: str, token: str) -> str:
    """
    Converts a dictionary of query data into a query string

    :param query_data: the query data dictionary to convert
    :param target: target end point of the data registry
    :param data_registry_url: the url of the data registry
    :param token: personal access token
    :return: the query string generated
    """
    def process(value: Any):
        if isinstance(value, str):
            if value is not None and value.startswith(data_registry_url):
                # retrieve the id from a url
                id_ = value.split("/")[-2]
                try:
                    int(id_)
                except ValueError:
                    return value  # it was some other reference than an id reference
                return id_
            return value
        elif isinstance(value, datetime):
            return f"{value.isoformat()}Z"
        else:
            return None

    fields = get_filter_fields(target, data_registry_url, token)

    processed = {k: process(v) for k, v in query_data.items()}
    valid = {k: v for k, v in processed.items() if k in fields and v is not None}

    return urllib.parse.urlencode(valid)


@lru_cache(maxsize=None)
def get_on_end_point(end_point: str, token: str, query_str: Optional[str] = None) -> JsonResult:
    """
    Calls GET on the target end point of the data registry and returns the result

    :param end_point: url of the data registry to get
    :param token: personal access token
    :param query_str: optional query string to append to the end point
    :return: data returned from calling GET on the end point
    """
    end_point = f"{end_point}{'?' + query_str if query_str else ''}"
    logger.info(f"GET {end_point}")
    result = requests.get(end_point, headers=get_headers(token))
    result.raise_for_status()
    logger.info(f"GET successful: {result.status_code}")
    json_result = result.json()
    if isinstance(json_result, List):
        return json_result
    else:
        if all(k in json_result for k in ("next", "results")):  # paginated
            results = json_result["results"]
            count = json_result.get("count")
            if count:
                pages = math.ceil(count / len(results))
                logger.info(f"{pages} of results returned")
            while json_result.get("next"):
                next_end_point = json_result.get("next")
                logger.info(f"GET {next_end_point}")
                result = requests.get(next_end_point, headers=get_headers(token))
                result.raise_for_status()
                logger.info(f"GET successful: {result.status_code}")
                json_result = result.json()
                results.extend(json_result["results"])
            return results
        else:
            return json_result


def get_data(
    query_data: YamlDict, target: str, data_registry_url: str, token: str, exact: bool = True
) -> Optional[Union[Dict[str, str], List[Dict[str, str]]]]:
    """
    Gets the full set of data matching the provided data at the target data registry end point. If the data is not
    present None is returned.

    :param query_data: Dict of data, keys are str and values are either str or a nested dict referring to another piece of data
    :param target: target end point of the data registry
    :param data_registry_url: base url of the data registry
    :param token: personal access token
    :param exact: If exact is True: 1 result -> return that result, 2+ results -> raise an error, 0 results -> return None
                  If exact is False: 1+ results -> return results in list, 0 results -> return None
    :return: existing data or None if it does not exist
    """
    query_str = build_query_string(query_data, target, data_registry_url, token)
    result = get_on_end_point(get_end_point(data_registry_url, target), token, query_str)
    if not result:
        logger.info(f"No matching data found for query '{query_str}' to target '{target}'")
        return None
    elif not exact:
        logger.info(f"{len(result)} matching data item(s) found for query '{query_str}' to target '{target}'")
        return result
    elif len(result) == 1:
        logger.info(f"1 matching data item found for exact query '{query_str}' to target '{target}'")
        return result[0]
    else:
        raise ValueError(
            f"{len(result)} matching data item(s) found for exact query '{query_str}' to target '{target}'"
        )


def get_reference(query_data: YamlDict, target: str, data_registry_url: str, token: str) -> Optional[str]:
    """
    Gets the reference url for the provided data at the target data registry end point. If the data is not present None
    is returned.

    :param query_data: Dict of data, keys are str and values are either str or a nested dict referring to another piece of data
    :param target: target end point of the data registry
    :param data_registry_url: base url of the data registry
    :param token: personal access token
    :return: reference url to existing data or None if it does not exist
    """
    result = get_data(query_data, target, data_registry_url, token)
    if result:
        reference = result["url"]
        logger.info(f"Found reference: {reference} from target '{target}'")
        return reference
    logger.info(f"No reference found from target '{target}'")
    return None


def upload_to_storage(
        remote_uri: str,
        storage_options: Dict[str, Any],
        data_directory: Path,
        filename: Path,
        upload_path: Optional[Union[str, Path]] = None,
        path_prefix: Optional[str] = None,
) -> str:
    """
    Uploads a file to the remote uri

    :param remote_uri: URI to the root of the storage
    :param storage_options: (key, value) pairs that are passed to the remote storage, e.g. credentials
    :param data_directory: root of the data directory read from the access log
    :param filename: file to upload
    :param upload_path: optional override to the upload path of the file
    :param path_prefix: Optional prefix onto the remote path, e.g. namespace
    :return: path of the file on the remote storage
    """
    split_result = urllib.parse.urlsplit(remote_uri)
    protocol = split_result.scheme
    path_prefix = Path(path_prefix) if path_prefix else Path()
    upload_path = (path_prefix / (upload_path or filename.absolute().relative_to(data_directory.absolute()))).as_posix()
    fs, path = get_remote_filesystem_and_path(protocol, remote_uri, upload_path, **storage_options)
    if protocol in {"file", "ssh", "sftp"}:
        fs.makedirs(Path(path).parent.as_posix(), exist_ok=True)
    sha1 = FileAPI.calculate_hash(filename)
    path_root, path_ext = os.path.splitext(path)
    path = f"{path_root}_{sha1}{path_ext}"
    logger.info(f"Uploading {filename.as_posix()} to {path} on {remote_uri}")
    fs.put(filename.as_posix(), path)
    if path.startswith(remote_uri):
        # some remote filesystems expect the root uri in the path, others don't, but the registry path doesn't
        return path[len(remote_uri):]
    elif path.startswith(split_result.path):
        # some remote_uri's include part of what the fs considers the path, so strip it off
        return path[len(split_result.path):]
    return path


def configure_cli_logging():
    logconf = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"standard": {"format": "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",},},
        "handlers": {
            "stdout": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {__package__: {"handlers": ["stdout"], "level": "DEBUG",},},
    }
    logging.config.dictConfig(logconf)


def unique_dicts(list_of_dicts: List[Dict[Any, Any]]) -> List[Dict[Any, Any]]:
    """
    Removes duplicate dictsfrom the list of dicts while maintaining their ordering.

    :param list_of_dicts: List of dicts
    :return: Unique list of dicts
    """
    set_of_yamls = {yaml.dump(d): None for d in list_of_dicts}
    return [yaml.load(t, Loader=yaml.FullLoader) for t in set_of_yamls.keys()]
