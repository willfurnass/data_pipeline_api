import re
import urllib
from pathlib import Path
from typing import Optional, Dict, Union, List, Any, Tuple
import requests
import logging
import logging.config
from functools import lru_cache

import semver
from fsspec import AbstractFileSystem
from fsspec.implementations.ftp import FTPFileSystem
from fsspec.implementations.github import GithubFileSystem
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.sftp import SFTPFileSystem
from fsspec.utils import infer_storage_options
from s3fs import S3FileSystem

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

    accessibility = "accessibility"
    data_product = "data_product"
    data_product_type = "data_product_type"
    data_product_version = "data_product_version"
    data_product_version_component = "data_product_version_component"
    groups = "groups"
    issue = "issue"
    model = "model"
    model_run = "model_run"
    model_version = "model_version"
    processing_script = "processing_script"
    processing_script_version = "processing_script_version"
    storage_location = "storage_location"
    storage_root = "storage_root"
    storage_type = "storage_type"
    source = "source"
    source_type = "source_type"
    source_version = "source_version"
    users = "users"


class DataRegistryField:
    """
    Incomplete set of Data Registry Fields - only those that are being used as constants
    """

    inputs = "inputs"
    outputs = "outputs"
    supersedes = "supersedes"
    submission_script = "submission_script"
    model_config = "model_config"
    description = "description"
    run_id = "run_id"
    responsible_person = "responsible_person"
    name = "name"
    components = "components"
    version_identifier = "version_identifier"
    model = "model"
    processing_script = "processing_script"
    source = "source"
    data_product = "data_product"
    run_date = "run_date"
    model_version = "model_version"
    data_product_version = "data_product_version"
    username = "username"
    accessibility = "accessibility"
    store = "store"
    path = "path"
    store_root = "store_root"
    uri = "uri"
    type = "type"
    hash = "hash"


class DataRegistryFilter:
    """
    Full set of Data Registry Filters
    """

    version_identifier = DataRegistryField.version_identifier
    model = DataRegistryField.model
    processing_script = DataRegistryField.processing_script
    source = DataRegistryField.source
    data_product = DataRegistryField.data_product
    run_date = DataRegistryField.run_date
    model_version = DataRegistryField.model_version
    data_product_version = DataRegistryField.data_product_version
    name = DataRegistryField.name
    username = DataRegistryField.username


FILTERS = set(
    [
        a
        for a, v in DataRegistryFilter.__dict__.items()
        if not a.startswith("__") and not callable(getattr(DataRegistryFilter, a))
    ]
)


def sort_by_semver(items: List[Dict[str, Any]], descending: bool = True) -> List[Dict[str, Any]]:
    """
    Sorts a list of dicts containing a version identifier by semver VersionInfo, defaults to descending

    :param items: list of items to sort
    :param descending: sort order
    :return: Sorted list of items
    """
    return sorted(
        items, key=lambda data: semver.parse_version_info(data[DataRegistryFilter.version_identifier]),
        reverse=descending
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
        uri = urllib.parse.urljoin(uri, path)
        fs_class = S3FileSystem if protocol == "s3" else HTTPFileSystem
        return fs_class(**storage_options), uri
    elif protocol in {"sftp", "ssh", "ftp"}:
        inferred_options = infer_storage_options(uri)
        username = storage_options.pop("username", None) or inferred_options.get("username")
        password = storage_options.pop("password", None) or inferred_options.get("password")
        uri = (Path(inferred_options["path"]) / Path(path)).as_posix()
        fs_class = FTPFileSystem if protocol == "ftp" else SFTPFileSystem
        return fs_class(host=inferred_options["host"], username=username, password=password, **storage_options), uri
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


def build_query_string(query_data: YamlDict, data_registry_url: str) -> str:
    """
    Converts a dictionary of query data into a query string

    :param query_data: the query data dictionary to convert
    :param data_registry_url: the url of the data registry
    :return: the query string generated
    """
    def id_from_url(url: str):
        if url is not None and url.startswith(data_registry_url):
            return url.split("/")[-2]
        return url

    return urllib.parse.urlencode(
        {k: id_from_url(v) for k, v in query_data.items() if k in FILTERS and isinstance(v, str)}
    )


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
    return result.json()


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
    :return: reference url to existing data or None if it does not exist
    """
    query_str = build_query_string(query_data, data_registry_url)
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
