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
    title = "title"
    updated_by = "updated_by"
    url = "url"
    username = "username"
    value = "value"
    version = "version"
    website = "website"


DATA_REGISTRY_FILTERS = {
    DataRegistryTarget.users: {DataRegistryField.username},
    DataRegistryTarget.groups: set(),
    DataRegistryTarget.issue: {DataRegistryField.name},
    DataRegistryTarget.object: {
        DataRegistryField.last_updated,
        DataRegistryField.updated_by,
        DataRegistryField.storage_location,
        DataRegistryField.data_product,
        DataRegistryField.code_repo_release,
        DataRegistryField.object,
    },
    DataRegistryTarget.object_component: {
        DataRegistryField.name,
        DataRegistryField.last_updated,
        DataRegistryField.object,
    },
    DataRegistryTarget.code_run: {
        DataRegistryField.run_date,
        DataRegistryField.run_identifier,
        DataRegistryField.last_updated,
    },
    DataRegistryTarget.storage_root: {
        DataRegistryField.name,
        DataRegistryField.root,
        DataRegistryField.last_updated,
        DataRegistryField.accessibility,
    },
    DataRegistryTarget.storage_location: {
        DataRegistryField.last_updated,
        DataRegistryField.path,
        DataRegistryField.hash,
    },
    DataRegistryTarget.source: {DataRegistryField.last_updated, DataRegistryField.name, DataRegistryField.abbreviation},
    DataRegistryTarget.external_object: {
        DataRegistryField.last_updated,
        DataRegistryField.doi_or_unique_name,
        DataRegistryField.release_date,
        DataRegistryField.title,
        DataRegistryField.version,
    },
    DataRegistryTarget.quality_controlled: set(),
    DataRegistryTarget.keyword: {DataRegistryField.last_updated, DataRegistryField.keyphrase},
    DataRegistryTarget.author: {
        DataRegistryField.last_updated,
        DataRegistryField.family_name,
        DataRegistryField.personal_name,
    },
    DataRegistryTarget.licence: {DataRegistryField.last_updated},
    DataRegistryTarget.namespace: {DataRegistryField.last_updated, DataRegistryField.name},
    DataRegistryTarget.data_product: {
        DataRegistryField.last_updated,
        DataRegistryField.namespace,
        DataRegistryField.name,
        DataRegistryField.version,
    },
    DataRegistryTarget.code_repo_release: {
        DataRegistryField.last_updated,
        DataRegistryField.name,
        DataRegistryField.version,
    },
    DataRegistryTarget.key_value: {DataRegistryField.last_updated, DataRegistryField.key},
}


def sort_by_semver(items: List[Dict[str, Any]], descending: bool = True) -> List[Dict[str, Any]]:
    """
    Sorts a list of dicts containing a version identifier by semver VersionInfo, defaults to descending

    :param items: list of items to sort
    :param descending: sort order
    :return: Sorted list of items
    """
    return sorted(
        items, key=lambda data: semver.parse_version_info(data[DataRegistryField.version]), reverse=descending,
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
            except TimeoutError:
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


def build_query_string(query_data: YamlDict, target: str, data_registry_url: str) -> str:
    """
    Converts a dictionary of query data into a query string

    :param query_data: the query data dictionary to convert
    :param target: target end point of the data registry
    :param data_registry_url: the url of the data registry
    :return: the query string generated
    """

    def id_from_url(url: str):
        if url is not None and url.startswith(data_registry_url):
            return url.split("/")[-2]
        return url

    filters = DATA_REGISTRY_FILTERS.get(target, set())

    return urllib.parse.urlencode(
        {k: id_from_url(v) for k, v in query_data.items() if k in filters and isinstance(v, str)}
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
    :return: existing data or None if it does not exist
    """
    query_str = build_query_string(query_data, target, data_registry_url)
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
