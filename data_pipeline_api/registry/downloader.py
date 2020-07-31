import fnmatch
import itertools
import logging
import os
import re
import urllib
from collections import defaultdict
from functools import partial
from pathlib import Path

from typing import Dict, Optional, List, Tuple, Any, Union, IO

import yaml
from fsspec.implementations.sftp import SFTPFileSystem

from data_pipeline_api.registry.common import (
    get_data,
    DataRegistryField,
    DataRegistryTarget,
    DATA_REGISTRY_URL,
    DEFAULT_DATA_REGISTRY_URL,
    DATA_REGISTRY_ACCESS_TOKEN,
    sort_by_semver,
    get_on_end_point,
    get_remote_filesystem_and_path,
    unique_dicts,
)
from data_pipeline_api.metadata import MetadataKey

logger = logging.getLogger(__name__)

DownloaderDict = Dict[Union[Tuple[str, str], str], Any]

OUTPUT_FILENAME = "output_filename"
FULL_OUTPUT_FILENAME = "full_output_filename"


class Downloader:
    """
    Class to handle downloading data products and external objects from the data registry to disk
    """

    def __init__(
        self, data_directory: Union[Path, str], data_registry_url: Optional[str] = None, token: Optional[str] = None
    ) -> None:
        """
        :param data_directory: The directory to download data to
        :param data_registry_url: base url of the data registry
        :param token: personal access token
        """
        self._data_directory = Path(data_directory)
        self._data_registry_url: str = data_registry_url or os.environ.get(DATA_REGISTRY_URL, DEFAULT_DATA_REGISTRY_URL)
        self._token: str = token or os.environ.get(DATA_REGISTRY_ACCESS_TOKEN)

        self._data_products: List[Dict[Tuple[str, str], str]] = []
        self._external_objects: List[Dict[Tuple[str, str], str]] = []
        self._resolved_data_products: List[DownloaderDict] = []
        self._resolved_external_objects: List[DownloaderDict] = []

    def add_data_product(
        self, namespace: str, data_product: str, component: Optional[str] = None, version: Optional[str] = None
    ) -> None:
        """
        Registers a data product to be downloaded
        """
        self._data_products.append(
            {
                (DataRegistryTarget.namespace, DataRegistryField.name): namespace,
                (DataRegistryTarget.data_product, DataRegistryField.name): data_product,
                (DataRegistryTarget.object_component, DataRegistryField.name): component,
                (DataRegistryTarget.data_product, DataRegistryField.version): version,
            }
        )

    def add_external_object(self, doi_or_unique_name: str, title: Optional[str] = None, component: Optional[str] = None, version: Optional[str] = None) -> None:
        """
        Registers an external object to be downloaded
        """
        self._external_objects.append(
            {
                (DataRegistryTarget.external_object, DataRegistryField.doi_or_unique_name): doi_or_unique_name,
                (DataRegistryTarget.external_object, DataRegistryField.title): title,
                (DataRegistryTarget.object_component, DataRegistryField.name): component,
                (DataRegistryTarget.external_object, DataRegistryField.version): version,
            }
        )

    def _resolve_namespaces(self, input_blocks: List[DownloaderDict]) -> List[DownloaderDict]:
        resolved = []
        for block in input_blocks:
            namespace_name = block[DataRegistryTarget.namespace, DataRegistryField.name]
            namespaces = get_data(
                {DataRegistryField.name: namespace_name},
                DataRegistryTarget.namespace,
                self._data_registry_url,
                self._token,
                exact=False,
            )
            if namespaces:
                for namespace in namespaces:
                    cblock = block.copy()
                    for k, v in namespace.items():
                        cblock[DataRegistryTarget.namespace, k] = v
                    resolved.append(cblock)
        return resolved

    def _resolve_data_products(self, input_blocks: List[DownloaderDict]) -> List[DownloaderDict]:
        resolved = []
        for block in input_blocks:
            query_data = {
                DataRegistryField.name: block[DataRegistryTarget.data_product, DataRegistryField.name],
                DataRegistryField.namespace: block[DataRegistryTarget.namespace, DataRegistryField.url],
            }
            version = block.get((DataRegistryTarget.data_product, DataRegistryField.version))
            if version is not None:
                query_data[DataRegistryField.version] = version
            data_products = get_data(
                query_data, DataRegistryTarget.data_product, self._data_registry_url, self._token, exact=False
            )
            if data_products:
                data_products = sort_by_semver(data_products)
                if block.get((DataRegistryTarget.object_component, DataRegistryField.name)) is None:
                    # if globbing has been used we might have multiple data products so take the first
                    # as we've sorted by semver, by name
                    grouped_data_products = {}
                    for data_product in data_products:
                        data_product_name = data_product[DataRegistryField.name]
                        if data_product_name not in grouped_data_products:
                            grouped_data_products[data_product_name] = data_product
                    data_products = list(grouped_data_products.values())
                for data_product in data_products:
                    cblock = block.copy()
                    for k, v in data_product.items():
                        cblock[DataRegistryTarget.data_product, k] = v
                    resolved.append(cblock)
        return resolved

    def _resolve_objects(self, input_blocks: List[DownloaderDict], external: bool = False) -> List[DownloaderDict]:
        resolved = []
        for block in input_blocks:
            obj = None
            component = block.get((DataRegistryTarget.object_component, DataRegistryField.name))
            target = DataRegistryTarget.external_object if external else DataRegistryTarget.data_product
            object_ref = block[target, DataRegistryField.object]
            if component:
                # if a component is specified, only resolve objects that have that component
                components = get_data(
                    {DataRegistryField.name: component, DataRegistryField.object: object_ref},
                    DataRegistryTarget.object_component,
                    self._data_registry_url,
                    self._token,
                    exact=False,
                )
                if components:
                    obj = get_on_end_point(object_ref, self._token)
            else:
                obj = get_on_end_point(object_ref, self._token)
                components = obj[DataRegistryField.components]
            if components:
                cblock = block.copy()
                for k, v in obj.items():
                    cblock[DataRegistryTarget.object, k] = v
                resolved.append(cblock)
        return resolved

    def _resolve_components(self, input_blocks: List[DownloaderDict], external: bool = False) -> List[DownloaderDict]:
        if not external:
            grouped = defaultdict(list)
            for block in input_blocks:
                grouped[
                    block[DataRegistryTarget.namespace, DataRegistryField.name],
                    block[DataRegistryTarget.data_product, DataRegistryField.name],
                    block[DataRegistryTarget.object_component, DataRegistryField.name],
                ].append(block)
            versioned_blocks = []
            for k, v in grouped.items():
                versioned_blocks.append(
                    next(iter(sort_by_semver(v, key=(DataRegistryTarget.data_product, DataRegistryField.version))))
                )
        else:
            grouped = defaultdict(list)
            for block in input_blocks:
                grouped[
                    block[DataRegistryTarget.external_object, DataRegistryField.doi_or_unique_name],
                    block[DataRegistryTarget.external_object, DataRegistryField.title],
                    block[DataRegistryTarget.object_component, DataRegistryField.name],
                ].append(block)
            versioned_blocks = []
            for k, v in grouped.items():
                versioned_blocks.append(
                    next(iter(sort_by_semver(v, key=(DataRegistryTarget.external_object, DataRegistryField.version))))
                )

        resolved = []
        for block in versioned_blocks:
            cname = block.get((DataRegistryTarget.object_component, DataRegistryField.name))
            components = block[DataRegistryTarget.object, DataRegistryField.components]
            for component_url in components:
                cblock = block.copy()
                component = get_on_end_point(component_url, self._token)
                if not cname or re.match(fnmatch.translate(cname), component[DataRegistryField.name]):
                    for k, v in component.items():
                        cblock[DataRegistryTarget.object_component, k] = v
                    resolved.append(cblock)
        return resolved

    def _resolve_storage_locations(
        self, input_blocks: List[DownloaderDict], external: bool = False
    ) -> List[DownloaderDict]:
        resolved = []
        for block in input_blocks:
            storage_location = get_on_end_point(
                block[DataRegistryTarget.object, DataRegistryField.storage_location], self._token
            )
            cblock = block.copy()
            for k, v in storage_location.items():
                cblock[DataRegistryTarget.storage_location, k] = v
            target = DataRegistryTarget.external_object if external else DataRegistryTarget.data_product
            name_fields = (DataRegistryField.doi_or_unique_name, DataRegistryField.title, DataRegistryField.version) if external else (DataRegistryField.name, DataRegistryField.version)
            name = Path("/".join(filter(None, (cblock.get((target, name_field)) for name_field in name_fields))))
            output_filename = (
                name / Path(cblock[DataRegistryTarget.storage_location, DataRegistryField.path]).name
            )
            cblock[OUTPUT_FILENAME] = output_filename.as_posix()
            cblock[FULL_OUTPUT_FILENAME] = (self._data_directory / output_filename).as_posix()
            resolved.append(cblock)
        return resolved

    def _resolve_storage_roots(self, input_blocks: List[DownloaderDict]) -> List[DownloaderDict]:
        resolved = []
        for block in input_blocks:
            storage_root = get_on_end_point(
                block[DataRegistryTarget.storage_location, DataRegistryField.storage_root], self._token
            )
            cblock = block.copy()
            for k, v in storage_root.items():
                cblock[DataRegistryTarget.storage_root, k] = v
            resolved.append(cblock)

        return resolved

    def _resolve_external_objects(self, input_blocks: List[DownloaderDict]) -> List[DownloaderDict]:
        resolved = []
        for block in input_blocks:
            query_data = {
                DataRegistryField.doi_or_unique_name: block[
                    DataRegistryTarget.external_object, DataRegistryField.doi_or_unique_name
                ]
            }
            version = block.get((DataRegistryTarget.external_object, DataRegistryField.version))
            if version is not None:
                query_data[DataRegistryField.version] = version
            title = block.get((DataRegistryTarget.external_object, DataRegistryField.title))
            if title is not None:
                query_data[DataRegistryField.title] = title
            external_objects = get_data(
                query_data, DataRegistryTarget.external_object, self._data_registry_url, self._token, exact=False
            )
            if external_objects:
                external_objects = sort_by_semver(external_objects)
                if block.get((DataRegistryTarget.object_component, DataRegistryField.name)) is None:
                    grouped_external_objects = {}
                    for external_object in external_objects:
                        external_object_name = external_object[DataRegistryField.doi_or_unique_name]
                        external_object_title = external_object[DataRegistryField.title]
                        if (external_object_name, external_object_title) not in grouped_external_objects:
                            grouped_external_objects[external_object_name, external_object_title] = external_object
                    external_objects = list(grouped_external_objects.values())
                for external_object in external_objects:
                    cblock = block.copy()
                    for k, v in external_object.items():
                        cblock[DataRegistryTarget.external_object, k] = v
                    resolved.append(cblock)
        return resolved

    def _write_metadata_data_product(self, stream: IO):
        metadatas = []
        for block in self._resolved_data_products:
            metadata = {
                MetadataKey.data_product: block[DataRegistryTarget.data_product, DataRegistryField.name],
                MetadataKey.namespace: block[DataRegistryTarget.namespace, DataRegistryField.name],
                MetadataKey.accessibility: block[DataRegistryTarget.storage_root, DataRegistryField.accessibility],
                MetadataKey.version: block[DataRegistryTarget.data_product, DataRegistryField.version],
                MetadataKey.verified_hash: block[DataRegistryTarget.storage_location, DataRegistryField.hash],
                MetadataKey.extension: Path(block[OUTPUT_FILENAME]).suffix[1:],
                MetadataKey.filename: block[OUTPUT_FILENAME],
                MetadataKey.component: block[DataRegistryTarget.object_component, DataRegistryField.name],
            }
            metadatas.append(metadata)
        if metadatas:
            yaml.safe_dump(metadatas, stream)

    def _write_metadata_external_object(self, stream):
        metadatas = []
        for block in self._resolved_external_objects:
            metadata = {
                MetadataKey.doi_or_unique_name: block[
                    DataRegistryTarget.external_object, DataRegistryField.doi_or_unique_name
                ],
                MetadataKey.title: block[
                    DataRegistryTarget.external_object, DataRegistryField.title
                ],
                MetadataKey.accessibility: block[DataRegistryTarget.storage_root, DataRegistryField.accessibility],
                MetadataKey.version: block[DataRegistryTarget.external_object, DataRegistryField.version],
                MetadataKey.verified_hash: block[DataRegistryTarget.storage_location, DataRegistryField.hash],
                MetadataKey.extension: Path(block[OUTPUT_FILENAME]).suffix[1:],
                MetadataKey.filename: block[OUTPUT_FILENAME],
                MetadataKey.component: block[DataRegistryTarget.object_component, DataRegistryField.name],
            }
            metadatas.append(metadata)
        if metadatas:
            yaml.safe_dump(metadatas, stream)

    def _download(self):
        downloaded_hashes = set()
        for block in itertools.chain(self._resolved_data_products, self._resolved_external_objects):
            accessibility = block[DataRegistryTarget.storage_root, DataRegistryField.accessibility]
            block_hash = block[DataRegistryTarget.storage_location, DataRegistryField.hash]
            if block_hash in downloaded_hashes:
                logger.debug(f"Storage location with hash {block_hash} has already been downloaded, skipping download")
            elif accessibility == 0:  # public
                source_uri = block[DataRegistryTarget.storage_root, DataRegistryField.root]
                source_path = block[DataRegistryTarget.storage_location, DataRegistryField.path]
                output_path = Path(block[FULL_OUTPUT_FILENAME])
                output_path.parent.mkdir(parents=True, exist_ok=True)

                logger.info(f"Downloading public data from uri: {source_uri}, path: {source_path} to {output_path}")

                protocol = urllib.parse.urlsplit(source_uri).scheme
                fs, source_path = get_remote_filesystem_and_path(protocol, source_uri, source_path)
                kwargs = {}
                if not isinstance(fs, SFTPFileSystem):
                    if fs.isdir(source_path):
                        kwargs["recursive"] = True
                    kwargs["block_size"] = 0
                    fs.get(source_path, output_path.as_posix(), **kwargs)
            else:
                logger.info(f"Data is not public, skipping download")
            downloaded_hashes.add(block_hash)

    def _data_product_pipe(self, input_blocks: List[DownloaderDict]) -> List[DownloaderDict]:
        for fn in [
            self._resolve_namespaces,
            self._resolve_data_products,
            self._resolve_objects,
            self._resolve_storage_locations,
            self._resolve_storage_roots,
            self._resolve_components,
            unique_dicts,
        ]:
            input_blocks = fn(input_blocks)
        return input_blocks

    def _external_object_pipe(self, input_blocks: List[DownloaderDict]) -> List[DownloaderDict]:
        for fn in [
            self._resolve_external_objects,
            partial(self._resolve_objects, external=True),
            partial(self._resolve_storage_locations, external=True),
            self._resolve_storage_roots,
            partial(self._resolve_components, external=True),
            unique_dicts,
        ]:
            input_blocks = fn(input_blocks)
        return input_blocks

    def resolve(self):
        """
        Resolves all registered data products and external objects to their expanded registry data
        """
        logger.info(f"Resolving {len(self._data_products)} data product references")
        while self._data_products:
            block = self._data_products.pop()
            try:
                resolved_block = self._data_product_pipe([block])
                if resolved_block:
                    self._resolved_data_products.extend(resolved_block)
                else:
                    raise ValueError(f"{block} could not be resolved.")
            except Exception:
                self._data_products.insert(0, block)
                raise

        logger.info(f"Resolving {len(self._external_objects)} external object references")
        while self._external_objects:
            block = self._external_objects.pop()
            try:
                resolved_block = self._external_object_pipe([block])
                if resolved_block:
                    self._resolved_external_objects.extend(resolved_block)
                else:
                    raise ValueError(f"{block} could not be resolved.")
            except Exception:
                self._external_objects.insert(0, block)
                raise

    def write_metadata(self):
        """
        Writes metadata for the resolved data products and external objects to metadata.yaml
        """
        logger.info("Writing metadata")
        self._data_directory.mkdir(parents=True, exist_ok=True)
        metadata_file = self._data_directory / "metadata.yaml"

        with open(metadata_file, "w") as stream:
            self._write_metadata_data_product(stream)
            self._write_metadata_external_object(stream)

    def download(self, write_metadata: bool = True):
        """
        Resolves, downloads and optionally writes metadata for data products and external objects that have been
        registered on this downloader

        :param write_metadata: If True the metadata.yaml file is written
        """
        logger.info("Starting download")
        logger.info("Resolving data registry references")
        self.resolve()

        self._data_directory.mkdir(parents=True, exist_ok=True)

        if write_metadata:
            self.write_metadata()
        else:
            logger.info("Not writing metadata")
        logger.info("Downloading data")
        self._download()
        logger.info("Completed download")
