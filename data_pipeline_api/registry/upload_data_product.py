import logging
import urllib
from pathlib import Path
import click
import semver
import yaml
from data_pipeline_api.registry.upload import upload_from_config
from data_pipeline_api.registry.common import (
    configure_cli_logging,
    DATA_REGISTRY_ACCESS_TOKEN,
    DEFAULT_DATA_REGISTRY_URL,
    DATA_REGISTRY_URL,
    get_data,
    DataRegistryField,
    DataRegistryTarget,
    sort_by_semver,
)
from data_pipeline_api.registry.access_upload import upload_to_storage
from data_pipeline_api.file_api import FileAPI


@click.command(context_settings=dict(max_content_width=200))
@click.option(
    "--data-product-path",
    type=click.Path(exists=True),
    required=True,
    help="Path to the data product on the local filesystem to upload to storage",
)
@click.option(
    "--namespace",
    type=str,
    default="SCRC",
    help="namespace of the data product that's being uploaded, defaults to SCRC",
)
@click.option(
    "--storage-root-name", type=str, help="Name of the storage root being uploaded to, defaults to the remote-uri arg"
)
@click.option(
    "--storage-location-path",
    type=str,
    help="Path to upload the file to on remote storage, if not provided no path is used, i.e. the file is uploaded to the root of remote-uri",
)
@click.option(
    "--accessibility",
    type=int,
    default=1,
    help="Accessibility of the data product, 0: public, 1: private. Defaults to 1.",
)
@click.option("--data-product-name", type=str, required=True, help="name of the data product to be uploaded")
@click.option("--data-product-description", type=str, help="free text description of the data product")
@click.option(
    "--data-product-version",
    type=str,
    required=True,
    help="semver version of the data product. If not provided defaults to 0.1.0 if this is the first version of the data product, else increments the minor version of the existing data product.",
)
@click.option(
    "--component",
    nargs=2,
    multiple=True,
    type=click.Tuple([str, str]),
    help="component (name, description) pairs that are part of this data product, if not provided defaults to data product name",
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
@click.option("--remote-uri", "-u", type=str, help=f"URI to the root of the remote storage, defaults to --root arg")
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
    help="URI to the root of the storage to post in the registry. Required if the uri to use for download from the "
    "registry is different from that used to upload the item",
)
def upload_data_product_cli(
    data_product_path,
    namespace,
    storage_root_name,
    storage_location_path,
    accessibility,
    data_product_name,
    data_product_description,
    data_product_version,
    component,
    data_registry,
    token,
    remote_uri,
    remote_option,
    remote_uri_override,
):
    configure_cli_logging()

    template_file = Path(__file__).parent / Path("templates/data_product.yaml")
    with open(template_file, "r") as f:
        template = f.read()

    data_registry = data_registry or DEFAULT_DATA_REGISTRY_URL
    remote_uri_override = remote_uri_override or remote_uri
    storage_root_name = storage_root_name or urllib.parse.urlparse(remote_uri_override).netloc
    storage_root = remote_uri_override
    remote_option = {} if remote_option is None else dict(remote_option)
    data_product_path = Path(data_product_path)

    storage_location_hash = FileAPI.calculate_hash(data_product_path)

    path = upload_to_storage(
        remote_uri, remote_option, data_product_path.parent, data_product_path, upload_path=storage_location_path
    )
    query = {DataRegistryField.name: data_product_name}
    if data_product_version:
        query["version"] = data_product_version
    data_products = get_data(query, DataRegistryTarget.data_product, data_registry, token, False)
    if data_products:
        latest = next(iter(sort_by_semver(data_products)))
        data_product_version = str(semver.parse_version_info(latest[DataRegistryField.version]).bump_minor())
    elif not data_product_version:
        data_product_version = "0.1.0"

    populated_yaml = template.format(
        namespace=namespace,
        storage_root_name=storage_root_name,
        storage_root=storage_root,
        accessibility=accessibility,
        storage_location_path=path,
        storage_location_hash=storage_location_hash,
        data_product_name=data_product_name,
        data_product_description=data_product_description,
        data_product_version=data_product_version,
        component_name="COMPONENT_NAME",
        component_description="COMPONENT_DESCRIPTION",
    )
    config = yaml.safe_load(populated_yaml)
    component_template = config["post"].pop(-1)
    if component:
        for component_name, component_description in component:
            c = component_template["data"].copy()
            c["name"] = component_name
            c["description"] = component_description
            config["post"].append({"data": c, "target": DataRegistryTarget.object_component})
    else:
        c = component_template["data"].copy()
        c["name"] = data_product_name
        c["description"] = data_product_description
        config["post"].append({"data": c, "target": DataRegistryTarget.object_component})
    upload_from_config(config, data_registry, token)


if __name__ == "__main__":
    logger = logging.getLogger(f"{__package__}.{__name__}")
    # pylint: disable=no-value-for-parameter
    upload_data_product_cli()
