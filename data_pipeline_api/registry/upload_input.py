import logging
import os
from pathlib import Path
from hashlib import sha1
import click
import yaml
from data_pipeline_api.registry.upload import upload_from_config
from data_pipeline_api.registry.common import configure_cli_logging, DATA_REGISTRY_ACCESS_TOKEN, \
    DEFAULT_DATA_REGISTRY_URL, DATA_REGISTRY_URL

BASE_YAML = """
reference:
- &responsible_person
  data:
    username: {responsible_person}
  target: users
post:
- &accessibility
  data:
    access_info: public
    description: accessible to everyone
    name: public
  target: accessibility
- &storage_type
  data:
    name: file
  target: storage_type
- &storage_root
  data:
    description: {responsible_person} local filesystem'
    name: local_filesystem
    type: *storage_type
    uri: {uri}
  target: storage_root
- &storage_location
  data:
    hash: {hexdigest}
    local_cache_url: ''
    name: {data_product}
    path: {path}
    responsible_person: *responsible_person
    store_root: *storage_root
  target: storage_location
- &data_product_type
  data:
    description: data product {data_product_type}
    name: {data_product_type}
  target: data_product_type
- &data_product
  data:
    description: {data_product}
    name: {data_product}
    responsible_person: *responsible_person
    type: *data_product_type
  target: data_product
- &storage_location_upload_script
  data:
    hash: {upload_script_hexdigest}
    local_cache_url: ''
    name: {processing_script_name}
    path: .
    responsible_person: *responsible_person
    store_root: *storage_root
  target: storage_location
- &id007
  data:
    name: {processing_script_name}
    responsible_person: *responsible_person
    store: *storage_location_upload_script
  target: processing_script
- &processing_script_version
  data:
    accessibility: *accessibility
    processing_script: *id007
    responsible_person: *responsible_person
    store: *storage_location_upload_script
    version_identifier: {processing_script_version}
  target: processing_script_version
- &data_product_version
  data:
    accessibility: *accessibility
    data_product: *data_product
    description: version {version} of {data_product}
    processing_script_version: *processing_script_version
    responsible_person: *responsible_person
    store: *storage_location
    version_identifier: {version}
  target: data_product_version
"""

COMPONENT_BASE_YAML = """- 
  data:
    data_product_version: *data_product_version
    name: {name}
    responsible_person: *responsible_person
  target: data_product_version_component
"""


@click.command()
@click.option(
    "--responsible-person", required=True, type=str, help="github username of person responsible for this model"
)
@click.option("--root", required=True, type=str, help="root location of input files")
@click.option("--path", required=True, type=str, help="path of the input file to upload in the context of the root")
@click.option("--data-product", required=True, type=str, help="name of the data product")
@click.option("--data-product-version", required=True, type=str, help="semver version of the data product")
@click.option("--data-product-type", required=True, type=str, help="type of the data product")
@click.option("--component", multiple=True, required=True, type=str, help="components of the data product")
@click.option("--processing-script-name", type=str, default="upload_input", help="name of the processing script, defaults to 'upload_input'")
@click.option("--processing-script-version", type=str, default="0.1.0", help="version of the processing script, defaults to '0.1.0'")
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
def upload_input_cli(responsible_person, root, path, data_product, data_product_version, data_product_type, component, processing_script_name, processing_script_version, data_registry, token):
    configure_cli_logging()
    data_registry = data_registry or os.environ.get(DATA_REGISTRY_URL, DEFAULT_DATA_REGISTRY_URL)
    token = token or os.environ.get(DATA_REGISTRY_ACCESS_TOKEN)
    root = Path(root)
    path = Path(path)
    uri = root.absolute().as_uri()
    with open(root / path, "rb") as file:
        hexdigest = sha1(file.read()).hexdigest()

    with open(__file__, "rb") as file:
        upload_script_hexdigest = sha1(file.read()).hexdigest()

    populated_yaml = BASE_YAML.format(
        responsible_person=responsible_person,
        path=path.as_posix(),
        data_product=data_product,
        version=data_product_version,
        data_product_type=data_product_type,
        processing_script_name=processing_script_name,
        processing_script_version=processing_script_version,
        uri=uri,
        hexdigest=hexdigest,
        upload_script_hexdigest=upload_script_hexdigest,
    )
    populated_yaml = "".join([populated_yaml] + [COMPONENT_BASE_YAML.format(name=c) for c in component])
    config = yaml.safe_load(populated_yaml)
    upload_from_config(config, data_registry, token)


if __name__ == "__main__":
    logger = logging.getLogger(f"{__package__}.{__name__}")
    upload_input_cli()
