import logging
from pathlib import Path
from hashlib import sha1
import click
import yaml
from data_pipeline_api.registry.upload import upload_from_config
from data_pipeline_api.registry.common import (
    DEFAULT_DATA_REGISTRY_URL,
    DATA_REGISTRY_ACCESS_TOKEN,
    DATA_REGISTRY_URL,
    configure_cli_logging,
)

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
    hash: {hash}
    local_cache_url: ''
    name: {model_name}
    path: .
    responsible_person: *responsible_person
    store_root: *storage_root
  target: storage_location
- &model
  data:
    description: {model_name} model'
    name: {model_name}
    responsible_person: *responsible_person
    store: *storage_location
  target: model
- data:
    accessibility: *accessibility
    description: {model_name} version {model_version}
    model: *model
    responsible_person: *responsible_person
    store: *storage_location
    version_identifier: {model_version}
  target: model_version
"""


@click.command()
@click.option(
    "--responsible-person", required=True, type=str, help="github username of person responsible for this model"
)
@click.option("--model-name", required=True, type=str, help="name of this model")
@click.option("--model-version", required=True, type=str, help="semver version of this model")
@click.option("--filename", required=True, type=str, help="location of this model")
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
def create_model_cli(responsible_person, model_name, model_version, filename, data_registry, token):
    configure_cli_logging()
    data_registry = data_registry or os.environ.get(DATA_REGISTRY_URL, DEFAULT_DATA_REGISTRY_URL)
    token = token or os.environ.get(DATA_REGISTRY_ACCESS_TOKEN)
    path = Path(filename).absolute()
    with open(path, "rb") as file:
        hexdigest = sha1(file.read()).hexdigest()
    uri = path.as_uri()
    populated_yaml = BASE_YAML.format(
        responsible_person=responsible_person,
        model_name=model_name,
        model_version=model_version,
        uri=uri,
        hash=hexdigest,
    )
    config = yaml.safe_load(populated_yaml)
    upload_from_config(config, data_registry, token)


if __name__ == "__main__":
    logger = logging.getLogger(f"{__package__}.{__name__}")
    create_model_cli()
