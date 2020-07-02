from pathlib import Path
from hashlib import sha1
import click
import yaml
from data_pipeline_api.registry.upload import upload_from_config
from pprint import pprint

DATA_REGISTRY_URL = "https://data.scrc.uk/api/"
TOKEN = "438e5798a336f4ebb68ef113060fe34e50df4577"


yaml_config = yaml.safe_load(click.get_text_stream("stdin"))
responsible_person = yaml_config["responsible_person"]
model_name = yaml_config["name"]
model_version = yaml_config["version"]
path = Path(yaml_config["path"])
hexdigest = "12345"

absolute_path = path.resolve()

responsible_person_obj = {"target": "users", "data": {"username": responsible_person}}
accessibility_obj = {
    "target": "accessibility",
    "data": {
        "name": "public",
        "description": "accessible to everyone",
        "access_info": "public",
    },
}
storage_type_obj = {"target": "storage_type", "data": {"name": "file"}}
storage_root_obj = {
    "target": "storage_root",
    "data": {
        "name": "local_filesystem",
        "description": f"{responsible_person} local filesystem",
        "uri": absolute_path.as_uri(),
        "type": storage_type_obj,
    },
}
model_storage_location_obj = {
    "target": "storage_location",
    "data": {
        "name": model_name,
        "path": ".",
        "hash": hexdigest,
        "local_cache_url": "",
        "responsible_person": responsible_person_obj,
        "store_root": storage_root_obj,
    },
}
model_obj = {
    "target": "model",
    "data": {
        "name": model_name,
        "description": f"{model_name} model",
        "responsible_person": responsible_person_obj,
        "store": model_storage_location_obj,
    },
}
model_version_obj = {
    "target": "model_version",
    "data": {
        "version_identifier": model_version,
        "description": f"{model_name} version {model_version}",
        "responsible_person": responsible_person_obj,
        "model": model_obj,
        "store": model_storage_location_obj,
        "accessibility": accessibility_obj,
    },
}


config = {
    "reference": [responsible_person_obj],
    "post": [
        accessibility_obj,
        responsible_person_obj,
        storage_type_obj,
        storage_root_obj,
        model_storage_location_obj,
        model_obj,
        model_version_obj,
    ]
}



upload_from_config(config, DATA_REGISTRY_URL, TOKEN)
