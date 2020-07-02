from pathlib import Path
from hashlib import sha1
import click
import yaml
from data_pipeline_api.registry.upload import upload_from_config
from pprint import pprint

DATA_PRODUCT_TYPE = "input_data_product"
PROCESSING_SCRIPT_VERSION = "0.1.0"
PROCESSING_SCRIPT_NAME = "upload_input"
DATA_REGISTRY_URL = "https://data.scrc.uk/api/"
TOKEN = "438e5798a336f4ebb68ef113060fe34e50df4577"

yaml_config = yaml.safe_load(click.get_text_stream("stdin"))
responsible_person = yaml_config["responsible_person"]
root = Path(yaml_config["root"])
path = Path(yaml_config["path"])
data_product = yaml_config["data_product"]
data_product_version = yaml_config["version"]
components = yaml_config["components"]

absolute_root = root.resolve()
with open(root / path, "rb") as file:
    hexdigest = sha1(file.read()).hexdigest()

with open(__file__, "rb") as file:
    upload_script_hexdigest = sha1(file.read()).hexdigest()


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
        "uri": absolute_root.as_uri(),
        "type": storage_type_obj,
    },
}
storage_location_obj = {
    "target": "storage_location",
    "data": {
        "name": data_product,
        "path": str(path),
        "hash": hexdigest,
        "local_cache_url": "",
        "responsible_person": responsible_person_obj,
        "store_root": storage_root_obj,
    },
}
data_product_type_obj = {
    "target": "data_product_type",
    "data": {
        "name": DATA_PRODUCT_TYPE,
        "description": f"data product {DATA_PRODUCT_TYPE}",
    },
}
data_product_obj = {
    "target": "data_product",
    "data": {
        "name": data_product,
        "description": data_product,
        "responsible_person": responsible_person_obj,
        "type": data_product_type_obj,
    },
}
processing_script_storage_location_obj = {
    "target": "storage_location",
    "data": {
        "name": PROCESSING_SCRIPT_NAME,
        "path": ".",
        "hash": upload_script_hexdigest,
        "local_cache_url": "",
        "responsible_person": responsible_person_obj,
        "store_root": storage_root_obj,
    },
}
processing_script_obj = {
    "target": "processing_script",
    "data": {
        "name": PROCESSING_SCRIPT_NAME,
        "responsible_person": responsible_person_obj,
        "store": processing_script_storage_location_obj,
    },
}
processing_script_version_obj = {
    "target": "processing_script_version",
    "data": {
        "version_identifier": PROCESSING_SCRIPT_VERSION,
        "responsible_person": responsible_person_obj,
        "processing_script": processing_script_obj,
        "store": processing_script_storage_location_obj,
        "accessibility": accessibility_obj,
    },
}
data_product_version_obj = {
    "target": "data_product_version",
    "data": {
        "version_identifier": data_product_version,
        "description": f"version {data_product_version} of {data_product}",
        "responsible_person": responsible_person_obj,
        "data_product": data_product_obj,
        "store": storage_location_obj,
        "accessibility": accessibility_obj,
        "processing_script_version": processing_script_version_obj,
    },
}
component_objs = [
    {
        "target": "data_product_version_component",
        "data": {
            "name": component,
            "responsible_person": responsible_person_obj,
            "data_product_version": data_product_version_obj,
        },
    }
    for component in components
]


config = {
    "reference": [responsible_person_obj],
    "post": [
        accessibility_obj,
        responsible_person_obj,
        storage_type_obj,
        storage_root_obj,
        storage_location_obj,
        data_product_type_obj,
        data_product_obj,
        processing_script_storage_location_obj,
        processing_script_obj,
        processing_script_version_obj,
        data_product_version_obj,
    ]
    + component_objs,
}

upload_from_config(config, DATA_REGISTRY_URL, TOKEN)
