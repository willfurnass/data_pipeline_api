import os
from typing import Dict, Optional

from data_pipeline_api.registry.common import DATA_REGISTRY_URL, DEFAULT_DATA_REGISTRY_URL, DATA_REGISTRY_ACCESS_TOKEN

DATA_PIPELINE_PREFIX = "DATA_PIPELINE_"


def get_access_token() -> Optional[str]:
    return os.environ.get(DATA_REGISTRY_ACCESS_TOKEN)


def get_data_registry_url() -> Optional[str]:
    return os.environ.get(DATA_REGISTRY_URL, DEFAULT_DATA_REGISTRY_URL)


def get_remote_options() -> Dict[str, str]:
    remote_options = {}
    for k, v in os.environ.items():
        if k.startswith(DATA_PIPELINE_PREFIX):
            remote_options[k[len(DATA_PIPELINE_PREFIX):].lower()] = v
    return remote_options
