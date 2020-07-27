from data_pipeline_api.registry.common import DATA_REGISTRY_ACCESS_TOKEN, DEFAULT_DATA_REGISTRY_URL, DATA_REGISTRY_URL
from data_pipeline_api.registry.utils import get_access_token, DATA_PIPELINE_PREFIX, get_remote_options, \
    get_data_registry_url


def test_get_access_token(monkeypatch):
    monkeypatch.setenv(DATA_REGISTRY_ACCESS_TOKEN, "some_access_token")
    assert get_access_token() == "some_access_token"
    monkeypatch.delenv(DATA_REGISTRY_ACCESS_TOKEN)
    assert get_access_token() is None


def test_get_data_registry_url(monkeypatch):
    monkeypatch.setenv(DATA_REGISTRY_URL, "some_url")
    assert get_data_registry_url() == "some_url"
    monkeypatch.delenv(DATA_REGISTRY_URL)
    assert get_data_registry_url() is DEFAULT_DATA_REGISTRY_URL


def test_get_remote_options(monkeypatch):
    monkeypatch.setenv(f"{DATA_PIPELINE_PREFIX}login_details", "myusername")
    monkeypatch.setenv(f"{DATA_PIPELINE_PREFIX}login_password", "mypassword")
    ro = get_remote_options()
    assert ro.get("login_details") == "myusername"
    assert ro.get("login_password") == "mypassword"
