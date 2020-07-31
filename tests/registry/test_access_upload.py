from hashlib import sha1
from datetime import datetime as dt

import pytest

from data_pipeline_api.registry.access_upload import _verify_hash, _create_target_data_dict, to_github_uri


@pytest.fixture()
def tmp_file_calculated_hash(tmp_path):
    d = tmp_path / "sub"
    d.mkdir()
    p = d / "test.txt"
    p.write_text("some content in a file")
    return p


@pytest.fixture()
def calculated_hash(tmp_file_calculated_hash):
    with open(tmp_file_calculated_hash, "rb") as f:
        return sha1(f.read()).hexdigest()


def test_verify_hash(tmp_file_calculated_hash, calculated_hash):
    _verify_hash(tmp_file_calculated_hash, calculated_hash)
    with pytest.raises(ValueError):
        _verify_hash(tmp_file_calculated_hash, "somemadeuphash")


def test_create_target_data_dict():
    assert _create_target_data_dict("a_target", {"key": "value"}) == {"target": "a_target", "data": {"key": "value"}}


@pytest.mark.parametrize(
    ["input_uri", "sha", "expected"],
    [
        ["https://github.com/Org/Repo/", "abcdef", "github://Org:Repo@abcdef/"],
        ["https://github.com/Org/Repo/", None, "github://Org:Repo@master/"],
        ["https://github.com/Org/Repo/some/path/", "abcdef", "github://Org:Repo@abcdef/some/path/"],
        ["git@github.com:Org/Repo.git", "abcdef", "github://Org:Repo@abcdef/"],
        ["git@github.com:Org/Repo.git", None, "github://Org:Repo@master/"],
        ["https://www.test.blah", "abcdef", "https://www.test.blah"],
        ["https://www.test.blah", None, "https://www.test.blah"],
        ["https://github.com/Org/Repo.git", "abcdef", "github://Org:Repo@abcdef/"],
        ["https://github.com/Org/Repo.git", None, "github://Org:Repo@master/"],
    ],
)
def test_to_github_uri(input_uri, sha, expected):
    if sha:
        assert to_github_uri(input_uri, sha) == expected
    else:
        assert to_github_uri(input_uri) == expected
