from pathlib import Path
import pytest
from data_pipeline_api.git_info import (
    get_repo,
    get_repo_info,
    get_path_relative_to_repo,
    Repo,
    RepoInfo,
    git_installed,
)


def test_get_repo():
    repo = get_repo(Path(__file__))
    if git_installed:
        assert isinstance(repo, Repo)
    else:
        assert repo is None


def test_get_repo_info():
    repo_info = get_repo_info(Path(__file__), "default_repo")
    assert isinstance(repo_info, RepoInfo)
    if git_installed:
        assert (
            repo_info.uri
            == "https://github.com/ScottishCovidResponse/data_pipeline_api.git"
        )
    else:
        assert repo_info == RepoInfo(git_sha="", uri="default_repo", is_dirty=True)


def test_get_path_relative_to_repo():
    if git_installed:
        assert get_path_relative_to_repo(Path(__file__)) == Path(
            "tests/test_git_info.py"
        )
    else:
        with pytest.raises(ImportError):
            get_path_relative_to_repo(Path(__file__))
