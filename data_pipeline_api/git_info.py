from typing import NamedTuple, Optional
from pathlib import Path
try:
    from git import Repo, InvalidGitRepositoryError  # type: ignore
    git_installed = True
except ImportError:
    Repo = None
    InvalidGitRepositoryError = None
    git_installed = False


class RepoInfo(NamedTuple):
    """
    The info needed by the data pipeline API
    """

    git_sha: str
    uri: str
    is_dirty: bool


def get_repo(path: Optional[Path] = None) -> Repo:
    """Attempt to get a repo from path, or the working directory.
    """
    if Repo is None:
        raise ImportError("Could not import git")
    else:
        return Repo(path=path, search_parent_directories=True)


def get_repo_info(path: Optional[Path] = None, default_repo: str = "") -> RepoInfo:
    """Get the git sha and uri for path, or the working directory.

    :path:         A path to use to start looking for the root of the repo.
    :default_repo: A default repo url to use if we cannot obtain one.
    :return:       A RepoInfo object. If not inside a git repo, is_dirty will be True, git_sha empty and uri will be a
                   default value
    """
    try:
        repo = get_repo(path)
        return RepoInfo(
            git_sha=repo.head.commit.hexsha,
            uri=next(repo.remote("origin").urls),
            is_dirty=repo.is_dirty(),
        )
    except (ImportError, InvalidGitRepositoryError):
        return RepoInfo(git_sha="", uri=default_repo, is_dirty=True)


def get_path_relative_to_repo(path: Path) -> Path:
    """Get path relative to the repo which contains it, or raise an exception.
    """
    result = get_repo(path).git.execute(['git', 'ls-files', '--full-name', str(path)])
    if result:
        return Path(result)
    raise ValueError(f"Could not get {path} relative to repo")
