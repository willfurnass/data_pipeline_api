from pathlib import Path
from pkg_resources import Requirement
import re

from setuptools import setup
import yaml

IGNORE_PACKAGES = ["python", "coverage", "pytest", "pytest-cov"]


def _convert_conda_deps_to_python_requirements(conda_deps):
    reqs = [Requirement(re.sub(r"\b=\b", "==", dep)) for dep in conda_deps]
    return [req for req in reqs if req.name not in IGNORE_PACKAGES]


def _read_requirements():
    """
    Read dependencies from environment.yml
    """
    with open(Path(__file__).parent / "environment.yml") as fp:
        env = yaml.safe_load(fp)

    requirements = []
    for req in _convert_conda_deps_to_python_requirements(env["dependencies"]):
        # Even though we want to pin the packages during development, we
        # need to allow people to download newer versions to avoid
        # conflicts downstream.
        if len(req.specs) == 1 and req.specs[0][0] == "==":
            _, version = req.specs[0]
            requirements.append(f"{req.name}>={version}")
        else:
            requirements.append(str(req))
    return requirements


setup(
    name="data_pipeline_api",
    description="Interacts with SCRC data pipeline",
    author="SCRC",
    author_email="scrc@glasgow.ac.uk",
    packages=["data_pipeline_api", "data_pipeline_api.file_formats"],
    install_requires=_read_requirements(),
    setup_requires=["setuptools_scm", "pyyaml"],
    use_scm_version=True,
    entry_points={
        'console_scripts': [
            'pipeline_download = data_pipeline_api.registry.download:download_cli',
            'pipeline_upload = data_pipeline_api.registry.access_upload:upload_model_run_cli',
            'pipeline_post = data_pipeline_api.registry.upload:upload_cli',
            'pipeline_create_model = data_pipeline_api.registry.create_model:create_model_cli',
            'pipeline_upload_input = data_pipeline_api.registry.upload_input:upload_input_cli',
            'pipeline_convert_access_to_config = scripts.convert_access_to_config:convert_cli',
        ],
    }
)
