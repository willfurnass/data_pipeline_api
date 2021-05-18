from setuptools import setup

#IGNORE_PACKAGES = ["python", "coverage", "pytest", "pytest-cov"]


setup(
    name="data_pipeline_api",
    description="Interacts with SCRC data pipeline",
    author="SCRC",
    author_email="scrc@glasgow.ac.uk",
    packages=["data_pipeline_api", "data_pipeline_api.file_formats", "data_pipeline_api.registry"],
    install_requires= [
      'networkx == 2.4',
      'matplotlib == 3.1.3',
      'pandas == 1.0.3',
      'toml == 0.9.4',
      'h5py == 2.10.0',
      'scipy==1.4.1',
      'pyyaml==5.3.1',
      'semver==2.9.0',
      'fsspec==0.7.4',
      's3fs==0.4.2',
      'click==7.1.2',
      'requests==2.23.0',
      'paramiko==2.7.1',
      'gitpython==3.1.3',

      # test dependencies
      'coverage==5.0',
      'pytest==5.4.1',
      'pytest-cov==2.8.1'
    ],
    setup_requires=["setuptools_scm", "pyyaml"],
    use_scm_version=True,
    entry_points={
        'console_scripts': [
            'pipeline_download = data_pipeline_api.registry.download:download_cli',
            'pipeline_upload = data_pipeline_api.registry.access_upload:upload_model_run_cli',
            'pipeline_post = data_pipeline_api.registry.upload:upload_cli',
            'pipeline_convert_access_to_config = scripts.convert_access_to_config:convert_cli',
            'pipeline_upload_data_product = data_pipeline_api.registry.upload_data_product:upload_data_product_cli'
        ],
    }
)
