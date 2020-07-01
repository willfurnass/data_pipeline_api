from distutils.core import setup

import yaml


with open("conda/meta.yaml") as fp:
    meta = yaml.load(fp)
    version = meta["package"]["version"]


setup(
   name='data_pipeline_api',
   version=version,
   description='Interacts with SCRC data pipeline',
   author='SCRC',
   author_email='scrc@glasgow.ac.uk',
   packages=['data_pipeline_api'],
   entry_points={
    'console_scripts': [
        'data_registry_download = data_pipeline_api.registry.download:download_cli',
        'data_registry_upload = data_pipeline_api.registry.access_upload:upload_model_run_cli',
        'data_registry_post = data_pipeline_api.registry.upload:upload_cli',
    ],
    }
)
