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
   package_dir = {'': 'src'},
   packages=['data_pipeline_api'],
)
