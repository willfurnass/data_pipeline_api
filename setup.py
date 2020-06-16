from setuptools import setup

setup(
   name='data_pipeline_api',
   version='0.0.1',
   description='Interacts with SCRC data pipeline',
   author='SCRC',
   author_email='scrc@glasgow.ac.uk',
   packages=['data_pipeline_api'],
#   install_requires=['h5py', 'toml'], # handle by conda, somehow?
)