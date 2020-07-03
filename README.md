# data_pipeline_api

[![Anaconda-Server Badge](https://anaconda.org/scottishcovidresponse/data_pipeline_api/badges/version.svg)](https://anaconda.org/scottishcovidresponse/data_pipeline_api)

API to access the data pipeline

# Current issues and questions
* Units thing seems ambitious - feels like we have to know quite a lot about what is stored where, and that it is easier to just do client side where you actually know.
* It isn't obvious that the version should be stored in the metadata file, rather than having a file per version.
* We should pick one of path (relative to root?) or location (relative to parameter), unless there is a compelling reason not to.
* Why is the warning stuff inside the metadata file - shouldn't that all be tracked by the DB?
* I'm not quite clear on how things tie together - feels like we should be just getting hashes for our inputs, and then recording them, and letting the DB figure it out?
* Are "components" only relevant for datasets? If not, how are components encoded in parameter files?
* I'm not clear how the attrs come into it with the dataset stuff.

# Releasing a new version

New versions can be released from the master branch at any time. Whenever a git
tag that points to a commit in master is pushed, that version will be
automatically released.

This is an example on how to release the tip of master:

```bash
git clone git@github.com:ScottishCovidResponse/data_pipeline_api.git
cd data_pipeline_api
# we are now seeing the HEAD of master
git tag -m"short description of this version" 1.0.0
git push --tags
```

That will release the version 1.0.0 of the data pipeline api

# Installing

## Conda

This package can be installed in conda with

```bash
conda install -c ScottishCovidResponse data-pipeline-api
```

## Pip

We don't have wheels pushed anywhere, but pip supports pulling installing from
git. You need to install PyYaml and setuptools_scm as they are setup
requirements.

```bash
pip install pyyaml setuptools-scm
pip install git+https://github.com/ScottishCovidResponse/data_pipeline_api.git@VERSION
```

Replace version with the latest version of this package. You can find it by running:

```bash
git clone https://github.com/ScottishCovidResponse/data_pipeline_api.git
cd data_pipeline_api
git tag --sort=v:refname
```
