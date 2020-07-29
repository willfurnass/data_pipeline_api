# data_pipeline_api

> :warning: **The code and data in this repository are currently for testing and development purposes only.**

[![Build Status](https://travis-ci.org/ScottishCovidResponse/data_pipeline_api.svg?branch=master)](https://travis-ci.org/ScottishCovidResponse/data_pipeline_api)
[![Code Coverage](https://codecov.io/github/ScottishCovidResponse/data_pipeline_api/coverage.svg?branch=master&token=)](https://codecov.io/gh/ScottishCovidResponse/data_pipeline_api)
[![Anaconda-Server Badge](https://anaconda.org/scottishcovidresponse/data_pipeline_api/badges/version.svg)](https://anaconda.org/scottishcovidresponse/data_pipeline_api)

## Summary

Python API to access files from the SCRC data pipeline.

## Features

- Loads files into memory for use in models.
- Ensures that data used in models can be traced to its source.
- Records model outputs such that the versions of code and data are recorded.

## Contributing

See [contributing](contributing.md).

## Installing

### Conda

Install [miniconda](https://docs.conda.io/en/latest/miniconda.html) (it also works with [anaconda](https://docs.anaconda.com/anaconda/install/), but we do not need the extra packages). With conda installed, run the following commands to create the virtual environment and activate it:

```
conda env create -f environment.yml
conda activate data_pipeline_api
```

This is most useful for testing and development.

A packaged version can be installed via [Anaconda Cloud](https://anaconda.org/scottishcovidresponse/data_pipeline_api) for use in other applications:

```bash
conda install -c ScottishCovidResponse data-pipeline-api
```

### Pip

You can install this package via pip with

```bash
pip install data-pipeline-api
```

```bash
git clone https://github.com/ScottishCovidResponse/data_pipeline_api.git
cd data_pipeline_api
git tag --sort=v:refname
```

## Data Registry Interactions

See [registry README](data_pipeline_api/registry/README.md).

## Releasing a new version

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

## Reproducible Builds

**ToDo**

## Tests

After activating your conda environment, execute the following command:

```{shell}
pytest --cov=data_pipeline_api tests
```

## Usage

**ToDo**

# Current issues and questions

**ToDo** Migrate these to GitHub issues.

* Units thing seems ambitious - feels like we have to know quite a lot about what is stored where, and that it is easier to just do client side where you actually know.
* It isn't obvious that the version should be stored in the metadata file, rather than having a file per version.
* We should pick one of path (relative to root?) or location (relative to parameter), unless there is a compelling reason not to.
* Why is the warning stuff inside the metadata file - shouldn't that all be tracked by the DB?
* I'm not quite clear on how things tie together - feels like we should be just getting hashes for our inputs, and then recording them, and letting the DB figure it out?
* Are "components" only relevant for datasets? If not, how are components encoded in parameter files?
* I'm not clear how the attrs come into it with the dataset stuff.

* Writes cannot subsequently be read.

## Static analysis

[Automated static analysis results](https://app.codacy.com/gh/ScottishCovidResponse/data_pipeline_api/issues/index) are available - these should be interpreted with caution and the importance of each issue must be assessed individually. The setup is to use pylint with a [configuration file](.pylintrc). This is the default plus we ignore C0103 (variable names) and C0301 (line lengths). We do not make use of the overall "quality standards" features of codacy at this time as they are pretty arbitrary.

## License

[BSD 3-Clause License](LICENSE).


