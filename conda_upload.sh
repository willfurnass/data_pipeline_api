#!/bin/sh
set -e

conda config --set anaconda_upload no
cd conda
conda build .
anaconda -t $CONDA_UPLOAD_TOKEN upload -u ScottishCovidResponse "$(conda build . --output)" || echo The package was already uploaded
