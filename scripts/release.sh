#!/bin/bash
set -eu -o pipefail

wheel=$(ls -1 dist/*.whl)
export TAG=$(pkginfo -f version --single "$wheel")

echo Checking if a new release needs to be made
echo Tag: $TAG
echo Wheel: $wheel
echo Available tags:
git tag

conda_release() {
    echo Releasing conda package ...
    conda config --set anaconda_upload no
    pushd conda
    conda build .
    anaconda -t $ANACONDA_TOKEN upload -u ScottishCovidResponse "$(conda build . --output)" || echo Package already uploaded
    popd
    echo Done
}

pypi_release() {
    echo Releasing python artefacts  ...
    echo Artefacts: dist/*
    twine upload -u __token__ dist/*.{whl,tar.gz}
    echo Done
}

if git rev-parse -q --verify "$TAG" >/dev/null && [[ -n "$(git branch -a --contains "tags/$TAG" | grep remotes/origin/master)" ]]; then
    conda_release
    pypi_release
else
    echo Not releasing version $TAG as that is not a tag which is part of the master branch.
fi
