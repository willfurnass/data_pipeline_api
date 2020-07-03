#!/bin/bash
set -eu -o pipefail

wheel=$(ls -1 dist/)
export TAG=$(pkginfo -f version --single dist/"$wheel")

echo Checking if a new release needs to be made
echo Tag: $TAG
echo Wheel: $wheel
echo Available tags:
git tag

if git rev-parse -q --verify "$TAG" >/dev/null && [[ -n "$(git branch -a --contains tags/$TAG | grep remotes/origin/master)" ]]; then
    conda config --set anaconda_upload no
    cd conda
    conda build .
    anaconda -t $ANACONDA_TOKEN upload -u ScottishCovidResponse "$(conda build . --output)" || echo Package already uploaded
else
    echo Not releasing version $TAG as that is not a tag which is part of the master branch.
fi
