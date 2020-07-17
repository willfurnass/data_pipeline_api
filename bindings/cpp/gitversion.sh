#!/bin/bash

set -e
set -u

outfile=$1
tmpfile=${outfile}.tmp

version=$(git describe --dirty --always)
git_hash=$(git rev-parse HEAD)
echo "GIT ${version}"
cat >$tmpfile <<EOF
#pragma once
#define GIT_VERSION "$version"
#define GIT_HASH "$git_hash"
EOF

if [ -r $outfile ]; then
    if diff -q $outfile $tmpfile >/dev/null; then
        rm -f $tmpfile
        # No change to version
        exit 0
    fi
fi

mv $tmpfile $outfile
