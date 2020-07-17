#!/bin/bash

set -e
set -u

outfile=$1
tmpfile=${outfile}.tmp

version=$(git describe --dirty --always)
git_hash=$(git rev-parse HEAD)
upstream_branch=$(git rev-parse --abbrev-ref --symbolic-full-name @{u})
upstream_remote=${upstream_branch%/*}
upstream_url=$(git remote get-url origin)

echo "GIT ${version}"
cat >$tmpfile <<EOF
#pragma once
#define GIT_VERSION "$version"
#define GIT_HASH "$git_hash"
#define GIT_URL "$upstream_url"
EOF

# TODO: probably want to convert the Git URL above from ssh to http

if [ -r $outfile ]; then
    if diff -q $outfile $tmpfile >/dev/null; then
        rm -f $tmpfile
        # No change to version
        exit 0
    fi
fi

mv $tmpfile $outfile
