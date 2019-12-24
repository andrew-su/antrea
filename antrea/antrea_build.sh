#! /bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

echo "antrea_build.sh start"

# Go into the antrea/src
SRC_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
REPO_ROOT=$(pwd)/src

OUTPUT_DIR=${BUILDROOT}/output/scripts
mkdir -p "${OUTPUT_DIR}"

cd "${REPO_ROOT}"
cp build/images/scripts/* "${OUTPUT_DIR}/" 

echo "antrea_build.sh end"
