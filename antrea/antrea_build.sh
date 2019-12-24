#! /bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

echo "antrea_build.sh start"

SRC_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
REPO_ROOT=$(pwd)/src

# Install docker-tool
cd "${GOBUILD_DOCKER_TOOL_ROOT}"
echo $PATH
sudo ./prepare_build_slave.sh
echo "docker tool installed"

# Build binary
cd "${REPO_ROOT}"
ls "${REPO_ROOT}"
make docker-bin
echo "build binary is done"

# Create archives for scripts and binaries
OUTPUT_DIR=${BUILDROOT}/output
mkdir -p ${OUTPUT_DIR}
cd ${REPO_ROOT}/build/images/scripts
tar -czf ${OUTPUT_DIR}/scripts.tar.gz *
cd ${REPO_ROOT}/bin
tar -czf ${OUTPUT_DIR}/bin.tar.gz *

echo "antrea_build.sh end"
