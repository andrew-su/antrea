#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace
set -u # report error and exit on undefined variable

echo "****** antrea_build.sh start ******"

env
git status
cat /proc/cpuinfo

source "${PROJECT_DIR}/antrea_var.sh"
source "${PROJECT_DIR}/antrea_lib.sh"

update_docker

docker version
docker pull nsx-ujo-docker-local.artifactory.eng.vmware.com/golang:1.19
docker tag nsx-ujo-docker-local.artifactory.eng.vmware.com/golang:1.19 golang:1.19

echo "===== Building Antrea Target ${ANTREA_TARGET} ====="
pushd "${REPO_ROOT}"
source "${PROJECT_DIR}/antrea_${ANTREA_TARGET}.sh"
popd

echo "****** antrea_build.sh end ******"
