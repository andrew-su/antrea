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

export_dependency_env

update_docker

docker version
docker pull nsx-ujo-docker-local.artifactory.eng.vmware.com/interworking/golang:1.19-buster
docker tag nsx-ujo-docker-local.artifactory.eng.vmware.com/interworking/golang:1.19-buster golang:1.19

echo "===== Building Antrea Target ${ANTREA_TARGET} ====="
pushd "${REPO_ROOT}"
source "${PROJECT_DIR}/antrea_${ANTREA_TARGET}.sh"
echo "====== Cleaning up Build Cache ======"
# Buildweb unarchives all .tar.gz files, scans all files, and generates a file list in compcache.
# Cayman captures all files under REPO_ROOT into publish/oss/antrea/antrea-$version-ODP-src.tar.gz
# Build cache has many files and occupies large space.
# We should avoid having cayman archive build cache and temp files into antrea-$version-ODP-src.tar.gz.
for tmpDir in .cache gopath gocache goenv ; do
  if [ -e "$tmpDir" ] ;then
    chmod -R ug+w "$tmpDir"
    rm -rf "$tmpDir"
  fi
done
make clean
git clean -fxd
popd

echo "====== Cleanup Docker Storage ======"
DOCKER_STORAGE_DIR="${BUILDROOT}/docker"
sudo systemctl stop docker
sudo rm -rf ${DOCKER_STORAGE_DIR}
echo "****** antrea_build.sh end ******"
