#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

echo "antrea_build.sh start"

env

SRC_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
REPO_ROOT=$(pwd)/src

# Update Docker to a version that supports multi-stage builds
echo  "Updating Docker"
chmod a+x install_docker.sh
sudo ./install_docker.sh

# Install docker-tool
pushd "${GOBUILD_DOCKER_TOOL_ROOT}"
echo $PATH
sudo ./prepare_build_slave.sh
sudo bash -c "echo '172.17.0.2 registry.local' >> /etc/hosts"
sudo bash -c "echo '172.17.0.3 registry2.nicira.eng.vmware.com' >> /etc/hosts"
cat /etc/hosts
sudo sysctl net.ipv4.conf.all.forwarding=1
sudo sysctl net.ipv4.conf.docker0.forwarding=1
sudo sysctl net.ipv4.conf.default.forwarding=1
sudo iptables -I FORWARD -j ACCEPT
popd

function run_python {
  PYTHON="${GOBUILD_CAYMAN_PYTHON_ROOT}/lin64/bin/python"
  "${PYTHON}" "$@"
}

mkdir venv
VENV="$(readlink -f venv)"
VIRTUAL_ENV="${GOBUILD_CAYMAN_PYTHON_ROOT}/virtualenv/virtualenv.py"
run_python -c 'import sys; print(sys.path)'
run_python "${VIRTUAL_ENV}" "${VENV}"

PIP_TRUSTED_HOST="--trusted-host=devpi.nicira.eng.vmware.com"
PIP_INDEX_URL="--index-url=http://devpi.nicira.eng.vmware.com/root/pypi-extended/+simple/"
PIP="${VENV}/bin/pip"

# old pip doesn't support --trusted-host
"${PIP}" install "${PIP_INDEX_URL}" -U pip setuptools==44.0
"${PIP}" "${PIP_TRUSTED_HOST}" install "${PIP_INDEX_URL}" "${GOBUILD_DOCKER_TOOL_ROOT}/vmware-docker-tool-1.0.tar.gz"

DOCKER_TOOL="${VENV}/bin/docker-tool"

echo "docker-tool installed"

docker version

COMPCACHE="$(readlink -f ${BUILDROOT}/../../compcache)"

"${DOCKER_TOOL}" --registry registry2.nicira.eng.vmware.com configure \
  "--buildid=vmware-master.${BUILD_NUMBER}" --build-dir=${BUILDDIR} --jobs=4 \
  "--compcache=${COMPCACHE}"

docker pull registry2.nicira.eng.vmware.com/ob-16069129/ubuntu16.04-alias

"${DOCKER_TOOL}" build "--build-dir=${BUILDDIR}" -s test-image

# Build binary
cd "${REPO_ROOT}"
ls "${REPO_ROOT}"

echo "Building Binaries"
make docker-bin

echo "Building Image"

VERSION=vmware-master make ubuntu

# Create archives for scripts and binaries
echo "Saving Deliverables"
OUTPUT_DIR=${BUILDROOT}/output
mkdir -p ${OUTPUT_DIR}/images
mkdir -p ${OUTPUT_DIR}/bin
cd ${REPO_ROOT}/build/images/scripts
tar -czf ${OUTPUT_DIR}/bin/scripts.tar.gz *
cd ${REPO_ROOT}/bin
tar -czf ${OUTPUT_DIR}/bin/bin.tar.gz *
docker save -o ${OUTPUT_DIR}/images/antrea-ubuntu.tar antrea/antrea-ubuntu:vmware-master

echo "antrea_build.sh end"
