#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

echo "****** antrea_build.sh start ******"

env
cat /proc/cpuinfo
source release.config

REPO_ROOT="${PROJECT_DIR}/src"

# Update Docker to a version that supports multi-stage builds
echo  "====== Updating Docker ======"
chmod a+x install_docker.sh
sudo ./install_docker.sh

echo "====== Installing docker-tool ======"
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

echo "====== docker-tool Installed ======"

docker version

COMPCACHE="$(readlink -f ${BUILDROOT}/../../compcache)"

if [ "${BRANCH_NAME}" = "vmware-master" ]; then
  IMAGE_VERSION=vmware-master
else
  IMAGE_VERSION="v${BRANCH_NAME#vmware-}_vmware.${VMWARE_RELEASE_VERSION}"
fi
"${DOCKER_TOOL}" --registry registry2.nicira.eng.vmware.com configure \
  "--buildid=${IMAGE_VERSION}" --build-dir=${BUILDDIR} --jobs=4 \
  "--compcache=${COMPCACHE}"

cd "${REPO_ROOT}"
git status

echo "====== Patching Antrea Repo ======"
# Patching build scripts and Dockerfiles
git cherry-pick HEAD..origin/build-debian
git status

echo "====== Building Binaries ======"
make docker-bin
rm -f bin/antrea-octant-plugin

echo "====== Building Images ======"

echo "====== Building Photon Images ======"
echo "====== Preparing local Photon Yum Repo ======"
mkdir -p /tmp/photo-iso
sudo mount -o loop "${GOBUILD_CSC_PHOTON_ROOT}/csc-photon-3.0.0-x86_64.iso" /tmp/photo-iso
pushd "/tmp/photo-iso"
run_python -m SimpleHTTPServer 8080 &
popd

REPO_URL="http://`ip -f inet -o address show scope global | head -n 1| cut -f 7 -d ' ' | cut -f 1 -d '/'`:8080/RPMS"
sed -i -e "s|baseurl=.*\$|baseurl=${REPO_URL}|g" "${PROJECT_DIR}/images/ovs-photon/photon-iso.repo"
sed -i -e "s|baseurl=.*\$|baseurl=${REPO_URL}|g" "${PROJECT_DIR}/images/antrea-photon/photon-iso.repo"

echo "====== Archiving OpenvSwitch Source Code ======"
OPENVSWITCH_DIR="$(readlink -e ${PROJECT_DIR}/../ovs/src)"
pushd "${OPENVSWITCH_DIR}"
git archive --format=tar.gz --prefix=openvswitch-2.13.0/ -o openvswitch-2.13.0.tar.gz HEAD
popd

echo "====== Buildling openvswitch-photon Image ======"
pushd "${PROJECT_DIR}/images/ovs-photon/"
cp "${GOBUILD_CSC_PHOTON_ROOT}/docker-image/photon-rootfs.tar.gz" .
cp "${OPENVSWITCH_DIR}/openvswitch-2.13.0.tar.gz" .
docker build --target ovs-rpms -t antrea/openvswitch-rpms-photon .
docker build --cache-from antrea/openvswitch-rpms-photon -t antrea/openvswitch-photon .
rm -f photon-rootfs.tar.gz
popd

echo "====== Buildling antrea-photon Image ======"
cp -vf ${PROJECT_DIR}/images/antrea-photon/* .
cp "${GOBUILD_CSC_PHOTON_ROOT}/docker-image/photon-rootfs.tar.gz" .
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
docker build -t vmware.io/antrea/antrea-photon:${IMAGE_VERSION} .
rm -f photon-rootfs.tar.gz

jobs -l
ps aux | grep python
pgrep -P $(jobs -p %?SimpleHTTPServer)
pkill -SIGTERM -P $(jobs -p %?SimpleHTTPServer)
wait %?SimpleHTTPServer || echo wait returns error $? as expected
sudo lsof /tmp/photo-iso || true  # If no process is using photon-iso, lsof returns 1
sudo umount /tmp/photo-iso

echo "====== Building Debian Images ======"
echo "====== Building openvswitch-debian Image ======"
pushd build/images/ovs
docker build -t antrea/openvswitch-debian .
popd

echo "====== Building antrea-debian Image ======"
make debian VERSION=${IMAGE_VERSION}

# Create archives for scripts and binaries
echo "====== Saving Deliverables ======"
OUTPUT_DIR="${BUILDROOT}/output"

mkdir -p "${OUTPUT_DIR}/manifests"
echo ANTREA_VERSION=${IMAGE_VERSION} >> "${OUTPUT_DIR}/manifests/version"
echo ANTREA_BRANCH=${BRANCH_NAME} >> "${OUTPUT_DIR}/manifests/version"
echo ANTREA_BUILD=${BUILD_NUMBER} >> "${OUTPUT_DIR}/manifests/version"

# Antrea yamls for TKG
cp "${REPO_ROOT}/build/yamls/antrea.yml" "${OUTPUT_DIR}/manifests"
cp "${REPO_ROOT}/build/yamls/antrea-ipsec.yml" "${OUTPUT_DIR}/manifests"
for YAML in ${OUTPUT_DIR}/manifests/*.yml ; do
  sed -i "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-debian:${IMAGE_VERSION}/g" "${YAML}"
done

# Antrea yamls for TGK Guest Cluster. antrea-ipsec is not supported yet
for k8s_version in "1.16" "1.17" "1.18"; do
  mkdir -p "${OUTPUT_DIR}/add-on/${k8s_version}"
  cat "${REPO_ROOT}/build/yamls/antrea.yml" | \
    sed "s/image: antrea\/antrea-.*\$/image: vmware.io\/antrea\/antrea-photon:${IMAGE_VERSION}/g" | \
    gawk -f "${PROJECT_DIR}/update-gc-config.awk" > "${OUTPUT_DIR}/add-on/${k8s_version}/antrea.yml"
done

mkdir -p "${OUTPUT_DIR}/bin"
cd "${REPO_ROOT}/bin"
tar -czf "${OUTPUT_DIR}/bin/bin.tar.gz" *
cd "${REPO_ROOT}/build/images/scripts"
tar -czf "${OUTPUT_DIR}/bin/scripts.tar.gz" *

mkdir -p "${OUTPUT_DIR}/images"
# We don't need openvswitch image in all-in-one yaml deployment, so don't publish it
# Just publish Antrea images.
docker save vmware.io/antrea/antrea-photon:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-photon-${IMAGE_VERSION}.tar.gz"
docker save antrea/antrea-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-debian-${IMAGE_VERSION}.tar.gz"

echo "====== Signing Image Deliverables ======"
CHECKSUM_FILENAME="antrea-${IMAGE_VERSION}-image-checksums.txt"
cd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${CHECKSUM_FILENAME}
gpgsignc textsign -i ${CHECKSUM_FILENAME} -o "${CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=001E5CC9

echo "====== Saving OpenvSwitch RPMs ======"
mkdir -p "${OUTPUT_DIR}/rpms/"
docker run -idt --rm --name ovs-rpms antrea/openvswitch-rpms-photon sh
docker cp ovs-rpms:/tmp/ovs-rpms "${OUTPUT_DIR}/rpms/photon"
docker stop ovs-rpms

echo "****** antrea_build.sh end ******"
