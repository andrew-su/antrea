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

cp open_source_licenses.txt "${PUBLISH_DIR}/"

# Update Docker to a version that supports multi-stage builds
echo  "====== Updating Docker ======"
chmod a+x install_docker.sh
sudo ./install_docker.sh

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

docker version

docker pull nsx-ujo-docker-local.artifactory.eng.vmware.com/golang:1.13
docker tag nsx-ujo-docker-local.artifactory.eng.vmware.com/golang:1.13 golang:1.13

if [ "${BRANCH_NAME}" = "vmware-master" ]; then
  IMAGE_VERSION=vmware-master
  BINARY_VERSION=vmware-master
else
  IMAGE_VERSION="v${BRANCH_NAME#vmware-}_vmware.${VMWARE_RELEASE_VERSION}"
  BINARY_VERSION="v${BRANCH_NAME#vmware-}+vmware.${VMWARE_RELEASE_VERSION}"
fi

cd "${REPO_ROOT}"
git status
UPSTREAM_COMMIT=$(git log -1 --pretty=format:%H)

echo "====== Patching Antrea Repo ======"
# Patching build scripts and Dockerfiles
git cherry-pick HEAD..origin/topic/0.9.0-tkg
git status

echo "====== Building Binaries ======"
make docker-bin
rm -f bin/antrea-octant-plugin

echo "====== Building Images ======"

echo "====== Building Photon Images ======"
echo Photon images are for local testing, they are not consumed by cayman_photon.
echo We maintain a dedicated Antrea Dockerfile in cayman_photon. Antrea photon
echo image is actually built there.
echo "====== Preparing local Photon Yum Repo ======"
mkdir -p /tmp/photo-iso
sudo mount -o loop "${GOBUILD_CSC_PHOTON_ROOT}/csc-photon-3.0.0-x86_64.iso" /tmp/photo-iso
pushd "/tmp/photo-iso"
run_python -m SimpleHTTPServer 8080 &
popd

function stop_local_repo {
  jobs -l
  ps aux | grep python
  pgrep -P $(jobs -p %?SimpleHTTPServer)
  pkill -SIGTERM -P $(jobs -p %?SimpleHTTPServer)
  wait %?SimpleHTTPServer || echo wait returns error $? as expected
  sudo lsof /tmp/photo-iso || true  # If no process is using photon-iso, lsof returns 1
  sudo umount /tmp/photo-iso
}
trap stop_local_repo Exit

REPO_URL="http://`ip -f inet -o address show scope global | head -n 1| cut -f 7 -d ' ' | cut -f 1 -d '/'`:8080/RPMS"
sed -i -e "s|baseurl=.*\$|baseurl=${REPO_URL}|g" "${PROJECT_DIR}/images/ovs-photon/photon-iso.repo"
sed -i -e "s|baseurl=.*\$|baseurl=${REPO_URL}|g" "${PROJECT_DIR}/images/antrea-photon/photon-iso.repo"

echo "====== Archiving OpenvSwitch Source Code ======"
OPENVSWITCH_DIR="$(readlink -e ${PROJECT_DIR}/../ovs/src)"
OPENVSWITCH_VERSION="2.13.1"
pushd "${OPENVSWITCH_DIR}"
git archive --format=tar.gz --prefix=openvswitch-${OPENVSWITCH_VERSION}/ -o openvswitch-${OPENVSWITCH_VERSION}.tar.gz HEAD
popd

echo "====== Buildling openvswitch-photon Image ======"
pushd "${PROJECT_DIR}/images/ovs-photon/"
cp "${GOBUILD_CSC_PHOTON_ROOT}/docker-image/photon-rootfs.tar.gz" .
cp ${OPENVSWITCH_DIR}/openvswitch-*.tar.gz .
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

echo "====== Building Debian Images ======"
echo "====== Building openvswitch-debian Image ======"
pushd build/images/ovs
cp ${OPENVSWITCH_DIR}/openvswitch-${OPENVSWITCH_VERSION}.tar.gz .
docker build -t antrea/openvswitch-debian .
popd

echo "====== Building antrea-debian Image ======"
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
make debian VERSION=${IMAGE_VERSION}

# Create archives for scripts and binaries
echo "====== Saving Deliverables ======"
OUTPUT_DIR="${BUILDROOT}/output"

mkdir -p "${OUTPUT_DIR}/manifests"
# Define some variables in manifests/version
# Used in cayman_photon when builing antrea image
echo ANTREA_VERSION=${IMAGE_VERSION} >> "${OUTPUT_DIR}/manifests/version"
echo ANTREA_BINARY_VERSION=${BINARY_VERSION} >> "${OUTPUT_DIR}/manifests/version"
echo ANTREA_BRANCH=${BRANCH_NAME} >> "${OUTPUT_DIR}/manifests/version"
echo ANTREA_BUILD=${BUILD_NUMBER} >> "${OUTPUT_DIR}/manifests/version"
# Used by cayman_photon support/scripts/customizeOvf/customizeGcOvf.py
# to read add-on versions in a normalized way
echo "${IMAGE_VERSION}" > "${PUBLISH_DIR}/VERSION"

# Antrea yamls for TKG
# Complicated Yaml customization is done directly in Antrea topic/tkg branch
# Here we only replace image version
cp "${REPO_ROOT}/build/yamls/antrea.yml" "${OUTPUT_DIR}/manifests/antrea-${BINARY_VERSION}.yml"
cp "${REPO_ROOT}/build/yamls/antrea-ipsec.yml" "${OUTPUT_DIR}/manifests/antrea-ipsec-${BINARY_VERSION}.yml"
for YAML in ${OUTPUT_DIR}/manifests/*.yml ; do
  sed -i -e "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-debian:${IMAGE_VERSION}/g" "${YAML}"
done

# Antrea yamls for TKG Service. antrea-ipsec is not supported yet
# Complicated Yaml customization is done directly in Antrea topic/tkgs branch
# Here we only replace image version
git reset --hard "${UPSTREAM_COMMIT}"
git cherry-pick HEAD..origin/topic/0.9.0-tkgs
for k8s_version in "1.16" "1.17" "1.18"; do
  mkdir -p "${PUBLISH_DIR}/add-on/${k8s_version}"
  cat "${REPO_ROOT}/build/yamls/antrea.yml" | \
    sed "s/image: antrea\/antrea-.*\$/image: vmware.io\/antrea\/antrea-photon:${IMAGE_VERSION}/g" > "${PUBLISH_DIR}/add-on/${k8s_version}/antrea.yaml"
done

# Binaries for building Antrea Photon image for TKG Service
mkdir -p "${PUBLISH_DIR}/photon/bin"
cd "${REPO_ROOT}/bin"
tar -czf "${PUBLISH_DIR}/photon/bin/bin.tar.gz" *
cd "${REPO_ROOT}/build/images/scripts"
tar -czf "${PUBLISH_DIR}/photon/bin/scripts.tar.gz" *

# RPMs for building Antrea Photon image for TKG Service
echo "====== Saving OpenvSwitch RPMs ======"
mkdir -p "${PUBLISH_DIR}/photon/rpms/"
docker run -idt --rm --name ovs-rpms antrea/openvswitch-rpms-photon sh
docker cp ovs-rpms:/tmp/ovs-rpms "${PUBLISH_DIR}/photon/rpms/"
docker stop ovs-rpms

echo "====== Saving and Signing Images ======"

# A test photon image
image_id="$(docker inspect -f '{{.ID}}' "vmware.io/antrea/antrea-photon:${IMAGE_VERSION}")"
digest_filename="antrea-photon-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-photon-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${PUBLISH_DIR}/photon/images"
docker save vmware.io/antrea/antrea-photon:${IMAGE_VERSION} | gzip -9 > "${PUBLISH_DIR}/photon/images/antrea-photon-${IMAGE_VERSION}.tar.gz"
echo "vmware.io/antrea/antrea-photon@${image_id}" > "${PUBLISH_DIR}/photon/images/${digest_filename}"
cd "${PUBLISH_DIR}/photon/images"
sha256sum -- * > ${checksum_filename}
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9

# Image for TKG
image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-debian:${IMAGE_VERSION}")"
digest_filename="antrea-debian-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-debian-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${OUTPUT_DIR}/images"
# We don't need openvswitch image in all-in-one yaml deployment, so don't publish it
# Just publish Antrea images.
docker save antrea/antrea-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-debian-${IMAGE_VERSION}.tar.gz"
echo "antrea/antrea-debian@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
cd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9

echo "====== Saving and Signing Executables ======"

mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
cd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- "antctl-${BINARY_VERSION}.gz" > ${BINARY_CHECKSUM_FILENAME}
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=001E5CC9

echo "****** antrea_build.sh end ******"
