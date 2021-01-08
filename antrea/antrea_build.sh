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

function fips_make {
  chmod +x ${GOBUILD_CAYMAN_GO_ROOT}/lin64/bin/go
  chmod +x -R ${GOBUILD_CAYMAN_GO_ROOT}/lin64/pkg/tool/linux_amd64
  mkdir -p "${REPO_ROOT}/gopath"
  mkdir -p "${REPO_ROOT}/gocache"
  mkdir -p "${REPO_ROOT}/goenv"
  ANTREA_VER=$(head -n 1 VERSION)
	docker run --rm -u $(id -u):$(id -g) \
		-e "GOCACHE=/tmp/gocache" \
		-e "GOPATH=/tmp/gopath" \
		-w /usr/src/github.com/vmware-tanzu/antrea \
		-v "${REPO_ROOT}/gopath":/tmp/gopath \
		-v "${REPO_ROOT}/gocache":/tmp/gocache \
		-v "${REPO_ROOT}/goenv":/.config/go \
		-v ${GOBUILD_CAYMAN_GO_ROOT}/lin64/src:/usr/local/go/src \
		-v ${GOBUILD_CAYMAN_GO_ROOT}/lin64/pkg:/usr/local/go/pkg \
		-v ${GOBUILD_CAYMAN_GO_ROOT}/lin64/bin:/usr/local/go/bin \
		-v ${REPO_ROOT}:/usr/src/github.com/vmware-tanzu/antrea \
		golang:1.15 /bin/bash -c "mkdir -p bin; go env -w CC='x86_64-linux-gnu-gcc'; GOOS=linux go build -o bin -ldflags ' -X github.com/vmware-tanzu/antrea/pkg/version.Version=${ANTREA_VER} -X github.com/vmware-tanzu/antrea/pkg/version.GitSHA= -X github.com/vmware-tanzu/antrea/pkg/version.GitTreeState=clean -X github.com/vmware-tanzu/antrea/pkg/version.ReleaseStatus=unreleased' github.com/vmware-tanzu/antrea/cmd/..."
  chmod -R 0755 bin
}

cp open_source_licenses.txt "${PUBLISH_DIR}/"
pushd "${PUBLISH_DIR}/"
curl -O https://build-artifactory.eng.vmware.com/nsx-ujo-local/antrea/VMware-Antrea-1.1.0-0.11.1-ODP.tar.gz
popd

# Update Docker to a version that supports multi-stage builds
echo  "====== Updating Docker ======"
chmod a+x install_docker.sh
sudo ./install_docker.sh

function run_python {
  PYTHON="${GOBUILD_CAYMAN_PYTHON_ROOT}/lin64/bin/python"
  "${PYTHON}" "$@"
}

docker version

docker pull nsx-ujo-docker-local.artifactory.eng.vmware.com/golang:1.15
docker tag nsx-ujo-docker-local.artifactory.eng.vmware.com/golang:1.15 golang:1.15

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

# NOTE:
# Antrea standard deiverables
# image and executable: upstream + cherry-pick(tkg) (tkg is for Debian build patches)
# manifest: upstream + sed(image_name)

# Antrea advanced deiverables
# image and executable: upstream + cherry-pick(enterprise-features + tkg)
# manifest: upstream + cherry-pick(enterprise-features) + sed(image_name)

# TKG deiverables
# image and executable: upstream + cherry-pick(enterprise-features + tkg)
# manifest: upstream + cherry-pick(enterprise-features + tkg) + sed(image_name)

# TKGS deiverables
# image and executable: upstream + cherry-pick(enterprise-features + tkgs)
# manifest: upstream + cherry-pick(tkgs) + sed(image_name)

echo "====== Preparing Antrea Standard Product Deliverables: Debian Manifests ======"
git reset --hard "${UPSTREAM_COMMIT}"
antrea_std_deliverables="antrea-standard-${BRANCH_NAME#vmware-}.${BUILD_NUMBER}"
mkdir -p "${PUBLISH_DIR}/${antrea_std_deliverables}"
mkdir -p "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests"
# antrea-ipsec is not used in commecial release
cp "${REPO_ROOT}/build/yamls/antrea.yml" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/antrea-standard-${BINARY_VERSION}.yml"
for YAML in ${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/*.yml ; do
  sed -i -e "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-standard-debian:${IMAGE_VERSION}/g" "${YAML}"
  sed -i -e "s/image: projects.registry.vmware.com\/antrea\/antrea-.*\$/image: antrea\/antrea-standard-debian:${IMAGE_VERSION}/g" "${YAML}"
done

echo "====== Preparing Antrea Advanced Product Deliverables: Debian Manifests ======"
git reset --hard "${UPSTREAM_COMMIT}"
git cherry-pick --keep-redundant-commits HEAD..origin/topic/${BRANCH_NAME#vmware-}-features
antrea_adv_deliverables="antrea-advanced-${BRANCH_NAME#vmware-}.${BUILD_NUMBER}"
mkdir -p "${PUBLISH_DIR}/${antrea_adv_deliverables}"
mkdir -p "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests"
# antrea-ipsec is not used in commecial release
cp "${REPO_ROOT}/build/yamls/antrea.yml" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/antrea-advanced-${BINARY_VERSION}.yml"
for YAML in ${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/*.yml ; do
  sed -i -e "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-advanced-debian:${IMAGE_VERSION}/g" "${YAML}"
  sed -i -e "s/image: projects.registry.vmware.com\/antrea\/antrea-.*\$/image: antrea\/antrea-advanced-debian:${IMAGE_VERSION}/g" "${YAML}"
done

echo "====== Archiving OpenvSwitch Source Code ======"
git reset --hard "${UPSTREAM_COMMIT}"
OPENVSWITCH_DIR="$(readlink -e ${PROJECT_DIR}/../ovs/src)"
OPENVSWITCH_VERSION="2.14.0"
pushd "${OPENVSWITCH_DIR}"
git archive --format=tar.gz --prefix=openvswitch-${OPENVSWITCH_VERSION}/ -o openvswitch-${OPENVSWITCH_VERSION}.tar.gz HEAD
popd


echo "====== Patching Antrea Repo for TKGS ======"
# Patching build scripts and Dockerfiles
git cherry-pick --keep-redundant-commits HEAD..origin/topic/${BRANCH_NAME#vmware-}-tkgs
git status

echo "====== Building Binaries for TKGS ======"
fips_make

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

echo "====== Buildling openvswitch-photon Image ======"
pushd "${PROJECT_DIR}/images/ovs-photon/"
cp "${GOBUILD_CSC_PHOTON_ROOT}/docker-image/photon-rootfs.tar.gz" .
cp ${OPENVSWITCH_DIR}/openvswitch-*.tar.gz .
docker build --target ovs-rpms -t antrea/openvswitch-rpms-photon .
docker build --cache-from antrea/openvswitch-rpms-photon -t antrea/openvswitch-photon .
rm -f photon-rootfs.tar.gz
popd

echo "====== Building antrea-photon Image ======"
cp -vf ${PROJECT_DIR}/images/antrea-photon/* .
cp "${GOBUILD_CSC_PHOTON_ROOT}/docker-image/photon-rootfs.tar.gz" .
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
docker build -t localhost:5000/vmware.io/antrea/antrea-photon:${IMAGE_VERSION} .
rm -f photon-rootfs.tar.gz

echo "====== Saving TKGS Deliverables ======"

echo "====== Saving TKGS Manifests ======"
# Antrea yamls for TKG Service. antrea-ipsec is not supported yet
# Complicated Yaml customization is done directly in Antrea topic/tkgs branch
# Here we only replace image version
for k8s_version in "1.18" "1.19" "1.20" "1.21"; do
  mkdir -p "${PUBLISH_DIR}/add-on/${k8s_version}"
  cp "${REPO_ROOT}/build/yamls/antrea.yml" "${PUBLISH_DIR}/add-on/${k8s_version}/antrea.yaml"
  sed -i -e "s/image: antrea\/antrea-.*\$/image: localhost:5000\/vmware.io\/antrea\/antrea-photon:${IMAGE_VERSION}/g" "${PUBLISH_DIR}/add-on/${k8s_version}/antrea.yaml"
  sed -i -e "s/image: projects.registry.vmware.com\/antrea\/antrea-.*\$/image: localhost:5000\/vmware.io\/antrea\/antrea-photon:${IMAGE_VERSION}/g" "${PUBLISH_DIR}/add-on/${k8s_version}/antrea.yaml"
done

echo "====== Saving TKGS Binaries ======"
# Binaries for building Antrea Photon image for TKG Service
mkdir -p "${PUBLISH_DIR}/photon/bin"
pushd "${REPO_ROOT}/bin"
tar -czf "${PUBLISH_DIR}/photon/bin/bin.tar.gz" *
popd
pushd "${REPO_ROOT}/build/images/scripts"
tar -czf "${PUBLISH_DIR}/photon/bin/scripts.tar.gz" *
popd

# RPMs for building Antrea Photon image for TKG Service
echo "====== Saving OpenvSwitch RPMs ======"
mkdir -p "${PUBLISH_DIR}/photon/rpms/"
docker run -idt --rm --name ovs-rpms antrea/openvswitch-rpms-photon sh
docker cp ovs-rpms:/tmp/ovs-rpms "${PUBLISH_DIR}/photon/rpms/"
docker stop ovs-rpms

echo "====== Saving and Signing TKGs Images ======"

# A test photon image
image_id="$(docker inspect -f '{{.ID}}' "localhost:5000/vmware.io/antrea/antrea-photon:${IMAGE_VERSION}")"
digest_filename="antrea-photon-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-photon-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${PUBLISH_DIR}/photon/images"
docker save localhost:5000/vmware.io/antrea/antrea-photon:${IMAGE_VERSION} | gzip -9 > "${PUBLISH_DIR}/photon/images/antrea-photon-${IMAGE_VERSION}.tar.gz"
echo "localhost:5000/vmware.io/antrea/antrea-photon@${image_id}" > "${PUBLISH_DIR}/photon/images/${digest_filename}"
pushd "${PUBLISH_DIR}/photon/images"
sha256sum -- * > ${checksum_filename}
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Cleanup TKGS Build Result ======"
make clean

echo "====== Patching Antrea Repo for Antrea Standard Product ======"
git reset --hard "${UPSTREAM_COMMIT}"
git cherry-pick --keep-redundant-commits HEAD..origin/topic/${BRANCH_NAME#vmware-}-tkg
git status

echo "====== Building Binaries for Antrea Standard Product ======"
fips_make

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
echo "====== Saving Antrea Standard Product Deliverables ======"
OUTPUT_DIR="${BUILDROOT}/standard-output"

echo "====== Saving and Signing Antrea Standard Product Images ======"

image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-debian:${IMAGE_VERSION}")"
digest_filename="antrea-standard-debian-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-standard-debian-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${OUTPUT_DIR}/images"
# We don't need openvswitch image in all-in-one yaml deployment, so don't publish it
# Just publish Antrea images.
docker tag antrea/antrea-debian:${IMAGE_VERSION} antrea/antrea-standard-debian:${IMAGE_VERSION}
docker save antrea/antrea-standard-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-standard-debian-${IMAGE_VERSION}.tar.gz"
echo "antrea/antrea-standard-debian@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Saving and Signing Antrea Standard Product Executables ======"

mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- "antctl-${BINARY_VERSION}.gz" > ${BINARY_CHECKSUM_FILENAME}
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Cleanup Antrea Standard Product Build Result ======"
make clean

echo "====== Preparing Antrea Standard Product Deliverables: Images, executables ======"
cp -r "${OUTPUT_DIR}/images" "${PUBLISH_DIR}/${antrea_std_deliverables}"
cp -r "${OUTPUT_DIR}/executables" "${PUBLISH_DIR}/${antrea_std_deliverables}"
pushd "${PUBLISH_DIR}"
zip --verbose -r "${antrea_std_deliverables}.zip" "${antrea_std_deliverables}"
rm -rf "${PUBLISH_DIR}/${antrea_std_deliverables}"
popd


echo "====== Patching Antrea Repo for TKGm ======"
git reset --hard "${UPSTREAM_COMMIT}"
git cherry-pick --keep-redundant-commits HEAD..origin/topic/${BRANCH_NAME#vmware-}-features
git cherry-pick --keep-redundant-commits HEAD..origin/topic/${BRANCH_NAME#vmware-}-tkg
git status

echo "====== Building Binaries for TKGm ======"
fips_make

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
echo "====== Saving TKGm Deliverables ======"
OUTPUT_DIR="${BUILDROOT}/output"

echo "====== Saving TKGm Manifests ======"
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
# Here we only replace image version.
# antrea-ipsec is not used in TKGm.
cp "${REPO_ROOT}/build/yamls/antrea.yml" "${OUTPUT_DIR}/manifests/antrea-${BINARY_VERSION}.yml"
for YAML in ${OUTPUT_DIR}/manifests/*.yml ; do
  sed -i -e "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-debian:${IMAGE_VERSION}/g" "${YAML}"
  sed -i -e "s/image: projects.registry.vmware.com\/antrea\/antrea-.*\$/image: antrea\/antrea-debian:${IMAGE_VERSION}/g" "${YAML}"
done

echo "====== Saving and Signing TKGm Images ======"

# Image for TKG
image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-debian:${IMAGE_VERSION}")"
digest_filename="antrea-debian-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-debian-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${OUTPUT_DIR}/images"
# We don't need openvswitch image in all-in-one yaml deployment, so don't publish it
# Just publish Antrea images.
docker save antrea/antrea-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-debian-${IMAGE_VERSION}.tar.gz"
echo "antrea/antrea-debian@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Saving and Signing TKGm Executables ======"
rm -rf "${OUTPUT_DIR}/executables"
mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- "antctl-${BINARY_VERSION}.gz" > ${BINARY_CHECKSUM_FILENAME}
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Saving Antrea Advanced Product Deliverables ======"
OUTPUT_DIR="${BUILDROOT}/advanced-output"

echo "====== Saving and Signing Antrea Advanced Product Images ======"

image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-debian:${IMAGE_VERSION}")"
digest_filename="antrea-advanced-debian-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-advanced-debian-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${OUTPUT_DIR}/images"
# We don't need openvswitch image in all-in-one yaml deployment, so don't publish it
# Just publish Antrea images.
docker tag antrea/antrea-debian:${IMAGE_VERSION} antrea/antrea-advanced-debian:${IMAGE_VERSION}
docker save antrea/antrea-advanced-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-advanced-debian-${IMAGE_VERSION}.tar.gz"
echo "antrea/antrea-advanced-debian@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Saving and Signing Antrea Advanced Product Executables ======"

mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- "antctl-${BINARY_VERSION}.gz" > ${BINARY_CHECKSUM_FILENAME}
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Preparing Antrea Advanced Product Deliverables: Images, executables ======"
cp -r "${OUTPUT_DIR}/images" "${PUBLISH_DIR}/${antrea_adv_deliverables}"
cp -r "${OUTPUT_DIR}/executables" "${PUBLISH_DIR}/${antrea_adv_deliverables}"
pushd "${PUBLISH_DIR}"
zip --verbose -r "${antrea_adv_deliverables}.zip" "${antrea_adv_deliverables}"
rm -rf "${PUBLISH_DIR}/${antrea_adv_deliverables}"
popd

echo "====== Cleanup TKGm Build Result ======"
make clean

echo "****** antrea_build.sh end ******"
