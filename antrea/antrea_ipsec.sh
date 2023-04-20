#!/usr/bin/env bash

echo "====== Disabling --pull in All Makefile Docker Build Target ======"
export NO_PULL=1

echo "====== Enable IPSec in Makefile ======"
export IPSEC=1

echo "====== Archiving OpenvSwitch Source Code ======"
archive_ovs_source

echo "====== Generating version Files for CI and Consumers ======"
publish_version_files

echo "====== Checkout Features Branch ======"
git reset --hard "origin/topic/${ANTREA_VERSION_DIGIT}-features"
check_manifests

echo "===== Compile antrea e2e testcases ======"
compile_e2e "ipsec" "advanced"
git status

echo "====== Building Binaries for Antrea IPsec ======"
fips_make

echo "====== Building OpenvSwitch Debian Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
./build.sh --distro debian --ipsec
popd

echo "====== Building Debian Base Image ======"
pushd build/images/base
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
prepare_whereabouts_tgz .
./build.sh --distro debian --ipsec
popd

echo "====== Building antrea-debian-ipsec Image ======"
make debian VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"
docker tag antrea/antrea-debian:${IMAGE_VERSION} antrea/antrea-debian-ipsec:${IMAGE_VERSION}

echo "====== Building OpenvSwitch UBI Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
./build.sh --distro ubi --ipsec
popd

echo "====== Building UBI Base Image ======"
pushd build/images/base
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
prepare_whereabouts_tgz .
./build.sh --distro ubi --ipsec
popd

echo "====== Building antrea-ubi-ipsec Image ======"
make ubi VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"
docker tag antrea/antrea-ubi:${IMAGE_VERSION} antrea/antrea-ubi-ipsec:${IMAGE_VERSION}
docker tag antrea/antrea-ubi:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-ubi-ipsec:${IMAGE_VERSION}

# Create archives for scripts and binaries
echo "====== Saving Antrea IPsec Deliverables ======"
# "${BUILDROOT}/output" will be published to lin64/antrea by antrea_defs.py:CaymanAntreaBuilderLin.install
OUTPUT_DIR="${BUILDROOT}/output"

echo "====== Saving Antrea IPsec Manifests ======"
mkdir -p "${OUTPUT_DIR}/manifests"

MANIFESTS_DIR=$(mktemp -d)
IMG_NAME=antrea/antrea-advanced-debian-ipsec IMG_TAG=${IMAGE_VERSION} "${REPO_ROOT}/hack/generate-standard-manifests.sh" --mode release --out "${MANIFESTS_DIR}"
cp "${MANIFESTS_DIR}/antrea-advanced-ipsec.yml" "${OUTPUT_DIR}/manifests/antrea-ipsec-${BINARY_VERSION}.yml"
MANIFESTS_DIR=$(mktemp -d)
IMG_NAME=localhost:5000/vmware.io/antrea/antrea-ubi-ipsec IMG_TAG=${IMAGE_VERSION} "${REPO_ROOT}/hack/generate-standard-manifests.sh" --mode release --out "${MANIFESTS_DIR}"
cp "${MANIFESTS_DIR}/antrea-advanced-ipsec.yml" "${OUTPUT_DIR}/manifests/antrea-ubi-ipsec-${BINARY_VERSION}.yml"

echo "====== Saving and Signing Antrea IPsec Images ======"
mkdir -p "${OUTPUT_DIR}/images"
docker tag antrea/antrea-debian-ipsec:${IMAGE_VERSION} antrea/antrea-advanced-debian-ipsec:${IMAGE_VERSION}
docker save antrea/antrea-advanced-debian-ipsec:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-advanced-debian-ipsec-${IMAGE_VERSION}.tar.gz"
docker save localhost:5000/vmware.io/antrea/antrea-ubi-ipsec:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-ubi-ipsec-${IMAGE_VERSION}.tar.gz"
image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-advanced-debian-ipsec:${IMAGE_VERSION}")"
digest_filename="antrea-advanced-debian-${IMAGE_VERSION}-image-digests.txt"
CHECKSUM_FILENAME="antrea-advanced-debian-${IMAGE_VERSION}-image-checksums.txt"
echo "antrea/antrea-advanced-debian@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-ubi-ipsec:${IMAGE_VERSION}")"
echo "localhost:5000/vmware.io/antrea/antrea-ubi@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > "${CHECKSUM_FILENAME}"
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i "${CHECKSUM_FILENAME}" -o "${CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

echo "====== Saving and Signing Antrea IPsec Executables ======"
rm -rf "${OUTPUT_DIR}/executables"
mkdir -p "${OUTPUT_DIR}/executables"
gzip -c "bin/e2e-advanced-ipsec-${ANTREA_VERSION}" > "${OUTPUT_DIR}/executables/e2e-advanced-ipsec-${ANTREA_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antrea-advanced-ipsec-${BINARY_VERSION}-checksums.txt"
sha256sum -- * > ${BINARY_CHECKSUM_FILENAME}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i "${BINARY_CHECKSUM_FILENAME}" -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

echo "====== Preparing Antrea IPsec Deliverables ======"
antrea_advanced_ipsec_deliverables="antrea-advanced-ipsec-${ANTREA_VERSION_DIGIT}"
antrea_ubi_ipsec_deliverables="antrea-ubi-ipsec-${ANTREA_VERSION_DIGIT}"
mkdir -p "${PUBLISH_DIR}/${antrea_advanced_ipsec_deliverables}"
echo "${BUILD_NUMBER}" > "${PUBLISH_DIR}/${antrea_advanced_ipsec_deliverables}/build_number.txt"
cp -r "${OUTPUT_DIR}/images/antrea-advanced-debian-ipsec-${IMAGE_VERSION}.tar.gz" "${PUBLISH_DIR}/${antrea_advanced_ipsec_deliverables}"
cp -r "${OUTPUT_DIR}/manifests/antrea-ipsec-${BINARY_VERSION}.yml" "${PUBLISH_DIR}/${antrea_advanced_ipsec_deliverables}"
pushd "${PUBLISH_DIR}"
zip --verbose -r "${antrea_advanced_ipsec_deliverables}.zip" "${antrea_advanced_ipsec_deliverables}"
popd
mkdir -p "${PUBLISH_DIR}/${antrea_ubi_ipsec_deliverables}"
echo "${BUILD_NUMBER}" > "${PUBLISH_DIR}/${antrea_ubi_ipsec_deliverables}/build_number.txt"
cp -r "${OUTPUT_DIR}/images/antrea-ubi-ipsec-${IMAGE_VERSION}.tar.gz" "${PUBLISH_DIR}/${antrea_ubi_ipsec_deliverables}"
cp -r "${OUTPUT_DIR}/manifests/antrea-ubi-ipsec-${BINARY_VERSION}.yml" "${PUBLISH_DIR}/${antrea_ubi_ipsec_deliverables}"
pushd "${PUBLISH_DIR}"
zip --verbose -r "${antrea_ubi_ipsec_deliverables}.zip" "${antrea_ubi_ipsec_deliverables}"
popd

echo "====== Cleanup Antrea IPsec Build Result ======"
make clean
rm -rf "${PUBLISH_DIR}/${antrea_advanced_ipsec_deliverables}"
rm -rf "${PUBLISH_DIR}/${antrea_ubi_ipsec_deliverables}"
rm -rf "${OUTPUT_DIR}/images"
rm -f ${OUTPUT_DIR}/manifests/antrea*.yml
rm -f ${OUTPUT_DIR}/executables/*-checksums.txt ${OUTPUT_DIR}/executables/*-checksums.txt.asc
