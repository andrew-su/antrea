
echo "====== Disabling --pull in All Makefile Docker Build Target ======"
export NO_PULL=1

echo "====== Archiving OpenvSwitch Source Code ======"
archive_ovs_source

echo "====== Generating version Files for CI and Consumers ======"
publish_version_files

echo "====== Checkout Features Branch ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-features
check_manifests

echo "===== Compile antrea e2e testcases ======"
compile_e2e "noipsec" "tkgm"
compile_e2e "ipsec" "tkgm"
git status

echo "====== Building Binaries for TKGm advanced ======"
fips_make

echo "====== Building Debian Images ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
echo "====== Building openvswitch-debian Image ======"
docker build -f Dockerfile.debian --build-arg OVS_VERSION=${OVS_VER} -t antrea/openvswitch-debian:standard .
echo "====== Building openvswitch-debian-ipsec Image ======"
docker build -f Dockerfile.debian --build-arg IPSEC=true --build-arg OVS_VERSION=${OVS_VER} -t antrea/openvswitch-debian:ipsec .
popd

cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
cp ${GOBUILD_CAYMAN_WHEREABOUTS_ROOT}/lin64/whereabouts/images/whereabouts-*.tar.gz .
echo "====== Building antrea-debian Image ======"
make debian VERSION=${IMAGE_VERSION}

echo "====== Building flow-aggregator-debian Image ======"
make flow-aggregator-image-debian VERSION=${IMAGE_VERSION}

echo "====== Building antrea-debian-ipsec Image ======"
make debian-ipsec VERSION=${IMAGE_VERSION}

# Create archives for scripts and binaries
echo "====== Saving TKGm Deliverables ======"
# "${BUILDROOT}/output" will be published to lin64/antrea by antrea_defs.py:CaymanAntreaBuilderLin.install
OUTPUT_DIR="${BUILDROOT}/output"

echo "====== Saving TKGm Manifests ======"
mkdir -p "${OUTPUT_DIR}/manifests"

# Antrea yamls for TKG
# antrea-ipsec is not used in TKGm.
MANIFESTS_DIR=$(mktemp -d)
IMG_NAME=antrea/antrea-advanced-debian IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-standard-manifests.sh --mode release --out "${MANIFESTS_DIR}"
IMG_NAME=antrea/flow-aggregator-debian IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-manifest-flow-aggregator.sh --mode release > "${MANIFESTS_DIR}/flow-aggregator.yml"
IMG_NAME=antrea/antrea-advanced-debian IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-manifest.sh --flow-exporter --extra-helm-values-file "${REPO_ROOT}/ci/kind/values-flow-exporter.yml" --mode release > "${MANIFESTS_DIR}"/antrea-flow-visibility-test.yml
cp ${MANIFESTS_DIR}/antrea-advanced.yml "${OUTPUT_DIR}/manifests/antrea-${BINARY_VERSION}.yml"
cp ${MANIFESTS_DIR}/antrea-advanced-fips.yml "${OUTPUT_DIR}/manifests/antrea-fips-${BINARY_VERSION}.yml"
cp ${MANIFESTS_DIR}/flow-aggregator.yml "${OUTPUT_DIR}/manifests/flow-aggregator-${BINARY_VERSION}.yml"
cp ${MANIFESTS_DIR}/antrea-flow-visibility-test.yml "${OUTPUT_DIR}/manifests/antrea-flow-visibility-test-${BINARY_VERSION}.yml"

MANIFESTS_DIR=$(mktemp -d)
IMG_NAME=antrea/antrea-advanced-debian-ipsec IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-standard-manifests.sh --mode release --out "${MANIFESTS_DIR}"
cp ${MANIFESTS_DIR}/antrea-advanced-ipsec.yml "${OUTPUT_DIR}/manifests/antrea-ipsec-${BINARY_VERSION}.yml"

echo "====== Saving and Signing TKGm Images ======"
# Image for TKG
mkdir -p "${OUTPUT_DIR}/images"
# We don't need openvswitch image in all-in-one yaml deployment, so don't publish it
# Just publish Antrea images.
docker tag antrea/antrea-debian:${IMAGE_VERSION} antrea/antrea-advanced-debian:${IMAGE_VERSION}
docker save antrea/antrea-advanced-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-advanced-debian-${IMAGE_VERSION}.tar.gz"
docker tag antrea/antrea-debian-ipsec:${IMAGE_VERSION} antrea/antrea-advanced-debian-ipsec:${IMAGE_VERSION}
docker save antrea/antrea-advanced-debian-ipsec:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-advanced-debian-ipsec-${IMAGE_VERSION}.tar.gz"
docker save antrea/flow-aggregator-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/flow-aggregator-debian-${IMAGE_VERSION}.tar.gz"
image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-advanced-debian:${IMAGE_VERSION}")"
digest_filename="antrea-advanced-debian-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-advanced-debian-${IMAGE_VERSION}-image-checksums.txt"
echo "antrea/antrea-advanced-debian@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

echo "====== Saving and Signing TKGm Executables ======"
rm -rf "${OUTPUT_DIR}/executables"
mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
gzip -c "bin/e2e-tkgm-${ANTREA_VERSION}" > "${OUTPUT_DIR}/executables/e2e-tkgm-${ANTREA_VERSION}.gz"
gzip -c "bin/e2e-tkgm-ipsec-${ANTREA_VERSION}" > "${OUTPUT_DIR}/executables/e2e-tkgm-ipsec-${ANTREA_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- * > ${BINARY_CHECKSUM_FILENAME}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

echo "====== Building Antrea Advanced Windows Deliverables ======"
build_windows "advanced" "signed"
rm ${PUBLISH_DIR}/antrea-windows-*.zip
