
echo "====== Disabling --pull in All Makefile Docker Build Target ======"
export NO_PULL=1

echo "====== Archiving OpenvSwitch Source Code ======"
archive_ovs_source

echo "====== Generating version Files for CI and Consumers ======"
publish_version_files

echo "====== Checkout Common Branch ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-common
check_manifests

echo "===== Compile antrea e2e testcases ======"
compile_e2e "noipsec" "tkgm-standard"
git status

echo "====== Building Binaries for TKGm ======"
fips_make

echo "====== Building openvswitch-debian Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
docker build -f Dockerfile.debian --build-arg OVS_VERSION=${OVS_VER} -t antrea/openvswitch-debian:standard .
popd

echo "====== Building Binaries for Antrea Standard Product ======"
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
prepare_whereabouts_tgz .
echo "====== Building Debian TKGm standard Images ======"
make debian VERSION=${IMAGE_VERSION}
make flow-aggregator-image-debian VERSION=${IMAGE_VERSION}

# Create archives for scripts and binaries
echo "====== Saving TKGm Deliverables ======"
# "${BUILDROOT}/output" will be published to lin64/antrea by antrea_defs.py:CaymanAntreaBuilderLin.install
OUTPUT_DIR="${BUILDROOT}/output"

echo "====== Generating TKGm Manifests ======"
mkdir -p "${OUTPUT_DIR}/manifests"
MANIFESTS_DIR=$(mktemp -d)
IMG_NAME=antrea/antrea-standard-debian IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-standard-manifests.sh --mode release --out "${MANIFESTS_DIR}"
IMG_NAME=antrea/flow-aggregator-debian IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-manifest-flow-aggregator.sh --mode release > "${MANIFESTS_DIR}"/flow-aggregator.yml
IMG_NAME=antrea/antrea-standard-debian IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-manifest.sh --feature-gates FlowExporter=true --extra-helm-values-file "${REPO_ROOT}/ci/kind/values-flow-exporter.yml" --mode release > "${MANIFESTS_DIR}"/antrea-flow-visibility-test.yml
cp "${MANIFESTS_DIR}/antrea-standard.yml" "${OUTPUT_DIR}/manifests/antrea-standard-${BINARY_VERSION}.yml"
cp "${MANIFESTS_DIR}/flow-aggregator.yml" "${OUTPUT_DIR}/manifests/flow-aggregator-${BINARY_VERSION}.yml"
cp "${MANIFESTS_DIR}/antrea-flow-visibility-test.yml" "${OUTPUT_DIR}/manifests/antrea-flow-visibility-test-${BINARY_VERSION}.yml"

echo "====== Saving and Signing TKGm Images ======"
mkdir -p "${OUTPUT_DIR}/images"
# We don't need openvswitch image in all-in-one yaml deployment, so don't publish it
# Just publish Antrea images.
docker tag antrea/antrea-debian:${IMAGE_VERSION} antrea/antrea-standard-debian:${IMAGE_VERSION}
docker save antrea/antrea-standard-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-standard-debian-${IMAGE_VERSION}.tar.gz"
docker save antrea/flow-aggregator-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/flow-aggregator-debian-${IMAGE_VERSION}.tar.gz"
image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-standard-debian:${IMAGE_VERSION}")"
digest_filename="antrea-standard-debian-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-standard-debian-${IMAGE_VERSION}-image-checksums.txt"
echo "antrea/antrea-standard-debian@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

echo "====== Saving and Signing TKGm Executables ======"
mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
gzip -c "bin/e2e-tkgm-standard-${ANTREA_VERSION}" > "${OUTPUT_DIR}/executables/e2e-tkgm-standard-${ANTREA_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- * > ${BINARY_CHECKSUM_FILENAME}
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

mkdir -p ${PUBLISH_DIR}/lin64/antrea/executables/
cp -r ${OUTPUT_DIR}/executables/* ${PUBLISH_DIR}/lin64/antrea/executables/

echo "====== Building Antrea Standard Windows Deliverables ======"
build_windows "standard" "unsigned"
rm ${PUBLISH_DIR}/antrea-windows-*.zip
