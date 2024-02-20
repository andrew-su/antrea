
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
git status

echo "====== Building Binaries for TKGm advanced ======"
fips_make

echo "====== Building OpenvSwitch Debian Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
./build.sh --distro debian
popd

echo "====== Building Debian Base Image ======"
pushd build/images/base
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
cp ${GOBUILD_CAYMAN_SURICATA_ROOT}/lin64/suricata/packages/debs/suricata_${SURICATA_VERSION}*.deb .
./build.sh --distro debian
popd

echo "====== Building antrea-agent-debian & antrea-controller-debian Images ======"
make debian VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"

echo "====== Building flow-aggregator-debian Image ======"
make flow-aggregator-image-debian VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"

# Create archives for scripts and binaries
echo "====== Saving TKGm Deliverables ======"
# "${BUILDROOT}/output" will be published to lin64/antrea by antrea_defs.py:CaymanAntreaBuilderLin.install
OUTPUT_DIR="${BUILDROOT}/output"

echo "====== Saving TKGm Scripts ======"
mkdir -p "${OUTPUT_DIR}/scripts"
mkdir -p "${OUTPUT_DIR}/scripts/capv-templates"
cp "${REPO_ROOT}/hack/wavefront-metrics.sh" "${OUTPUT_DIR}/scripts/"
cp "${REPO_ROOT}/ci/jenkins/test-vmc.sh" "${OUTPUT_DIR}/scripts/"
cp -r "${REPO_ROOT}/ci/cluster-api/vsphere/templates/" "${OUTPUT_DIR}/scripts/capv-templates/"
tar -zcf ${OUTPUT_DIR}/scripts/capv-templates.tar.gz -C ${OUTPUT_DIR}/scripts/ capv-templates
rm -rf "${OUTPUT_DIR}/scripts/capv-templates"

echo "====== Saving TKGm Manifests ======"
mkdir -p "${OUTPUT_DIR}/manifests"
# Antrea yamls for TKG
MANIFESTS_DIR=$(mktemp -d)
agent_img_name=antrea/antrea-advanced-agent-debian
controller_img_name=antrea/antrea-advanced-controller-debian
AGENT_IMG_NAME=$agent_img_name CONTROLLER_IMG_NAME=$controller_img_name IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-standard-manifests.sh --mode release --out "${MANIFESTS_DIR}"
IMG_NAME=antrea/flow-aggregator-debian IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-manifest-flow-aggregator.sh --mode release > "${MANIFESTS_DIR}/flow-aggregator.yml"
AGENT_IMG_NAME=$agent_img_name CONTROLLER_IMG_NAME=$controller_img_name IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-manifest.sh --feature-gates FlowExporter=true --extra-helm-values-file "${REPO_ROOT}/ci/kind/values-flow-exporter.yml" --mode release > "${MANIFESTS_DIR}"/antrea-flow-exporter-enabled.yml
cp ${MANIFESTS_DIR}/antrea-advanced.yml "${OUTPUT_DIR}/manifests/antrea-${BINARY_VERSION}.yml"
cp ${MANIFESTS_DIR}/antrea-advanced-fips.yml "${OUTPUT_DIR}/manifests/antrea-fips-${BINARY_VERSION}.yml"
cp ${MANIFESTS_DIR}/flow-aggregator.yml "${OUTPUT_DIR}/manifests/flow-aggregator-${BINARY_VERSION}.yml"
cp ${MANIFESTS_DIR}/antrea-flow-exporter-enabled.yml "${OUTPUT_DIR}/manifests/antrea-flow-exporter-enabled-${BINARY_VERSION}.yml"

generate_flow_visibility_e2e_manifests "${REPO_ROOT}" "${BINARY_VERSION}" "${OUTPUT_DIR}/manifests"

echo "====== Saving and Signing TKGm Images ======"
# Image for TKG
mkdir -p "${OUTPUT_DIR}/images"
# We don't need openvswitch image in all-in-one yaml deployment, so don't publish it
# Just publish Antrea images.
docker tag antrea/antrea-agent-debian:${IMAGE_VERSION} $agent_img_name:${IMAGE_VERSION}
docker tag antrea/antrea-controller-debian:${IMAGE_VERSION} $controller_img_name:${IMAGE_VERSION}
docker save $agent_img_name:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-advanced-agent-debian-${IMAGE_VERSION}.tar.gz"
docker save $controller_img_name:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-advanced-controller-debian-${IMAGE_VERSION}.tar.gz"
docker save antrea/flow-aggregator-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/flow-aggregator-debian-${IMAGE_VERSION}.tar.gz"
agent_image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-advanced-agent-debian:${IMAGE_VERSION}")"
controller_image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-advanced-controller-debian:${IMAGE_VERSION}")"
agent_digest_filename="antrea-advanced-agent-debian-${IMAGE_VERSION}-image-digests.txt"
controller_digest_filename="antrea-advanced-controller-debian-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-advanced-debian-${IMAGE_VERSION}-image-checksums.txt"
echo "antrea/antrea-advanced-agent-debian@${agent_image_id}" > "${OUTPUT_DIR}/images/${agent_digest_filename}"
echo "antrea/antrea-advanced-controller-debian@${controller_image_id}" > "${OUTPUT_DIR}/images/${controller_digest_filename}"
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
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- * > ${BINARY_CHECKSUM_FILENAME}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

echo "====== Building Antrea Advanced Windows Deliverables ======"
build_windows "advanced" "signed"
rm ${PUBLISH_DIR}/antrea-windows-*.zip
