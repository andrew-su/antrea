
echo "====== Disabling --pull in All Makefile Docker Build Target ======"
export NO_PULL=1

echo "====== Archiving OpenvSwitch Source Code ======"
archive_ovs_source

echo "====== Generating version Files for CI and Consumers ======"
publish_version_files

echo "====== Checkout Common Branch ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-common
check_manifests

echo "====== Compiling standard antrea e2e testcases ======"
compile_e2e "noipsec" "standard"

echo "====== Building Binaries for Antrea Standard Product ======"
fips_make

echo "====== Building flow-aggregator Image ======"
FLOW_AGGREGATOR_DELIVERABLES_DIR=$(mktemp -d)
FLOW_AGGREGATOR_MANIFESTS_DIR=$(mktemp -d)
make flow-aggregator-image-debian VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"
make flow-aggregator-image-ubi VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"
echo "====== Preparing Manifests for flow-aggregator ======"
IMG_NAME=antrea/flow-aggregator-debian IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-manifest-flow-aggregator.sh --mode release > "${FLOW_AGGREGATOR_MANIFESTS_DIR}/flow-aggregator-${BINARY_VERSION}.yml"
echo "====== Saving flow-aggregator Image ======"
docker save antrea/flow-aggregator-debian:${IMAGE_VERSION} | gzip -9 > "${FLOW_AGGREGATOR_DELIVERABLES_DIR}/flow-aggregator-debian-${IMAGE_VERSION}.tar.gz"
digest_filename_flow_aggregator="flow-aggregator-debian-${IMAGE_VERSION}-image-digests.txt"
image_id_flow_aggregator_debian="$(docker inspect -f '{{.ID}}' "antrea/flow-aggregator-debian:${IMAGE_VERSION}")"
echo "antrea/flow-aggregator-debian@${image_id_flow_aggregator_debian}" > "${FLOW_AGGREGATOR_DELIVERABLES_DIR}/${digest_filename_flow_aggregator}"

echo "====== Building OpenvSwitch Debian Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
./build.sh --distro debian
popd

echo "====== Building Antrea ODS Debian Image ======"
ods_img_name=antrea/antrea-ods-debian
ODS_DELIVERABLES_DIR=$(mktemp -d)
ODS_MANIFESTS_DIR=$(mktemp -d)
make build-ods-debian VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"
echo "====== Preparing Manifests for ODS ======"
ODS_IMG_NAME=$ods_img_name IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-manifest.sh --ods-only --mode release > "${ODS_MANIFESTS_DIR}/antrea-ods-${BINARY_VERSION}.yml"
echo "====== Saving ODS Image ======"
docker save ${ods_img_name}:${IMAGE_VERSION} | gzip -9 > "${ODS_DELIVERABLES_DIR}/antrea-ods-debian-${IMAGE_VERSION}.tar.gz"
digest_filename_ods="antrea-ods-debian-${IMAGE_VERSION}-image-digests.txt"
image_id_antrea_ods_debian="$(docker inspect -f '{{.ID}}' "${ods_img_name}:${IMAGE_VERSION}")"
echo "${ods_img_name}@${image_id_antrea_ods_debian}" > "${ODS_DELIVERABLES_DIR}/${digest_filename_ods}"

echo "====== Building Debian Base Image ======"
pushd build/images/base
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
cp ${GOBUILD_CAYMAN_SURICATA_ROOT}/lin64/suricata/packages/debs/suricata_${SURICATA_VERSION}*.deb .
./build.sh --distro debian
popd

echo "====== Building Debian standard Images ======"
make debian VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"

echo "====== Preparing Antrea Standard Product Deliverables: Standard Manifests & Scripts ======"
antrea_std_deliverables="antrea-standard-${ANTREA_VERSION_DIGIT}"
mkdir -p "${PUBLISH_DIR}/${antrea_std_deliverables}"
mkdir -p "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests"
mkdir -p "${PUBLISH_DIR}/${antrea_std_deliverables}/scripts"
mkdir -p "${OUTPUT_DIR}/scripts"
mkdir -p "${OUTPUT_DIR}/scripts/capv-templates"
echo "${BUILD_NUMBER}" > "${PUBLISH_DIR}/${antrea_std_deliverables}/build_number.txt"

# antrea-ipsec is not used in commecial release
MANIFESTS_DIR=$(mktemp -d)
agent_img_name=antrea/antrea-standard-agent-debian
controller_img_name=antrea/antrea-standard-controller-debian
# TODO: add --ods option in standard manifest generation if we want to enable ods deployment by default in later releases
AGENT_IMG_NAME=$agent_img_name CONTROLLER_IMG_NAME=$controller_img_name IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-standard-manifests.sh --mode release --out "${MANIFESTS_DIR}"
AGENT_IMG_NAME=$agent_img_name CONTROLLER_IMG_NAME=$controller_img_name IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-manifest.sh --feature-gates FlowExporter=true --extra-helm-values-file "${REPO_ROOT}/ci/kind/values-flow-exporter.yml" --mode release > "${MANIFESTS_DIR}"/antrea-flow-exporter-enabled.yml
cp "${MANIFESTS_DIR}/antrea-standard.yml" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/antrea-standard-${BINARY_VERSION}.yml"
cp "${MANIFESTS_DIR}/antrea-standard-fips.yml" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/antrea-standard-fips-${BINARY_VERSION}.yml"
cp "${MANIFESTS_DIR}/antrea-standard-nponly.yml" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/antrea-standard-nponly-${BINARY_VERSION}.yml"
cp "${ODS_MANIFESTS_DIR}/antrea-ods-${BINARY_VERSION}.yml" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/"
cp "${FLOW_AGGREGATOR_MANIFESTS_DIR}/flow-aggregator-${BINARY_VERSION}.yml" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/"
cp "${MANIFESTS_DIR}/antrea-flow-exporter-enabled.yml" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/antrea-flow-exporter-enabled-${BINARY_VERSION}.yml"
cp "${REPO_ROOT}/hack/wavefront-metrics.sh" "${PUBLISH_DIR}/${antrea_std_deliverables}/scripts/"
cp "${REPO_ROOT}/ci/jenkins/test-vmc.sh" "${PUBLISH_DIR}/${antrea_std_deliverables}/scripts/"
cp -r "${REPO_ROOT}/ci/cluster-api/vsphere/templates/" "${OUTPUT_DIR}/scripts/capv-templates/"
tar -zcf ${OUTPUT_DIR}/scripts/capv-templates.tar.gz -C ${OUTPUT_DIR}/scripts/ capv-templates
rm -rf "${OUTPUT_DIR}/scripts/capv-templates"

generate_flow_visibility_e2e_manifests "${REPO_ROOT}" "${BINARY_VERSION}" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests"

# Create archives for scripts and binaries
echo "====== Saving Antrea Standard Product Deliverables ======"
OUTPUT_DIR="${BUILDROOT}/standard-output"

echo "====== Saving and Signing Antrea Standard Product Images ======"
agent_image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-agent-debian:${IMAGE_VERSION}")"
controller_image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-controller-debian:${IMAGE_VERSION}")"
agent_digest_filename="antrea-standard-agent-debian-${IMAGE_VERSION}-image-digests.txt"
controller_digest_filename="antrea-standard-controller-debian-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-standard-debian-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${OUTPUT_DIR}/images"
# We don't need openvswitch image in all-in-one yaml deployment, so don't publish it
# Just publish Antrea images.
docker tag antrea/antrea-agent-debian:${IMAGE_VERSION} $agent_img_name:${IMAGE_VERSION}
docker tag antrea/antrea-controller-debian:${IMAGE_VERSION} $controller_img_name:${IMAGE_VERSION}
docker save $agent_img_name:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-standard-agent-debian-${IMAGE_VERSION}.tar.gz"
docker save $controller_img_name:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-standard-controller-debian-${IMAGE_VERSION}.tar.gz"
echo "antrea/antrea-standard-agent-debian@${agent_image_id}" > "${OUTPUT_DIR}/images/${agent_digest_filename}"
echo "antrea/antrea-standard-controller-debian@${controller_image_id}" > "${OUTPUT_DIR}/images/${controller_digest_filename}"

# Saving flow-aggregator image
cp -rf "${FLOW_AGGREGATOR_DELIVERABLES_DIR}/." "${OUTPUT_DIR}/images/"
# Saving ODS image
cp -rf "${ODS_DELIVERABLES_DIR}/." "${OUTPUT_DIR}/images/"

pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
/build/apps/signing/gpgsign/gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

echo "====== Saving and Signing Antrea Standard Product Executables ======"
mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- * > ${BINARY_CHECKSUM_FILENAME}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
/build/apps/signing/gpgsign/gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

echo "====== Preparing Antrea Standard Product Deliverables: Images, executables ======"
cp -r "${OUTPUT_DIR}/images" "${PUBLISH_DIR}/${antrea_std_deliverables}"
cp -r "${OUTPUT_DIR}/executables" "${PUBLISH_DIR}/${antrea_std_deliverables}"
pushd "${PUBLISH_DIR}"
zip --verbose -r "${antrea_std_deliverables}.zip" "${antrea_std_deliverables}"
rm -rf "${PUBLISH_DIR}/${antrea_std_deliverables}" "${OUTPUT_DIR}"
popd

echo "====== Cleanup Antrea Standard Product Build Result ======"
make clean

echo "====== Checkout Features Branch ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-features
check_manifests

echo "====== Compiling advanced antrea e2e testcases ======"
compile_e2e "noipsec" "advanced"

echo "====== Building Binaries for Antrea Advanced Product ======"
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

echo "====== Building Debian Advanced Images ======"
make debian VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"

echo "====== Preparing Antrea Advanced Product Deliverables: Advanced Manifests & Scripts ======"
antrea_adv_deliverables="antrea-advanced-${ANTREA_VERSION_DIGIT}"
mkdir -p "${PUBLISH_DIR}/${antrea_adv_deliverables}"
mkdir -p "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests"
mkdir -p "${PUBLISH_DIR}/${antrea_adv_deliverables}/scripts"
mkdir -p "${OUTPUT_DIR}/scripts"
mkdir -p "${OUTPUT_DIR}/scripts/capv-templates"
echo "${BUILD_NUMBER}" > "${PUBLISH_DIR}/${antrea_adv_deliverables}/build_number.txt"
# antrea-ipsec is not used in commecial release
MANIFESTS_DIR=$(mktemp -d)
agent_img_name=antrea/antrea-advanced-agent-debian
controller_img_name=antrea/antrea-advanced-controller-debian
# TODO: add --ods option in standard manifest generation if we want to enable ods deployment by default in later releases
AGENT_IMG_NAME=$agent_img_name CONTROLLER_IMG_NAME=$controller_img_name IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-standard-manifests.sh --mode release --out "${MANIFESTS_DIR}"
AGENT_IMG_NAME=$agent_img_name CONTROLLER_IMG_NAME=$controller_img_name IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-manifest.sh --feature-gates FlowExporter=true --extra-helm-values-file "${REPO_ROOT}/ci/kind/values-flow-exporter.yml" --mode release > "${MANIFESTS_DIR}"/antrea-flow-exporter-enabled.yml
cp ${MANIFESTS_DIR}/antrea-advanced.yml "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/antrea-advanced-${BINARY_VERSION}.yml"
cp ${MANIFESTS_DIR}/antrea-advanced-fips.yml "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/antrea-advanced-fips-${BINARY_VERSION}.yml"
cp ${MANIFESTS_DIR}/antrea-advanced-nponly.yml "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/antrea-advanced-nponly-${BINARY_VERSION}.yml"
cp "${ODS_MANIFESTS_DIR}/antrea-ods-${BINARY_VERSION}.yml" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/"
cp "${FLOW_AGGREGATOR_MANIFESTS_DIR}/flow-aggregator-${BINARY_VERSION}.yml" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/"
cp "${MANIFESTS_DIR}/antrea-flow-exporter-enabled.yml" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/antrea-flow-exporter-enabled-${BINARY_VERSION}.yml"
cp "${REPO_ROOT}/hack/wavefront-metrics.sh" "${PUBLISH_DIR}/${antrea_adv_deliverables}/scripts/"
cp "${REPO_ROOT}/ci/jenkins/test-vmc.sh" "${PUBLISH_DIR}/${antrea_adv_deliverables}/scripts/"
cp "${REPO_ROOT}/ci/test-conformance-eks.sh" "${PUBLISH_DIR}/${antrea_adv_deliverables}/scripts/"
cp -r "${REPO_ROOT}/ci/cluster-api/vsphere/templates/" "${OUTPUT_DIR}/scripts/capv-templates/"
tar -zcf ${OUTPUT_DIR}/scripts/capv-templates.tar.gz -C ${OUTPUT_DIR}/scripts/ capv-templates
rm -rf "${OUTPUT_DIR}/scripts/capv-templates"

generate_flow_visibility_e2e_manifests "${REPO_ROOT}" "${BINARY_VERSION}" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests"

echo "====== Saving Antrea Advanced Product Deliverables ======"
OUTPUT_DIR="${BUILDROOT}/advanced-output"

echo "====== Saving and Signing Antrea Advanced Product Images ======"
agent_image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-agent-debian:${IMAGE_VERSION}")"
controller_image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-controller-debian:${IMAGE_VERSION}")"
agent_digest_filename="antrea-advanced-agent-debian-${IMAGE_VERSION}-image-digests.txt"
controller_digest_filename="antrea-advanced-controller-debian-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-advanced-debian-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${OUTPUT_DIR}/images"
# We don't need openvswitch image in all-in-one yaml deployment, so don't publish it
# Just publish Antrea images.
docker tag antrea/antrea-agent-debian:${IMAGE_VERSION} $agent_img_name:${IMAGE_VERSION}
docker tag antrea/antrea-controller-debian:${IMAGE_VERSION} $controller_img_name:${IMAGE_VERSION}
docker save $agent_img_name:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-advanced-agent-debian-${IMAGE_VERSION}.tar.gz"
docker save $controller_img_name:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-advanced-controller-debian-${IMAGE_VERSION}.tar.gz"
echo "antrea/antrea-advanced-agent-debian@${agent_image_id}" > "${OUTPUT_DIR}/images/${agent_digest_filename}"
echo "antrea/antrea-advanced-controller-debian@${controller_image_id}" > "${OUTPUT_DIR}/images/${controller_digest_filename}"

# Saving flow-aggregator image
cp -rf "${FLOW_AGGREGATOR_DELIVERABLES_DIR}/." "${OUTPUT_DIR}/images/"
# Saving ODS image
cp -rf "${ODS_DELIVERABLES_DIR}/." "${OUTPUT_DIR}/images/"

pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
/build/apps/signing/gpgsign/gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

echo "====== Saving and Signing Antrea Advanced Product Executables ======"
# also publish a copy of antctl to lin64/antrea/executables
ANTCTL_STANDALONE_DIR="${BUILDROOT}/output/executables/"
mkdir -p "${ANTCTL_STANDALONE_DIR}"
mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- "antctl-${BINARY_VERSION}.gz" > ${BINARY_CHECKSUM_FILENAME}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
/build/apps/signing/gpgsign/gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd
cp ${OUTPUT_DIR}/executables/antctl* "${ANTCTL_STANDALONE_DIR}/"

echo "====== Preparing Antrea Advanced Product Deliverables: Images, executables ======"
cp -r "${OUTPUT_DIR}/images" "${PUBLISH_DIR}/${antrea_adv_deliverables}"
cp -r "${OUTPUT_DIR}/executables" "${PUBLISH_DIR}/${antrea_adv_deliverables}"
pushd "${PUBLISH_DIR}"
zip --verbose -r "${antrea_adv_deliverables}.zip" "${antrea_adv_deliverables}"
rm -rf "${PUBLISH_DIR}/${antrea_adv_deliverables}" "${OUTPUT_DIR}"
popd

echo "====== Building OpenvSwitch UBI Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
./build.sh --distro ubi
popd

echo "====== Building UBI Base Image ======"
pushd build/images/base
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
curl -k -LO https://packages.vcfd.broadcom.net/artifactory/nsx-ujo-local/antrea/epel/suricata-7.0.6-1.el9.x86_64.rpm
./build.sh --distro ubi
popd

echo "====== Building antrea-agent-ubi & antrea-controller-ubi Images ======"
make ubi VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"
docker tag antrea/antrea-agent-ubi:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-agent-ubi:${IMAGE_VERSION}
docker tag antrea/antrea-controller-ubi:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-controller-ubi:${IMAGE_VERSION}

echo "====== Saving and Signing UBI Images ======"
agent_image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-agent-ubi:${IMAGE_VERSION}")"
controller_image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-controller-ubi:${IMAGE_VERSION}")"
agent_digest_filename="antrea-agent-ubi-${IMAGE_VERSION}-image-digests.txt"
controller_digest_filename="antrea-controller-ubi-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-ubi-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${PUBLISH_DIR}/ubi/images/"
docker save localhost:5000/vmware.io/antrea/antrea-agent-ubi:${IMAGE_VERSION} | gzip -9 > "${PUBLISH_DIR}/ubi/images/antrea-agent-ubi-${IMAGE_VERSION}.tar.gz"
docker save localhost:5000/vmware.io/antrea/antrea-controller-ubi:${IMAGE_VERSION} | gzip -9 > "${PUBLISH_DIR}/ubi/images/antrea-controller-ubi-${IMAGE_VERSION}.tar.gz"
echo "localhost:5000/vmware.io/antrea/antrea-agent-ubi@${agent_image_id}" > "${PUBLISH_DIR}/ubi/images/${agent_digest_filename}"
echo "localhost:5000/vmware.io/antrea/antrea-controller-ubi@${controller_image_id}" > "${PUBLISH_DIR}/ubi/images/${controller_digest_filename}"

flow_aggregator_ubi_image_id="$(docker inspect -f '{{.ID}}' "antrea/flow-aggregator-ubi:${IMAGE_VERSION}")"
flow_aggregator_ubi_digest_filename="flow-aggregator-ubi-${IMAGE_VERSION}-image-digests.txt"
docker tag antrea/flow-aggregator-ubi:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/flow-aggregator-ubi:${IMAGE_VERSION}
docker save localhost:5000/vmware.io/antrea/flow-aggregator-ubi:${IMAGE_VERSION} | gzip -9 >  "${PUBLISH_DIR}/ubi/images/flow-aggregator-ubi-${IMAGE_VERSION}.tar.gz"
echo "localhost:5000/vmware.io/antrea/flow-aggregator-ubi@${flow_aggregator_ubi_image_id}" > "${PUBLISH_DIR}/ubi/images/${flow_aggregator_ubi_digest_filename}"

pushd "${PUBLISH_DIR}/ubi/images"
sha256sum -- * > ${checksum_filename}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
/build/apps/signing/gpgsign/gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

echo "====== Preparing Manifests for flow-aggregator-ubi ======"
antrea_ubi_deliverables="antrea-ubi-${ANTREA_VERSION_DIGIT}"
mkdir -p "${PUBLISH_DIR}/${antrea_ubi_deliverables}/manifests"
IMG_NAME=localhost:5000/vmware.io/antrea/flow-aggregator-ubi IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-manifest-flow-aggregator.sh --mode release > "${PUBLISH_DIR}/${antrea_ubi_deliverables}/manifests/flow-aggregator-ubi-${IMAGE_VERSION}.yml"

echo "====== Preparing UBI Deliverables ======"
cp -r "${PUBLISH_DIR}/ubi/images" "${PUBLISH_DIR}/${antrea_ubi_deliverables}"
echo "${BUILD_NUMBER}" > "${PUBLISH_DIR}/${antrea_ubi_deliverables}/build_number.txt"
pushd "${PUBLISH_DIR}"
zip --verbose -r "${antrea_ubi_deliverables}.zip" "${antrea_ubi_deliverables}"
popd
mkdir -p "${PUBLISH_DIR}/ubi/zip/"
mv "${PUBLISH_DIR}/${antrea_ubi_deliverables}.zip" "${PUBLISH_DIR}/ubi/zip/"
rm -rf "${PUBLISH_DIR}/${antrea_ubi_deliverables}"

echo "====== Cleanup Antrea Advanced Product Build Result ======"
make clean

echo "====== Windows build ======"

echo "====== Building Antrea Standard Windows Deliverables ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-common
check_manifests
build_windows "standard" "signed" "${IMAGE_VERSION}"
generate_windows_manifests "standard" "${IMAGE_VERSION}" "${BINARY_VERSION}"

echo "====== Building Antrea Advanced Windows Deliverables ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-features
check_manifests
build_windows "advanced" "signed" "${IMAGE_VERSION}"
generate_windows_manifests "advanced" "${IMAGE_VERSION}" "${BINARY_VERSION}"
