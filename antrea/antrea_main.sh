
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
make flow-aggregator-image-debian VERSION=${IMAGE_VERSION}
echo "====== Preparing Manifests for flow-aggregator ======"
IMG_NAME=antrea/flow-aggregator-debian IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-manifest-flow-aggregator.sh --mode release > "${FLOW_AGGREGATOR_DELIVERABLES_DIR}/flow-aggregator-debian-${IMAGE_VERSION}.yml"
echo "====== Saving flow-aggregator Image ======"
docker save antrea/flow-aggregator-debian:${IMAGE_VERSION} | gzip -9 > "${FLOW_AGGREGATOR_DELIVERABLES_DIR}/flow-aggregator-debian-${IMAGE_VERSION}.tar.gz"

echo "====== Building openvswitch-debian Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
docker build -f Dockerfile.debian --build-arg OVS_VERSION=${OVS_VER} -t antrea/openvswitch-debian:standard .
popd

echo "====== Building Debian standard Images ======"
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
make debian VERSION=${IMAGE_VERSION}

echo "====== Preparing Antrea Standard Product Deliverables: Standard Manifests ======"
antrea_std_deliverables="antrea-standard-${ANTREA_VERSION_DIGIT}"
mkdir -p "${PUBLISH_DIR}/${antrea_std_deliverables}"
mkdir -p "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests"
echo "${BUILD_NUMBER}" > "${PUBLISH_DIR}/${antrea_std_deliverables}/build_number.txt"

# antrea-ipsec is not used in commecial release
MANIFESTS_DIR=$(mktemp -d)
IMG_NAME=antrea/antrea-standard-debian IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-standard-manifests.sh --mode release --out "${MANIFESTS_DIR}"
cp "${MANIFESTS_DIR}/antrea-standard.yml" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/antrea-standard-${BINARY_VERSION}.yml"
cp "${MANIFESTS_DIR}/antrea-standard-fips.yml" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/antrea-standard-fips-${BINARY_VERSION}.yml"
cp "${FLOW_AGGREGATOR_DELIVERABLES_DIR}/flow-aggregator-debian-${IMAGE_VERSION}.yml" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/"

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
cp "${FLOW_AGGREGATOR_DELIVERABLES_DIR}/flow-aggregator-debian-${IMAGE_VERSION}.tar.gz" "${OUTPUT_DIR}/images/"
echo "antrea/antrea-standard-debian@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Saving and Signing Antrea Standard Product Executables ======"
mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- * > ${BINARY_CHECKSUM_FILENAME}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=001E5CC9
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

echo "====== Building openvswitch-debian Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
docker build -f Dockerfile.debian --build-arg OVS_VERSION=${OVS_VER} -t antrea/openvswitch-debian:standard .
popd

echo "====== Building Debian Advanced Images ======"
make debian VERSION=${IMAGE_VERSION}

echo "====== Preparing Antrea Advanced Product Deliverables: Advanced Manifests ======"
antrea_adv_deliverables="antrea-advanced-${ANTREA_VERSION_DIGIT}"
mkdir -p "${PUBLISH_DIR}/${antrea_adv_deliverables}"
mkdir -p "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests"
echo "${BUILD_NUMBER}" > "${PUBLISH_DIR}/${antrea_adv_deliverables}/build_number.txt"
# antrea-ipsec is not used in commecial release
MANIFESTS_DIR=$(mktemp -d)
IMG_NAME=antrea/antrea-advanced-debian IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-standard-manifests.sh --mode release --out "${MANIFESTS_DIR}"
cp ${MANIFESTS_DIR}/antrea-advanced.yml "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/antrea-advanced-${BINARY_VERSION}.yml"
cp ${MANIFESTS_DIR}/antrea-advanced-fips.yml "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/antrea-advanced-fips-${BINARY_VERSION}.yml"
cp "${FLOW_AGGREGATOR_DELIVERABLES_DIR}/flow-aggregator-debian-${IMAGE_VERSION}.yml" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/"

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
cp "${FLOW_AGGREGATOR_DELIVERABLES_DIR}/flow-aggregator-debian-${IMAGE_VERSION}.tar.gz" "${OUTPUT_DIR}/images/"
echo "antrea/antrea-advanced-debian@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
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
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=001E5CC9
popd
cp ${OUTPUT_DIR}/executables/antctl* "${ANTCTL_STANDALONE_DIR}/"

echo "====== Preparing Antrea Advanced Product Deliverables: Images, executables ======"
cp -r "${OUTPUT_DIR}/images" "${PUBLISH_DIR}/${antrea_adv_deliverables}"
cp -r "${OUTPUT_DIR}/executables" "${PUBLISH_DIR}/${antrea_adv_deliverables}"
pushd "${PUBLISH_DIR}"
zip --verbose -r "${antrea_adv_deliverables}.zip" "${antrea_adv_deliverables}"
rm -rf "${PUBLISH_DIR}/${antrea_adv_deliverables}" "${OUTPUT_DIR}"
popd

echo "====== Building openvswitch-ubi Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
docker build -f Dockerfile.ubi --build-arg OVS_VERSION=${OVS_VER} -t antrea/openvswitch-ubi .
popd
echo "====== Building antrea-ubi Images ======"
make ubi VERSION=${IMAGE_VERSION}
docker tag antrea/antrea-ubi:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-ubi:${IMAGE_VERSION}

echo "====== Saving and Signing UBI Images ======"
image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-ubi:${IMAGE_VERSION}")"
digest_filename="antrea-ubi-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-ubi-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${PUBLISH_DIR}/ubi/images/"
docker save localhost:5000/vmware.io/antrea/antrea-ubi:${IMAGE_VERSION} | gzip -9 > "${PUBLISH_DIR}/ubi/images/antrea-ubi-${IMAGE_VERSION}.tar.gz"
echo "localhost:5000/vmware.io/antrea/antrea-ubi@${image_id}" > "${PUBLISH_DIR}/ubi/images/${digest_filename}"
pushd "${PUBLISH_DIR}/ubi/images"
sha256sum -- * > ${checksum_filename}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Cleanup Antrea Advanced Product Build Result ======"
make clean

echo "====== Windows build ======"

echo "====== Building Antrea Standard Windows Deliverables ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-common
check_manifests
build_windows "standard" "signed"

echo "====== Building Antrea Advanced Windows Deliverables ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-features
check_manifests
build_windows "advanced" "signed"
