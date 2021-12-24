
echo "====== Archiving OpenvSwitch Source Code ======"
archive_ovs_source

echo "====== Compiling antrea e2e testcases ======"
compile_e2e "standard" "advanced"

echo "====== Generating version Files for CI and Consumers ======"
publish_version_files

echo "====== Preparing Antrea Standard Product Deliverables: Standard Manifests ======"
git reset --hard "origin/topic/${ANTREA_VERSION_DIGIT}-standard-release"
antrea_std_deliverables="antrea-standard-${ANTREA_VERSION_DIGIT}.${BUILD_NUMBER}"
mkdir -p "${PUBLISH_DIR}/${antrea_std_deliverables}"
mkdir -p "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests"
# antrea-ipsec is not used in commecial release
cp "${REPO_ROOT}/build/yamls/antrea.yml" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/antrea-standard-fips-${BINARY_VERSION}.yml"
sed -e 's/tlsCipherSuites:.\+/#tlsCipherSuites:/g' < "${REPO_ROOT}/build/yamls/antrea.yml" > "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/antrea-standard-${BINARY_VERSION}.yml"
cp "${REPO_ROOT}/build/yamls/flow-aggregator.yml" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/flow-aggregator-${BINARY_VERSION}.yml"
sed -i -e "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-standard-debian:${IMAGE_VERSION}/g" \
  -e "s/image: projects.registry.vmware.com\/antrea\/antrea-.*\$/image: antrea\/antrea-standard-debian:${IMAGE_VERSION}/g" \
  "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/antrea-standard-${BINARY_VERSION}.yml" \
  "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/antrea-standard-fips-${BINARY_VERSION}.yml"
sed -i -e "s/image: projects.registry.vmware.com\/antrea\/flow-aggregator:latest/image: antrea\/flow-aggregator-debian:${IMAGE_VERSION}/g" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/flow-aggregator-${BINARY_VERSION}.yml"

echo "====== Preparing Antrea Advanced Product Deliverables: Advanced Manifests ======"
git reset --hard "origin/topic/${ANTREA_VERSION_DIGIT}-advanced-release"
antrea_adv_deliverables="antrea-advanced-${ANTREA_VERSION_DIGIT}.${BUILD_NUMBER}"
mkdir -p "${PUBLISH_DIR}/${antrea_adv_deliverables}"
mkdir -p "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests"
# antrea-ipsec is not used in commecial release
cp "${REPO_ROOT}/build/yamls/antrea.yml" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/antrea-advanced-fips-${BINARY_VERSION}.yml"
sed -e 's/tlsCipherSuites:.\+/#tlsCipherSuites:/g' < "${REPO_ROOT}/build/yamls/antrea.yml" > "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/antrea-advanced-${BINARY_VERSION}.yml"
cp "${REPO_ROOT}/build/yamls/flow-aggregator.yml" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/flow-aggregator-${BINARY_VERSION}.yml"
sed -i -e "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-advanced-debian:${IMAGE_VERSION}/g" \
  -e "s/image: projects.registry.vmware.com\/antrea\/antrea-.*\$/image: antrea\/antrea-advanced-debian:${IMAGE_VERSION}/g" \
  "${REPO_ROOT}/build/yamls/antrea.yml" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/antrea-advanced-${BINARY_VERSION}.yml" \
  "${REPO_ROOT}/build/yamls/antrea.yml" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/antrea-advanced-fips-${BINARY_VERSION}.yml"
sed -i -e "s/image: projects.registry.vmware.com\/antrea\/flow-aggregator:latest/image: antrea\/flow-aggregator-debian:${IMAGE_VERSION}/g" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/flow-aggregator-${BINARY_VERSION}.yml"

echo "====== Building antrea-ubi Images ======"
git reset --hard "origin/topic/${ANTREA_VERSION_DIGIT}-advanced-release"
fips_make
pushd ${PROJECT_DIR}/../antrea-operator/build/antrea
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz ./base
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz ./ovs
mkdir -p bin
mkdir -p scripts
cp ${REPO_ROOT}/bin/* bin/
cp ${REPO_ROOT}/build/images/scripts/* scripts/
./build_ubi.sh --tag ${IMAGE_VERSION} --ovs-version ${OVS_VER}
docker tag antrea/antrea-ubi:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-ubi:${IMAGE_VERSION}
popd
make clean

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

echo "====== Checkout Antrea Standard Release Branch ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-standard-release
git status

echo "====== Building Binaries for Antrea Standard Product ======"
fips_make

echo "====== Building openvswitch-debian Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
docker build --build-arg OVS_VERSION=${OVS_VER} -t antrea/openvswitch-debian .
popd

echo "====== Building flow-aggregator-debian Image ======"
make flow-aggregator-image
docker tag antrea/flow-aggregator-debian antrea/flow-aggregator-debian:${IMAGE_VERSION}

echo "====== Building antrea-debian Image ======"
cp -f ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
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
docker save antrea/flow-aggregator-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/flow-aggregator-debian-${IMAGE_VERSION}.tar.gz"
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

echo "====== Cleanup Antrea Standard Product Build Result ======"
make clean

echo "====== Preparing Antrea Standard Product Deliverables: Images, executables ======"
cp -r "${OUTPUT_DIR}/images" "${PUBLISH_DIR}/${antrea_std_deliverables}"
cp -r "${OUTPUT_DIR}/executables" "${PUBLISH_DIR}/${antrea_std_deliverables}"
pushd "${PUBLISH_DIR}"
zip --verbose -r "${antrea_std_deliverables}.zip" "${antrea_std_deliverables}"
rm -rf "${PUBLISH_DIR}/${antrea_std_deliverables}" "${OUTPUT_DIR}"
popd


echo "====== Checkout Antrea Advanced Release Branch ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-advanced-release
git status

echo "====== Building Binaries for Antrea Advanced Product ======"
fips_make

echo "====== Building openvswitch-debian Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
docker build --build-arg OVS_VERSION=${OVS_VER} -t antrea/openvswitch-debian .
popd

echo "====== Building flow-aggregator-debian Image ======"
make flow-aggregator-image
docker tag antrea/flow-aggregator-debian antrea/flow-aggregator-debian:${IMAGE_VERSION}

echo "====== Building antrea-debian Image ======"
cp -f ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
make debian VERSION=${IMAGE_VERSION}

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
docker save antrea/flow-aggregator-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/flow-aggregator-debian-${IMAGE_VERSION}.tar.gz"
echo "antrea/antrea-advanced-debian@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Saving and Signing Antrea Advanced Product Executables ======"

mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- "antctl-${BINARY_VERSION}.gz" > ${BINARY_CHECKSUM_FILENAME}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Preparing Antrea Advanced Product Deliverables: Images, executables ======"
cp -r "${OUTPUT_DIR}/images" "${PUBLISH_DIR}/${antrea_adv_deliverables}"
cp -r "${OUTPUT_DIR}/executables" "${PUBLISH_DIR}/${antrea_adv_deliverables}"
pushd "${PUBLISH_DIR}"
zip --verbose -r "${antrea_adv_deliverables}.zip" "${antrea_adv_deliverables}"
rm -rf "${PUBLISH_DIR}/${antrea_adv_deliverables}" "${OUTPUT_DIR}"
popd

echo "====== Cleanup Antrea Standard Product Build Result ======"
make clean

echo "====== Windows build ======"

echo "====== Building Antrea Standard Windows Deliverables ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-standard-release
build_windows "standard"

echo "====== Building Antrea Advanced Windows Deliverables ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-advanced-release
build_windows "advanced"
