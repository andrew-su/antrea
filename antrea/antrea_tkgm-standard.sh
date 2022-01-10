
echo "====== Archiving OpenvSwitch Source Code ======"
archive_ovs_source

echo "===== Compile antrea e2e testcases ======"
compile_e2e "noipsec" "tkgm-standard"

echo "====== Generating version Files for CI and Consumers ======"
publish_version_files

echo "====== Checkout TKGm standard Release Branch ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-tkgm-standard-release
git status

echo "====== Building Binaries for TKGm ======"
fips_make

echo "====== Building Debian standard Images ======"
echo "====== Building openvswitch-debian Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
docker build --build-arg OVS_VERSION=${OVS_VER} -t antrea/openvswitch-debian .
popd

echo "====== Building Binaries for Antrea Standard Product ======"
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
make debian VERSION=${IMAGE_VERSION}
make flow-aggregator-image
docker tag antrea/flow-aggregator-debian antrea/flow-aggregator-debian:${IMAGE_VERSION}
echo "====== Building Debian Images ======"
echo "====== Building openvswitch-debian Image ======"

# Create archives for scripts and binaries
echo "====== Saving TKGm Deliverables ======"
# "${BUILDROOT}/output" will be published to lin64/antrea by antrea_defs.py:CaymanAntreaBuilderLin.install
OUTPUT_DIR="${BUILDROOT}/output"

echo "====== Saving TKGm Manifests ======"
mkdir -p "${OUTPUT_DIR}/manifests"

cp "${REPO_ROOT}/build/yamls/antrea.yml" "${OUTPUT_DIR}/manifests/antrea-standard-${BINARY_VERSION}.yml"
cp "${REPO_ROOT}/build/yamls/flow-aggregator.yml" "${OUTPUT_DIR}/manifests/flow-aggregator-${BINARY_VERSION}.yml"
sed -i -e "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-standard-debian:${IMAGE_VERSION}/g" "${OUTPUT_DIR}/manifests/antrea-standard-${BINARY_VERSION}.yml"
sed -i -e "s/image: projects.registry.vmware.com\/antrea\/antrea-.*\$/image: antrea\/antrea-standard-debian:${IMAGE_VERSION}/g" "${OUTPUT_DIR}/manifests/antrea-standard-${BINARY_VERSION}.yml"
sed -i -e "s/image: projects.registry.vmware.com\/antrea\/flow-aggregator:latest/image: antrea\/flow-aggregator-debian:${IMAGE_VERSION}/g" "${OUTPUT_DIR}/manifests/flow-aggregator-${BINARY_VERSION}.yml"
# Antrea standard doesn't support enterpriseAntrea config, if present, Antrea controller will crash.
sed -i -e 's/enterpriseAntrea:.\+//g' "${OUTPUT_DIR}/manifests/antrea-standard-${BINARY_VERSION}.yml"
sed -i -e 's/tlsCipherSuites:.\+/#tlsCipherSuites:/g' "${OUTPUT_DIR}/manifests/antrea-standard-${BINARY_VERSION}.yml"

echo "====== Saving and Signing TKGm Images ======"
mkdir -p "${OUTPUT_DIR}/images"
# We don't need openvswitch image in all-in-one yaml deployment, so don't publish it
# Just publish Antrea images.
docker tag antrea/antrea-debian:${IMAGE_VERSION} antrea/antrea-standard-debian:${IMAGE_VERSION}
docker save antrea/antrea-standard-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-debian-${IMAGE_VERSION}-standard.tar.gz"
docker save antrea/flow-aggregator-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/flow-aggregator-debian-${IMAGE_VERSION}.tar.gz"
image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-standard-debian:${IMAGE_VERSION}")"
digest_filename="antrea-debian-${IMAGE_VERSION}-standard-image-digests.txt"
checksum_filename="antrea-debian-${IMAGE_VERSION}-standard-image-checksums.txt"
echo "antrea/antrea-standard-debian@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Saving and Signing TKGm Executables ======"
mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
gzip -c "bin/e2e-tkgm-standard-${ANTREA_VERSION}" > "${OUTPUT_DIR}/executables/e2e-tkgm-standard-${ANTREA_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- * > ${BINARY_CHECKSUM_FILENAME}
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=001E5CC9
popd

mkdir -p ${PUBLISH_DIR}/lin64/antrea/executables/
cp -r ${OUTPUT_DIR}/executables/* ${PUBLISH_DIR}/lin64/antrea/executables/

echo "====== Building Antrea Standard Windows Deliverables ======"
build_windows "standard" "unsigned"
