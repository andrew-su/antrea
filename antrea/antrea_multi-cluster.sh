
echo "====== Disabling --pull in All Makefile Docker Build Target ======"
export NO_PULL=1

echo "====== Generating version Files for CI and Consumers ======"
publish_version_files

echo "====== Checkout Common Branch ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-common
check_manifests

echo "====== Compiling antrea e2e testcases ======"
compile_e2e "noipsec" "multi-cluster"

echo "====== Preparing Antrea Multi-cluster Deliverables: Manifests ======"
git status
antrea_mc_deliverables="antrea-multicluster-${ANTREA_VERSION_DIGIT}"
mkdir -p "${PUBLISH_DIR}/${antrea_mc_deliverables}"
mkdir -p "${PUBLISH_DIR}/${antrea_mc_deliverables}/manifests"
echo "${BUILD_NUMBER}" > "${PUBLISH_DIR}/${antrea_mc_deliverables}/build_number.txt"
for yml_file in ${REPO_ROOT}/multicluster/build/yamls/*.yml; do
  base_name="$(basename $yml_file .yml)"
  sed \
    -e "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-mc-controller-debian:${IMAGE_VERSION}/g" \
    -e "s/image: projects.registry.vmware.com\/antrea\/antrea-.*\$/image: antrea\/antrea-mc-controller-debian:${IMAGE_VERSION}/g" \
    "$yml_file" > "${PUBLISH_DIR}/${antrea_mc_deliverables}/manifests/${base_name}-${BINARY_VERSION}.yml"
done
cp -r "${REPO_ROOT}/multicluster/config/samples/clusterset_init" "${PUBLISH_DIR}/${antrea_mc_deliverables}/manifests"

echo "====== Building antrea-mc-controller-debian Image ======"
make antrea-mc-controller
docker tag antrea/antrea-mc-controller "antrea/antrea-mc-controller-debian:${IMAGE_VERSION}"

echo "====== Saving Antrea Multi-cluster Product Deliverables ======"
OUTPUT_DIR="${BUILDROOT}/mc-output"

echo "====== Saving and Signing Antrea Multi-cluster Product Images ======"

image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-mc-controller-debian:${IMAGE_VERSION}")"
digest_filename="antrea-mc-controller-debian-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-mc-controller-debian-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${OUTPUT_DIR}/images"
docker save antrea/antrea-mc-controller-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-mc-controller-debian-${IMAGE_VERSION}.tar.gz"
echo "antrea/antrea-mc-controller-debian@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Preparing Antrea Multi-cluster Deliverables: Images ======"
cp -r "${OUTPUT_DIR}/images" "${PUBLISH_DIR}/${antrea_mc_deliverables}"
pushd "${PUBLISH_DIR}"
zip --verbose -r "${antrea_mc_deliverables}.zip" "${antrea_mc_deliverables}"
rm -rf "${PUBLISH_DIR}/${antrea_mc_deliverables}" "${OUTPUT_DIR}"
popd

echo "====== Cleanup Antrea Multi-cluster Product Build Result ======"
make clean
