
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
OUTPUT_DIR="${BUILDROOT}/mc-output"
mkdir -p "${OUTPUT_DIR}/manifests"
echo "${BUILD_NUMBER}" > "${OUTPUT_DIR}/manifests/build_number.txt"
for yml_file in ${REPO_ROOT}/multicluster/build/yamls/*.yml; do
  base_name="$(basename $yml_file .yml)"
  sed \
    -e "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-mc-controller:${IMAGE_VERSION}/g" \
    -e "s/image: projects.packages.vcfd.broadcom.net\/antrea\/antrea-.*\$/image: antrea\/antrea-mc-controller:${IMAGE_VERSION}/g" \
    -e "s/image: projects.packages.vcfd.broadcom.net\/antrea\/antrea-.*\$/image: antrea\/antrea-mc-controller:${IMAGE_VERSION}/g" \
    "$yml_file" > "${OUTPUT_DIR}/manifests/${base_name}-${BINARY_VERSION}.yml"
done
cp -r "${REPO_ROOT}/multicluster/config/samples/clusterset_init" "${OUTPUT_DIR}/manifests"

echo "====== Building antrea-mc-controller Binaries ======"
fips_make "go build -o bin/antrea-mc-controller antrea.io/antrea/multicluster/cmd/..."

echo "====== Building antrea-mc-controller Debian Image ======"
make antrea-mc-controller-debian VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"

echo "====== Building antrea-mc-controller UBI Image ======"
make antrea-mc-controller-ubi VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"

echo "====== Saving and Signing Antrea Multi-cluster Product Images ======"
for variant in "debian" "ubi"; do
  mkdir -p "${OUTPUT_DIR}/images-${variant}"
  image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-mc-controller-${variant}:${IMAGE_VERSION}")"
  docker tag antrea/antrea-mc-controller-${variant}:${IMAGE_VERSION} antrea/antrea-mc-controller:${IMAGE_VERSION}
  docker save antrea/antrea-mc-controller-${variant}:${IMAGE_VERSION} antrea/antrea-mc-controller:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images-${variant}/antrea-mc-controller-${IMAGE_VERSION}.tar.gz"
  digest_filename="antrea-mc-controller-${IMAGE_VERSION}-image-digests.txt"
  echo "antrea/antrea-mc-controller@${image_id}" > "${OUTPUT_DIR}/images-${variant}/${digest_filename}"

  checksum_filename="antrea-mc-controller-${IMAGE_VERSION}-image-checksums.txt"
  pushd "${OUTPUT_DIR}/images-${variant}/"
  sha256sum -- * > ${checksum_filename}
  # See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
  gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
  popd

  antrea_mc_deliverables="antrea-multicluster-${variant}-${ANTREA_VERSION_DIGIT}"
  mkdir -p "${PUBLISH_DIR}/${antrea_mc_deliverables}"
  cp -r "${OUTPUT_DIR}/images-${variant}" "${PUBLISH_DIR}/${antrea_mc_deliverables}/images"
  cp -r "${OUTPUT_DIR}/manifests" "${PUBLISH_DIR}/${antrea_mc_deliverables}"
  pushd "${PUBLISH_DIR}"
  zip --verbose -r "${antrea_mc_deliverables}.zip" "${antrea_mc_deliverables}"
  rm -rf "${PUBLISH_DIR}/${antrea_mc_deliverables}"
  rm -rf "${OUTPUT_DIR}/images-${variant}"
  popd
done

echo "====== Cleanup Antrea Multi-cluster Product Build Result ======"
rm -rf "${OUTPUT_DIR}"
make clean
