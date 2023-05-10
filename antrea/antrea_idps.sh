#!/usr/bin/env bash

echo "====== Disabling --pull in All Makefile Docker Build Target ======"
export NO_PULL=1

echo "====== Generating version Files for CI and Consumers ======"
publish_version_files

echo "====== Checkout Features Branch ======"
git reset --hard "origin/topic/${ANTREA_VERSION_DIGIT}-features"
check_manifests

echo "====== Building Binaries for Antrea IDPS ======"
fips_make

echo "====== Building IDPS Debian Images ======"
make idps-image-debian VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"

echo "====== Building IDPS UBI Images ======"
make idps-image-ubi VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"

echo "====== Building Suricata Image ======"
make suricata-image VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"

echo "====== Prepairing Antrea IDPS Manifests ======"
MANIFESTS_DIR=$(mktemp -d)
IDPS_IMG_NAME=projects.registry.vmware.com/antreainterworking/idps SURICATA_IMG_NAME=projects.registry.vmware.com/antreainterworking/suricata IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-manifest-idps.sh --mode release --out "${MANIFESTS_DIR}"

echo "====== Saving and Signing Antrea IDPS Product Images ======"
for variant in "debian" "ubi"; do
  mkdir -p "${OUTPUT_DIR}/images-${variant}"
  image_id_idps="$(docker inspect -f '{{.ID}}' "projects.registry.vmware.com/antreainterworking/idps-${variant}:${IMAGE_VERSION}")"
  docker tag projects.registry.vmware.com/antreainterworking/idps-${variant}:${IMAGE_VERSION} projects.registry.vmware.com/antreainterworking/idps:${IMAGE_VERSION}
  docker save projects.registry.vmware.com/antreainterworking/idps-${variant}:${IMAGE_VERSION} projects.registry.vmware.com/antreainterworking/idps:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images-${variant}/antrea-idps-${IMAGE_VERSION}.tar.gz"
  digest_filename_idps="antrea-idps-${IMAGE_VERSION}-image-digests.txt"
  echo "projects.registry.vmware.com/antreainterworking/idps@${image_id_idps}" > "${OUTPUT_DIR}/images-${variant}/${digest_filename_idps}"

  image_id_suricata="$(docker inspect -f '{{.ID}}' "projects.registry.vmware.com/antreainterworking/suricata:${IMAGE_VERSION}")"
  docker save projects.registry.vmware.com/antreainterworking/suricata:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images-${variant}/antrea-suricata-${IMAGE_VERSION}.tar.gz"
  digest_filename_suricata="antrea-suricata-${IMAGE_VERSION}-image-digests.txt"
  echo "projects.registry.vmware.com/antreainterworking@${image_id_suricata}" > "${OUTPUT_DIR}/images-${variant}/${digest_filename_suricata}"

  checksum_filename="antrea-${IMAGE_VERSION}-images-checksums.txt"
  pushd "${OUTPUT_DIR}/images-${variant}/"
  sha256sum -- * > ${checksum_filename}
  # See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
  gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
  popd

  antrea_idps_deliverables="antrea-idps-${variant}-${ANTREA_VERSION_DIGIT}"
  mkdir -p "${PUBLISH_DIR}/${antrea_idps_deliverables}"
  cp -r "${OUTPUT_DIR}/images-${variant}" "${PUBLISH_DIR}/${antrea_idps_deliverables}/images"
  cp -r "${MANIFESTS_DIR}" "${PUBLISH_DIR}/${antrea_idps_deliverables}/manifests"
  pushd "${PUBLISH_DIR}"
  zip --verbose -r "${antrea_idps_deliverables}.zip" "${antrea_idps_deliverables}"
  rm -rf "${PUBLISH_DIR}/${antrea_idps_deliverables}"
  rm -rf "${OUTPUT_DIR}/images-${variant}"
  popd
done

echo "====== Saving Antrea IDPS Scripts ======"
mkdir -p "${PUBLISH_DIR}/scripts"
cp hack/deploy_idps.sh "${PUBLISH_DIR}/scripts/"

echo "====== Cleanup Antrea IDPS Product Build Result ======"
rm -rf "${MANIFESTS_DIR}"
make clean
