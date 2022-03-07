#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

echo "****** antrea_release_build.sh start ******"

env
cat /proc/cpuinfo

REPO_ROOT="${PROJECT_DIR}"

cd "${REPO_ROOT}"
git status
if [ -n "$(git status --porcelain)" ]; then
  git commit -a -m "commit sandbox build changeset"
fi
git status

echo "====== Copying OSL and ODP ======"
mkdir -p "${PUBLISH_DIR}/"
cp open_source_licenses.txt "${PUBLISH_DIR}/"
pushd "${PUBLISH_DIR}/"
curl https://build-artifactory.eng.vmware.com/artifactory/nsx-ujo-local/VMware-Antrea-1.3.1-1.2.3-ODP.tar.gz > VMware-Antrea-1.3.1-1.2.3-ODP.tar.gz
popd

echo "====== Copying antrea-interworking Product Deliverables ======"
antrea_interworking_publish="${PUBLISH_DIR}/antrea-interworking"
mkdir -p "${antrea_interworking_publish}/images"
cp -rv ${GOBUILD_ANTREA_INTERWORKING_ROOT}/antrea-interworking/images/interworking-* "${antrea_interworking_publish}/images"
mkdir -p "${antrea_interworking_publish}/manifests"
cp -rv ${GOBUILD_ANTREA_INTERWORKING_ROOT}/antrea-interworking/manifests/{interworking,deregisterjob,bootstrap-config,ns-label-webhook,inventorycleanup}.yaml \
  "${antrea_interworking_publish}/manifests"
cp -rv ${GOBUILD_ANTREA_INTERWORKING_ROOT}/VERSION ${GOBUILD_ANTREA_INTERWORKING_ROOT}/antrea-interworking-*.zip "${antrea_interworking_publish}/"

echo "====== Copying cayman_antrea Product Deliverables ======"
cayman_antrea_publish="${PUBLISH_DIR}/cayman_antrea"
mkdir -p "${cayman_antrea_publish}/TKGS"
cp -rv ${GOBUILD_CAYMAN_ANTREA_TKGS_ADVANCED_ROOT}/{add-on,photon,ubuntu}  "${cayman_antrea_publish}/TKGS"
mkdir -p "${cayman_antrea_publish}/TKGM"
cp -rv ${GOBUILD_CAYMAN_ANTREA_TKGM_ADVANCED_ROOT}/{lin64,windows-advanced}  "${cayman_antrea_publish}/TKGM"

mkdir -p "${cayman_antrea_publish}/standard-release"
cp -rv ${GOBUILD_CAYMAN_ANTREA_ROOT}/windows-standard/antrea-windows-standard.zip  "${cayman_antrea_publish}/standard-release"
cp -rv ${GOBUILD_CAYMAN_ANTREA_ROOT}/antrea-standard-*.zip  "${cayman_antrea_publish}/standard-release"
mkdir -p "${cayman_antrea_publish}/advanced-release"
cp -rv ${GOBUILD_CAYMAN_ANTREA_ROOT}/windows-advanced/antrea-windows-advanced.zip  "${cayman_antrea_publish}/advanced-release"
cp -rv ${GOBUILD_CAYMAN_ANTREA_ROOT}/antrea-advanced-*.zip  "${cayman_antrea_publish}/advanced-release"

cp -rv ${GOBUILD_CAYMAN_ANTREA_ROOT}/VERSION "${cayman_antrea_publish}/"

echo "====== Copying cayman_antrea-operator-for-kubernetes Product Deliverables ======"
operator_publish="${PUBLISH_DIR}/openshift"
mkdir -p "${operator_publish}"
cp -rv ${GOBUILD_CAYMAN_ANTREA_ROOT}/ubi/images  "${operator_publish}/antrea"
cp -rv ${GOBUILD_CAYMAN_ANTREA_ROOT}/VERSION  "${operator_publish}/antrea"
cp -rv ${GOBUILD_CAYMAN_ANTREA_OPERATOR_FOR_KUBERNETES_ROOT}/lin64 "${operator_publish}/operator"
cp -rv ${GOBUILD_CAYMAN_ANTREA_OPERATOR_FOR_KUBERNETES_ROOT}/VERSION "${operator_publish}/operator"

echo "****** antrea_release_build.sh finished ******"
