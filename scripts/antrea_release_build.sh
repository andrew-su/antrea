#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

echo "****** antrea_release_build.sh start ******"

env
cat /proc/cpuinfo

# before updating RELEASE_VERSION, need to upload the new ODP file to artifactory for that release,
# https://build-artifactory.eng.vmware.com/artifactory/nsx-ujo-local/antrea/VMware-Antrea-${RELEASE_VERSION}-ODP.tar.gz
RELEASE_VERSION=1.9.0
REPO_ROOT="${PROJECT_DIR}"

cd "${REPO_ROOT}"
git status
if [ -n "$(git status --porcelain)" ]; then
  git config user.email "sandboxbuild@example.com"
  git config user.name "Sandbox Build"
  git commit -a -m "commit sandbox build changeset"
fi
git status

echo "====== Copying OSL and ODP ======"
mkdir -p "${PUBLISH_DIR}/"
gunzip open_source_licenses.txt.gz
mv open_source_licenses.txt "${PUBLISH_DIR}/"
pushd "${PUBLISH_DIR}/"
curl https://build-artifactory.eng.vmware.com/artifactory/nsx-ujo-local/antrea/VMware-Antrea-${RELEASE_VERSION}-ODP.tar.gz > VMware-Antrea-${RELEASE_VERSION}-ODP.tar.gz
popd

echo "====== Copying antrea-interworking Product Deliverables ======"
antrea_interworking_publish="${PUBLISH_DIR}/antrea-interworking"
mkdir -p "${antrea_interworking_publish}/images"
cp -rv ${GOBUILD_ANTREA_INTERWORKING_ROOT}/antrea-interworking/images/interworking-* "${antrea_interworking_publish}/images"
# UBI image will be added to openshift dir, so remove it from ${antrea_interworking_publish}/images
rm -rf ${antrea_interworking_publish}/images/interworking-ubi-*.tar
mkdir -p "${antrea_interworking_publish}/manifests"
cp -rv ${GOBUILD_ANTREA_INTERWORKING_ROOT}/antrea-interworking/manifests/{interworking,deregisterjob,bootstrap-config,ns-label-webhook,inventorycleanup}.yaml \
  "${antrea_interworking_publish}/manifests"
cp -rv "${GOBUILD_ANTREA_INTERWORKING_ROOT}/antrea-interworking/bin" "${antrea_interworking_publish}"
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
cp -rv ${GOBUILD_CAYMAN_ANTREA_IPSEC_ROOT}/antrea-debian-ipsec-*.zip "${cayman_antrea_publish}/advanced-release"
cp -rv ${GOBUILD_CAYMAN_ANTREA_IPSEC_ROOT}/antrea-photon-ipsec-*.zip "${cayman_antrea_publish}/advanced-release"
mkdir -p "${cayman_antrea_publish}/multi-cluster"
cp -rv ${GOBUILD_CAYMAN_ANTREA_MULTI_CLUSTER_ROOT}/lin64 "${cayman_antrea_publish}/multi-cluster"
cp -rf ${GOBUILD_CAYMAN_ANTREA_MULTI_CLUSTER_ROOT}/antrea-multicluster-debian-*.zip "${cayman_antrea_publish}/multi-cluster"
cp -rf ${GOBUILD_CAYMAN_ANTREA_MULTI_CLUSTER_ROOT}/antrea-multicluster-ubi-*.zip "${cayman_antrea_publish}/multi-cluster"
cp -rv ${GOBUILD_CAYMAN_ANTREA_ROOT}/VERSION "${cayman_antrea_publish}/"
mkdir -p "${cayman_antrea_publish}/idps"
cp -rf ${GOBUILD_CAYMAN_ANTREA_IDPS_ROOT}/antrea-idps-debian-*.zip "${cayman_antrea_publish}/idps"
cp -rf ${GOBUILD_CAYMAN_ANTREA_IDPS_ROOT}/antrea-idps-ubi-*.zip "${cayman_antrea_publish}/idps"

echo "====== Copying cayman_antrea-operator-for-kubernetes Product Deliverables ======"
operator_publish="${PUBLISH_DIR}/openshift"
mkdir -p "${operator_publish}"
cp -rv ${GOBUILD_CAYMAN_ANTREA_ROOT}/ubi  "${operator_publish}/antrea"
cp -rv ${GOBUILD_CAYMAN_ANTREA_ROOT}/VERSION  "${operator_publish}/antrea"
mkdir -p "${operator_publish}/antrea-interworking"
cp -rv ${GOBUILD_ANTREA_INTERWORKING_ROOT}/antrea-interworking/images/interworking-ubi-*.tar "${operator_publish}/antrea-interworking"
cp -rv ${GOBUILD_CAYMAN_ANTREA_OPERATOR_FOR_KUBERNETES_ROOT}/lin64 "${operator_publish}/operator"
cp -rv ${GOBUILD_CAYMAN_ANTREA_OPERATOR_FOR_KUBERNETES_ROOT}/VERSION "${operator_publish}/operator"
mkdir -p "${BUILDROOT}/tmp-ipsec"
pushd "${BUILDROOT}/tmp-ipsec"
unzip ${GOBUILD_CAYMAN_ANTREA_IPSEC_ROOT}/antrea-ubi-ipsec-*.zip
cp antrea-ubi-ipsec-*/antrea-agent-ubi-ipsec-*.tar.gz antrea-ubi-ipsec-*/antrea-controller-ubi-ipsec-*.tar.gz "${operator_publish}/antrea/images/"
popd
rm -rf "${BUILDROOT}/tmp-ipsec"

echo "====== Copying nsx-management-proxy-package Product Deliverables ======"
nsx_management_proxy_package_publish="${PUBLISH_DIR}/nsx-management-proxy-package"
mkdir -p "${nsx_management_proxy_package_publish}/images"
cp -rv ${GOBUILD_NSX_MANAGEMENT_PROXY_PACKAGE_ROOT}/nsx-management-proxy-package/images/nsx-management-proxy-* "${nsx_management_proxy_package_publish}/images"
mkdir -p "${nsx_management_proxy_package_publish}/manifests"
cp -rv ${GOBUILD_NSX_MANAGEMENT_PROXY_PACKAGE_ROOT}/nsx-management-proxy-package/manifests/{nsx-management-proxy-data-values,nsx-management-proxy,package-install}.yml \
  "${nsx_management_proxy_package_publish}/manifests"
cp -rv ${GOBUILD_NSX_MANAGEMENT_PROXY_PACKAGE_ROOT}/VERSION ${GOBUILD_NSX_MANAGEMENT_PROXY_PACKAGE_ROOT}/nsx-management-proxy-package-*.zip "${nsx_management_proxy_package_publish}/"

echo "====== Copying CI Deliverables ======"
ci_publish="${PUBLISH_DIR}/ci"
mkdir -p "${ci_publish}"
cp scripts/publish-artifactory.sh ${ci_publish}/

echo "****** antrea_release_build.sh finished ******"
