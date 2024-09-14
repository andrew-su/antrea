#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

HOME_MTS_DIR=$(pwd)
export HOME="${HOME_MTS_DIR}"

echo "====== Disabling --pull in All Makefile Docker Build Target ======"
export NO_PULL=1

echo "====== Archiving OpenvSwitch Source Code ======"
archive_ovs_source

echo "====== Generating version Files for CI and Consumers ======"
publish_version_files

echo "====== Checkout Features Branch ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-features
#check_manifests

# Build antrea packages
# how GOBUILD-<component>-ROOT is populated: https://wiki.eng.vmware.com/Build/Gobuild/Deliverables

echo "... Installing carvel tools ..."
CARVEL_TOOLS_BIN="${BUILDROOT}/carvel_tools"
mkdir -p ${CARVEL_TOOLS_BIN}
export PATH=${CARVEL_TOOLS_BIN}:$PATH
export PATH=${GOBUILD_CAYMAN_GO_ROOT}/lin64/bin/:$PATH
chmod +x ${GOBUILD_CAYMAN_GO_ROOT}/lin64/bin/go ${GOBUILD_CAYMAN_GO_ROOT}/lin64/pkg/tool/linux_amd64/*


# build binary
GO111MODULE=${GO111MODULE:-on}
GOEXPERIMENT=${GOEXPERIMENT:-boringcrypto}

echo "... Getting imgpkg ..."
gzip -d -c ${GOBUILD_CAYMAN_IMGPKG_ROOT}/lin64/imgpkg/executables/imgpkg-linux-amd64-v*.gz > "${CARVEL_TOOLS_BIN}/imgpkg"
chmod +x ${CARVEL_TOOLS_BIN}/imgpkg
imgpkg version


echo "... Getting ytt ..."
gzip -d -c ${GOBUILD_CAYMAN_K14S_YTT_ROOT}/lin64/ytt/executables/ytt-linux-amd64-v*.gz > "${CARVEL_TOOLS_BIN}/ytt"
chmod +x ${CARVEL_TOOLS_BIN}/ytt
ytt version

echo "... Getting kbld ..."
gzip -d -c ${GOBUILD_CAYMAN_KBLD_ROOT}/lin64/kbld/executables/kbld-linux-amd64-v*.gz > "${CARVEL_TOOLS_BIN}/kbld"
chmod +x ${CARVEL_TOOLS_BIN}/kbld
kbld version


## Workaround till release-machinery carvel-package module starts  using crane go libraries
## Since GOPROXY is set to artifactory, this doesn't violate SRP requirements
echo "... Getting crane cli ..."
export GOPROXY="https://packages.vcfd.broadcom.net/artifactory/proxy-golang-remote"
export GOSUMDB="sum.golang.org https://packages.vcfd.broadcom.net/artifactory/go-gosumdb-remote"
export GOPATH="${BUILDROOT}/go-path"
export GOCACHE=$GOPATH/cache
mkdir -p ${GOPATH}
git config --global url.ssh://git@gitlab-vmw.devops.broadcom.net/.insteadOf https://gitlab-vmw.devops.broadcom.net/
git clone https://gitlab-vmw.devops.broadcom.net/zhongchengl/go-containerregistry.git
pushd ./go-containerregistry && CGO_ENABLED=0 go install -mod=readonly ./cmd/crane && popd 
#CGO_ENABLED=0 go install github.com/google/go-containerregistry/cmd/crane@latest


ls $(go env GOPATH)/bin
mv $(go env GOPATH)/bin/crane ${CARVEL_TOOLS_BIN}/crane
crane version



# Create archives for scripts and binaries
echo "====== Saving TKGs Deliverables ======"
# "${BUILDROOT}/output" will be published to lin64/antrea by antrea_defs.py:CaymanAntreaBuilderLin.install
OUTPUT_DIR="${BUILDROOT}/output"
INTERWORKING_IMAGE_VERSION=$(cat "${GOBUILD_ANTREA_INTERWORKING_ROOT}"/VERSION)
ANTREA_IMAGE_VERSION=$(cat "${GOBUILD_CAYMAN_ANTREA_TKGM_ADVANCED_ROOT}"/VERSION)


echo "====== Export Antrea Images ENV ======"
export "IMAGE_FILEPATH_ANTREA_AGENT"="${GOBUILD_CAYMAN_ANTREA_TKGM_ADVANCED_ROOT}/lin64/antrea/images/antrea-advanced-agent-debian-${ANTREA_IMAGE_VERSION}.tar.gz"
export "IMAGE_FILEPATH_ANTREA_CONTROLLER"="${GOBUILD_CAYMAN_ANTREA_TKGM_ADVANCED_ROOT}/lin64/antrea/images/antrea-advanced-controller-debian-${ANTREA_IMAGE_VERSION}.tar.gz"
# export antrea windows image env
export "IMAGE_FILEPATH_ANTREA_WINDOWS"="${GOBUILD_CAYMAN_ANTREA_TKGM_ADVANCED_ROOT}/windows-advanced/images/antrea-advanced-windows-${ANTREA_IMAGE_VERSION}.tar.gz"


# export linux/win images for antrea
mkdir -p ${OUTPUT_DIR}/images/
cp "${IMAGE_FILEPATH_ANTREA_AGENT}" "${OUTPUT_DIR}"/images
cp "${IMAGE_FILEPATH_ANTREA_CONTROLLER}" "${OUTPUT_DIR}"/images
# interworking ob doest not have v in the filepath. adding it --> interworking-debian-v1.1.0_vmware.1.tar
cp "${GOBUILD_ANTREA_INTERWORKING_ROOT}/antrea-interworking/images/interworking-debian-${INTERWORKING_IMAGE_VERSION}.tar" "${OUTPUT_DIR}"/images/interworking-debian-v"${INTERWORKING_IMAGE_VERSION}".tar
export "IMAGE_FILEPATH_ANTREA_INTERWORKING"="${OUTPUT_DIR}/images/interworking-debian-v${INTERWORKING_IMAGE_VERSION}.tar"
cp "${IMAGE_FILEPATH_ANTREA_WINDOWS}" "${OUTPUT_DIR}"/images


# Build carvel package
# package metadata. version format example: 2.1.0+vmware.1-tkg.1
mkdir -p ${OUTPUT_DIR}/package-crs
cp ${PROJECT_DIR}/package/upstream-metadata.yaml ${OUTPUT_DIR}/package-crs/metadata.yml
ytt -v semver_version=${ANTREA_SEMVER} -v current_time=`date -u +"%Y-%m-%dT%H:%M:%SZ"`  -v version=${ANTREA_SEMVER}+${BUILD_VERSION_SUFFIX} -f ${PROJECT_DIR}/package/upstream-package.yaml > ${OUTPUT_DIR}/package-crs/${ANTREA_SEMVER}+${BUILD_VERSION_SUFFIX}.yml


# copy version files for ci use.
cp "${GOBUILD_CAYMAN_ANTREA_TKGM_ADVANCED_ROOT}/lin64/antrea/manifests/version" ${OUTPUT_DIR}/ANTREA_VERSIONS
cat  "${GOBUILD_ANTREA_INTERWORKING_ROOT}/antrea-interworking/manifests/version" >> ${OUTPUT_DIR}/ANTREA_VERSIONS


# Used by release-machinery carvel-package module to generate thick package tarballs
# After build.sh executes, gobuild copies artifacts to publish directory (See cloud_provider_vsphere_defs.py)
mkdir -p ${BUILDROOT}/package-bundle/

pushd "${PROJECT_DIR}/carvelpackage"
echo "=== GIT_SSH_COMMAND=${GIT_SSH_COMMAND}"

  # Configure git to use 'ssh' instead of 'https'
  git config --global url.ssh://git@gitlab.eng.vmware.com/.insteadOf https://gitlab.eng.vmware.com/
  export GOPRIVATE=gitlab.eng.vmware.com
  export GOPROXY=https://build-artifactory.eng.vmware.com/artifactory/proxy-golang-remote,direct
  export GOSUMDB=off



  CGO_ENABLED=0 \
  ANTREA_VERSION_DIGIT="$ANTREA_VERSION_DIGIT" \
  ANTREA_IMAGE_VERSION="$ANTREA_IMAGE_VERSION" \
  INTERWORKING_IMAGE_VERSION="$INTERWORKING_IMAGE_VERSION"  \
  PACKAGE_VERSION_SUFFIX="$PACKAGE_VERSION_SUFFIX" \
  go run main.go
popd

ls -lrtha ${BUILDROOT}/package-bundle/
mv "${BUILDROOT}/package-bundle" "${OUTPUT_DIR}"


