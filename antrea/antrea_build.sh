#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

echo "****** antrea_build.sh start ******"

env
cat /proc/cpuinfo
source release.config

REPO_ROOT="${PROJECT_DIR}/src"
OVS_VER=$(cat src/build/images/deps/ovs-version)
if [ -z $OVS_VER ]; then
  OVS_VER="2.14.2"
fi

function fips_make() {
  chmod +x ${GOBUILD_CAYMAN_GO_ROOT}/lin64/bin/go
  chmod +x -R ${GOBUILD_CAYMAN_GO_ROOT}/lin64/pkg/tool/linux_amd64
  mkdir -p "${REPO_ROOT}/gopath"
  mkdir -p "${REPO_ROOT}/gocache"
  mkdir -p "${REPO_ROOT}/goenv"
  GIT_SHA="$(git rev-parse --short HEAD)"
  ANTREA_VER=$(head -n 1 VERSION)

  if [ $# -eq 0 ]; then
    cmd="mkdir -p bin; go env -w CC='x86_64-linux-gnu-gcc'; GOOS=linux go build -o bin -ldflags ' -X ${ANTREA_DOMAIN}/pkg/version.Version=${ANTREA_VER} -X ${ANTREA_DOMAIN}/pkg/version.GitSHA=${GIT_SHA} -X ${ANTREA_DOMAIN}/pkg/version.GitTreeState=clean -X ${ANTREA_DOMAIN}/pkg/version.ReleaseStatus=unreleased' ${ANTREA_DOMAIN}/cmd/..."
  else
    cmd="mkdir -p bin; go env -w CC='x86_64-linux-gnu-gcc'; GOOS=linux $1"
  fi

	docker run --rm -u $(id -u):$(id -g) \
		-e "GOCACHE=/tmp/gocache" \
		-e "GOPATH=/tmp/gopath" \
		-w /usr/src/${ANTREA_DOMAIN} \
		-v "${REPO_ROOT}/gopath":/tmp/gopath \
		-v "${REPO_ROOT}/gocache":/tmp/gocache \
		-v "${REPO_ROOT}/goenv":/.config/go \
		-v ${GOBUILD_CAYMAN_GO_ROOT}/lin64/src:/usr/local/go/src \
		-v ${GOBUILD_CAYMAN_GO_ROOT}/lin64/pkg:/usr/local/go/pkg \
		-v ${GOBUILD_CAYMAN_GO_ROOT}/lin64/bin:/usr/local/go/bin \
		-v ${REPO_ROOT}:/usr/src/${ANTREA_DOMAIN} \
		golang:1.15 /bin/bash -c "${cmd}"
  chmod -R 0755 bin
}

cp open_source_licenses.txt "${PUBLISH_DIR}/"
pushd "${PUBLISH_DIR}/"
curl -O https://build-artifactory.eng.vmware.com/nsx-ujo-local/antrea/VMware-Antrea-1.2.0-0.13.0-ODP.tar.gz
popd

# Update Docker to a version that supports multi-stage builds
echo  "====== Updating Docker ======"
chmod a+x install_docker.sh
sudo ./install_docker.sh

function run_python {
  PYTHON="${GOBUILD_CAYMAN_PYTHON_ROOT}/lin64/bin/python"
  "${PYTHON}" "$@"
}

docker version

docker pull nsx-ujo-docker-local.artifactory.eng.vmware.com/golang:1.15
docker tag nsx-ujo-docker-local.artifactory.eng.vmware.com/golang:1.15 golang:1.15

if [ "${BRANCH_NAME}" = "vmware-master" ]; then
  IMAGE_VERSION=vmware-master
  BINARY_VERSION=vmware-master
elif [[ "${BRANCH_NAME}" == vmware-*+vmware.* ]]; then
  BRANCH_NAME_TRIM="${BRANCH_NAME%+vmware.*}"
  IMAGE_VERSION="v${BRANCH_NAME_TRIM#vmware-}_vmware.${VMWARE_RELEASE_VERSION}"
  BINARY_VERSION="v${BRANCH_NAME_TRIM#vmware-}+vmware.${VMWARE_RELEASE_VERSION}"
else
  IMAGE_VERSION="v${BRANCH_NAME#vmware-}_vmware.${VMWARE_RELEASE_VERSION}"
  BINARY_VERSION="v${BRANCH_NAME#vmware-}+vmware.${VMWARE_RELEASE_VERSION}"
fi

cd "${REPO_ROOT}"
git status
COMMON_COMMIT=$(git log -1 --pretty=format:%H)

echo "===== Compile antrea e2e testcases  ====="
git reset --hard "${COMMON_COMMIT}"

ANTREA_BRANCH=${BRANCH_NAME}
ANTREA_VERSION=${IMAGE_VERSION}

if [[ "${ANTREA_BRANCH}" == vmware-*+vmware.* ]] ; then
  BRANCH_NAME_TRIM="${ANTREA_BRANCH%+vmware.*}"
else
  BRANCH_NAME_TRIM="${ANTREA_BRANCH}"
fi
upstream_release="${BRANCH_NAME_TRIM#vmware-}"
if [ "${upstream_release}" != "master" ]; then
  upstream_release="v${upstream_release}"
fi

# remove vmware-* from beginning
antreaVersion="${ANTREA_BRANCH#*-}"
# remove *-rc from ending
antreaVersionDigit="${antreaVersion%-*}"
antreaVersionDigit="${antreaVersionDigit%+vmware.*}"

function version_ge()
{
    if [[ $1 == $2 ]]
    then
        return 0
    fi
    local IFS=.
    local i ver1=($1) ver2=($2)
    # fill empty fields in ver1 with zeros
    for ((i=${#ver1[@]}; i<${#ver2[@]}; i++))
    do
        ver1[i]=0
    done
    for ((i=0; i<${#ver1[@]}; i++))
    do
        if [[ -z ${ver2[i]} ]]
        then
            # fill empty fields in ver2 with zeros
            ver2[i]=0
        fi
        if ((10#${ver1[i]} > 10#${ver2[i]}))
        then
            return 0
        fi
        if ((10#${ver1[i]} < 10#${ver2[i]}))
        then
            return 1
        fi
    done
    return 0
}

if version_ge "$antreaVersionDigit" "0.13.0"; then
  git checkout "origin/topic/${ANTREA_BRANCH#vmware-}-common"
else
  git checkout "${upstream_release}"
fi

if version_ge "$antreaVersionDigit" "1.2.0"; then
  ANTREA_DOMAIN="antrea.io/antrea"
else
  ANTREA_DOMAIN="github.com/vmware-tanzu/antrea"
fi

if version_ge "$antreaVersionDigit" "1.2.0"; then
    for test_image in "standard" "advanced" "tkgs" "tkgm"
    do
      git reset --hard remotes/origin/topic/${antreaVersion}-${test_image}-release
      rm -f test/e2e/ipsec_test.go
      fips_make "go test -c -v -x -o bin/e2e-${test_image}-${ANTREA_VERSION} ${ANTREA_DOMAIN}/test/e2e"
    done
else
    # Make standard package
    rm -f test/e2e/ipsec_test.go
    fips_make "go test -c -o bin/e2e-standard-${ANTREA_VERSION} ${ANTREA_DOMAIN}/test/e2e"
    # Make TKGm package
    git reset --hard "${COMMON_COMMIT}"
    git cherry-pick --keep-redundant-commits "HEAD..origin/topic/${ANTREA_BRANCH#vmware-}-features"
    git cherry-pick --keep-redundant-commits "HEAD..origin/topic/${ANTREA_BRANCH#vmware-}-tkg"
    rm -f test/e2e/ipsec_test.go
    fips_make "go test -c -o bin/e2e-tkgm-${ANTREA_VERSION} ${ANTREA_DOMAIN}/test/e2e"
    # Make advanced or TKGs package
    git reset --hard "${COMMON_COMMIT}"
    git cherry-pick --keep-redundant-commits "HEAD..origin/topic/${ANTREA_BRANCH#vmware-}-features"
    rm -f test/e2e/ipsec_test.go
    fips_make "go test -c -o bin/e2e-advanced-${ANTREA_VERSION} ${ANTREA_DOMAIN}/test/e2e"
    fips_make "go test -c -o bin/e2e-tkgs-${ANTREA_VERSION} ${ANTREA_DOMAIN}/test/e2e"
fi

mkdir -p "${PUBLISH_DIR}/lin64/antrea/executables/"
for test_image in "standard" "advanced" "tkgs" "tkgm"
do
  gzip -c bin/e2e-${test_image}-${ANTREA_VERSION} > ${PUBLISH_DIR}/lin64/antrea/executables/e2e-${test_image}-${ANTREA_VERSION}.gz
done

# NOTE:
# Antrea standard deiverables
# image and executable: common + cherry-pick(tkg) (tkg is for Debian build patches)
# manifest: common + sed(image_name)

# Antrea advanced deiverables
# image and executable: common + cherry-pick(enterprise-features + tkg)
# manifest: common + cherry-pick(enterprise-features) + sed(image_name)

# TKG deiverables
# image and executable: common + cherry-pick(enterprise-features + tkg)
# manifest: common + cherry-pick(enterprise-features + tkg) + sed(image_name)

# TKGS deiverables
# image and executable: common + cherry-pick(enterprise-features + tkgs)
# manifest: common + cherry-pick(enterprise-features + tkgs) + sed(image_name)

# Antrea Windows deliverables

echo "====== Preparing Antrea Standard Product Deliverables: Debian Manifests ======"
git reset --hard "${COMMON_COMMIT}"
antrea_std_deliverables="antrea-standard-${BRANCH_NAME#vmware-}.${BUILD_NUMBER}"
mkdir -p "${PUBLISH_DIR}/${antrea_std_deliverables}"
mkdir -p "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests"
# antrea-ipsec is not used in commecial release
cp "${REPO_ROOT}/build/yamls/antrea.yml" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/antrea-standard-${BINARY_VERSION}.yml"
cp "${REPO_ROOT}/build/yamls/flow-aggregator.yml" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/flow-aggregator-${BINARY_VERSION}.yml"
sed -i -e "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-standard-debian:${IMAGE_VERSION}/g" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/antrea-standard-${BINARY_VERSION}.yml"
sed -i -e "s/image: projects.registry.vmware.com\/antrea\/antrea-.*\$/image: antrea\/antrea-standard-debian:${IMAGE_VERSION}/g" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/antrea-standard-${BINARY_VERSION}.yml"
sed -i -e "s/image: projects.registry.vmware.com\/antrea\/flow-aggregator:latest/image: antrea\/flow-aggregator-debian:${IMAGE_VERSION}/g" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/flow-aggregator-${BINARY_VERSION}.yml"
echo "====== Preparing Antrea Advanced Product Deliverables: Debian Manifests ======"
git reset --hard "${COMMON_COMMIT}"
git cherry-pick --keep-redundant-commits HEAD..origin/topic/${BRANCH_NAME#vmware-}-features
antrea_adv_deliverables="antrea-advanced-${BRANCH_NAME#vmware-}.${BUILD_NUMBER}"
mkdir -p "${PUBLISH_DIR}/${antrea_adv_deliverables}"
mkdir -p "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests"
# antrea-ipsec is not used in commecial release
cp "${REPO_ROOT}/build/yamls/antrea.yml" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/antrea-advanced-${BINARY_VERSION}.yml"
cp "${REPO_ROOT}/build/yamls/flow-aggregator.yml" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/flow-aggregator-${BINARY_VERSION}.yml"
sed -i -e "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-advanced-debian:${IMAGE_VERSION}/g" "${REPO_ROOT}/build/yamls/antrea.yml" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/antrea-advanced-${BINARY_VERSION}.yml"
sed -i -e "s/image: projects.registry.vmware.com\/antrea\/antrea-.*\$/image: antrea\/antrea-advanced-debian:${IMAGE_VERSION}/g" "${REPO_ROOT}/build/yamls/antrea.yml" "${PUBLISH_DIR}/${antrea_adv_deliverables}/manifests/antrea-advanced-${BINARY_VERSION}.yml"
sed -i -e "s/image: projects.registry.vmware.com\/antrea\/flow-aggregator:latest/image: antrea\/flow-aggregator-debian:${IMAGE_VERSION}/g" "${PUBLISH_DIR}/${antrea_std_deliverables}/manifests/flow-aggregator-${BINARY_VERSION}.yml"
echo "====== Archiving OpenvSwitch Source Code ======"
git reset --hard "${COMMON_COMMIT}"
OPENVSWITCH_DIR="$(readlink -e ${PROJECT_DIR}/../ovs/src)"
OPENVSWITCH_VERSION="2.14.2"
pushd "${OPENVSWITCH_DIR}"
git archive --format=tar.gz --prefix=openvswitch-${OPENVSWITCH_VERSION}/ -o openvswitch-${OPENVSWITCH_VERSION}.tar.gz HEAD
popd


echo "====== Patching Antrea Repo for TKGS ======"
# Patching build scripts and Dockerfiles
git reset --hard origin/topic/${BRANCH_NAME#vmware-}-tkgs-release
git status

echo "====== Building Binaries for TKGS ======"
fips_make
echo "====== Building Photon Images ======"
echo Photon images are for local testing, they are not consumed by cayman_photon.
echo We maintain a dedicated Antrea Dockerfile in cayman_photon. Antrea photon
echo image is actually built there.
echo "====== Preparing local Photon Yum Repo ======"
mkdir -p /tmp/photo-iso
sudo mount -o loop "${GOBUILD_CSC_PHOTON_ROOT}/csc-photon-3.0.0-x86_64.iso" /tmp/photo-iso
pushd "/tmp/photo-iso"
run_python -m SimpleHTTPServer 8080 &
popd

function stop_local_repo {
  jobs -l
  ps aux | grep python
  pgrep -P $(jobs -p %?SimpleHTTPServer)
  pkill -SIGTERM -P $(jobs -p %?SimpleHTTPServer)
  wait %?SimpleHTTPServer || echo wait returns error $? as expected
  sudo lsof /tmp/photo-iso || true  # If no process is using photon-iso, lsof returns 1
  sudo umount /tmp/photo-iso
}
trap stop_local_repo Exit

REPO_URL="http://`ip -f inet -o address show scope global | head -n 1| cut -f 7 -d ' ' | cut -f 1 -d '/'`:8080/RPMS"
sed -i -e "s|baseurl=.*\$|baseurl=${REPO_URL}|g" "${PROJECT_DIR}/images/ovs-photon/photon-iso.repo"
sed -i -e "s|baseurl=.*\$|baseurl=${REPO_URL}|g" "${PROJECT_DIR}/images/antrea-photon/photon-iso.repo"

echo "====== Buildling openvswitch-photon Image ======"
pushd "${PROJECT_DIR}/images/ovs-photon/"
cp "${GOBUILD_CSC_PHOTON_ROOT}/docker-image/photon-rootfs.tar.gz" .
cp ${OPENVSWITCH_DIR}/openvswitch-*.tar.gz .
docker build --build-arg OVS_VERSION=${OVS_VER} --target ovs-rpms -t antrea/openvswitch-rpms-photon .
docker build --build-arg OVS_VERSION=${OVS_VER} --cache-from antrea/openvswitch-rpms-photon -t antrea/openvswitch-photon .
rm -f photon-rootfs.tar.gz
popd

echo "====== Building antrea-photon Image ======"
cp -vf ${PROJECT_DIR}/images/antrea-photon/* .
cp "${GOBUILD_CSC_PHOTON_ROOT}/docker-image/photon-rootfs.tar.gz" .
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
docker build -t localhost:5000/vmware.io/antrea/antrea-photon:${IMAGE_VERSION} .
rm -f photon-rootfs.tar.gz

echo "====== Building Ubuntu Images ======"
echo "====== Building openvswitch-ubuntu Image ======"
pushd build/images/ovs
cp ${OPENVSWITCH_DIR}/openvswitch-${OPENVSWITCH_VERSION}.tar.gz .
docker build --build-arg OVS_VERSION=${OVS_VER} -t antrea/openvswitch-ubuntu .
popd

echo "====== Building antrea-ubuntu Image ======"
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
make ubuntu VERSION=${IMAGE_VERSION}
docker tag antrea/antrea-ubuntu:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-ubuntu:${IMAGE_VERSION}

echo "====== Saving and Signing Ubuntu Images ======"
OUTPUT_DIR="${BUILDROOT}/output"
image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-ubuntu:${IMAGE_VERSION}")"
digest_filename="antrea-ubuntu-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-ubuntu-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${OUTPUT_DIR}/images"
#openvswitch-ubuntu only for antrea-ubuntu build reference, so no need to publish openvswitch
mkdir -p "${PUBLISH_DIR}/ubuntu/images/"
docker save localhost:5000/vmware.io/antrea/antrea-ubuntu:${IMAGE_VERSION} | gzip -9 > "${PUBLISH_DIR}/ubuntu/images/antrea-ubuntu-${IMAGE_VERSION}.tar.gz"
echo "localhost:5000/vmware.io/antrea/antrea-ubuntu@${image_id}" > "${PUBLISH_DIR}/ubuntu/images/${digest_filename}"
pushd "${PUBLISH_DIR}/ubuntu/images"
sha256sum -- * > ${checksum_filename}
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Saving TKGS Deliverables ======"
echo "====== Saving TKGS Manifests ======"
# Antrea yamls for TKG Service. antrea-ipsec is not supported yet
# Complicated Yaml customization is done directly in Antrea topic/tkgs branch
# Here we only replace image version
for k8s_version in "1.18" "1.19" "1.20" "1.21"; do
  mkdir -p "${PUBLISH_DIR}/add-on/${k8s_version}"
  cp "${REPO_ROOT}/build/yamls/antrea.yml" "${PUBLISH_DIR}/add-on/${k8s_version}/antrea.yaml"
  sed -i -e "s/image: antrea\/antrea-.*\$/image: localhost:5000\/vmware.io\/antrea\/antrea-photon:${IMAGE_VERSION}/g" "${PUBLISH_DIR}/add-on/${k8s_version}/antrea.yaml"
  sed -i -e "s/image: projects.registry.vmware.com\/antrea\/antrea-.*\$/image: localhost:5000\/vmware.io\/antrea\/antrea-photon:${IMAGE_VERSION}/g" "${PUBLISH_DIR}/add-on/${k8s_version}/antrea.yaml"
done

echo "====== Saving TKGS Binaries ======"
# Binaries for building Antrea Photon image for TKG Service
mkdir -p "${PUBLISH_DIR}/photon/bin"
pushd "${REPO_ROOT}/bin"
tar -czf "${PUBLISH_DIR}/photon/bin/bin.tar.gz" *
popd
pushd "${REPO_ROOT}/build/images/scripts"
tar -czf "${PUBLISH_DIR}/photon/bin/scripts.tar.gz" *
popd

# RPMs for building Antrea Photon image for TKG Service
echo "====== Saving OpenvSwitch RPMs ======"
mkdir -p "${PUBLISH_DIR}/photon/rpms/"
docker run -idt --rm --name ovs-rpms antrea/openvswitch-rpms-photon sh
docker cp ovs-rpms:/tmp/ovs-rpms "${PUBLISH_DIR}/photon/rpms/"
docker stop ovs-rpms

echo "====== Saving and Signing TKGs Images ======"

# A test photon image
image_id="$(docker inspect -f '{{.ID}}' "localhost:5000/vmware.io/antrea/antrea-photon:${IMAGE_VERSION}")"
digest_filename="antrea-photon-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-photon-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${PUBLISH_DIR}/photon/images"
docker save localhost:5000/vmware.io/antrea/antrea-photon:${IMAGE_VERSION} | gzip -9 > "${PUBLISH_DIR}/photon/images/antrea-photon-${IMAGE_VERSION}.tar.gz"
echo "localhost:5000/vmware.io/antrea/antrea-photon@${image_id}" > "${PUBLISH_DIR}/photon/images/${digest_filename}"
pushd "${PUBLISH_DIR}/photon/images"
sha256sum -- * > ${checksum_filename}
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Cleanup TKGS Build Result ======"
make clean

echo "====== Patching Antrea Repo for Antrea Standard Product ======"
git reset --hard origin/topic/${BRANCH_NAME#vmware-}-standard-release
git status

echo "====== Building Binaries for Antrea Standard Product ======"
fips_make
make flow-aggregator-image
docker tag antrea/flow-aggregator-debian antrea/flow-aggregator-debian:${IMAGE_VERSION}
echo "====== Building Debian Images ======"
echo "====== Building openvswitch-debian Image ======"
pushd build/images/ovs
cp ${OPENVSWITCH_DIR}/openvswitch-${OPENVSWITCH_VERSION}.tar.gz .
docker build --build-arg OVS_VERSION=${OVS_VER} -t antrea/openvswitch-debian .
popd

echo "====== Building antrea-debian Image ======"
git reset --hard origin/topic/${BRANCH_NAME#vmware-}-advanced-release
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
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
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Saving and Signing Antrea Standard Product Executables ======"

mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- * > ${BINARY_CHECKSUM_FILENAME}
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Cleanup Antrea Standard Product Build Result ======"
make clean

echo "====== Preparing Antrea Standard Product Deliverables: Images, executables ======"
cp -r "${OUTPUT_DIR}/images" "${PUBLISH_DIR}/${antrea_std_deliverables}"
cp -r "${OUTPUT_DIR}/executables" "${PUBLISH_DIR}/${antrea_std_deliverables}"
pushd "${PUBLISH_DIR}"
zip --verbose -r "${antrea_std_deliverables}.zip" "${antrea_std_deliverables}"
rm -rf "${PUBLISH_DIR}/${antrea_std_deliverables}"
popd


echo "====== Patching Antrea Repo for TKGm ======"
git reset --hard origin/topic/${BRANCH_NAME#vmware-}-tkgm-release
git status

echo "====== Building Binaries for TKGm ======"
fips_make

echo "====== Building Debian Images ======"
echo "====== Building openvswitch-debian Image ======"
pushd build/images/ovs
cp ${OPENVSWITCH_DIR}/openvswitch-${OPENVSWITCH_VERSION}.tar.gz .
docker build --build-arg OVS_VERSION=${OVS_VER} -t antrea/openvswitch-debian .
popd

echo "====== Building antrea-debian Image ======"
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
make debian VERSION=${IMAGE_VERSION}

# Create archives for scripts and binaries
echo "====== Saving TKGm Deliverables ======"
OUTPUT_DIR="${BUILDROOT}/output"

echo "====== Saving TKGm Manifests ======"
mkdir -p "${OUTPUT_DIR}/manifests"
# Define some variables in manifests/version
# Used in cayman_photon when builing antrea image
echo ANTREA_VERSION=${IMAGE_VERSION} >> "${OUTPUT_DIR}/manifests/version"
echo ANTREA_BINARY_VERSION=${BINARY_VERSION} >> "${OUTPUT_DIR}/manifests/version"
echo ANTREA_BRANCH=${BRANCH_NAME} >> "${OUTPUT_DIR}/manifests/version"
echo ANTREA_BUILD=${BUILD_NUMBER} >> "${OUTPUT_DIR}/manifests/version"
# Used by cayman_photon support/scripts/customizeOvf/customizeGcOvf.py
# to read add-on versions in a normalized way
echo "${IMAGE_VERSION}" > "${PUBLISH_DIR}/VERSION"

# Antrea yamls for TKG
# Complicated Yaml customization is done directly in Antrea topic/tkg branch
# Here we only replace image version.
# antrea-ipsec is not used in TKGm.
cp "${REPO_ROOT}/build/yamls/antrea.yml" "${OUTPUT_DIR}/manifests/antrea-${BINARY_VERSION}.yml"
cp "${REPO_ROOT}/build/yamls/flow-aggregator.yml" "${OUTPUT_DIR}/manifests/flow-aggregator-${BINARY_VERSION}.yml"
sed -i -e "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-debian:${IMAGE_VERSION}/g" "${OUTPUT_DIR}/manifests/antrea-${BINARY_VERSION}.yml"
sed -i -e "s/image: projects.registry.vmware.com\/antrea\/antrea-.*\$/image: antrea\/antrea-debian:${IMAGE_VERSION}/g" "${OUTPUT_DIR}/manifests/antrea-${BINARY_VERSION}.yml"
sed -i -e "s/image: projects.registry.vmware.com\/antrea\/flow-aggregator:latest/image: antrea\/flow-aggregator-debian:${IMAGE_VERSION}/g" "${OUTPUT_DIR}/manifests/flow-aggregator-${BINARY_VERSION}.yml"

echo "====== Saving and Signing TKGm Images ======"

# Image for TKG
image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-debian:${IMAGE_VERSION}")"
digest_filename="antrea-debian-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-debian-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${OUTPUT_DIR}/images"
# We don't need openvswitch image in all-in-one yaml deployment, so don't publish it
# Just publish Antrea images.
docker save antrea/antrea-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-debian-${IMAGE_VERSION}.tar.gz"
docker save antrea/flow-aggregator-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/flow-aggregator-debian-${IMAGE_VERSION}.tar.gz"
echo "antrea/antrea-debian@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Saving and Signing TKGm Executables ======"
rm -rf "${OUTPUT_DIR}/executables"
mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- * > ${BINARY_CHECKSUM_FILENAME}
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=001E5CC9
popd

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
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Saving and Signing Antrea Advanced Product Executables ======"

mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- "antctl-${BINARY_VERSION}.gz" > ${BINARY_CHECKSUM_FILENAME}
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Preparing Antrea Advanced Product Deliverables: Images, executables ======"
cp -r "${OUTPUT_DIR}/images" "${PUBLISH_DIR}/${antrea_adv_deliverables}"
cp -r "${OUTPUT_DIR}/executables" "${PUBLISH_DIR}/${antrea_adv_deliverables}"
pushd "${PUBLISH_DIR}"
zip --verbose -r "${antrea_adv_deliverables}.zip" "${antrea_adv_deliverables}"
rm -rf "${PUBLISH_DIR}/${antrea_adv_deliverables}"
popd

echo "====== Cleanup TKGm Build Result ======"
make clean

echo "====== Windows build ======"
function build_windows {
  antrea_deliverable_kind=$1
  rm -rf "${PUBLISH_DIR}/windows"
  mkdir -p "${PUBLISH_DIR}/windows"
  mkdir -p "${PUBLISH_DIR}/windows/etc"
  cp build/yamls/windows/base/conf/antrea-agent.conf "${PUBLISH_DIR}/windows/etc/antrea-agent.conf"
  cp build/yamls/windows/base/conf/antrea-cni.conflist "${PUBLISH_DIR}/windows/etc/antrea-cni.conflist"

  mkdir -p "${PUBLISH_DIR}/windows/bin"
  make docker-windows-bin
  cp bin/antrea-agent.exe "${PUBLISH_DIR}/windows/bin/antrea-agent.exe"
  cp bin/antrea-cni.exe "${PUBLISH_DIR}/windows/bin/antrea-cni.exe"

  DownloadDir="${REPO_ROOT}/download"
  rm -rf "${DownloadDir}"
  mkdir -p "${DownloadDir}"
  CNI_WINDOWS_URL="https://github.com/containernetworking/plugins/releases/download/v0.8.1/cni-plugins-windows-amd64-v0.8.1.tgz"
  wget -q "${CNI_WINDOWS_URL}" -O "${DownloadDir}/cni-plugins-windows.tgz"
  mkdir -p "${DownloadDir}/cni-plugins-windows"
  tar zxf "${DownloadDir}/cni-plugins-windows.tgz" -C "${DownloadDir}/cni-plugins-windows"
  cp "${DownloadDir}/cni-plugins-windows/host-local.exe" "${PUBLISH_DIR}/windows/bin/host-local.exe"

  cp hack/windows/Helper.psm1 "${PUBLISH_DIR}/windows/Helper.psm1"
  cp hack/windows/Start.ps1 "${PUBLISH_DIR}/windows/Start.ps1"
  cp hack/windows/Stop.ps1 "${PUBLISH_DIR}/windows/Stop.ps1"
  cp hack/windows/Install-OVS.ps1 "${PUBLISH_DIR}/windows/Install-OVS.ps1"

  # If the NSX OVS is unsigned, set false here.
  if true; then
    sed -i 's|$ImportCertificate = $true|$ImportCertificate = $false|g' "${PUBLISH_DIR}/windows/Install-OVS.ps1"
  fi

  echo "==== NSX OVS build ===="
  NSXOVS_PATH=$(find "${GOBUILD_NSX_OVS_BUILD_ROOT}/windows_x64" -name "openvswitch*-win64.zip")
  VCRedistUrl="http://build-artifactory.eng.vmware.com/artifactory/nsbu-windows-local/vcredists.zip"
  TempDir="${REPO_ROOT}/nsx-ovs-temp"
  rm -rf "${TempDir}"
  mkdir -p "${TempDir}"

  cp "${NSXOVS_PATH}" "${DownloadDir}/nsx-ovs.zip"
  wget -q "${VCRedistUrl}" -O "${DownloadDir}/vcredists.zip"
  docker run --rm --user $(id -u):$(id -g) -v "${REPO_ROOT}":/tmp/windows -w /tmp/windows projects.registry.vmware.com/library/busybox /bin/sh -c "unzip -q download/nsx-ovs.zip -d nsx-ovs-temp ; unzip -q download/vcredists.zip -d nsx-ovs-temp"
  OVSDir="${TempDir}/openvswitch"
  OVSDriverDir="${OVSDir}/driver"
  VCRedistDir="${OVSDir}/redist"
  cp -r "${TempDir}/include" "${OVSDir}"
  cp -r "${TempDir}/lib" "${OVSDir}"
  cp -r "${TempDir}/scripts" "${OVSDir}"
  cp -r "${TempDir}/vcredist2017" "${VCRedistDir}"
  cp -r "${TempDir}/ovsext/win10_x64" "${OVSDriverDir}"
  pushd "${TempDir}"
  zip --verbose -r "${PUBLISH_DIR}/windows/ovs-win64.zip" openvswitch
  popd

  antrea_windows_deliverables="antrea-windows-${antrea_deliverable_kind}-${BRANCH_NAME#vmware-}.${BUILD_NUMBER}"
  antrea_windows_deliverables_tkg="antrea-windows-${antrea_deliverable_kind}"
  pushd "${PUBLISH_DIR}/windows"
  zip --verbose -r "${PUBLISH_DIR}/${antrea_windows_deliverables}.zip" *
  popd
  cp "${PUBLISH_DIR}/${antrea_windows_deliverables}.zip" "${PUBLISH_DIR}/windows/${antrea_windows_deliverables_tkg}.zip"
  mv "${PUBLISH_DIR}/windows" "${PUBLISH_DIR}/windows-${antrea_deliverable_kind}"
  rm -rf bin
}

echo "====== Building Antrea Standard Windows Deliverables ======"
git reset --hard "${COMMON_COMMIT}"
build_windows "standard"

echo "====== Building Antrea Advanced Windows Deliverables ======"
git reset --hard "${COMMON_COMMIT}"
git cherry-pick --keep-redundant-commits HEAD..origin/topic/${BRANCH_NAME#vmware-}-features
build_windows "advanced"

echo "****** antrea_build.sh end ******"
