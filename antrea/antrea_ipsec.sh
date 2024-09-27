#!/usr/bin/env bash

echo "====== Disabling --pull in All Makefile Docker Build Target ======"
export NO_PULL=1

echo "====== Enable IPSec in Makefile ======"
export IPSEC=1

echo "====== Archiving OpenvSwitch Source Code ======"
archive_ovs_source

echo "====== Generating version Files for CI and Consumers ======"
publish_version_files

echo "====== Checkout Features Branch ======"
git reset --hard "origin/topic/${ANTREA_VERSION_DIGIT}-features"
check_manifests

echo "===== Compile antrea e2e testcases ======"
compile_e2e "ipsec" "advanced"
git status

echo "====== Building Binaries for Antrea IPsec ======"
fips_make

echo "====== Building OpenvSwitch Debian Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
./build.sh --distro debian --ipsec
popd

echo "====== Building Debian Base Image ======"
pushd build/images/base
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
cp ${GOBUILD_CAYMAN_SURICATA_ROOT}/lin64/suricata/packages/debs/suricata_${SURICATA_VERSION}*.deb .
./build.sh --distro debian --ipsec
popd

echo "====== Building antrea-agent-debian-ipsec & antrea-controller-debian-ipsec Images ======"
make debian VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"
docker tag antrea/antrea-agent-debian:${IMAGE_VERSION} antrea/antrea-agent-debian-ipsec:${IMAGE_VERSION}
docker tag antrea/antrea-controller-debian:${IMAGE_VERSION} antrea/antrea-controller-debian-ipsec:${IMAGE_VERSION}

echo "====== Building OpenvSwitch UBI Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
./build.sh --distro ubi --ipsec
popd

echo "====== Building UBI Base Image ======"
pushd build/images/base
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
curl -k -LO https://packages.vcfd.broadcom.net/artifactory/nsx-ujo-local/antrea/epel/suricata-7.0.6-1.el9.x86_64.rpm
./build.sh --distro ubi --ipsec
rm suricata-7.0.6-1.el9.x86_64.rpm
popd

echo "====== Building antrea-agent-ubi-ipsec & antrea-controller-ubi-ipsec Images ======"
make ubi VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"
docker tag antrea/antrea-agent-ubi:${IMAGE_VERSION} antrea/antrea-agent-ubi-ipsec:${IMAGE_VERSION}
docker tag antrea/antrea-controller-ubi:${IMAGE_VERSION} antrea/antrea-controller-ubi-ipsec:${IMAGE_VERSION}
docker tag antrea/antrea-agent-ubi:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-agent-ubi-ipsec:${IMAGE_VERSION}
docker tag antrea/antrea-controller-ubi:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-controller-ubi-ipsec:${IMAGE_VERSION}

prepare_local_yum_repo
trap stop_local_yum_repo Exit

echo "====== Buildling OpenvSwitch Photon Image ======"
pushd build/images/ovs
cp "${GOBUILD_CSC_PHOTON_ROOT}/docker-image/photon-rootfs.tar.gz" .
cp ${OVS_DIR}/openvswitch-*.tar.gz .
./build.sh --distro photon --rpm-repo-url ${LOCAL_YUM_REPO_URL} --ipsec
popd

echo "====== Building Photon Base Image ======"
pushd build/images/base
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
cp ${GOBUILD_CAYMAN_SURICATA_ROOT}/lin64/suricata/packages/rpms/suricata-${SURICATA_VERSION}*.rpm .
cp ${GOBUILD_CAYMAN_SURICATA_ROOT}/lin64/suricata/packages/rpms/libnet-1*.rpm .
./build.sh --distro photon --rpm-repo-url ${LOCAL_YUM_REPO_URL} --ipsec
popd

echo "====== Building antrea-agent-photon-ipsec & antrea-controller-photon-ipsec Images ======"
make photon VERSION=${IMAGE_VERSION} RPM_REPO_URL=${LOCAL_YUM_REPO_URL} BUILD_INFO="${BUILD_NUMBER}"
docker tag antrea/antrea-agent-photon:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-agent-photon-ipsec:${IMAGE_VERSION}
docker tag antrea/antrea-controller-photon:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-controller-photon-ipsec:${IMAGE_VERSION}

# Create archives for scripts and binaries
echo "====== Saving Antrea IPsec Deliverables ======"
# "${BUILDROOT}/output" will be published to lin64/antrea by antrea_defs.py:CaymanAntreaBuilderLin.install
OUTPUT_DIR="${BUILDROOT}/output"

echo "====== Saving and Signing Antrea IPsec Executables ======"
rm -rf "${OUTPUT_DIR}/executables"
mkdir -p "${OUTPUT_DIR}/executables"
gzip -c "bin/e2e-advanced-ipsec-${ANTREA_VERSION}" > "${OUTPUT_DIR}/executables/e2e-advanced-ipsec-${ANTREA_VERSION}.gz"

echo "=== Saving Antrea IPsec Scripts ==="
mkdir -p "${OUTPUT_DIR}/scripts"
mkdir -p "${OUTPUT_DIR}/scripts/capv-templates"
cp "${REPO_ROOT}/hack/wavefront-metrics.sh" "${OUTPUT_DIR}/scripts/"
cp "${REPO_ROOT}/ci/jenkins/test-vmc.sh" "${OUTPUT_DIR}/scripts/"
cp -r "${REPO_ROOT}/ci/cluster-api/vsphere/templates/" "${OUTPUT_DIR}/scripts/capv-templates/"
tar -zcf ${OUTPUT_DIR}/scripts/capv-templates.tar.gz -C ${OUTPUT_DIR}/scripts/ capv-templates
rm -rf "${OUTPUT_DIR}/scripts/capv-templates"

function build_ipsec_zip_for_distro {
    local distro=$1
    local agent_image_name=$2
    local controller_image_name=$3
    echo "====== Building Antrea ${distro^} IPsec Deliverables ======"
    local antrea_ipsec_deliverables_dir="antrea-${distro}-ipsec-${ANTREA_VERSION_DIGIT}"
    mkdir -p "${PUBLISH_DIR}/${antrea_ipsec_deliverables_dir}"
    pushd "${PUBLISH_DIR}/${antrea_ipsec_deliverables_dir}"
    echo "${BUILD_NUMBER}" > build_number.txt
    MANIFESTS_DIR=$(mktemp -d)
    AGENT_IMG_NAME=${agent_image_name} CONTROLLER_IMG_NAME=${controller_image_name} IMG_TAG=${IMAGE_VERSION} "${REPO_ROOT}/hack/generate-standard-manifests.sh" --mode release --out "${MANIFESTS_DIR}"
    cp "${MANIFESTS_DIR}/antrea-advanced-ipsec.yml" "antrea-${distro}-ipsec-${BINARY_VERSION}.yml"
    cp "${MANIFESTS_DIR}/antrea-advanced.yml" "antrea-${distro}-${BINARY_VERSION}.yml"
    save_image_and_digest ${agent_image_name} ${IMAGE_VERSION} .
    save_image_and_digest ${controller_image_name} ${IMAGE_VERSION} .
    sign_binaries antrea-${distro}-ipsec-${BINARY_VERSION}-checksums.txt .
    pushd "${PUBLISH_DIR}"
    zip --verbose -r "${antrea_ipsec_deliverables_dir}.zip" "${antrea_ipsec_deliverables_dir}"
    popd
    popd
    rm -r "${PUBLISH_DIR}/${antrea_ipsec_deliverables_dir}"
    rm -r "${MANIFESTS_DIR}"
}

build_ipsec_zip_for_distro "debian" "antrea/antrea-agent-debian-ipsec" "antrea/antrea-controller-debian-ipsec"
build_ipsec_zip_for_distro "ubi" "localhost:5000/vmware.io/antrea/antrea-agent-ubi-ipsec" "localhost:5000/vmware.io/antrea/antrea-controller-ubi-ipsec"
build_ipsec_zip_for_distro "photon" "localhost:5000/vmware.io/antrea/antrea-agent-photon-ipsec" "localhost:5000/vmware.io/antrea/antrea-controller-photon-ipsec"

echo "====== Cleanup Antrea IPsec Build Result ======"
make clean
