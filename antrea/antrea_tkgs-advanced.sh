
echo "====== Disabling --pull in All Makefile Docker Build Target ======"
export NO_PULL=1

echo "====== Archiving OpenvSwitch Source Code ======"
archive_ovs_source

echo "====== Generating version Files for CI and Consumers ======"
publish_version_files

echo "====== Checkout Features Branch ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-features
check_manifests

echo "===== Compile antrea e2e testcases ======"
compile_e2e "noipsec" "tkgs"
git status

echo "====== Building Binaries for TKGS ======"
fips_make

echo "====== Building Photon Images ======"
echo "Photon images are for local testing, they are not consumed by cayman_photon.
We maintain a dedicated Antrea Dockerfile in cayman_photon. Antrea photon
image is actually built there."

prepare_local_yum_repo
trap stop_local_yum_repo Exit

OVS_BUILD_TAG=$(build/images/build-tag.sh)

echo "====== Buildling OpenvSwitch Photon Image ======"
pushd build/images/ovs
cp "${GOBUILD_CSC_PHOTON_ROOT}/docker-image/photon-rootfs.tar.gz" .
cp ${OVS_DIR}/openvswitch-*.tar.gz .
./build.sh --distro photon --rpm-repo-url ${LOCAL_YUM_REPO_URL}
popd

echo "====== Building Photon Base Image ======"
pushd build/images/base
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
cp ${GOBUILD_CAYMAN_SURICATA_ROOT}/lin64/suricata/packages/rpms/suricata-${SURICATA_VERSION}*.rpm .
cp ${GOBUILD_CAYMAN_SURICATA_ROOT}/lin64/suricata/packages/rpms/libnet-1*.rpm .
./build.sh --distro photon --rpm-repo-url ${LOCAL_YUM_REPO_URL}
popd

echo "====== Building antrea-agent-photon & antrea-controller-photon Images ======"
make photon VERSION=${IMAGE_VERSION} RPM_REPO_URL=${LOCAL_YUM_REPO_URL} BUILD_INFO="${BUILD_NUMBER}"
docker tag antrea/antrea-agent-photon:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-agent-photon:${IMAGE_VERSION}
docker tag antrea/antrea-controller-photon:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-controller-photon:${IMAGE_VERSION}

echo "====== Saving and Signing TKGs Images ======"
# A test photon image
agent_image_id="$(docker inspect -f '{{.ID}}' "localhost:5000/vmware.io/antrea/antrea-agent-photon:${IMAGE_VERSION}")"
controller_image_id="$(docker inspect -f '{{.ID}}' "localhost:5000/vmware.io/antrea/antrea-controller-photon:${IMAGE_VERSION}")"
agent_digest_filename="antrea-agent-photon-${IMAGE_VERSION}-image-digests.txt"
controller_digest_filename="antrea-controller-photon-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-photon-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${PUBLISH_DIR}/photon/images"
docker save localhost:5000/vmware.io/antrea/antrea-agent-photon:${IMAGE_VERSION} | gzip -9 > "${PUBLISH_DIR}/photon/images/antrea-agent-photon-${IMAGE_VERSION}.tar.gz"
docker save localhost:5000/vmware.io/antrea/antrea-controller-photon:${IMAGE_VERSION} | gzip -9 > "${PUBLISH_DIR}/photon/images/antrea-controller-photon-${IMAGE_VERSION}.tar.gz"
echo "localhost:5000/vmware.io/antrea/antrea-agent-photon@${agent_image_id}" > "${PUBLISH_DIR}/photon/images/${agent_digest_filename}"
echo "localhost:5000/vmware.io/antrea/antrea-controller-photon@${controller_image_id}" > "${PUBLISH_DIR}/photon/images/${controller_digest_filename}"
pushd "${PUBLISH_DIR}/photon/images"
sha256sum -- * > ${checksum_filename}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

echo "====== Buildling OpenvSwitch Ubuntu Image ======"
pushd build/images/ovs
cp "${GOBUILD_CSC_PHOTON_ROOT}/docker-image/photon-rootfs.tar.gz" .
cp ${OVS_DIR}/openvswitch-*.tar.gz .
./build.sh --distro ubuntu
popd

echo "====== Building Ubuntu Base Image ======"
pushd build/images/base
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
cp ${GOBUILD_CAYMAN_SURICATA_ROOT}/lin64/suricata/packages/debs/suricata_${SURICATA_VERSION}*.deb .
./build.sh --distro ubuntu
popd

echo "====== Building antrea-agent-ubuntu & antrea-controller-ubuntu Images ======"
make ubuntu VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"
docker tag antrea/antrea-agent-ubuntu:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-agent-ubuntu:${IMAGE_VERSION}
docker tag antrea/antrea-controller-ubuntu:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-controller-ubuntu:${IMAGE_VERSION}

echo "====== Saving and Signing Ubuntu Images ======"
OUTPUT_DIR="${BUILDROOT}/output"
agent_image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-agent-ubuntu:${IMAGE_VERSION}")"
controller_image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-controller-ubuntu:${IMAGE_VERSION}")"
agent_digest_filename="antrea-agent-ubuntu-${IMAGE_VERSION}-image-digests.txt"
controller_digest_filename="antrea-controller-ubuntu-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-ubuntu-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${OUTPUT_DIR}/images"
#openvswitch-ubuntu only for antrea-ubuntu build reference, so no need to publish openvswitch
mkdir -p "${PUBLISH_DIR}/ubuntu/images/"
docker save localhost:5000/vmware.io/antrea/antrea-agent-ubuntu:${IMAGE_VERSION} | gzip -9 > "${PUBLISH_DIR}/ubuntu/images/antrea-agent-ubuntu-${IMAGE_VERSION}.tar.gz"
docker save localhost:5000/vmware.io/antrea/antrea-controller-ubuntu:${IMAGE_VERSION} | gzip -9 > "${PUBLISH_DIR}/ubuntu/images/antrea-controller-ubuntu-${IMAGE_VERSION}.tar.gz"
echo "localhost:5000/vmware.io/antrea/antrea-agent-ubuntu@${agent_image_id}" > "${PUBLISH_DIR}/ubuntu/images/${agent_digest_filename}"
echo "localhost:5000/vmware.io/antrea/antrea-controller-ubuntu@${controller_image_id}" > "${PUBLISH_DIR}/ubuntu/images/${controller_digest_filename}"
pushd "${PUBLISH_DIR}/ubuntu/images"
sha256sum -- * > ${checksum_filename}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

echo "====== Saving TKGS Scripts ======"
mkdir -p "${OUTPUT_DIR}/scripts"
mkdir -p "${OUTPUT_DIR}/scripts/capv-templates"
cp "${REPO_ROOT}/hack/wavefront-metrics.sh" "${OUTPUT_DIR}/scripts/"
cp "${REPO_ROOT}/ci/jenkins/test-vmc.sh" "${OUTPUT_DIR}/scripts/"
cp -r "${REPO_ROOT}/ci/cluster-api/vsphere/templates/" "${OUTPUT_DIR}/scripts/capv-templates/"
tar -zcf ${OUTPUT_DIR}/scripts/capv-templates.tar.gz -C ${OUTPUT_DIR}/scripts/ capv-templates
rm -rf "${OUTPUT_DIR}/scripts/capv-templates"

echo "====== Saving TKGS Manifests ======"
# Antrea yamls for TKG Service. antrea-ipsec is not supported yet
# Complicated Yaml customization is done directly in Antrea topic/tkgs branch
# Here we only replace image version
for k8s_version in "1.26" "1.27" "1.28" "1.29"; do
  MANIFESTS_DIR=$(mktemp -d)
  mkdir -p "${PUBLISH_DIR}/add-on/${k8s_version}"
  AGENT_IMG_NAME=localhost:5000/vmware.io/antrea/antrea-agent-ubuntu CONTROLLER_IMG_NAME=localhost:5000/vmware.io/antrea/antrea-controller-ubuntu IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-standard-manifests.sh --mode release --out "${MANIFESTS_DIR}"
  cp ${MANIFESTS_DIR}/antrea-advanced-tkgs.yml ${PUBLISH_DIR}/add-on/${k8s_version}/antrea.yaml
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
docker run -idt --rm --name ovs-rpms antrea/openvswitch-photon-rpms:${OVS_BUILD_TAG} sh
docker cp ovs-rpms:/tmp/ovs-rpms "${PUBLISH_DIR}/photon/rpms/"
docker stop ovs-rpms

echo "====== Cleanup TKGS Build Result ======"
make clean

# antrea_main.sh have nothing to publish under lin64/antrea. lin64/antrea is used by TKGM.
# CI script may depend on version file under lin64/antrea/manifests, so only publish version file
OUTPUT_DIR="${BUILDROOT}/output"
mkdir -p "${OUTPUT_DIR}/manifests"
echo ANTREA_VERSION=${IMAGE_VERSION} >> "${OUTPUT_DIR}/manifests/version"
echo ANTREA_BINARY_VERSION=${BINARY_VERSION} >> "${OUTPUT_DIR}/manifests/version"
echo ANTREA_BRANCH=${BRANCH_NAME} >> "${OUTPUT_DIR}/manifests/version"
echo ANTREA_BUILD=${BUILD_NUMBER} >> "${OUTPUT_DIR}/manifests/version"
# Used by cayman_photon support/scripts/customizeOvf/customizeGcOvf.py
# to read add-on versions in a normalized way
echo "${IMAGE_VERSION}" > "${PUBLISH_DIR}/VERSION"
