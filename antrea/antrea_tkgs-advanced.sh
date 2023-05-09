
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

OVS_BUILD_TAG=$(build/images/build-tag.sh)

echo "====== Buildling OpenvSwitch Photon Image ======"
pushd build/images/ovs
cp "${GOBUILD_CSC_PHOTON_ROOT}/docker-image/photon-rootfs.tar.gz" .
cp ${OVS_DIR}/openvswitch-*.tar.gz .
./build.sh --distro photon --rpm-repo-url ${REPO_URL}
popd

echo "====== Building Photon Base Image ======"
pushd build/images/base
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
prepare_whereabouts_tgz .
./build.sh --distro photon --rpm-repo-url ${REPO_URL}
popd

echo "====== Building antrea-photon Image ======"
make photon VERSION=${IMAGE_VERSION} RPM_REPO_URL=${REPO_URL} BUILD_INFO="${BUILD_NUMBER}"
docker tag antrea/antrea-photon:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-photon:${IMAGE_VERSION}
docker tag antrea/antrea-photon:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea:${IMAGE_VERSION}

echo "====== Saving and Signing TKGs Images ======"
# A test photon image
image_id="$(docker inspect -f '{{.ID}}' "localhost:5000/vmware.io/antrea/antrea-photon:${IMAGE_VERSION}")"
digest_filename="antrea-photon-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-photon-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${PUBLISH_DIR}/photon/images"
docker save localhost:5000/vmware.io/antrea/antrea-photon:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea:${IMAGE_VERSION} | gzip -9 > "${PUBLISH_DIR}/photon/images/antrea-photon-${IMAGE_VERSION}.tar.gz"
echo "localhost:5000/vmware.io/antrea/antrea-photon@${image_id}" > "${PUBLISH_DIR}/photon/images/${digest_filename}"
pushd "${PUBLISH_DIR}/photon/images"
sha256sum -- * > ${checksum_filename}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

echo "====== Building antrea-ubuntu Image ======"
make ubuntu VERSION=${IMAGE_VERSION} BUILD_INFO="${BUILD_NUMBER}"
docker tag antrea/antrea-ubuntu:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea-ubuntu:${IMAGE_VERSION}
docker tag antrea/antrea-ubuntu:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea:${IMAGE_VERSION}

echo "====== Saving and Signing Ubuntu Images ======"
OUTPUT_DIR="${BUILDROOT}/output"
image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-ubuntu:${IMAGE_VERSION}")"
digest_filename="antrea-ubuntu-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-ubuntu-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${OUTPUT_DIR}/images"
#openvswitch-ubuntu only for antrea-ubuntu build reference, so no need to publish openvswitch
mkdir -p "${PUBLISH_DIR}/ubuntu/images/"
docker save localhost:5000/vmware.io/antrea/antrea-ubuntu:${IMAGE_VERSION} localhost:5000/vmware.io/antrea/antrea:${IMAGE_VERSION}| gzip -9 > "${PUBLISH_DIR}/ubuntu/images/antrea-ubuntu-${IMAGE_VERSION}.tar.gz"
echo "localhost:5000/vmware.io/antrea/antrea-ubuntu@${image_id}" > "${PUBLISH_DIR}/ubuntu/images/${digest_filename}"
pushd "${PUBLISH_DIR}/ubuntu/images"
sha256sum -- * > ${checksum_filename}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
popd

echo "====== Saving TKGS Manifests ======"
# Antrea yamls for TKG Service. antrea-ipsec is not supported yet
# Complicated Yaml customization is done directly in Antrea topic/tkgs branch
# Here we only replace image version
for k8s_version in "1.22" "1.23" "1.24" "1.25"; do
  MANIFESTS_DIR=$(mktemp -d)
  mkdir -p "${PUBLISH_DIR}/add-on/${k8s_version}"
  IMG_NAME=localhost:5000/vmware.io/antrea/antrea IMG_TAG=${IMAGE_VERSION} ${REPO_ROOT}/hack/generate-standard-manifests.sh --mode release --out "${MANIFESTS_DIR}"
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
