
echo "====== Archiving OpenvSwitch Source Code ======"
archive_ovs_source

echo "====== Generating version Files for CI and Consumers ======"
publish_version_files

echo "====== Checkout TKGm Release Branch ======"
compile_e2e "noipsec" "tkgm"
compile_e2e "ipsec" "tkgm"
git status

echo "====== Building Binaries for TKGm ======"
git reset --hard origin/topic/${ANTREA_VERSION_DIGIT}-tkgm-release
fips_make

echo "====== Building Debian Images ======"
echo "====== Building openvswitch-debian Image ======"
pushd build/images/ovs
cp ${OVS_DIR}/openvswitch-${OVS_VER}.tar.gz .
docker build --build-arg OVS_VERSION=${OVS_VER} -t antrea/openvswitch-debian .
popd

echo "====== Building antrea-debian Image ======"
cp ${GOBUILD_CAYMAN_CNI_PLUGINS_ROOT}/lin64/cni_plugins/executables/cni-plugins-*.tgz .
make debian VERSION=${IMAGE_VERSION}

echo "====== Building openvswitch-debian-ipec Image ======"
pushd build/images/ovs
docker build --cache-from ovs-debs -f Dockerfile-ipsec --build-arg OVS_VERSION=${OVS_VER} -t antrea/openvswitch-debian-ipsec .
popd
echo "====== Building antrea-debian-ipsec Image ======"
make debian-ipsec VERSION=${IMAGE_VERSION}

# Create archives for scripts and binaries
echo "====== Saving TKGm Deliverables ======"
# "${BUILDROOT}/output" will be published to lin64/antrea by antrea_defs.py:CaymanAntreaBuilderLin.install
OUTPUT_DIR="${BUILDROOT}/output"

echo "====== Saving TKGm Manifests ======"
mkdir -p "${OUTPUT_DIR}/manifests"

# Antrea yamls for TKG
# Complicated Yaml customization is done directly in Antrea topic/tkg branch
# Here we only replace image version.
# antrea-ipsec is not used in TKGm.
cp "${REPO_ROOT}/build/yamls/antrea.yml" "${OUTPUT_DIR}/manifests/antrea-fips-${BINARY_VERSION}.yml"
cp "${REPO_ROOT}/build/yamls/antrea-ipsec.yml" "${OUTPUT_DIR}/manifests/antrea-ipsec-${BINARY_VERSION}.yml"
sed -i -e "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-debian:${IMAGE_VERSION}/g" "${OUTPUT_DIR}/manifests/antrea-fips-${BINARY_VERSION}.yml"
sed -i -e "s/image: projects.registry.vmware.com\/antrea\/antrea-.*\$/image: antrea\/antrea-debian:${IMAGE_VERSION}/g" "${OUTPUT_DIR}/manifests/antrea-fips-${BINARY_VERSION}.yml"
sed -i -e "s/image: antrea\/antrea-.*\$/image: antrea\/antrea-debian-ipsec:${IMAGE_VERSION}/g" "${OUTPUT_DIR}/manifests/antrea-ipsec-${BINARY_VERSION}.yml"
sed -i -e "s/image: projects.registry.vmware.com\/antrea\/antrea-.*\$/image: antrea\/antrea-debian-ipsec:${IMAGE_VERSION}/g" "${OUTPUT_DIR}/manifests/antrea-ipsec-${BINARY_VERSION}.yml"
cp "${OUTPUT_DIR}/manifests/antrea-fips-${BINARY_VERSION}.yml" "${OUTPUT_DIR}/manifests/antrea-${BINARY_VERSION}.yml"
sed -i -e 's/tlsCipherSuites:.\+/#tlsCipherSuites:/g' "${OUTPUT_DIR}/manifests/antrea-${BINARY_VERSION}.yml"
echo "====== Saving and Signing TKGm Images ======"

# Image for TKG
image_id="$(docker inspect -f '{{.ID}}' "antrea/antrea-debian:${IMAGE_VERSION}")"
digest_filename="antrea-debian-${IMAGE_VERSION}-image-digests.txt"
checksum_filename="antrea-debian-${IMAGE_VERSION}-image-checksums.txt"
mkdir -p "${OUTPUT_DIR}/images"
# We don't need openvswitch image in all-in-one yaml deployment, so don't publish it
# Just publish Antrea images.
docker save antrea/antrea-debian:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-debian-${IMAGE_VERSION}.tar.gz"
docker save antrea/antrea-debian-ipsec:${IMAGE_VERSION} | gzip -9 > "${OUTPUT_DIR}/images/antrea-debian-ipsec-${IMAGE_VERSION}.tar.gz"
echo "antrea/antrea-debian@${image_id}" > "${OUTPUT_DIR}/images/${digest_filename}"
pushd "${OUTPUT_DIR}/images/"
sha256sum -- * > ${checksum_filename}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Saving and Signing TKGm Executables ======"
rm -rf "${OUTPUT_DIR}/executables"
mkdir -p "${OUTPUT_DIR}/executables"
cat "${REPO_ROOT}/bin/antctl" | gzip -9 > "${OUTPUT_DIR}/executables/antctl-${BINARY_VERSION}.gz"
gzip -c "bin/e2e-tkgm-${ANTREA_VERSION}" > "${OUTPUT_DIR}/executables/e2e-tkgm-${ANTREA_VERSION}.gz"
gzip -c "bin/e2e-tkgm-ipsec-${ANTREA_VERSION}" > "${OUTPUT_DIR}/executables/e2e-tkgm-ipsec-${ANTREA_VERSION}.gz"
pushd "${OUTPUT_DIR}/executables"
BINARY_CHECKSUM_FILENAME="antctl-${BINARY_VERSION}-checksums.txt"
sha256sum -- * > ${BINARY_CHECKSUM_FILENAME}
# See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
gpgsignc textsign -i ${BINARY_CHECKSUM_FILENAME} -o "${BINARY_CHECKSUM_FILENAME}.asc" --hash=sha256 --keyid=001E5CC9
popd

echo "====== Building Antrea Advanced Windows Deliverables ======"
build_windows "advanced" "signed"
