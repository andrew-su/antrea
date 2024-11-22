REPO_ROOT="${PROJECT_DIR}/src"

OVS_VER=$(cat src/build/images/deps/ovs-version)
if [ -z $OVS_VER ]; then
  OVS_VER="2.17.0"
fi
OVS_DIR="$(readlink -e ${PROJECT_DIR}/../ovs/src)"

SURICATA_VERSION="$(cat src/build/images/deps/suricata-version)"

pushd "${REPO_ROOT}"
COMMON_COMMIT=$(git log -1 --pretty=format:%H)
popd

ANTREA_DOMAIN="antrea.io/antrea"

# This is for "make ubuntu" to use commercial release Dockerfile instead of open-source Dockerfile
export OSS_UBUNTU_BUILD=n

# Buildkit is required to skip unused stages in Dockerfile
export DOCKER_BUILDKIT=1

# FIPS requirements for building linux executables
export CC=x86_64-linux-gnu-gcc
export GOEXPERIMENT=boringcrypto

# BRANCH_NAME can be
# vmware-master # In this case ANTREA_TARGET=main
# vmware-master-$ANTREA_TARGET
#
# vmware-x.y.z+vmware.n # In this case ANTREA_TARGET=main
# vmware-x.y.z+vmware.n-$ANTREA_TARGET
#
# vmware-x.y.z # In this case ANTREA_TARGET=main
# vmware-x.y.z-$ANTREA_TARGET
#
# Note: Since Antrea >=1.5.2+vmware.2 the $ANTREA_TARGET is not encoded in branch name.
# The $ANTREA_TARGET is determined in the next "if" block according to $BUILD_PRODUCT.
source release.config
source versions.config
if [[ "${BRANCH_NAME}" == vmware-master* ]]; then
  IMAGE_VERSION=vmware-master
  BINARY_VERSION=vmware-master
  ANTREA_VERSION_DIGIT=vmware-master
  ANTREA_TARGET="${BRANCH_NAME#vmware-master-}"
elif [[ "${BRANCH_NAME}" == vmware-*+vmware.* ]]; then
  # vmware-x.y.z+vmware.n
  # vmware-x.y.z+vmware.n-$ANTREA_TARGET
  branch_name_trim="${BRANCH_NAME%+vmware.*}" # delete +vmware.N-$ANTREA_TARGET string like -tkgm-advanced
  IMAGE_VERSION="v${branch_name_trim#vmware-}_vmware.${VMWARE_RELEASE_VERSION}"
  BINARY_VERSION="v${branch_name_trim#vmware-}+vmware.${VMWARE_RELEASE_VERSION}"
  ANTREA_VERSION_DIGIT="${branch_name_trim#vmware-}+vmware.${VMWARE_RELEASE_VERSION}"
  ANTREA_TARGET="${BRANCH_NAME#vmware-*+vmware.*-}"
elif [[ "${BRANCH_NAME}" == vmware-* ]]; then
  # vmware-x.y.z
  # vmware-x.y.z-$ANTREA_TARGET
  branch_name_trim="${BRANCH_NAME#vmware-}"
  branch_name_trim="${branch_name_trim%%-*}" # delete ANTREA_TARGET string like -tkgm-advanced
  IMAGE_VERSION="v${branch_name_trim}"
  BINARY_VERSION="v${branch_name_trim}"
  ANTREA_VERSION_DIGIT="${branch_name_trim}"
  ANTREA_TARGET="${BRANCH_NAME#vmware-*-}"
else
  ANTREA_TARGET="${BRANCH_NAME}"
fi
if [ "$ANTREA_TARGET" = "${BRANCH_NAME}" ]; then
  if [ "${BUILD_PRODUCT}" = "cayman_antrea" ]; then
    ANTREA_TARGET="main"
  else
    ANTREA_TARGET="${BUILD_PRODUCT#cayman_antrea_}"
  fi
fi
echo "BRANCH_NAME=${BRANCH_NAME}" "IMAGE_VERSION=$IMAGE_VERSION" "BINARY_VERSION=$BINARY_VERSION" "ANTREA_TARGET=$ANTREA_TARGET"

ANTREA_BRANCH=${BRANCH_NAME}
ANTREA_VERSION=${IMAGE_VERSION}

# https://confluence.eng.vmware.com/pages/viewpage.action?spaceKey=BT&title=Product+signing+-+official+vs+test+keys
if !(env | grep GOBUILD_OFFICIAL_SIGNING_ALLOWED); then
  export GOBUILD_OFFICIAL_SIGNING_ALLOWED=0
fi
if [ "${GOBUILD_OFFICIAL_SIGNING_ALLOWED}" = "1" ]; then  
  GPG_KEY_ID="001E5CC9" # official key ID 
  GPGSIGNC_OPTS=""
else
  GPG_KEY_ID="B2418631" # test key ID
  GPGSIGNC_OPTS="-t"
fi
