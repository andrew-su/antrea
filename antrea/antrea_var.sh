source release.config

REPO_ROOT="${PROJECT_DIR}/src"

OVS_VER=$(cat src/build/images/deps/ovs-version)
if [ -z $OVS_VER ]; then
  OVS_VER="2.15.1"
fi
OVS_DIR="$(readlink -e ${PROJECT_DIR}/../ovs/src)"

pushd "${REPO_ROOT}"
COMMON_COMMIT=$(git log -1 --pretty=format:%H)
popd

ANTREA_DOMAIN="antrea.io/antrea"

# BRANCH_NAME can be
# vmware-master # In this case ANTREA_TARGET=main
# vmware-master-$ANTREA_TARGET
#
# vmware-x.y.z+vmware.n # In this case ANTREA_TARGET=main
# vmware-x.y.z+vmware.n-$ANTREA_TARGET
#
# vmware-x.y.z # In this case ANTREA_TARGET=main
# vmware-x.y.z-$ANTREA_TARGET
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
  echo Unsupported branch pattern "${BRANCH_NAME}" >&2
  return 1
fi
if [ "$ANTREA_TARGET" = "${BRANCH_NAME}" ]; then
  ANTREA_TARGET="main"
fi
echo "BRANCH_NAME=${BRANCH_NAME}" "IMAGE_VERSION=$IMAGE_VERSION" "BINARY_VERSION=$BINARY_VERSION" "ANTREA_TARGET=$ANTREA_TARGET"

ANTREA_BRANCH=${BRANCH_NAME}
ANTREA_VERSION=${IMAGE_VERSION}
