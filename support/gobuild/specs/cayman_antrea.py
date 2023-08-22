LINUX_HOSTTYPE = 'linux-centos8-fw'

CAYMAN_BRANCH = 'master'
CAYMAN_CLN = '71c887b9ee67bd2298b85464ff14401b978ff885'
CAYMAN_BUILDTYPE = 'release'
CAYMAN_HOSTTYPES = {
    LINUX_HOSTTYPE: 'linux',
}

CAYMAN_PYTHON_BRANCH = "vmware-master"
CAYMAN_PYTHON_CLN = "9dec3e3ec2bb443b7dddcde8f936b0d69e946692"
CAYMAN_PYTHON_BUILDTYPE = "release"
CAYMAN_PYTHON_HOSTTYPES = {
    LINUX_HOSTTYPE: "linux64"
}

CAYMAN_OPENSSL_BRANCH = "vmware-master"
CAYMAN_OPENSSL_CLN = "4bef6fc6dc831a68256eb3051eae39071f1080c3"
CAYMAN_OPENSSL_BUILDTYPE = "release"
CAYMAN_OPENSSL_HOSTTYPES = {
    LINUX_HOSTTYPE: "linux64",
}

CSC_PHOTON_BRANCH = "photon3-vmw-updates"
CSC_PHOTON_CLN = "11621485"
CSC_PHOTON_BUILDTYPE = 'release'
CSC_PHOTON_FILES = {
    LINUX_HOSTTYPE: ["publish/docker-image/photon-rootfs.tar.gz",
                     "publish/csc-photon-3.0.0-x86_64.iso"]
}

CAYMAN_CNI_PLUGINS_BRANCH = "470990532345124695-v1.1.1+vmware.25-cni_plugins"
CAYMAN_CNI_PLUGINS_CLN = "9dc24376db61c42f1231df7e5951fe21e3794b0a"
CAYMAN_CNI_PLUGINS_BUILDTYPE = 'release'
CAYMAN_CNI_PLUGINS_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/cni_plugins/executables/.*"]
}

CAYMAN_GO_BRANCH = "vmware-go1.19-boringcrypto"
CAYMAN_GO_CLN = "ad8702a3a2ff8a70987480a4ef4d5928cd891a6b"
CAYMAN_GO_BUILDTYPE = "release"
CAYMAN_GO_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/bin/.*",
        "publish/lin64/src/.*",
        "publish/lin64/pkg/.*"]
}

NSX_OVS_BUILD_BRANCH = "nsx-jetfire"
NSX_OVS_BUILD_CLN = "5be2f79b6a97887c253a675854115482e4cf53ae"
NSX_OVS_BUILD_BUILDTYPE = "release"
NSX_OVS_BUILD_FILES = {
    LINUX_HOSTTYPE: [
        "publish/windows_x64/.*"]
}

CAYMAN_WHEREABOUTS_BRANCH = "vmware-0.6.1"
CAYMAN_WHEREABOUTS_CLN = "0f015c1203971ff051359f9a23c8e1dff79ee8a3"
CAYMAN_WHEREABOUTS_BUILDTYPE = "release"
CAYMAN_WHEREABOUTS_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/whereabouts/executables/.*",
        "publish/lin64/whereabouts/images/.*"]
}

CAYMAN_HELM_BRANCH = "vmware-3.12.3"
CAYMAN_HELM_CLN = "e77bc98b8823e23edc4268e542efec1cf6bd11b1"
CAYMAN_HELM_BUILDTYPE = "release"
CAYMAN_HELM_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/.*"]
}

CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_BRANCH = "vmware-master"
CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_CLN = "2c005cd97a5f9bce23fcab61f24f4cca1b1d64ec"
CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_BUILDTYPE = "release"
CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/*."]
}
