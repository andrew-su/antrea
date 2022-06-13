LINUX_HOSTTYPE = 'linux-centos72-gc32'

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
CSC_PHOTON_CLN = "9313915"
CSC_PHOTON_BUILDTYPE = 'release'
CSC_PHOTON_FILES = {
    LINUX_HOSTTYPE: ["publish/docker-image/photon-rootfs.tar.gz",
                     "publish/csc-photon-3.0.0-x86_64.iso"]
}

CAYMAN_CNI_PLUGINS_BRANCH = "vmware-0.8.7"
CAYMAN_CNI_PLUGINS_CLN = "c13c3e2947188c3f48a0c0f1d9ca1618e50d2ea6"
CAYMAN_CNI_PLUGINS_BUILDTYPE = 'release'
CAYMAN_CNI_PLUGINS_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/cni_plugins/executables/.*"]
}

CAYMAN_GO_BRANCH = "vmware-go1.17-boringcrypto"
CAYMAN_GO_CLN = "11a0a36db6b4aa789ad9c07ca9ca2a1850f29839"
CAYMAN_GO_BUILDTYPE = "release"
CAYMAN_GO_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/bin/.*",
        "publish/lin64/src/.*",
        "publish/lin64/pkg/.*"]
}

NSX_OVS_BUILD_BRANCH = "nsx-impactor-ufo"
NSX_OVS_BUILD_CLN = "551c5ff38818ffa3f9c702984682b701190f81b8"
NSX_OVS_BUILD_BUILDTYPE = "release"
NSX_OVS_BUILD_FILES = {
    LINUX_HOSTTYPE: [
        "publish/windows_x64/.*"]
}
