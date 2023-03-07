LINUX_HOSTTYPE = 'linux-centos8'

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
CSC_PHOTON_CLN = "10662627"
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

CAYMAN_GO_BRANCH = "vmware-go1.19-boringcrypto"
CAYMAN_GO_CLN = "4e80abb739129ed17b478d54b0c6b67838b6a28c"
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

CAYMAN_WHEREABOUTS_BRANCH = "vmware-0.5.4"
CAYMAN_WHEREABOUTS_CLN = "d69a3940c9c333862328165ef6ba71ef1385932e"
CAYMAN_WHEREABOUTS_BUILDTYPE = "release"
CAYMAN_WHEREABOUTS_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/whereabouts/images/whereabouts-v0.5.4_vmware.1.tar.gz"]
}
