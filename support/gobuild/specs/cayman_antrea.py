LINUX_HOSTTYPE = 'linux-centos8-fw'

CAYMAN_BRANCH = 'master'
CAYMAN_CLN = '71c887b9ee67bd2298b85464ff14401b978ff885'
CAYMAN_BUILDTYPE = 'release'
CAYMAN_HOSTTYPES = {
    LINUX_HOSTTYPE: 'linux',
}

CAYMAN_PYTHON_BRANCH = "vmware-python-3.11-openssl-3.0"
CAYMAN_PYTHON_CLN = "41dbc9b06fb1251d6484c883c8a0d53360adb139"
CAYMAN_PYTHON_BUILDTYPE = "release"
CAYMAN_PYTHON_HOSTTYPES = {
    LINUX_HOSTTYPE: "linux64"
}

CAYMAN_OPENSSL_BRANCH = "3.0-latest"
CAYMAN_OPENSSL_CLN = "844976217bcfe9f4616483cc87cc205f6ebc676f"
CAYMAN_OPENSSL_BUILDTYPE = "release"
CAYMAN_OPENSSL_HOSTTYPES = {
    LINUX_HOSTTYPE: "linux-centos8",
}

CSC_PHOTON_BRANCH = "photon5-vmw-updates"
CSC_PHOTON_CLN = "13796192"
CSC_PHOTON_BUILDTYPE = 'release'
CSC_PHOTON_FILES = {
    LINUX_HOSTTYPE: ["publish/docker-image/photon-rootfs.tar.gz",
                     "publish/csc-photon-5.0.0-x86_64.iso"]
}

CAYMAN_CNI_PLUGINS_BRANCH = "501583962099980330-v1.3.0+vmware.5-fips.1-cni_plugins"
CAYMAN_CNI_PLUGINS_CLN = "c7078b124bd43a017e8f1d3c9cb86ba5dbf016a5"
CAYMAN_CNI_PLUGINS_BUILDTYPE = 'release'
CAYMAN_CNI_PLUGINS_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/cni_plugins/executables/.*"]
}

CAYMAN_GO_BRANCH = "vmware-go1.21-boringcrypto"
CAYMAN_GO_CLN = "a7036facf2603cda735da92456877af62232e8b3"
CAYMAN_GO_BUILDTYPE = "release"
CAYMAN_GO_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/bin/.*",
        "publish/lin64/src/.*",
        "publish/lin64/pkg/.*",
        "publish/lin64/go.env"]
}

NSX_OVS_BUILD_BRANCH = "nsx-keeper-412-rel"
NSX_OVS_BUILD_CLN = "502c8f19b6b45ed3d500913b053d48e8ff311b5d"
NSX_OVS_BUILD_BUILDTYPE = "release"
NSX_OVS_BUILD_FILES = {
    LINUX_HOSTTYPE: [
        "publish/windows_x64/.*"]
}

# helm without the kubeVersion<=1.20 limitation
CAYMAN_HELM_BRANCH = "vmware-3.12.3-antrea"
CAYMAN_HELM_CLN = "bcc709d6441aa2fbd3b9abaa9f8e56a68a96f3b0"
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

CAYMAN_SURICATA_BRANCH = "vmware-6.0.13"
CAYMAN_SURICATA_CLN = "047291b652ff2ff52e9acf5f5960279d4089fc24"
CAYMAN_SURICATA_BUILDTYPE = "release"
CAYMAN_SURICATA_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/.*"]
}

CAYMAN_MSVC_REDISTS_BRANCH = "vc140-redists-latest"
CAYMAN_MSVC_REDISTS_CLN = "b73a5666f16de7000dee91f701ffcd8efbbd035c"
CAYMAN_MSVC_REDISTS_BUILDTYPE = "release"
CAYMAN_MSVC_REDISTS_FILES = {
    LINUX_HOSTTYPE: [
        "publish/win/.*"]
}
