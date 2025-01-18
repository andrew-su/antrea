LINUX_HOSTTYPE = 'linux-rocky8-vm-fw'

CAYMAN_BRANCH = 'master'
CAYMAN_CLN = '606f1b0b766042ac1311efa546ffd683671deb7b'
CAYMAN_BUILDTYPE = 'release'
CAYMAN_HOSTTYPES = {
    LINUX_HOSTTYPE: 'linux',
}

CAYMAN_PYTHON_BRANCH = "vmware-python-3.11-openssl-3.0"
CAYMAN_PYTHON_CLN = '3d30db2c3e02eb22d727cb343d4851da3e097f5f'
CAYMAN_PYTHON_BUILDTYPE = "release"
CAYMAN_PYTHON_HOSTTYPES = {
    LINUX_HOSTTYPE: "linux64"
}

CAYMAN_OPENSSL_BRANCH = "3.0-latest"
CAYMAN_OPENSSL_CLN = '2a775410b8855d4019c13cbd95857700d1fd9f21'
CAYMAN_OPENSSL_BUILDTYPE = "release"
CAYMAN_OPENSSL_HOSTTYPES = {
    LINUX_HOSTTYPE: "linux-centos8",
}

CSC_PHOTON_BRANCH = "photon5-vmw-updates"
CSC_PHOTON_CLN = 14919745
CSC_PHOTON_BUILDTYPE = 'release'
CSC_PHOTON_FILES = {
    LINUX_HOSTTYPE: ["publish/docker-image/photon-rootfs.tar.gz",
                     "publish/csc-photon-5.0.0-x86_64.iso"]
}

CAYMAN_CNI_PLUGINS_BRANCH = "release-1.6.0-fips"
CAYMAN_CNI_PLUGINS_CLN = '00d4e6328a55281b03778dbb9b589fa8ee74f4ed'
CAYMAN_CNI_PLUGINS_BUILDTYPE = 'release'
CAYMAN_CNI_PLUGINS_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/cni_plugins/executables/.*"]
}

CAYMAN_GO_BRANCH = "vmware-go1.23-unified"
CAYMAN_GO_CLN = 'd7272c90d3ec15db79f9448f724b6f2741c19b03'
CAYMAN_GO_BUILDTYPE = "release"
CAYMAN_GO_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/amd64/bin/.*",
        "publish/lin64/amd64/src/.*",
        "publish/lin64/amd64/pkg/.*",
        "publish/lin64/amd64/go.env",
        "publish/lin64/amd64/ubuntu-docker-image-amd64.tar"]
}

NSX_OVS_BUILD_BRANCH = "nsx-magnus"
NSX_OVS_BUILD_CLN = "65e2b3556aeff10e3bb109722c909b750e107635"
NSX_OVS_BUILD_BUILDTYPE = "release"
NSX_OVS_BUILD_FILES = {
    LINUX_HOSTTYPE: [
        "publish/windows_x64/.*"]
}

# helm without the kubeVersion<=1.20 limitation
CAYMAN_HELM_BRANCH = "vmware-3.15.2-antrea"
CAYMAN_HELM_CLN = '27dda691b0bd65f494ac1ccf675ae2ad542df468'
CAYMAN_HELM_BUILDTYPE = "release"
CAYMAN_HELM_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/.*"]
}

CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_BRANCH = "vmware-master"
CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_CLN = "c575f3c013f289b67e0002e9f993bb9c9482ad19"
CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_BUILDTYPE = "release"
CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/*."]
}

CAYMAN_SURICATA_BRANCH = "vmware-7.0.6"
CAYMAN_SURICATA_CLN = "5c2f05ea56b8049b7be039d90c2d552f65adf778"
CAYMAN_SURICATA_BUILDTYPE = "release"
CAYMAN_SURICATA_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/.*"]
}

CAYMAN_MSVC_REDISTS_BRANCH = "vc140-redists-latest"
CAYMAN_MSVC_REDISTS_CLN = '975c0ba85ee95c1a22a357d8ad5424cbd2537cf4'
CAYMAN_MSVC_REDISTS_BUILDTYPE = "release"
CAYMAN_MSVC_REDISTS_FILES = {
    LINUX_HOSTTYPE: [
        "publish/win/.*"]
}

CAYMAN_IMGPKG_BRANCH = 'release-0.42.3'
CAYMAN_IMGPKG_CLN = 'cf85eccf0f07c736c49e5a6b063adcc4c505fe44'
CAYMAN_IMGPKG_BUILDTYPE = 'release'
CAYMAN_IMGPKG_FILES = {
    LINUX_HOSTTYPE: [ r'imgpkg-linux-amd64-.*.gz$' ],
}

CAYMAN_K14S_YTT_BRANCH = 'vmware-0.46.3+vmware.2'
CAYMAN_K14S_YTT_CLN = 'f6b7ec1f0cff5155091e31ab03df80ef9044fab5'
CAYMAN_K14S_YTT_BUILDTYPE = 'release'
CAYMAN_K14S_YTT_FILES = {
    LINUX_HOSTTYPE: [ r'ytt-linux-amd64-.*.gz$' ],
}

CAYMAN_KBLD_BRANCH = 'vmware-0.38.2+vmware.2'
CAYMAN_KBLD_CLN = '3a588177f419b56b57520bac1217e1facc4087aa'
CAYMAN_KBLD_BUILDTYPE = 'release'
CAYMAN_KBLD_FILES = {
    LINUX_HOSTTYPE: [ r'kbld-linux-amd64-.*.gz$' ],
}


CAYMAN_ANTREA_TKGM_ADVANCED_BRANCH = 'vmware-2.2.0+vmware.2'
CAYMAN_ANTREA_TKGM_ADVANCED_CLN = 'b40e0fb3384d6375c11a96443d0f6652ac2a4ed8'
CAYMAN_ANTREA_TKGM_ADVANCED_BUILDTYPE = 'release'
CAYMAN_ANTREA_TKGM_ADVANCED_FILES = {
    LINUX_HOSTTYPE: [
        "publish/.*"
    ]
}

ANTREA_INTERWORKING_BRANCH = 'release-1.2'
ANTREA_INTERWORKING_CLN = 'eb14e9fc42422b737e244c8439eeae1e2d7f0a77'
ANTREA_INTERWORKING_BUILDTYPE = 'release'
ANTREA_INTERWORKING_FILES = {
    LINUX_HOSTTYPE: [
        "publish/antrea-interworking/.*",
        "publish/VERSION"
    ]
}
