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

DOCKER_TOOL_BRANCH = "master"
DOCKER_TOOL_CLN = '90b11ee5925ca2cd3d7be653449d62626adb492c'
DOCKER_TOOL_BUILDTYPE = 'release'
DOCKER_TOOL_HOSTTYPES = {
    LINUX_HOSTTYPE: 'linux-centos8',
}

NSBU_DOCKER_IMAGES_BRANCH = "nsx-highline"
NSBU_DOCKER_IMAGES_CLN = "9e572766d42429646c3a1f7ad78f9288593406f7"
NSBU_DOCKER_IMAGES_BUILDTYPE = 'release'
NSBU_DOCKER_IMAGES_FILES = {
    LINUX_HOSTTYPE: [r"publish/docker-images/ubuntu16.04-alias/.*",
                     r"publish/docker-images/blobs/.*"]
}

NSBU_REPOS_BRANCH = "nsx-highline"
NSBU_REPOS_CLN = "37582b3c8e0c7d7e17cbfa98cd2c95843a56590f"
NSBU_REPOS_BUILDTYPE = 'release'
NSBU_REPOS_FILES = {
    LINUX_HOSTTYPE: [r'publish/default/.*']
}

CSC_PHOTON_BRANCH = "photon3-vmw-updates"
CSC_PHOTON_CLN = "7999124"
CSC_PHOTON_BUILDTYPE = 'release'
CSC_PHOTON_FILES = {
    LINUX_HOSTTYPE: ["publish/docker-image/photon-rootfs.tar.gz",
                     "publish/csc-photon-3.0.0-x86_64.iso"]
}

CAYMAN_CNI_PLUGINS_BRANCH = "vmware-0.7.5+vmware.6"
CAYMAN_CNI_PLUGINS_CLN = "262b0fd5b3518bad0d96e4f3af28ac3b1ce44e0d"
CAYMAN_CNI_PLUGINS_BUILDTYPE = 'release'
CAYMAN_CNI_PLUGINS_FILES = {
    LINUX_HOSTTYPE: [
        "publish/lin64/cni_plugins/executables/.*"]
}
