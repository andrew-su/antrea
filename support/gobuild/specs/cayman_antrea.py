LINUX_HOSTTYPE = 'linux-centos64-kernel-3.18.21'

CAYMAN_BRANCH = 'master'
CAYMAN_CLN = '71c887b9ee67bd2298b85464ff14401b978ff885'
CAYMAN_BUILDTYPE = 'release'
CAYMAN_HOSTTYPES = {
    LINUX_HOSTTYPE: 'linux',
}

DOCKER_TOOL_BRANCH = "master"
DOCKER_TOOL_CLN = 'de4a43fbaba7db782dee53ec81c2586c5b854df0'
DOCKER_TOOL_BUILDTYPE = 'release'
DOCKER_TOOL_HOSTTYPES = {
    LINUX_HOSTTYPE: 'linux-centos64-kernel-3.18.21',
}
DOCKER_TOOL_FILES = {
    "linux-centos64-kernel-3.18.21": ["publish/.*", "publish/.*/.*"]
}
