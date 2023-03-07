#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

# Contents below are copied from
# cayman_calico_all/calico_all/install_docker.sh

# Copyright (C) 2019 VMware, Inc. All rights reserved.

# This script is used to prepare gobuild slave using
# template linux-centos72-gc32 to install docker.

# This is a stop gap measure as mentioned in
# this bug: http://go/docker-on-build

# A more enhanced and easy to use functionality is being
# worked on through docker project type in Gobuild
# YAML interface. Read more here: http://go/docker-uru-yaml

yum install -y yum-utils device-mapper-persistent-data lvm2

yum remove -y docker docker-client docker-client-latest docker-common docker-latest docker-latest-logrotate docker-logrotate docker-selinux  docker-engine-selinux docker-engine
echo "Removed old docker"

#cat /etc/yum.repos.d/centos-internal.repo
#cat /etc/yum.repos.d/rdops.repo
rm -rf /etc/yum.repos.d/*
echo "Removed centos-internal and RDOPS Repos"

function add_to_yum {
    local artifactory_base="https://build-artifactory.eng.vmware.com/artifactory/"
    local repo_path="${1}"
    yum-config-manager --add-repo $artifactory_base$repo_path;
}

echo "Installing docker-ce dependencies."
rpm --import https://build-artifactory.eng.vmware.com/artifactory/download.docker.com/linux/centos/gpg
echo "Imported Artifactory GPG key"

add_to_yum "download.docker.com/linux/centos/7/x86_64/stable"

add_to_yum centos-remote/8/BaseOS/x86_64/os
add_to_yum centos-remote/8/extras/x86_64/os
add_to_yum centos-remote/8/infra/x86_64/buildtools-common/
add_to_yum centos-remote/8/infra/x86_64/gitforge-pagure/
add_to_yum centos-remote/8/infra/x86_64/infra-common/
add_to_yum centos-remote/8/PowerTools/x86_64/os/
add_to_yum centos-remote/8/AppStream/x86_64/os/

echo "Added yum repos from artifactory."

# Installing docker
# Newer docker version + containerd version doesn't work in build slave
yum install -y https://artifactory.eng.vmware.com/artifactory/download.docker.com/linux/centos/7/x86_64/stable/Packages/containerd.io-1.6.9-3.1.el7.x86_64.rpm
yum install -y docker-ce-20.10.9 docker-ce-cli-20.10.9
#groupadd docker
/usr/sbin/usermod -aG docker mts
/usr/sbin/service docker restart
echo "Installing Docker Version 20.10.9 complete... now fixing storage situation...."
# Printing docker info
#echo "Docker Version"
#docker version
#echo "Docker Info"
#docker info
# The recommended docker driver is overlay
# The default storage directory for docker on the build vm is /var/lib/docker
# which is on the root partition. Since the storage on this partition is limited
# we move the storage directory under ${BUILDROOT} which is 100G in total space.
DOCKER_STORAGE_DIR="${BUILDROOT}/docker"
mkdir -m777 -p ${DOCKER_STORAGE_DIR}
# TODO: do not edit docker.service with sed
sed -i 's#ExecStart=/usr/bin/dockerd#ExecStart=/usr/bin/dockerd --data-root '"$DOCKER_STORAGE_DIR"' --icc --ip-forward --ip-masq --iptables#g' /lib/systemd/system/docker.service
cat /lib/systemd/system/docker.service
chown mts:docker /var/run/docker.sock
#chown -R mts:docker /var/lib/docker
systemctl daemon-reload
systemctl restart docker
ls -a /build/mts/docker
systemctl status docker.service
echo {} > /etc/docker/daemon.json
journalctl -xe -u docker.service
sysctl net.ipv4.conf.all.forwarding=1
sysctl net.ipv4.conf.docker0.forwarding=1
sysctl net.ipv4.conf.default.forwarding=1
iptables -I FORWARD -j ACCEPT
ip link set docker0 promisc on
echo "*********"
echo "*********"
echo "*********"
echo "*********"

echo "docker debug info"

docker info

echo "*********"
echo "*********"
echo "*********"
echo "*********"
