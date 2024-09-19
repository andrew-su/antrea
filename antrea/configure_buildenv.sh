#!/bin/bash

set -e

# Disable default repos since they are still using vmware.com artifactory
# which will cause package installation failure when LVN simulator is enabled.
sudo /usr/bin/sed -i -e 's/^enabled=.*/enabled=0/g' /etc/yum.repos.d/*.repo

# This repository information should be updated accordingly whenever
# the LINUX_HOSTTYPE is changed. It's linux-rocky8-vm-fw for now.
sudo /bin/cat >> /tmp/CentOS.repo << EOF
[AppStream]
name=CentOS-8 - AppStream
baseurl=https://packages.vcfd.broadcom.net/artifactory/centos-remote/8/AppStream/x86_64/os/
gpgcheck=0
enabled=1

[BaseOS]
name=CentOS-8 - BaseOS
baseurl=https://packages.vcfd.broadcom.net/artifactory/centos-remote/8/BaseOS/x86_64/os/
gpgcheck=0
enabled=1
EOF

sudo /bin/mv /tmp/CentOS.repo /etc/yum.repos.d/CentOS.repo

# Installing jq
sudo yum install -y jq

echo "Configuring Docker..."
sudo /usr/sbin/usermod -aG docker mts

DOCKER_STORAGE_DIR="${PWD}/docker"
mkdir -m777 -p ${DOCKER_STORAGE_DIR}

DOCKER_STORAGE_CFG_PATH=/etc/sysconfig/docker-storage-setup
echo "STORAGE_DRIVER=overlay" | \
    sudo /usr/bin/tee -a ${DOCKER_STORAGE_CFG_PATH}
echo "EXTRA_DOCKER_STORAGE_OPTIONS='-g ${DOCKER_STORAGE_DIR}'" | \
    sudo /usr/bin/tee -a ${DOCKER_STORAGE_CFG_PATH}

sudo /usr/bin/systemctl daemon-reload
sudo /usr/bin/systemctl restart docker
sudo /usr/bin/chown mts /var/run/docker.sock
sudo /usr/sbin/iptables -I FORWARD -j ACCEPT
sudo /usr/sbin/ip link set docker0 promisc on
sudo /usr/bin/systemctl status docker.service
