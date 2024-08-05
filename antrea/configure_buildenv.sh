#!/bin/bash

set -e

sudo yum install -y yum-utils

function add_to_yum {
    local artifactory_base="https://build-artifactory.eng.vmware.com/artifactory/"
    local repo_path="${1}"
    sudo /usr/bin/yum-config-manager --add-repo $artifactory_base$repo_path;
}

echo "Added yum repos from artifactory."
for repo_type in AppStream BaseOS extras;
do
    add_to_yum centos-remote/8/$repo_type/x86_64/os
done

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



# This step is required on firewalled machines, since host to container traffic is blocked by default
echo "Enabling firewall rule to allow traffic from host to docker bridge network"
bridge_net=$(sudo /usr/bin/docker network inspect --format '{{range .IPAM.Config}}{{.Subnet}}{{end}}' bridge)
sudo /usr/sbin/iptables -A BUILD_OUT -d ${bridge_net} -m comment --comment "allow traffic from host to docker bridge network" -j ACCEPT

