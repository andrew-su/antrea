#!/bin/bash

# This script is responsible to do cleanup.
# Commands inside of this script requires sudo permission.

echo "====== Cleanup Docker Storage ======"
DOCKER_STORAGE_DIR="${BUILDROOT}/docker"
sudo systemctl stop docker
sudo /usr/bin/rm -rf ${DOCKER_STORAGE_DIR}

# Try to umount the directoy if it does exist.
if [ -e "/tmp/photo-iso" ]; then
    echo "====== Umount the directory which is used by the local yum repo ======"
    set +e
    sudo /usr/bin/umount /tmp/photo-iso
    set -e
fi
