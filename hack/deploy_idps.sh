#!/usr/bin/env bash

# ******************************************************************************
# Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
# ******************************************************************************

set -eo pipefail

IDPS_BUILD=""
NSX_LICENSE=""
DEPLOY_TYPE=""
DEPLOY_USER="k8s-docker"
CTR=""

DEFAULT_WORKDIR="./"
DEFAULT_KUBECONFIG_PATH="$HOME/.kube/config"
WORKDIR=$DEFAULT_WORKDIR
KUBECONFIG_PATH=$DEFAULT_KUBECONFIG_PATH

_usage="USAGE: $0 [--idps-build (ob-xxxx|dev)] [--nsx-license (NSX license)] [--kubeconfig (KubeconfigSavePath)]
                  [--workdir (HomePath)] [--deploy-type (tkgm|k8s-containerd|k8s-docker)] [--deploy-user (username)]
        --idps-build             For 'ob-xxxx', download images and manifests from build web; For 'dev', generate images and manifests from local.
        --nsx-license            The NSX license to be used for authenticating NTICS.
        --kubeconfig             Path of cluster kubeconfig.
        --workdir                Work directory.
        --deploy-type            Type of environment to deploy Antrea IDPS.
        --deploy-user            User to deploy Antrea IDPS."

function print_usage {
    echoerr "$_usage"
}

function echoerr {
    >&2 echo "$@"
}

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --idps-build)
    IDPS_BUILD="$2"
    shift 2
    ;;
    --nsx-license)
    NSX_LICENSE="$2"
    shift 2
    ;;
    --kubeconfig)
    KUBECONFIG_PATH="$2"
    shift 2
    ;;
    --workdir)
    WORKDIR="$2"
    shift 2
    ;;
    --deploy-type)
    DEPLOY_TYPE="$2"
    shift 2
    ;;
    --deploy-user)
    DEPLOY_USER="$2"
    shift 2
    ;;
    -h|--help)
    print_usage
    exit 0
    ;;
    *) # unknown option
    echoerr "Unknown option $1, please run deploy_idps.sh -h to see what options are supported"
    exit 1
    ;;
esac
done

if [[ "${IDPS_BUILD}" == "" ]] || [[ "${NSX_LICENSE}" == "" ]]; then
    echoerr "--idps-build and --nsx-license are both required."
    print_usage
    exit 1
fi

export KUBECONFIG=${KUBECONFIG_PATH}

if [[ "${DEPLOY_TYPE}" == "tkgm" ]]; then
    CTR="containerd"
    if [[ "${DEPLOY_USER}" == "" ]]; then
       DEPLOY_USER="capv"
    fi
elif [[ "${DEPLOY_TYPE}" == "k8s-containerd" ]]; then
    CTR="containerd"
    if [[ "${DEPLOY_USER}" == "" ]]; then
        DEPLOY_USER="jenkins"
    fi
elif [[ "${DEPLOY_TYPE}" == "k8s-docker" ]] || [[ "${DEPLOY_TYPE}" == "" ]]; then
    CTR="docker"
    if [[ "${DEPLOY_USER}" == "" ]]; then
        DEPLOY_USER="ubuntu"
    fi
fi

IDPS_BUILD_KIND="$(echo "${IDPS_BUILD}" | cut -d - -f 1)"
IDPS_BUILD_NUMBER="$(echo "${IDPS_BUILD}" | cut -d - -f 2)"
IDPS_VERSION=""

if [[ ${IDPS_BUILD_KIND} == "ob" ]] || [[ ${IDPS_BUILD_KIND} == "sb" ]] ;then
    IDPS_BUILD_KIND="bora"
    IDPS_VERSION_STR=$(curl -s http://build-squid.vcfd.broadcom.net/build/mts/release/"${IDPS_BUILD_KIND}"-"${IDPS_BUILD_NUMBER}"/publish/VERSION)
    IDPS_VERSION_PREFIX=$(echo "${IDPS_VERSION_STR}" | cut -d '_' -f 1)
    IDPS_VERSION_SUFFIX=$(echo "${IDPS_VERSION_STR}" | cut -d '_' -f 2)
    IDPS_VERSION="${IDPS_VERSION_PREFIX=$}+${IDPS_VERSION_SUFFIX}"
elif [[ ${IDPS_BUILD_KIND} == "dev" ]];then
    IDPS_VERSION="dev"
else
    echoerr "Unknown build kind ${IDPS_BUILD}"
    exit 1
fi

if [[ ${IDPS_BUILD_KIND} == "dev" ]];then
    echo "======== Generating Antrea IDPS images and manifests ========"
    make idps-image suricata-image

    mkdir -p "${WORKDIR}"/idps/images
    docker save -o "${WORKDIR}"/idps/images/idps.tar projects.packages.broadcom.com/antreainterworking/idps:latest
    docker save -o "${WORKDIR}"/idps/images/suricata.tar projects.packages.broadcom.com/antreainterworking/suricata:latest
    cp ./build/yamls/idps.yml "${WORKDIR}"/idps.yml
else
    echo "======== Downloading Antrea IDPS images and manifests ========"
    mkdir -p "${WORKDIR}"/idps/images
    cd "${WORKDIR}"/idps
    wget http://build-squid.vcfd.broadcom.net/build/mts/release/"${IDPS_BUILD_KIND}"-"${IDPS_BUILD_NUMBER}"/publish/antrea-idps-debian-"${IDPS_VERSION#"v"}".zip -O antrea-idps-debian-"${IDPS_VERSION#"v"}".zip

    sudo apt install -y unzip
    unzip -o antrea-idps-debian-"${IDPS_VERSION#"v"}".zip
    cp antrea-idps-debian-"${IDPS_VERSION#"v"}"/manifests/idps.yml "${WORKDIR}"/idps.yml
    cp antrea-idps-debian-"${IDPS_VERSION#"v"}"/images/antrea-*.tar.gz ./images
    rm -rf antrea-idps-debian-"${IDPS_VERSION#"v"}"
    rm -rf antrea-idps-debian-"${IDPS_VERSION#"v"}".zip
    gzip -d ./images/antrea-*.tar.gz
fi

echo "======== Syncing Antrea IDPS images to all Nodes ========"
NODE_IPS=$(kubectl get node -owide --no-headers | awk '{print $6}')
IMAGES=$(ls "${WORKDIR}"/idps/images)

for NODE_IP in ${NODE_IPS};
do
    echo "Cleaning stale images to Node ${NODE_IP}"
    if [[ ${CTR} == "docker" ]]; then
        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "${DEPLOY_USER}@${NODE_IP}" "sudo docker system prune --force --all --filter until=48h"
    elif [[ ${CTR} == "containerd" ]]; then
        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "${DEPLOY_USER}@${NODE_IP}" "sudo crictl rmi --prune"
    fi
    echo "Syncing images to Node ${NODE_IP}"
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r "${WORKDIR}"/idps/images "${DEPLOY_USER}@${NODE_IP}:/tmp/idps";
    for IMAGE in ${IMAGES};
    do
        if [[ ${CTR} == "docker" ]]; then
            ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "${DEPLOY_USER}@${NODE_IP}" "sudo docker load -i /tmp/idps/${IMAGE}"
        elif [[ ${CTR} == "containerd" ]]; then
            ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "${DEPLOY_USER}@${NODE_IP}" "sudo ctr -n k8s.io images import /tmp/idps/${IMAGE}"
        fi
    done
done

echo "======== Applying Antrea IDPS ${IDPS_VERSION} manifest ========"
sed -i "s/.*nsx-license: \"\"/ nsx-license: \"$(echo "${NSX_LICENSE}" | base64 -w 0)\"/" "${WORKDIR}"/idps.yml
kubectl apply -f "${WORKDIR}"/idps.yml

echo "======= Cleanup ========"
rm -rf "${WORKDIR}"/idps

for NODE_IP in ${NODE_IPS};
do
   ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "${DEPLOY_USER}@${NODE_IP}" "rm -rf /tmp/idps"
done

echo "======= Done ========"
