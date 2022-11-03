#!/usr/bin/env bash

# ******************************************************************************
# Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
# ******************************************************************************

set -eo pipefail

function echoerr {
    >&2 echo "$@"
}

_usage="Usage: $0 [--mode (dev|release)] --out <DIR>
Generate standard YAML manifests for Antrea IDPS using Helm and writes them to output directory.
        --mode (dev|release)          Choose the configuration variant that you need (default is 'dev')
        --out <DIR>                   Output directory for generated manifetss
        --help, -h                    Print this message and exit

In 'release' mode, environment variables IMG_NAME and IMG_TAG must be set.

In 'dev' mode, environment variable IMG_NAME can be set to use a custom image.

This tool uses Helm 3 (https://helm.sh/) to generate the \"standard\" manifests for Antrea. These
are the manifests that are checked-in into the Antrea source tree, and that are uploaded as release
assets for each new Antrea release. This script looks for all the Helm values YAML files under
/build/yamls/chart-values/, and generates the corresponding manifest for each one.

You can set the HELM environment variable to the path of the helm binary you wan t us to
use. Otherwise we will download the appropriate version of the helm binary and use it (this is the
recommended approach since different versions of helm may create different output YAMLs)."

function print_usage {
    echoerr "$_usage"
}

function print_help {
    echoerr "Try '$0 --help' for more information."
}

MODE="dev"
OUTPUT_DIR=""

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --mode)
    MODE="$2"
    shift 2
    ;;
    --out)
    OUTPUT_DIR="$2"
    shift 2
    ;;
    -h|--help)
    print_usage
    exit 0
    ;;
    *)    # unknown option
    echoerr "Unknown option $1"
    exit 1
    ;;
esac
done

if [ "$MODE" != "dev" ] && [ "$MODE" != "release" ]; then
    echoerr "--mode must be one of 'dev' or 'release'"
    print_help
    exit 1
fi

if [ "$MODE" == "release" ] && [ -z "$SURICATA_IMG_NAME" ]; then
    echoerr "In 'release' mode, environment variable SURICATA_IMG_NAME must be set"
    print_help
    exit 1
fi

if [ "$MODE" == "release" ] && [ -z "$IDPS_IMG_NAME" ]; then
    echoerr "In 'release' mode, environment variable IDPS_IMG_NAME must be set"
    print_help
    exit 1
fi

if [ "$MODE" == "release" ] && [ -z "$IMG_TAG" ]; then
    echoerr "In 'release' mode, environment variable IMG_TAG must be set"
    print_help
    exit 1
fi

if [ "$OUTPUT_DIR" == "" ]; then
    echoerr "--out is required to provide output directory for generated manifests"
    print_help
    exit 1
fi

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source $THIS_DIR/verify-helm.sh

if [ -z "$HELM" ]; then
    HELM="$(verify_helm)"
elif ! $HELM version > /dev/null 2>&1; then
    echoerr "$HELM does not appear to be a valid helm binary"
    print_help
    exit 1
fi

EXTRA_VALUES=""
if [ "$MODE" == "release" ]; then
    EXTRA_VALUES="--set idpsImage.repository=$IDPS_IMG_NAME,idpsImage.tag=$IMG_TAG,suricataImage.repository=$SURICATA_IMG_NAME,suricataImage.tag=$IMG_TAG"
fi

IDPS_CHART="$THIS_DIR/../build/charts/idps"
VALUES_DIR="$THIS_DIR/../build/charts/idps/chart-values"
VALUES_FILES=$(cd $VALUES_DIR && find * -type f -name "*.yml" )
# Suppress potential Helm warnings about invalid permissions for Kubeconfig file
# by throwing away related warnings.
for values in $VALUES_FILES; do
  $HELM template \
        --namespace kube-system \
        -f "$VALUES_DIR/$values" \
        $EXTRA_VALUES \
        "$IDPS_CHART" \
        > "$OUTPUT_DIR/$values" \
        2> >(grep -v 'This is insecure' >&2)
done