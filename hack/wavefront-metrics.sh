#!/usr/bin/env bash

# Copyright 2023 Antrea Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script is a copy of script maintained in
# https://github.com/kubernetes/enhancements

set -eo pipefail

function echoerr {
    >&2 echo "$@"
}

CLUSTER_NAME="default"
WAVEFRONT_URL=""
WAVEFRONT_TOKEN=""
OPERATOR_YAML="https://raw.githubusercontent.com/wavefrontHQ/wavefront-operator-for-kubernetes/main/deploy/kubernetes/wavefront-operator.yaml"

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "$THIS_DIR"

_usage="Usage: $0 [--cluster-name <WavefrontClusterName>] [--wavefront-url <WavefrontURL>] [--wavefront-token <WavefrontToken>]
                  [--wavefront-operator-yaml <WavefrontOperatorYAML>]

Setup metrics monitoring with Wavefront.

        --cluster-name            The cluster name to be used on Wavefront dashboard.
        --wavefront-url           The Wavefront URL to connect to the wavefront and send the data, e.g. https://<WAVEFRONT_URL>.wavefront.com.
                                  Only input the Wavefront URL part.
        --wavefront-token         The Wavefront URL/API access token.
        --wavefront-operator-yaml Link to the YAML file of Wavefront Operator for Kubernetes."

function print_usage {
    echoerr "$_usage"
}

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    --cluster-name)
    CLUSTER_NAME="$2"
    shift 2
    ;;
    --wavefront-url)
    WAVEFRONT_URL="$2"
    shift 2
    ;;
    --wavefront-token)
    WAVEFRONT_TOKEN="$2"
    shift 2
    ;;
    --wavefront-operator-yaml)
    OPERATOR_YAML="$2"
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

function install_wavefront_operator() {
    echo "=== Installing Wavefront Operator for Kubernetes Cluster ==="
    if [[ $(kubectl apply -f ${OPERATOR_YAML}) ]]; then
        echo "=== Wavefront Operator Successfully Deployed ==="
    else
        echoerr "Something went wrong! Please check the Operator YAML and try again."
        exit 1
    fi
}

function create_token() {
    echo "=== Add Wavefront access token to connect to the wavefront API ==="
    if [[ $(kubectl create -n observability-system secret generic wavefront-secret --from-literal token=${WAVEFRONT_TOKEN}) ]]; then
        echo "=== Secret is successfully created ===="
    else
        echoerr "Something went wrong! Please check the Wavefront Token and try again."
        exit 1
    fi
}

function start_wavefront_collector() {
    echo "=== Starting Wavefront Proxy and Wavefront Collectors ==="
    cat << EOF > wavefront.yaml
apiVersion: wavefront.com/v1alpha1
kind: Wavefront
metadata:
 name: wavefront
 namespace: observability-system
spec:
 clusterName: $CLUSTER_NAME
 wavefrontUrl: https://$WAVEFRONT_URL.wavefront.com
 wavefrontTokenSecret: wavefront-secret
 dataExport:
  wavefrontProxy:
   enable: true
   metricPort: 2878
   otlp:
    grpcPort: 4317
    httpPort: 4318
    resourceAttrsOnMetricsIncluded: false
 dataCollection:
  logging:
   enable: true
  metrics:
   enable: true
---
apiVersion: v1
kind: ConfigMap
metadata:
 name: antrea-config
 namespace: observability-system
 annotations:
  wavefront.com/discovery-config: 'true'
data:
 collector.yaml: |
   plugins:
   # auto-discover antrea-controller
   - name: antrea-controller
     type: prometheus
     selectors:
      images:
      - '*antrea*'
     port: 10349
     path: /metrics
     scheme: https
     prefix: antrea.controller.
     conf: |
      bearer_token_file: '/var/run/secrets/kubernetes.io/serviceaccount/token'
      tls_config:
       ca_file: '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'
       insecure_skip_verify: true
   # auto-discover antrea-agent
   - name: antrea-agent
     type: prometheus
     selectors:
      images:
      - '*antrea*'
     port: 10350
     path: /metrics
     scheme: https
     prefix: antrea.agent.
     conf: |
      bearer_token_file: '/var/run/secrets/kubernetes.io/serviceaccount/token'
      tls_config:
       ca_file: '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'
       insecure_skip_verify: true
EOF

    if [[ $(kubectl apply -f "wavefront.yml") ]]; then
        echo "=== Wavefront Collectors and Proxy successfully deployed ==="
    else
        echoerr "Something went wrong! Please check your Wavefront URL and try again."
        exit 1
    fi
}

install_wavefront_operator
create_token
start_wavefront_collector
