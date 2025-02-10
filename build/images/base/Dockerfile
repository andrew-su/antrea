# Copyright 2022 Antrea Authors
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

FROM dockerhub.artifactory.vcfd.broadcom.net/library/buildpack-deps:24.04-curl AS certificate-base

FROM dockerhub.artifactory.vcfd.broadcom.net/ubuntu:24.04 as cni-binaries

ARG CNI_BINARIES_VERSION
ARG DOWNLOAD_CNI_BINARIES=false

COPY cni-plugins-*.tgz /tmp

RUN mkdir -p /usr/local/share/ca-certificates
RUN mkdir -p /etc/ssl/certs
COPY --from=certificate-base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

RUN mv /etc/apt/sources.list.d/ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources.bak

COPY <<EOF /etc/apt/sources.list.d/broadcom.sources
Types: deb
URIs: https://packages.vcfd.broadcom.net/ubuntu-remote/ubuntu/
Suites: noble noble-updates noble-backports
Components: main universe restricted multiverse
Trusted: yes
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg

Types: deb
URIs: https://packages.vcfd.broadcom.net/ubuntu-remote/ubuntu/
Suites: noble-security
Components: main universe restricted multiverse
Trusted: yes
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
EOF

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget ca-certificates && \
    rm -rf /var/cache/apt/* /var/lib/apt/lists/*

# Leading dot is required for the tar command below
ENV CNI_PLUGINS="./host-local ./loopback ./portmap ./bandwidth"

# Download containernetworking plugin binaries for the correct architecture
RUN set -eux; \
    mkdir -p /opt/cni/bin; \
    if ${DOWNLOAD_CNI_BINARIES}; then \
        dpkgArch="$(dpkg --print-architecture)"; \
        case "${dpkgArch##*-}" in \
            amd64) pluginsArch='amd64' ;; \
        armhf) pluginsArch='arm' ;; \
        arm64) pluginsArch='arm64' ;; \
            *) pluginsArch=''; echo >&2; echo >&2 "unsupported architecture '$dpkgArch'"; echo >&2 ; exit 1 ;; \
        esac; \
        wget --no-check-certificate -q -O - https://github.com/containernetworking/plugins/releases/download/$CNI_BINARIES_VERSION/cni-plugins-linux-${pluginsArch}-$CNI_BINARIES_VERSION.tgz | tar xz -C /opt/cni/bin $CNI_PLUGINS; \
    else \
        tar -zxvf /tmp/cni-plugins-*.tgz -C /opt/cni/bin $CNI_PLUGINS; \
    fi

FROM antrea-openvswitch

ARG SURICATA_VERSION
ARG INSTALL_SURICATA_FROM_PACKAGE=false

LABEL maintainer="Antrea <projectantrea-dev@googlegroups.com>"
LABEL description="An Ubuntu based Docker base image for Antrea."

USER root

COPY iptables-wrapper-installer.sh /iptables-wrapper-installer.sh
COPY suricata*.deb /tmp

RUN apt-get update && apt-get install -y --no-install-recommends wget ipset jq iproute2 inotify-tools gpg-agent software-properties-common && \
    # ethtool is for commercial release Antrea to toggle Geneve checksum offload.
    apt-get install -y --no-install-recommends ethtool && \
    if ${INSTALL_SURICATA_FROM_PACKAGE}; then \
        apt install -y /tmp/suricata*.deb --no-install-recommends && \
        apt-mark hold suricata; \
    else \
        SURICATA_VERSION=$(echo ${SURICATA_VERSION} | cut -d'.' -f1-2); \
        add-apt-repository ppa:oisf/suricata-${SURICATA_VERSION} && apt-get update && apt-get install -y suricata; \
    fi && \
    apt-get remove -y gpg-agent software-properties-common && apt-get autoremove -y && rm -rf /var/cache/apt/* /var/lib/apt/lists/* && \
    /iptables-wrapper-installer.sh

COPY --from=cni-binaries /opt/cni/bin /opt/cni/bin
