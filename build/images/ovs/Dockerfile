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

# Prerequisite to build this Dockerfile:
# - openvswitch-$OVS_VERSION.tar.gz - archive of the source code of OpenvSwitch.
# IPsec related packages will not be installed by default. Specify `--build-arg IPsec=true` to enable IPsec support.

FROM dockerhub.artifactory.vcfd.broadcom.net/library/buildpack-deps:24.04-curl AS certificate-base

FROM dockerhub.artifactory.vcfd.broadcom.net/ubuntu:24.04 as ovs-ubuntu-debs

# Some patches may not apply cleanly if a non-default version is provided.
# See build/images/deps/ovs-version for the default version.
ARG OVS_VERSION
ARG IPSEC

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

# Install dependencies for building OVS deb packages
# We only install python3 packages and we only support building OVS >= 2.13.0.
RUN apt-get update && \
    DEBIAN_FRONTEND="noninteractive" apt-get install -y --no-install-recommends wget curl git ca-certificates build-essential fakeroot \
            bzip2 autoconf automake debhelper dh-python dh-autoreconf libssl-dev libtool openssl procps \
            python3-all python3-twisted python3-zope.interface python3-sphinx \
            libunbound-dev

COPY apply-patches.sh /
COPY openvswitch-$OVS_VERSION.tar.gz /tmp/

RUN tar -xzf /tmp/openvswitch-$OVS_VERSION.tar.gz -C /tmp && \
    cd /tmp/openvswitch* && \
    /apply-patches.sh && \
    DEB_BUILD_OPTIONS='parallel=8 nocheck' fakeroot debian/rules binary && \
    cd /tmp && mkdir ovs-debs && \
    mv libopenvswitch_*.deb openvswitch-common_*.deb openvswitch-switch_*.deb python*-openvswitch_*.deb ovs-debs/ && \
    if ${IPSEC}; then mv openvswitch-ipsec_*.deb ovs-debs/; fi && \
    cd / && rm -rf /tmp/openvswitch*

FROM dockerhub.artifactory.vcfd.broadcom.net/ubuntu:24.04

LABEL maintainer="Antrea <projectantrea-dev@googlegroups.com>"
LABEL description="A Docker image based on Ubuntu 24.04 which includes Open vSwitch built from source."

ARG IPSEC

COPY --from=certificate-base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN mv /etc/apt/sources.list.d/ubuntu.sources /etc/apt/sources.list.d/ubuntu.sources.bak
COPY --from=ovs-ubuntu-debs /etc/apt/sources.list.d/broadcom.sources /etc/apt/sources.list.d/broadcom.sources

COPY --from=ovs-ubuntu-debs /tmp/ovs-debs/* /tmp/ovs-debs/
COPY charon-logging.conf /tmp

# Install OVS debs, iptables, logrotate, and strongSwan; update the OVS
# logrotate config file; update the strongSwan logging config.
# We clean-up apt cache after installing packages to reduce the size of the
# final image.
RUN apt-get update && \
    # -f / --fix-broken will take care of installing all missing dependencies
    apt-get -f -y --no-install-recommends install /tmp/ovs-debs/*.deb && \
    # make sure that openvswitch packages will not be upgraded by mistake
    # in practice, the packages should not be upgraded anyway since they were installed using a .deb
    apt-mark hold *openvswitch* && \
    apt-get install -y --no-install-recommends iptables logrotate && \
    sed -i "/rotate /a\    #size 100M" /etc/logrotate.d/openvswitch-switch && \
    if ${IPSEC}; then \
        apt-get install -y --no-install-recommends libstrongswan-standard-plugins; \
        sed -i "/^.*filelog.*{/r /tmp/charon-logging.conf" /etc/strongswan.d/charon-logging.conf; \
    fi && \
    rm -rf /var/cache/apt/* /var/lib/apt/lists/* && \
    rm -rf /tmp/*
