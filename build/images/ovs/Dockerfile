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

# Use a known good image to build the OVS debs to prevent dependency issues. This will not be used in the final image.
FROM nsx-ujo-docker-local.packages.vcfd.broadcom.net/interworking/ubuntu:22.04 as ovs-ubuntu-debs

# Disable SSL peer verification, host verification and allow unauthenticated packages
# to fix build failure. This is usually not recommended, but should be Ok for a trusted
# site from Broadcom.
RUN echo 'Acquire::https::Verify-Peer "false";' > /etc/apt/apt.conf.d/99insecure && \
    echo 'Acquire::https::Verify-Host "false";' >> /etc/apt/apt.conf.d/99insecure && \
    echo 'APT::Get::AllowUnauthenticated "true";' >> /etc/apt/apt.conf.d/99insecure && \
    echo "deb https://packages.vcfd.broadcom.net/ubuntu-remote/ubuntu jammy main restricted universe" > /etc/apt/sources.list && \
    echo "deb https://packages.vcfd.broadcom.net/ubuntu-remote/ubuntu jammy-security main restricted" >> /etc/apt/sources.list && \
    echo "deb https://packages.vcfd.broadcom.net/ubuntu-remote/ubuntu jammy-updates main restricted" >> /etc/apt/sources.list

# Some patches may not apply cleanly if a non-default version is provided.
# See build/images/deps/ovs-version for the default version.
ARG OVS_VERSION
ARG IPSEC

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

FROM nsx-ujo-docker-local.packages.vcfd.broadcom.net/interworking/ubuntu:22.04-adbb9011

LABEL maintainer="Antrea <projectantrea-dev@googlegroups.com>"
LABEL description="A Docker image based on Ubuntu 24.04 which includes Open vSwitch built from source."

ARG IPSEC

# Disable SSL peer verification, host verification and allow unauthenticated packages
# to fix build failure. This is usually not recommended, but should be Ok for a trusted
# site from Broadcom.
RUN echo 'Acquire::https::Verify-Peer "false";' > /etc/apt/apt.conf.d/99insecure && \
    echo 'Acquire::https::Verify-Host "false";' >> /etc/apt/apt.conf.d/99insecure && \
    echo 'APT::Get::AllowUnauthenticated "true";' >> /etc/apt/apt.conf.d/99insecure && \
    echo "deb https://packages.vcfd.broadcom.net/ubuntu-remote/ubuntu jammy main restricted universe" > /etc/apt/sources.list && \
    echo "deb https://packages.vcfd.broadcom.net/ubuntu-remote/ubuntu jammy-security main restricted" >> /etc/apt/sources.list && \
    echo "deb https://packages.vcfd.broadcom.net/ubuntu-remote/ubuntu jammy-updates main restricted" >> /etc/apt/sources.list

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
