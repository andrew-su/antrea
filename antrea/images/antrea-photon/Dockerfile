FROM scratch as cni-binaries

ADD photon-rootfs.tar.gz /
RUN rm -rf /etc/yum.repos.d/*.repo
COPY photon-iso.repo /etc/yum.repos.d/

RUN tdnf install -y wget ca-certificates tar gzip

# Leading dot is required for the tar command below
ENV CNI_PLUGINS="./host-local ./loopback ./portmap"

RUN mkdir -p /opt/cni/bin && \
    wget -q -O - https://dl.k8s.io/network-plugins/cni-plugins-amd64-v0.7.5.tgz | tar xz -C /opt/cni/bin $CNI_PLUGINS


FROM antrea/openvswitch-photon

LABEL maintainer="Antrea <projectantrea-dev@googlegroups.com>"
LABEL description="A Docker image to deploy the Antrea CNI. Requires the Antrea binaries to be built prior to building the image."

RUN tdnf remove -y toybox && \
    tdnf install -y \
      coreutils \
      iproute2 \
      ipset \
      sed \
      tar \
      util-linux

COPY --from=cni-binaries /opt/cni/bin /opt/cni/bin

COPY build/images/scripts/* /usr/local/bin/
COPY bin/* /usr/local/bin/

USER root
