FROM scratch as cni-binaries
# Photon images are for local testing, they are not consumed by cayman_photon.
# We maintain a dedicated Antrea Dockerfile in cayman_photon. Antrea photon
# image is actually built there.

ADD photon-rootfs.tar.gz /
RUN /usr/bin/sed -i -e 's/^enabled=.*/enabled=0/g' /etc/yum.repos.d/*.repo
COPY photon-iso.repo /etc/yum.repos.d/
RUN tdnf install -y ca-certificates tar gzip

# Leading dot is required for the tar command below
ENV CNI_PLUGINS="./host-local ./loopback ./portmap"
COPY cni-plugins-*.tgz /tmp

RUN mkdir -p /opt/cni/bin && \
    tar -zxvf /tmp/cni-plugins-*.tgz -C /opt/cni/bin $CNI_PLUGINS


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
      util-linux && \
    tdnf clean all && \
    rm -rf /var/cache/tdnf/* /usr/share/doc/*  /usr/share/man/* /etc/yum.repos.d/photon-iso.repo

COPY --from=cni-binaries /opt/cni/bin /opt/cni/bin

COPY build/images/scripts/* /usr/local/bin/
COPY bin/* /usr/local/bin/

USER root
