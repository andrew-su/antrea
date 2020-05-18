FROM scratch as ovs-rpms

ADD photon-rootfs.tar.gz /

RUN /usr/bin/sed -i -e 's/^enabled=.*/enabled=0/g' /etc/yum.repos.d/*.repo
COPY photon-iso.repo /etc/yum.repos.d/
COPY openvswitch.spec /tmp/
COPY openvswitch-*.tar.gz /tmp/

RUN tdnf remove -y toybox && \
    tdnf install -y \
      bzip2 \
      coreutils \
      cpio \
      diffutils \
      e2fsprogs \
      file \
      findutils \
      grep \
      gzip \
      iotop \
      kbd \
      net-tools \
      parted \
      patch \
      pciutils \
      procps-ng \
      psmisc \
      python-xml \
      python3-xml \
      sed \
      tar \
      usbutils \
      util-linux \
      which && \
    tdnf install -y \
      autoconf \
      automake \
      build-essential \
      ca-certificates \
      curl \
      ethtool \
      gawk \
      git \
      groff \
      libcap-ng \
      libcap-ng-devel \
      libgcc-atomic \
      libtool \
      linux-devel \
      make \
      net-tools \
      netcat \
      openssl \
      openssl-devel \
      python2 \
      python2-libs \
      python3 \
      python3-devel \
      python3-libs \
      python3-six \
      python3-xml \
      rpm-build \
      systemd \
      wget
RUN SOURCE_DIR=$(rpm --eval "%{_sourcedir}") RPM_DIR=$(rpm --eval "%{_rpmdir}") && \
    mkdir -p "${SOURCE_DIR}" && \
    mv /tmp/openvswitch-*.tar.gz "${SOURCE_DIR}" && \
    rpmbuild -bb --nocheck /tmp/openvswitch.spec && \
    mkdir -p /tmp/ovs-rpms && mv ${RPM_DIR}/*/*.rpm /tmp/ovs-rpms

FROM scratch

MAINTAINER wcp-dev@vmware.com
LABEL vendor="VMware, Inc."
LABEL name="VMware Stateless Photon OS 3.0 Base Image"

ADD photon-rootfs.tar.gz /
RUN /usr/bin/sed -i -e 's/^enabled=.*/enabled=0/g' /etc/yum.repos.d/*.repo
COPY photon-iso.repo /etc/yum.repos.d/
COPY --from=ovs-rpms /tmp/ovs-rpms/* /tmp/ovs-rpms/
COPY charon-logging.conf /tmp


RUN tdnf install -y strongswan iptables rpm && \
    tdnf install -y \
      bash \
      gawk \
      jq \
      libcap-ng \
      libgcc-atomic \
      openssl \
      python2 \
      python2-libs \
      python3 \
      python3-libs \
      python3-six \
      python3-xml && \
    rpm -ivh /tmp/ovs-rpms/openvswitch-2.13.0*.rpm /tmp/ovs-rpms/python*-openvswitch*.rpm && \
    sed -i "/^.*filelog.*{/r /tmp/charon-logging.conf" /etc/strongswan.d/charon-logging.conf && \
    rm -rf /tmp/* && \
    tdnf clean all && \
    rm -rf /var/cache/tdnf/* /usr/share/doc/*  /usr/share/man/*
