# Base image is from the latest build task from https://jenkins-lastline.svc.eng.vmware.com/job/suricata-lastline/
FROM nsx-ujo-docker-local.artifactory.vcfd.broadcom.net/antrea/suricata:6.0.4_ll-0_710-aee881a0.bionic
ARG BUILD_INFO

LABEL maintainer="Antrea <projectantrea-dev@googlegroups.com>"
LABEL description="A Docker image based on Lastline Suricata docker image."
LABEL build-info="${BUILD_INFO}"

RUN echo "${BUILD_INFO}" > /build-info
