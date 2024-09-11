ARG GO_VERSION
FROM nsx-ujo-docker-local.packages.vcfd.broadcom.net/interworking/golang:${GO_VERSION}-buster as antrea-idps-build

WORKDIR /antrea

COPY go.mod /antrea/go.mod

RUN go mod download

COPY . /antrea

RUN make antrea-idps-agent antrea-idps-controller

FROM nsx-ujo-docker-local.packages.vcfd.broadcom.net/antrea/debian:12.5

RUN apt update && apt install curl ca-certificates -y

LABEL maintainer="Antrea <projectantrea-dev@googlegroups.com>"
LABEL description="The Docker image to deploy the Antrea IDPS."

USER root

COPY --from=antrea-idps-build /antrea/bin/antrea-idps-* /usr/local/bin/
