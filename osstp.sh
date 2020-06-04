#!/bin/bash

set -xe

RESULTS=`pwd`/osstp_results
ANTREA_ROOTDIR=`pwd`/antrea/src

mkdir -p "${RESULTS}"

# https://gitlab.eng.vmware.com/core-build/mirrors_internal_osstptool
pushd antrea/src
go mod vendor
popd
osstptool generate -o "${RESULTS}/osstp_golang.yml" "--root=${ANTREA_ROOTDIR}" antrea gitlab.eng.vmware.com/core-build/mirrors_github_antrea

pushd "${RESULTS}"
osstptool download "--root=${ANTREA_ROOTDIR}"

# Need to install virtualenv first
# https://osm.eng.vmware.com/doc/utilities/access.html
echo Please do the following steps manually
echo cd $(pwd) ; workon osstp
# Upload the source code and create master package on OSM site.
# https://osm.eng.vmware.com/doc/utilities/loading.html
echo ~/antrea-repos/osstpclients/bin/osstp-load.py -I 'Distributed - Static Link w/ VMW' -U zhengshengz osstp_golang.yml
echo deactivate
popd
