#!/bin/bash
# refer to
# https://confluence.eng.vmware.com/display/CNA/Cascade+OSSTP
# https://gitlab.eng.vmware.com/core-build/mirrors_internal_osstptool/blob/master/README.md

set -xe

RELEASE_VERSION=1.4.0
RESULTS=`pwd`/osstp_results
ANTREA_ROOTDIR=`pwd`/antrea/src

mkdir -p "${RESULTS}"

# https://gitlab.eng.vmware.com/core-build/mirrors_internal_osstptool
# https://gitlab.eng.vmware.com/frapposelli/osstptool/-/releases
pushd antrea/src
go mod vendor
popd
./osstptool generate -o "${RESULTS}/osstp_golang.yml" "--root=${ANTREA_ROOTDIR}" antrea gitlab.eng.vmware.com/core-build/mirrors_github_antrea

pushd "${RESULTS}"
../osstptool download "--root=${ANTREA_ROOTDIR}"

# Need to install virtualenv first
# https://osm.eng.vmware.com/doc/utilities/access.html
# Upload the source code and create master package on OSM site.
# https://osm.eng.vmware.com/doc/utilities/loading.html
# In case neeed to setup Python venv for osstpclients:
# mkvirtualenv osstp
# pip install -r "$(dirname $0)/osstp-requirements.txt"
cat > /tmp/osm-apikey <<EOF
zhengshengz@vmware.com 3d8a2d9af7542d4bf4901fd5c7b72d47ee218872
EOF
echo ===================================================
echo Uploading packages and creating tickets on OSM site
echo ===================================================
wget -O osstp-load https://osm.eng.vmware.com/utilities/osstp-load/
chmod a+x osstp-load
./osstp-load --noinput -I 'Distributed - Static Link w/ VMW' -A /tmp/osm-apikey -R "Antrea/${RELEASE_VERSION}" osstp_golang.yml
# ~/antrea-repos/osstpclients/bin/osstp-load.py --noinput -I 'Distributed - Dynamic Link w/ OSS' -A /tmp/osm-apikey -R Antrea/1.2.0-0.13.0 osstp_golang.yml
rm -f /tmp/osm-apikey
popd
