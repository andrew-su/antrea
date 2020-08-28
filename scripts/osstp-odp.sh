#!/bin/bash
set -e
# refer to
# https://osm.eng.vmware.com/doc/utilities/odp-template.html

if [ -z "$1" ]; then
  echo Usage: $0 ticket1,ticket2,ticket3,... >&2
  exit 1
fi

mkdir osstpclients
# In case neeed to setup Python venv for osstpclients:
# mkvirtualenv osstp
# pip install -r "$(dirname $0)/osstp-requirements.txt"
( cd osstpclients
  curl -LO https://osm.eng.vmware.com/utilities/osstpclients.zip
  unzip osstpclients.zip )
cd osstpclients/bin


cat > /tmp/apikey <<EOF
zhengshengz@vmware.com 3d8a2d9af7542d4bf4901fd5c7b72d47ee218872
EOF
workon osstp
./odp-template.py -A /tmp/apikey -R Antrea/1.0.0-0.9.0 -T "$1"
deactivate

echo Generated ODP template in $(pwd)/VMware-Antrea-1.0.0-0.9.0-ODP.
echo Please check if any package needs SBR, then provide BUILD.txt and INSTALL.txt in each package directory.
echo After finish checking all the packages, create a tar.gz file from this directoy and upload for review.
