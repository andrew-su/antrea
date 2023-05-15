#!/bin/bash
set -e
# refer to
# https://confluence.eng.vmware.com/display/public/OSMUserGuide/OSL+File+Generation

RELEASE_VERSION=1.7.0
RELEASE_ODP="VMware-Antrea-${RELEASE_VERSION}-ODP"

if [ -z "$1" ]; then
  echo Usage: $0 ticket1,ticket2,ticket3,... >&2
  exit 1
fi

full_path="$(readlink -f $0)"
dir_path="$(dirname "$full_path")"
template_dir="${dir_path}/odp-sbr-template"

mkdir -p osstpclients
# In case neeed to setup Python venv for osstpclients:
# mkvirtualenv osstp
# pip install -r "$(dirname $0)/osstp-requirements.txt"
( cd osstpclients
  curl -LO https://osm.eng.vmware.com/utilities/osstpclients.zip
  unzip osstpclients.zip )
cd osstpclients/bin


# Need to install virtualenv first
# https://osm.eng.vmware.com/doc/utilities/access.html
cat > /tmp/osm-apykey <<EOF
zhengshengz@vmware.com 3d8a2d9af7542d4bf4901fd5c7b72d47ee218872
EOF
#source /usr/bin/virtualenvwrapper.sh
#workon osstp
python2 ./odp-template.py -A /tmp/osm-apykey -R "Antrea/${RELEASE_VERSION}" -T "$1"
rm -f /tmp/osm-apykey
#deactivate

pushd ${RELEASE_ODP}
for OS in debian ubuntu photon rhel9 ; do
  if [ -d "$OS" ]; then
    cp -f "${template_dir}/${OS}-README.txt" "$OS/README.txt"
  fi
done
popd

echo Generated ODP template in $(pwd)/${RELEASE_ODP}.
echo Please check if any package needs SBR, then provide BUILD.txt and INSTALL.txt in each package directory.
echo After finish checking all the packages, create a tar.gz file from this directoy and upload for review.
