#!/bin/bash
set -xe
# refer to
# https://confluence.eng.vmware.com/display/OSMUserGuide/VM+%28vApp+Virtual+Machines%29+or+Containers?src=contextnavpagetreemode
# https://confluence.eng.vmware.com/display/CNA/Cascade+OSSTP

RELEASE_VERSION=1.5.0

echo Scann OS packages for Antrea commercial release $RELEASE_VERSION

if [ -z "$1" ]; then
  # Need to manually create package ct-track-ubuntu on https://osm.eng.vmware.com/ first
  echo Usage: $0 BaseOSTicketNum
  exit 1
else
  CT_TRACKER="$1"
fi

echo ============================================================
echo Make sure run inside container created from the docker image
echo ============================================================
cat > /etc/apt/sources.list.d/source.list <<EOF
deb-src http://build-artifactory.eng.vmware.com/ubuntu-remote focal main restricted universe
deb-src http://build-artifactory.eng.vmware.com/ubuntu-remote focal-security main restricted
deb-src http://build-artifactory.eng.vmware.com/ubuntu-remote focal-updates main restricted
EOF
apt update
# Mannually install if failed
DEBIAN_FRONTEND="noninteractive" apt install -y --no-install-recommends vim unzip rpm gawk curl wget git || true

mkdir -p osstpclients
cd osstpclients
curl -LO https://osm.eng.vmware.com/utilities/osstpclients3.zip
unzip osstpclients3.zip
cd bin
./vm-inventory.sh -s deb ubuntu

cat > /tmp/osm-apykey <<EOF
zhengshengz@vmware.com 3d8a2d9af7542d4bf4901fd5c7b72d47ee218872
EOF
./client_executables/linux-amd64/osstp-load --baseos-append --baseos-ct-tracker "$CT_TRACKER" --noinput -A /tmp/osm-apykey -R "Antrea/${RELEASE_VERSION}" --baseos-srcdir ~/source osstpmgt.yaml | tee oss.log

source_packages="$(cat oss.log | grep 'No BaseOS package available for' | awk '{print $8}' | awk -F ':' '{print $3}')"

mkdir -p ../../source
cd ../../source
DEBIAN_FRONTEND="noninteractive" apt install -y --no-install-recommends dpkg-dev || true
for pkg in ${source_packages} ; do
  apt-get source "${pkg}" || true
done

cd ../osstpclients

rm -f bin/osstpmgt.yaml
ls ../source/*.dsc || exit 0
for dsc in ../source/*.dsc ; do
  dsc="$(readlink -f "${dsc}")"
  # sometimes osstpmgt.yaml is generated but dsc-inventory returns 1
  ./bin/client_executables/linux-amd64/dsc-inventory -n ubuntu -d ../source "${dsc}" || true
  if ! grep -q "Homepage:" "${dsc}" ; then
    url="$(cat "${dsc}" | awk '/Vcs-Browser:/{print $2}')"
    sed -i -e "s|${dsc}|${url}|g" osstpmgt.yaml
  fi
done
mv osstpmgt.yaml bin/
if [ -f bin/osstpmgt.yaml ]; then
  ./bin/client_executables/linux-amd64/osstp-load --baseos-append --noinput --baseos-ct-tracker "$CT_TRACKER" --debug -A /tmp/osm-apykey -R "Antrea/${RELEASE_VERSION}" --baseos-srcdir ../source bin/osstpmgt.yaml || true
fi

rm -f /tmp/osm-apykey
