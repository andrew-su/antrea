#!/bin/bash
set -xe
# refer to
# https://confluence.eng.vmware.com/display/OSMUserGuide/VM+%28vApp+Virtual+Machines%29+or+Containers?src=contextnavpagetreemode
# https://confluence.eng.vmware.com/display/CNA/Cascade+OSSTP

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
apt install -y vim unzip rpm gawk python3 python3-pip curl golang wget git || true #python-deb822
ln -s /usr/bin/python3 /usr/bin/python || true
pip3 install pyaml requests retrying

mkdir -p osstpclients
cd osstpclients
curl -LO https://osm.eng.vmware.com/utilities/osstpclients3.zip
unzip osstpclients3.zip
cd bin
# Mannually modify lib/python/osstpinventory.py return 0
pip3 install -r ../etc/requirements.txt
./vm-inventory.sh -s deb ubuntu

cat > /tmp/osm-apykey <<EOF
zhengshengz@vmware.com 3d8a2d9af7542d4bf4901fd5c7b72d47ee218872
EOF
./osstp-load.py --noinput -A /tmp/osm-apykey -R Antrea/1.3.1-1.2.3 --baseos-srcdir ~/source osstpmgt.yaml | tee oss.log

source_packages="$(cat oss.log| sed -e '1,/missing BaseOS tickets/d'|awk -F"source package" '{print $2}'|grep -v None|sed 's/ (.*)//'|sed 's/"//g')"

mkdir -p ../../source
cd ../../source
apt install -y dpkg-dev || true
for pkg in ${source_packages} ; do
  apt-get source "${pkg}" || true
done

cd ../osstpclients
export PYTHONPATH=../osstpclients

rm -f bin/osstpmgt.yaml
for dsc in ../source/*.dsc ; do
  dsc="$(readlink -f "${dsc}")"
  ./bin/dsc-inventory.py -n ubuntu -d ../source "${dsc}"
  if ! grep -q "Homepage:" "${dsc}" ; then
    url="$(cat "${dsc}" | awk '/Vcs-Browser:/{print $2}')"
    sed -i -e "s|${dsc}|${url}|g" osstpmgt.yaml
  fi
done
mv osstpmgt.yaml bin/
if [ -f bin/osstpmgt.yaml ]; then
  ./bin/osstp-load.py --noinput --debug -A /tmp/osm-apykey -R Antrea/1.3.1-1.2.3 --baseos-srcdir ../source bin/osstpmgt.yaml || true
fi

rm -f /tmp/osm-apykey
