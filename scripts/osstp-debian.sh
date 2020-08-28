#!/bin/bash
set -e
# refer to
# https://osm.eng.vmware.com/doc/utilities/vm.html

echo ============================================================
echo Make sure run inside container created from the docker image
echo ============================================================

cat > /etc/apt/sources.list.d/source.list <<EOF
deb-src http://build-artifactory.eng.vmware.com/debian-remote stable main
deb-src http://build-artifactory.eng.vmware.com/debian-remote stable-updates main
deb-src http://build-artifactory.eng.vmware.com/debian-security-remote buster/updates main
deb-src http://deb.debian.org/debian testing main
EOF
apt update
apt install -y vim unzip rpm gawk python2 python-pip curl python-deb822
pip install pyaml requests

mkdir ~/source
cd ~/source

source_packages="
libc-bin
libc6
libcrypt1
libgcc-s1
gcc-10-base
libnftnl11
"
for pkg in ${source_packages} ; do
  apt-get source "${pkg}"
done

cd ~
mkdir osstpclients
cd osstpclients
curl -LO https://osm.eng.vmware.com/utilities/osstpclients.zip
unzip osstpclients.zip
cd bin
./vm-inventory.sh -s deb debian

for dsc in ~/source/*.dsc ; do
  ./dsc-inventory.py -n debian -d ~/source "${dsc}"
done

cat > /tmp/apikey <<EOF
zhengshengz@vmware.com 3d8a2d9af7542d4bf4901fd5c7b72d47ee218872
EOF
./osstp-load.py -A /tmp/apikey -R Antrea/1.0.0-0.9.0 --baseos-srcdir ~/source osstpmgt.yaml
rm -f /tmp/apikey
