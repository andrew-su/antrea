#!/bin/bash
set -xe
# refer to
# https://osm.eng.vmware.com/doc/utilities/vm.html

echo ============================================================
echo Make sure run inside container created from the docker image
echo ============================================================

cat > /etc/apt/sources.list.d/source.list <<EOF
deb-src http://build-artifactory.eng.vmware.com/debian-remote stable main
deb-src http://build-artifactory.eng.vmware.com/debian-remote stable-updates main
deb-src http://build-artifactory.eng.vmware.com/debian-security-remote buster/updates main
EOF
apt update
apt install -y vim unzip rpm gawk python2 python-pip curl python-deb822
pip install pyaml requests retrying
echo "deb-src http://deb.debian.org/debian testing main" >> /etc/apt/sources.list.d/source.list
apt update

cd ~
mkdir osstpclients
cd osstpclients
curl -LO https://osm.eng.vmware.com/utilities/osstpclients.zip
unzip osstpclients.zip
cd bin
./vm-inventory.sh -s deb debian

cat > /tmp/osm-apykey <<EOF
zhengshengz@vmware.com 3d8a2d9af7542d4bf4901fd5c7b72d47ee218872
EOF
./osstp-load.py --noinput -A /tmp/osm-apykey -R Antrea/1.1.0-0.11.1 --baseos-srcdir ~/source osstpmgt.yaml | tee oss.log

source_packages="$(cat oss.log | awk 'BEGIN{baseos=0} /missing BaseOS tickets/{baseos=1} /source package/{if(baseos==1)print $NF}' | sort | uniq  | grep -Eo '[^"]+')"

mkdir ~/source
cd ~/source
apt install -y dpkg-dev
for pkg in ${source_packages} ; do
  apt-get source "${pkg}"
done

cd ~/osstpclients/bin

rm -f osstpmgt.yaml
for dsc in ~/source/*.dsc ; do
  dsc="$(readlink -f "${dsc}")"
  ./dsc-inventory.py -n debian -d ~/source "${dsc}"
  if ! grep -q "Homepage:" "${dsc}" ; then
    url="$(cat "${dsc}" | awk '/Vcs-Browser:/{print $2}')"
    sed -i -e "s|${dsc}|${url}|g" osstpmgt.yaml
  fi
done
if [ -f osstpmgt.yaml ]; then
  ./osstp-load.py --noinput --debug -A /tmp/osm-apykey -R Antrea/1.1.0-0.11.1 --baseos-srcdir ~/source osstpmgt.yaml
fi

rm -f /tmp/osm-apykey
