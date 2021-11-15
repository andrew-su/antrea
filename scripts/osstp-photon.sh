#!/bin/bash
set -xe
# refer to
# https://confluence.eng.vmware.com/display/OSMUserGuide/VM+%28vApp+Virtual+Machines%29+or+Containers?src=contextnavpagetreemode
# https://confluence.eng.vmware.com/display/CNA/Cascade+OSSTP

echo ============================================================
echo Make sure run inside container created from the docker image
echo ============================================================

cat > /etc/yum.repos.d/photon-extras.repo <<EOF
[photon-extras]
name=VMware Photon Extras $releasever ($basearch)
baseurl=https://packages.vmware.com/photon/\$releasever/photon_extras_\$releasever_\$basearch
gpgkey=file:///etc/pki/rpm-gpg/VMWARE-RPM-GPG-KEY
gpgcheck=1
enabled=1
skip_if_unavailable=True
EOF

cat > /etc/yum.repos.d/photon-updates.repo <<EOF
[photon-updates]
name=VMware Photon Linux $releasever ($basearch) Updates
baseurl=https://packages.vmware.com/photon/\$releasever/photon_updates_\$releasever_\$basearch
gpgkey=file:///etc/pki/rpm-gpg/VMWARE-RPM-GPG-KEY
gpgcheck=1
enabled=1
skip_if_unavailable=True
EOF

cat >/etc/yum.repos.d/photon.repo <<EOF
[photon]
name=VMware Photon Linux $releasever ($basearch)
baseurl=https://packages.vmware.com/photon/\$releasever/photon_release_\$releasever_\$basearch
gpgkey=file:///etc/pki/rpm-gpg/VMWARE-RPM-GPG-KEY
gpgcheck=1
enabled=1
skip_if_unavailable=True
EOF

tdnf update
# Mannually install if failed
tdnf install -y unzip python3 python3-pip curl || true #python-deb822

ln -s /usr/bin/python3 /usr/bin/python || true
pip3 install pyaml requests retrying

mkdir -p osstpclients
cd osstpclients
curl -LO https://osm.eng.vmware.com/utilities/osstpclients3.zip
unzip osstpclients3.zip
cd bin

# Mannually modify lib/python/osstpinventory.py return 0
pip3 install -r ../etc/requirements.txt
./vm-inventory.sh -s rpm photon

cat > /tmp/osm-apykey <<EOF
zhengshengz@vmware.com 3d8a2d9af7542d4bf4901fd5c7b72d47ee218872
EOF
python3 ./osstp-load.py --noinput -A /tmp/osm-apykey -R Antrea/1.3.1-1.2.3 --baseos-srcdir ~/source osstpmgt.yaml | tee oss.log

source_packages="$(cat oss.log| sed -e '1,/missing BaseOS tickets/d'|awk -F"source package" '{print $2}'|grep -v None|sed 's/ (.*)//'|sed 's/"//g')"

mkdir -p ../../source
cd ../../source
for pkg in ${source_packages} ; do
  tdnf --downloadonly --downloaddir=. install "${pkg}" || true
done

cd ../osstpclients
export PYTHONPATH=../osstpclients

rm -f bin/osstpmgt.yaml
for dsc in ../source/*.dsc ; do
  dsc="$(readlink -f "${dsc}")"
  python3 ./bin/dsc-inventory.py -n photon -d ../source "${dsc}"
  if ! grep -q "Homepage:" "${dsc}" ; then
    url="$(cat "${dsc}" | awk '/Vcs-Browser:/{print $2}')"
    sed -i -e "s|${dsc}|${url}|g" bin/osstpmgt.yaml
  fi
done
mv osstpmgt.yaml bin/
if [ -f bin/osstpmgt.yaml ]; then
  python3 ./bin/osstp-load.py --noinput --debug -A /tmp/osm-apykey -R Antrea/1.3.1-1.2.3 --baseos-srcdir ../source bin/osstpmgt.yaml || true
fi

rm -f /tmp/osm-apykey
