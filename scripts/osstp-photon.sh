#!/bin/bash
set -xe
# refer to
# https://confluence.eng.vmware.com/display/OSMUserGuide/VM+%28vApp+Virtual+Machines%29+or+Containers?src=contextnavpagetreemode
# https://confluence.eng.vmware.com/display/CNA/Cascade+OSSTP

RELEASE_VERSION=1.5.0

echo Scann OS packages for Antrea commercial release $RELEASE_VERSION

if [ -z "$1" ]; then
  # Need to manually create package ct-track-photon on https://osm.eng.vmware.com/ first
  echo Usage: $0 BaseOSTicketNum
  exit 1
else
  CT_TRACKER="$1"
fi

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

tdnf makecache
# Mannually install if failed
tdnf install -y gawk unzip curl || true

mkdir -p osstpclients
cd osstpclients
curl -LO https://osm.eng.vmware.com/utilities/osstpclients3.zip
unzip osstpclients3.zip
cd bin

./vm-inventory.sh -s rpm photon

cat > /tmp/osm-apykey <<EOF
zhengshengz@vmware.com 3d8a2d9af7542d4bf4901fd5c7b72d47ee218872
EOF
./client_executables/linux-amd64/osstp-load --baseos-append --baseos-ct-tracker "$CT_TRACKER" --noinput -A /tmp/osm-apykey -R "Antrea/${RELEASE_VERSION}" --baseos-srcdir ~/source osstpmgt.yaml | tee oss.log

source_packages="$(cat oss.log | grep 'No BaseOS package available for' | awk '{print $8}' | awk -F ':' '{print $3}')"

if [ -n "${source_packages}" ]; then
  echo Please manually handle the following source_packages:
  echo "${source_packages}"
  exit 1
else
  exit 0
fi

# The following code is from debian/ubuntu, need to write specific code to process photon package
mkdir -p ../../source
cd ../../source
for pkg in ${source_packages} ; do
  tdnf --downloadonly --downloaddir=. install "${pkg}" || true
done

cd ../osstpclients

rm -f bin/osstpmgt.yaml
for dsc in ../source/*.dsc ; do
  dsc="$(readlink -f "${dsc}")"
  ./bin/./client_executables/linux-amd64/dsc-inventory -n photon -d ../source "${dsc}"
  if ! grep -q "Homepage:" "${dsc}" ; then
    url="$(cat "${dsc}" | awk '/Vcs-Browser:/{print $2}')"
    sed -i -e "s|${dsc}|${url}|g" bin/osstpmgt.yaml
  fi
done
mv osstpmgt.yaml bin/
if [ -f bin/osstpmgt.yaml ]; then
  ./bin/client_executables/linux-amd64/osstp-load --baseos-append --noinput --baseos-ct-tracker "$CT_TRACKER" --debug -A /tmp/osm-apykey -R "Antrea/${RELEASE_VERSION}" --baseos-srcdir ../source bin/osstpmgt.yaml || true
fi

rm -f /tmp/osm-apykey
