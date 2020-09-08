#!/bin/bash
set -e

if [ -z "$1" ] ; then
  echo "Usage: $0 ob-1234567" >&2
  exit 1
fi

if [ -e tmp-zip ] ; then
  echo "tmp-zip dir exists, please delete it first" >&2
  exit 1
fi

if [ ! -e /build/ob/release/ ] ; then
  echo "Need to be run with /build nfs mount" >&2
  exit 1
fi

echo "Creating tmp dir"
mkdir tmp-zip/
echo Reading Antrea version
source /build/ob/release/bora-${1#ob-}/publish/lin64/antrea/manifests/version
produce_version="${ANTREA_BRANCH#vmware-}.${1#ob-}"
echo "Copying Antrea publish dir to tmp dir"
cp -rv /build/ob/release/bora-${1#ob-}/publish/lin64/antrea "tmp-zip/antrea-${produce_version}"
echo "Generating zip"
pushd tmp-zip
zip -r "antrea-${produce_version}.zip" "antrea-${produce_version}"
popd
mv "tmp-zip/antrea-${produce_version}.zip" .
rm -rf tmp-zip
echo "Generated antrea-${produce_version}.zip"
