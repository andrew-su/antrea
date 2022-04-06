#!/bin/bash
set -e

antrea_release_build="$1"
harbor_user="$2"

HARBOR="projects.registry.vmware.com"
HARBOR_REPO="${HARBOR}/antreainterworking"

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 antrea-release-build 'harbor_user' 'harbor_password'"
    echo "Example: $0 ob-xxxx 'dummyUser'"
    echo "antrea-release-build can be found in https://buildweb.eng.vmware.com/ob/?product=antrea-release"
    exit 1
fi


build_kind="$(echo ${antrea_release_build}| cut -d - -f 1)"
build_number="$(echo ${antrea_release_build}| cut -d - -f 2)"

if [[ ${antrea_build_kind} == "sb" ]];then
  echo "Sandbox build cannot be published, please use official build."
  exit 1
fi

echo ====== Logging in to "${HARBOR}" ======
docker login -u "${harbor_user}" "${HARBOR}"

mkdir -p tmp-harbor-upload
pushd tmp-harbor-upload
echo -n > publish_images.txt

wget "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/antrea-interworking/VERSION" -O interworking_version_file
interworking_version="$(cat interworking_version_file)"
echo ====== Publishing Interworking "${interworking_version}" Images ======
for base_os in debian ubuntu photon ubi ; do
  echo === Downloading Interworking $base_os Image ===
  if [ "$base_os" = "ubi" ]; then
    wget "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/openshift/antrea-interworking/interworking-${base_os}-${interworking_version}.tar" -O "interworking-${base_os}.tar"
  else
    wget "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/antrea-interworking/images/interworking-${base_os}-${interworking_version}.tar" -O "interworking-${base_os}.tar"
  fi
  docker load -i "interworking-${base_os}.tar" && rm -f "interworking-${base_os}.tar"
  docker tag "vmware.io/antrea/interworking-${base_os}:${interworking_version}" "${HARBOR_REPO}/interworking-${base_os}:${interworking_version}"
  echo === Pushing "${HARBOR_REPO}/interworking-${base_os}:${interworking_version}" ===
  docker push "${HARBOR_REPO}/interworking-${base_os}:${interworking_version}"
  echo "${HARBOR_REPO}/interworking-${base_os}:${interworking_version}" >> publish_images.txt
done

wget "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/cayman_antrea/VERSION" -O antrea_version_file
antrea_version="$(cat antrea_version_file)"

echo === Downloading Antrea $base_os Image ===
wget "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/openshift/antrea/antrea-${base_os}-${antrea_version}.tar.gz" -O "antrea-${base_os}.tar.gz"
docker load -i "antrea-${base_os}.tar.gz" && rm -f "antrea-${base_os}.tar.gz"
docker tag "localhost:5000/vmware.io/antrea/antrea-${base_os}:${antrea_version}" "${HARBOR_REPO}/antrea-${base_os}:${antrea_version}"
echo === Pushing "${HARBOR_REPO}/antrea-${base_os}:${antrea_version}" ===
docker push "${HARBOR_REPO}/antrea-${base_os}:${antrea_version}"
echo "${HARBOR_REPO}/antrea-${base_os}:${antrea_version}" >> publish_images.txt

for flavor in standard advanced ; do
  echo === Downloading Antrea $flavor Zip File ===
  antrea_bin_version="${antrea_version#v}"
  antrea_bin_version="${antrea_bin_version/_/+}"
  zip_file="$(curl -s "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/cayman_antrea/${flavor}-release/" | grep -o "antrea-${flavor}-${antrea_bin_version}\.zip")"
  wget "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/cayman_antrea/${flavor}-release/${zip_file}" -O "${zip_file}"
  unzip "${zip_file}" && rm -f "${zip_file}"
  zip_dir="${zip_file%.zip}"
  pushd "${zip_dir}/images"
  # flow-aggregator-debian is not pushed because it's pushed in previous TKGM step
  for img in "antrea-${flavor}-debian" ; do
    docker load -i "${img}-${antrea_version}.tar.gz"
    docker tag "antrea/${img}:${antrea_version}" "${HARBOR_REPO}/${img}:${antrea_version}"
    echo === Pushing "${HARBOR_REPO}/${img}:${antrea_version}" ===
    docker push "${HARBOR_REPO}/${img}:${antrea_version}"
    echo "${HARBOR_REPO}/${img}:${antrea_version}" >> ../../publish_images.txt
  done
  popd
  rm -rf "${zip_dir}"
done

wget "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/openshift/operator/VERSION" -O operator_version_file
operator_version="$(cat operator_version_file)"
echo ====== Publishing Operator "${operator_version}" Image ======
echo === Downloading Operator Image ===
wget "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/openshift/operator/images/antrea-operator-${operator_version}.tar.gz" -O operator.tar.gz
docker load -i operator.tar.gz && rm -f operator.tar.gz
docker tag "localhost:5000/vmware.io/antrea/antrea-operator:${operator_version}" "${HARBOR_REPO}/antrea-operator:${operator_version}"
echo === Pushing "${HARBOR_REPO}/antrea-operator:${operator_version}" ===
docker push "${HARBOR_REPO}/antrea-operator:${operator_version}"
echo "${HARBOR_REPO}/antrea-operator:${operator_version}" >> publish_images.txt

echo ====== Cleaning up All Antrea Releated Images ======
docker images | grep -v '<none>' | grep antrea | awk '{print $1":"$2}' | xargs -r docker rmi || true
docker images | grep '<none>' | awk '{print $3}' | xargs -r docker rmi || true

echo ====== Finshed Publishing Images ======
cat publish_images.txt

popd >/dev/null
