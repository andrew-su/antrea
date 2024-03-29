#!/bin/bash
set -e

antrea_release_build="$1"
harbor_user="$2"

HARBOR="projects.registry.vmware.com"
HARBOR_REPO="${HARBOR}/antreainterworking"

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 antrea-release-build 'harbor_user'"
    echo "  antrea-release-build: antrea-release official build. Build can be found in https://buildweb.eng.vmware.com/ob/?product=antrea-release ."
    echo "  harbor_user: projects.registry.vmware.com user name (without \"@vmware.com\")"
    echo "Example: $0 ob-xxxx 'dummyUser'"
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

#### Interworking debian ubuntu photon ubi
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
antrea_bin_version="${antrea_version#v}"
antrea_bin_version="${antrea_bin_version/_/+}"

#### Antrea UBI
base_os=ubi
echo === Downloading Antrea $base_os Image ===
for component in "controller" "agent"; do
  wget "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/openshift/antrea/images/antrea-${component}-${base_os}-${antrea_version}.tar.gz" \
    -O "antrea-${component}-${base_os}.tar.gz"
  docker load -i "antrea-${component}-${base_os}.tar.gz" && rm -f "antrea-${component}-${base_os}.tar.gz"
  docker tag "localhost:5000/vmware.io/antrea/antrea-${component}-${base_os}:${antrea_version}" \
    "${HARBOR_REPO}/antrea-${component}-${base_os}:${antrea_version}"
  echo === Pushing "${HARBOR_REPO}/antrea-${component}-${base_os}:${antrea_version}" ===
  docker push "${HARBOR_REPO}/antrea-${component}-${base_os}:${antrea_version}"
  echo "${HARBOR_REPO}/antrea-${component}-${base_os}:${antrea_version}" >> publish_images.txt
done

#### Flow-aggregator UBI
base_os=ubi
echo === Downloading Flow-aggregator $base_os Image ===
wget "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/openshift/antrea/images/flow-aggregator-${base_os}-${antrea_version}.tar.gz" -O "flow-aggregato-${base_os}.tar.gz"
docker load -i "flow-aggregato-${base_os}.tar.gz" && rm -f "flow-aggregator-${base_os}.tar.gz"
docker tag "localhost:5000/vmware.io/antrea/flow-aggregator-${base_os}:${antrea_version}" "${HARBOR_REPO}/flow-aggregator-${base_os}:${antrea_version}"
echo === Pushing "${HARBOR_REPO}/flow-aggregator-${base_os}:${antrea_version}" ===
docker push "${HARBOR_REPO}/flow-aggregator-${base_os}:${antrea_version}"
echo "${HARBOR_REPO}/flow-aggregator-${base_os}:${antrea_version}" >> publish_images.txt

#### Antrea standard, advanced (debian)
#### Flow-aggregator debian
for flavor in standard advanced ; do
  echo === Downloading Antrea $flavor Zip File ===
  zip_file="$(curl -s "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/cayman_antrea/${flavor}-release/" | grep -o "antrea-${flavor}-${antrea_bin_version}\.zip")"
  wget "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/cayman_antrea/${flavor}-release/${zip_file}" -O "${zip_file}"
  unzip "${zip_file}" && rm -f "${zip_file}"
  zip_dir="${zip_file%.zip}"
  pushd "${zip_dir}/images"
  for img in "antrea-${flavor}-controller-debian" "antrea-${flavor}-agent-debian" "flow-aggregator-debian" ; do
    # flow-aggregator image is the same for Antrea standard and advaced. We just need to update it once.
    if [ "$flavor" = "standard" -a "${img}" = "flow-aggregator-debian" ]; then continue; fi
    docker load -i "${img}-${antrea_version}.tar.gz"
    docker tag "antrea/${img}:${antrea_version}" "${HARBOR_REPO}/${img}:${antrea_version}"
    echo === Pushing "${HARBOR_REPO}/${img}:${antrea_version}" ===
    docker push "${HARBOR_REPO}/${img}:${antrea_version}"
    echo "${HARBOR_REPO}/${img}:${antrea_version}" >> ../../publish_images.txt
  done
  popd
  rm -rf "${zip_dir}"
done

#### Multi-cluster debian ubi
for base_os in debian ubi; do
  echo === Downloading Multi-cluster-controller $base_os Image ===
  zip_file="antrea-multicluster-${base_os}-${antrea_bin_version}.zip"
  zip_dir="antrea-multicluster-${base_os}-${antrea_bin_version}"
  wget "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/cayman_antrea/multi-cluster/${zip_file}"
  unzip "${zip_file}"
  docker load -i "${zip_dir}/images/antrea-mc-controller-${antrea_version}.tar.gz"
  docker tag "antrea/antrea-mc-controller-${base_os}:${antrea_version}" "${HARBOR_REPO}/antrea-mc-controller-${base_os}:${antrea_version}"
  echo === Pushing "${HARBOR_REPO}/antrea-mc-controller-${base_os}:${antrea_version}" ====
  docker push "${HARBOR_REPO}/antrea-mc-controller-${base_os}:${antrea_version}"
  echo "${HARBOR_REPO}/antrea-mc-controller-${base_os}:${antrea_version}" >> publish_images.txt
done

#### IDPS debian ubi
#### suricata
for base_os in debian ubi; do
  echo === Downloading IDPS $base_os Image ===
  zip_file="antrea-idps-${base_os}-${antrea_bin_version}.zip"
  zip_dir="antrea-idps-${base_os}-${antrea_bin_version}"
  wget "http://build-squid.eng.vmware.com/build/mts/release/bora-${build_number}/publish/cayman_antrea/idps/${zip_file}"
  unzip "${zip_file}"
  docker load -i "${zip_dir}/images/antrea-idps-${antrea_version}.tar.gz"
  echo === Pushing "${HARBOR_REPO}/idps-${base_os}:${antrea_version}" ====
  docker push "${HARBOR_REPO}/idps-${base_os}:${antrea_version}"
  echo "${HARBOR_REPO}/idps-${base_os}:${antrea_version}" >> publish_images.txt
  if [ "$base_os" = "debian" ]; then
    # suricata image is based on ubuntu actually.
    # suricata image is the same for idps-debian and idps-ubi zip, only need to upload it once.
    docker load -i "${zip_dir}/images/antrea-suricata-${antrea_version}.tar.gz"
    echo === Pushing "${HARBOR_REPO}/suricata:${antrea_version}" ====
    docker push "${HARBOR_REPO}/suricata:${antrea_version}"
    echo "${HARBOR_REPO}/suricata:${antrea_version}" >> publish_images.txt
  fi
done


#### operator
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

echo ====== Finished Publishing Images ======
cat publish_images.txt

popd >/dev/null
