#!/bin/bash
set -e

antrea_release_build="$1"
artifactory_user="$2"
artifactory_token="$3"

ARTIFACTORY_URL="antrea-docker-dev-local.artifactory.vcfd.broadcom.net"
ARTIFACTORY_REPO="${ARTIFACTORY_URL}/antreainterworking"

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 antrea-release-build 'artifactory_user' 'artifactory_token'"
    echo "  antrea-release-build: antrea-release official build. Build can be found in https://buildweb.eng.vmware.com/ob/?product=antrea-release."
    echo "  artifactory_user: artifactory.vcfd.broadcom.net user name (Broadcom ID without @broadcom.net)"
    echo "  artifactory_token: user's token to login artifactory.vcfd.broadcom.net"
    echo "Example: $0 ob-xxxx 'ab12345678' '*****'"
    exit 1
fi


build_kind="$(echo ${antrea_release_build}| cut -d - -f 1)"
build_number="$(echo ${antrea_release_build}| cut -d - -f 2)"

if [[ ${build_kind} == "sb" ]];then
  echo "Sandbox build cannot be published, please use official build."
  exit 1
fi

# Traverse all components of a build, and find the specific component name, and output the component build.
find_component_build() {
  local build=$1
  local component=$2
  local b

  local component_builds=(`curl -s "https://buildapi.lvn.broadcom.net/ob/buildcomponent/?build=$build" | jq -r '._list[].component_buildid'`)

  for b in "${component_builds[@]}"; do
    p=$(curl -s https://buildapi.lvn.broadcom.net/ob/build/$b | jq -r .product)
    if [ "$p" = "$component" ]; then
      echo $b
      break
    fi
  done
}

echo ====== Logging in to "${ARTIFACTORY_URL}" ======
docker login -u "${artifactory_user}" -p "${artifactory_token}" "${ARTIFACTORY_URL}"

mkdir -p tmp-artifactory-upload
pushd tmp-artifactory-upload
echo -n > publish_images.txt

#### Interworking debian ubuntu photon ubi
wget "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${build_number}/publish/antrea-interworking/VERSION" -O interworking_version_file
interworking_version="$(cat interworking_version_file)"
echo ====== Publishing Interworking "${interworking_version}" Images ======
for base_os in debian ubuntu photon ubi ; do
  echo === Downloading Interworking $base_os Image ===
  if [ "$base_os" = "ubi" ]; then
    wget "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${build_number}/publish/openshift/antrea-interworking/interworking-${base_os}-${interworking_version}.tar" -O "interworking-${base_os}.tar"
  else
    wget "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${build_number}/publish/antrea-interworking/images/interworking-${base_os}-${interworking_version}.tar" -O "interworking-${base_os}.tar"
  fi
  docker load -i "interworking-${base_os}.tar" && rm -f "interworking-${base_os}.tar"
  docker tag "vmware.io/antrea/interworking-${base_os}:${interworking_version}" "${ARTIFACTORY_REPO}/interworking-${base_os}:${interworking_version}"
  echo === Pushing "${ARTIFACTORY_REPO}/interworking-${base_os}:${interworking_version}" ===
  docker push "${ARTIFACTORY_REPO}/interworking-${base_os}:${interworking_version}"
  echo "${ARTIFACTORY_REPO}/interworking-${base_os}:${interworking_version}" >> publish_images.txt
done

wget "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${build_number}/publish/cayman_antrea/VERSION" -O antrea_version_file
antrea_version="$(cat antrea_version_file)"
antrea_bin_version="${antrea_version#v}"
antrea_bin_version="${antrea_bin_version/_/+}"

#### Antrea UBI
base_os=ubi
echo === Downloading Antrea $base_os Image ===
for component in "controller" "agent"; do
  wget "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${build_number}/publish/openshift/antrea/images/antrea-${component}-${base_os}-${antrea_version}.tar.gz" \
    -O "antrea-${component}-${base_os}.tar.gz"
  docker load -i "antrea-${component}-${base_os}.tar.gz" && rm -f "antrea-${component}-${base_os}.tar.gz"
  docker tag "localhost:5000/vmware.io/antrea/antrea-${component}-${base_os}:${antrea_version}" \
    "${ARTIFACTORY_REPO}/antrea-${component}-${base_os}:${antrea_version}"
  echo === Pushing "${ARTIFACTORY_REPO}/antrea-${component}-${base_os}:${antrea_version}" ===
  docker push "${ARTIFACTORY_REPO}/antrea-${component}-${base_os}:${antrea_version}"
  echo "${ARTIFACTORY_REPO}/antrea-${component}-${base_os}:${antrea_version}" >> publish_images.txt
done

#### Flow-aggregator UBI
base_os=ubi
echo === Downloading Flow-aggregator $base_os Image ===
wget "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${build_number}/publish/openshift/antrea/images/flow-aggregator-${base_os}-${antrea_version}.tar.gz" -O "flow-aggregato-${base_os}.tar.gz"
docker load -i "flow-aggregato-${base_os}.tar.gz" && rm -f "flow-aggregator-${base_os}.tar.gz"
docker tag "localhost:5000/vmware.io/antrea/flow-aggregator-${base_os}:${antrea_version}" "${ARTIFACTORY_REPO}/flow-aggregator-${base_os}:${antrea_version}"
echo === Pushing "${ARTIFACTORY_REPO}/flow-aggregator-${base_os}:${antrea_version}" ===
docker push "${ARTIFACTORY_REPO}/flow-aggregator-${base_os}:${antrea_version}"
echo "${ARTIFACTORY_REPO}/flow-aggregator-${base_os}:${antrea_version}" >> publish_images.txt

#### Antrea standard, advanced (debian)
#### Flow-aggregator, ODS debian
for flavor in standard advanced ; do
  echo === Downloading Antrea $flavor Zip File ===
  zip_file="$(curl -s "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${build_number}/publish/cayman_antrea/${flavor}-release/" | grep -o "antrea-${flavor}-${antrea_bin_version}\.zip")"
  wget "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${build_number}/publish/cayman_antrea/${flavor}-release/${zip_file}" -O "${zip_file}"
  unzip "${zip_file}" && rm -f "${zip_file}"
  zip_dir="${zip_file%.zip}"
  pushd "${zip_dir}/images"
  for img in "antrea-${flavor}-controller-debian" "antrea-${flavor}-agent-debian" "flow-aggregator-debian" "antrea-ods-debian"; do
    # flow-aggregator and ODS images are the same for Antrea standard and advanced. We just need to update it once.
    if [ "$flavor" = "standard" -a "${img}" = "flow-aggregator-debian" ] || [ "$flavor" = "standard" -a "${img}" = "antrea-ods-debian" ]; then continue; fi
    docker load -i "${img}-${antrea_version}.tar.gz"
    docker tag "antrea/${img}:${antrea_version}" "${ARTIFACTORY_REPO}/${img}:${antrea_version}"
    echo === Pushing "${ARTIFACTORY_REPO}/${img}:${antrea_version}" ===
    docker push "${ARTIFACTORY_REPO}/${img}:${antrea_version}"
    echo "${ARTIFACTORY_REPO}/${img}:${antrea_version}" >> ../../publish_images.txt
  done
  popd
  rm -rf "${zip_dir}"
done

#### Multi-cluster debian ubi
for base_os in debian ubi; do
  echo === Downloading Multi-cluster-controller $base_os Image ===
  zip_file="antrea-multicluster-${base_os}-${antrea_bin_version}.zip"
  zip_dir="antrea-multicluster-${base_os}-${antrea_bin_version}"
  wget "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${build_number}/publish/cayman_antrea/multi-cluster/${zip_file}"
  unzip "${zip_file}"
  docker load -i "${zip_dir}/images/antrea-mc-controller-${antrea_version}.tar.gz"
  docker tag "antrea/antrea-mc-controller-${base_os}:${antrea_version}" "${ARTIFACTORY_REPO}/antrea-mc-controller-${base_os}:${antrea_version}"
  echo === Pushing "${ARTIFACTORY_REPO}/antrea-mc-controller-${base_os}:${antrea_version}" ====
  docker push "${ARTIFACTORY_REPO}/antrea-mc-controller-${base_os}:${antrea_version}"
  echo "${ARTIFACTORY_REPO}/antrea-mc-controller-${base_os}:${antrea_version}" >> publish_images.txt
done

#### IDPS debian ubi
#### suricata
for base_os in debian ubi; do
  echo === Downloading IDPS $base_os Image ===
  zip_file="antrea-idps-${base_os}-${antrea_bin_version}.zip"
  zip_dir="antrea-idps-${base_os}-${antrea_bin_version}"
  wget "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${build_number}/publish/cayman_antrea/idps/${zip_file}"
  unzip "${zip_file}"
  docker load -i "${zip_dir}/images/antrea-idps-${antrea_version}.tar.gz"
  docker tag projects.packages.broadcom.com/antreainterworking/idps-${base_os}:${antrea_version} ${ARTIFACTORY_URL}/antreainterworking/idps-${base_os}:${antrea_version}
  echo === Pushing "${ARTIFACTORY_REPO}/idps-${base_os}:${antrea_version}" ====
  docker push "${ARTIFACTORY_REPO}/idps-${base_os}:${antrea_version}"
  echo "${ARTIFACTORY_REPO}/idps-${base_os}:${antrea_version}" >> publish_images.txt
  if [ "$base_os" = "debian" ]; then
    # suricata image is based on ubuntu actually.
    # suricata image is the same for idps-debian and idps-ubi zip, only need to upload it once.
    docker load -i "${zip_dir}/images/antrea-suricata-${antrea_version}.tar.gz"
    echo === Pushing "${ARTIFACTORY_REPO}/suricata:${antrea_version}" ====
    docker tag projects.packages.broadcom.com/antreainterworking/suricata:${antrea_version} ${ARTIFACTORY_URL}/antreainterworking/suricata:${antrea_version}
    docker push "${ARTIFACTORY_REPO}/suricata:${antrea_version}"
    echo "${ARTIFACTORY_REPO}/suricata:${antrea_version}" >> publish_images.txt
  fi
done


#### operator
wget "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${build_number}/publish/openshift/operator/VERSION" -O operator_version_file
operator_version="$(cat operator_version_file)"
echo ====== Publishing Operator "${operator_version}" Image ======
echo === Downloading Operator Image ===
wget "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${build_number}/publish/openshift/operator/images/antrea-operator-${operator_version}.tar.gz" -O operator.tar.gz
docker load -i operator.tar.gz && rm -f operator.tar.gz
docker tag "localhost:5000/vmware.io/antrea/antrea-operator:${operator_version}" "${ARTIFACTORY_REPO}/antrea-operator:${operator_version}"
echo === Pushing "${ARTIFACTORY_REPO}/antrea-operator:${operator_version}" ===
docker push "${ARTIFACTORY_REPO}/antrea-operator:${operator_version}"
echo "${ARTIFACTORY_REPO}/antrea-operator:${operator_version}" >> publish_images.txt

#### nsx-management-proxy-package

proxy_build_number=$(find_component_build $build_number nsx-management-proxy-package)
imgpkg_build_number=$(find_component_build $proxy_build_number cayman_imgpkg)
echo "=== Installing imgpkg tool from official build ob-${imgpkg_build_number} ==="
imgpkg_build_url="http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${imgpkg_build_number}/publish/lin64/imgpkg/executables"
imgpkg_linux_path=$(curl -s "https://buildapi.lvn.broadcom.net/ob/deliverable/?build=${imgpkg_build_number}&path__startswith=publish/lin64/imgpkg/executables/imgpkg-linux-amd64-" | jq -r '._list[0].path')
wget -O imgpkg.gz "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${imgpkg_build_number}/${imgpkg_linux_path}"
gzip -d -c imgpkg.gz > imgpkg && rm -f imgpkg.gz
chmod +x imgpkg
./imgpkg version

wget "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${build_number}/publish/nsx-management-proxy-package/VERSION" -O proxy_version_file
proxy_version="$(cat proxy_version_file)"
echo ====== Publishing nsx-management-proxy-bundle "${proxy_version}" ======
echo === Downloading nsx-management-proxy-bundle ===
wget "http://build-squid.vcfd.broadcom.net/build/mts/release/bora-${build_number}/publish/nsx-management-proxy-package/images/nsx-management-proxy-bundle-${proxy_version}.tar" -O nsx-management-proxy.tar
echo === Pushing "${ARTIFACTORY_REPO}/nsx-management-proxy-bundle:${proxy_version}" ===
./imgpkg copy --tar nsx-management-proxy.tar --to-repo "${ARTIFACTORY_REPO}/nsx-management-proxy-bundle" && rm -f nsx-management-proxy.tar
echo "${ARTIFACTORY_REPO}/nsx-management-proxy-bundle:${proxy_version}" >> publish_images.txt

echo ====== Cleaning up All Antrea Releated Images ======
docker images | grep -v '<none>' | grep antrea | awk '{print $1":"$2}' | xargs -r docker rmi || true
docker images | grep '<none>' | awk '{print $3}' | xargs -r docker rmi || true

echo ====== Promoting All Images to Broadcom Artifactory artifactory.vcfd.broadcom.net ======
images=$(cat publish_images.txt | awk -F'/' '{print $2"/"$3}' | awk -F':' '{print $1}')
for image in $images; do
  echo "Promoting image $image from antrea-docker-dev-local to antrea-docker-prod-local"
  curl -i -u$artifactory_user:$artifactory_token -X POST "https://artifactory.vcfd.broadcom.net/artifactory/api/docker/antrea-docker-dev-local/v2/promote" -H "Content-Type: application/json" -d "{\"targetRepo\":\"antrea-docker-prod-local\",\"dockerRepository\":\"$image\"}"
done

echo ====== Finished Publishing Images ======
cat publish_images.txt

popd >/dev/null
