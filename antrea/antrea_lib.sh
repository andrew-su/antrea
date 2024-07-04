#### Build utility functions ####

function check_manifests() {
  make manifest
  diff="$(git status --porcelain --untracked-files=no)"
  if [ ! -z "$diff" ]; then
    echo "Antrea manifests is not up-to-date. Run 'make manifest' to update."
    return 1
  else
    return 0
  fi
}

function fips_make() {
  chmod +x ${GOBUILD_CAYMAN_GO_ROOT}/lin64/bin/go
  chmod +x -R ${GOBUILD_CAYMAN_GO_ROOT}/lin64/pkg/tool/linux_amd64
  mkdir -p "${REPO_ROOT}/gopath"
  mkdir -p "${REPO_ROOT}/gocache"
  mkdir -p "${REPO_ROOT}/goenv"
  GIT_SHA="$(git rev-parse --short HEAD)"
  ANTREA_VER=$(head -n 1 VERSION)

  # antrea/src is a gitsubmodule, the .git file under is a text file containing a path to parent .git/modules/antrea/src.
  # We don't map parent .git/modules/antrea/src to Golang container, so go build fails to get VCS information from .git.
  # We add -buildvcs=false in GOFLAGS to disable this go build behavior.
  if [ $# -eq 0 ]; then
    cmd="mkdir -p bin; go env -w CC='x86_64-linux-gnu-gcc' GOFLAGS='-buildvcs=false' GOOS=linux; go build -o bin -ldflags ' -X ${ANTREA_DOMAIN}/pkg/version.Version=${ANTREA_VER} -X ${ANTREA_DOMAIN}/pkg/version.GitSHA=${GIT_SHA} -X ${ANTREA_DOMAIN}/pkg/version.GitTreeState=clean -X ${ANTREA_DOMAIN}/pkg/version.ReleaseStatus=unreleased' ${ANTREA_DOMAIN}/cmd/..."
  elif [ "$1" = "windows-bin" ]; then
    cmd="mkdir -p bin; go env -w GOFLAGS='-buildvcs=false' GOOS=windows; go build -o bin -ldflags ' -X ${ANTREA_DOMAIN}/pkg/version.Version=${ANTREA_VER} -X ${ANTREA_DOMAIN}/pkg/version.GitSHA=${GIT_SHA} -X ${ANTREA_DOMAIN}/pkg/version.GitTreeState=clean -X ${ANTREA_DOMAIN}/pkg/version.ReleaseStatus=unreleased' ${ANTREA_DOMAIN}/cmd/antrea-cni ${ANTREA_DOMAIN}/cmd/antrea-agent ${ANTREA_DOMAIN}/cmd/antctl"
  else
    cmd="mkdir -p bin; go env -w CC='x86_64-linux-gnu-gcc' GOFLAGS='-buildvcs=false' GOOS=linux; $1"
  fi

	docker run --rm -u $(id -u):$(id -g) \
		-e "GOCACHE=/tmp/gocache" \
		-e "GOPATH=/tmp/gopath" \
		-w /usr/src/${ANTREA_DOMAIN} \
		-v "${REPO_ROOT}/gopath":/tmp/gopath \
		-v "${REPO_ROOT}/gocache":/tmp/gocache \
		-v "${REPO_ROOT}/goenv":/.config/go \
		-v ${GOBUILD_CAYMAN_GO_ROOT}/lin64/src:/usr/local/go/src \
		-v ${GOBUILD_CAYMAN_GO_ROOT}/lin64/pkg:/usr/local/go/pkg \
		-v ${GOBUILD_CAYMAN_GO_ROOT}/lin64/bin:/usr/local/go/bin \
		-v ${GOBUILD_CAYMAN_GO_ROOT}/lin64/go.env:/usr/local/go/go.env \
		-v ${REPO_ROOT}:/usr/src/${ANTREA_DOMAIN} \
		golang:1.19 /bin/bash -c "${cmd}"
  chmod -R 0755 bin
}

function export_dependency_env() {
  export GOBUILD_KUSTOMIZE_BIN_PATH="${GOBUILD_CAYMAN_KUBERNETES_SIGS_KUSTOMIZE_ROOT}/lin64/linux/amd64/kustomize"
  export GOBUILD_HELM_BIN_PATH="${GOBUILD_CAYMAN_HELM_ROOT}/lin64/bin/helm"
}

function run_python {
  PYTHON="${GOBUILD_CAYMAN_PYTHON_ROOT}/lin64+gcc6/bin/python3"
  "${PYTHON}" "$@"
}


function version_ge()
{
    if [[ $1 == $2 ]]
    then
        return 0
    fi
    local IFS=.
    local i ver1=($1) ver2=($2)
    # fill empty fields in ver1 with zeros
    for ((i=${#ver1[@]}; i<${#ver2[@]}; i++))
    do
        ver1[i]=0
    done
    for ((i=0; i<${#ver1[@]}; i++))
    do
        if [[ -z ${ver2[i]} ]]
        then
            # fill empty fields in ver2 with zeros
            ver2[i]=0
        fi
        if ((10#${ver1[i]} > 10#${ver2[i]}))
        then
            return 0
        fi
        if ((10#${ver1[i]} < 10#${ver2[i]}))
        then
            return 1
        fi
    done
    return 0
}

function compile_e2e() {
  ipsec=$1
  shift 1
  for test_image in "$@"
  do
    image_name=${test_image}
    if [ "$image_name" = "multi-cluster" ]; then
        fips_make "go test -c -v -o bin/e2e-${image_name}-${ANTREA_VERSION} ${ANTREA_DOMAIN}/multicluster/test/e2e"
        continue
    fi
    git checkout -f -- test
    if [ ${ipsec} = 'ipsec' ]; then
      image_name="${test_image}-ipsec"
    else
      rm -f test/e2e/ipsec_test.go
    fi
    fips_make "go test -c -v -o bin/e2e-${image_name}-${ANTREA_VERSION} ${ANTREA_DOMAIN}/test/e2e"
  done

  # "${BUILDROOT}/output" will be published to lin64/antrea by antrea_defs.py:CaymanAntreaBuilderLin.install
  OUTPUT_DIR="${BUILDROOT}/output"
  mkdir -p "${OUTPUT_DIR}/executables/"
  for test_image in "$@"
  do
    gzip -c bin/e2e-${image_name}-${ANTREA_VERSION} > ${OUTPUT_DIR}/executables/e2e-${test_image}-${ANTREA_VERSION}.gz
  done
}

function archive_ovs_source() {
  pushd "${OVS_DIR}"
  git archive --format=tar.gz --prefix=openvswitch-${OVS_VER}/ -o openvswitch-${OVS_VER}.tar.gz HEAD
  popd
}

function publish_version_files(){
  # "${BUILDROOT}/output" will be published to lin64/antrea by antrea_defs.py:CaymanAntreaBuilderLin.install
  OUTPUT_DIR="${BUILDROOT}/output"
  mkdir -p "${OUTPUT_DIR}/manifests"
  # Used in cayman_photon when builing antrea image
  echo ANTREA_VERSION=${IMAGE_VERSION} >> "${OUTPUT_DIR}/manifests/version"
  echo ANTREA_BINARY_VERSION=${BINARY_VERSION} >> "${OUTPUT_DIR}/manifests/version"
  echo ANTREA_BRANCH=${BRANCH_NAME} >> "${OUTPUT_DIR}/manifests/version"
  echo ANTREA_BUILD=${BUILD_NUMBER} >> "${OUTPUT_DIR}/manifests/version"
  # Used by cayman_photon support/scripts/customizeOvf/customizeGcOvf.py
  # to read add-on versions in a normalized way
  echo "${IMAGE_VERSION}" > "${PUBLISH_DIR}/VERSION"
}

function build_windows() {
  antrea_deliverable_kind=$1
  image_version=$3
  rm -rf "${PUBLISH_DIR}/windows"
  mkdir -p "${PUBLISH_DIR}/windows"
  mkdir -p "${PUBLISH_DIR}/windows/etc"
  cp build/charts/antrea-windows/conf/antrea-agent.conf "${PUBLISH_DIR}/windows/etc/antrea-agent.conf"
  cp build/charts/antrea-windows/conf/antrea-cni.conflist "${PUBLISH_DIR}/windows/etc/antrea-cni.conflist"

  mkdir -p "${PUBLISH_DIR}/windows/bin"
  # antrea/src is a gitsubmodule, the .git file under is a text file containing a path to parent .git/modules/antrea/src.
  # We don't map parent .git/modules/antrea/src to Golang container, so go build fails to get VCS information from .git.
  # We add -buildvcs=false in GOFLAGS to disable this go build behavior.
  fips_make windows-bin
  cp bin/antrea-agent.exe "${PUBLISH_DIR}/windows/bin/antrea-agent.exe"
  cp bin/antrea-cni.exe "${PUBLISH_DIR}/windows/bin/antrea-cni.exe"
  cp bin/antctl.exe "${PUBLISH_DIR}/windows/bin/antctl.exe"

  DownloadDir="${REPO_ROOT}/download"
  rm -rf "${DownloadDir}"
  mkdir -p "${DownloadDir}"
  CNI_WINDOWS_URL="https://artifactory.eng.vmware.com/artifactory/nsx-ujo-local/cayman_antrea/cni-plugins-windows-amd64-v1.1.1.tgz"
  wget -q "${CNI_WINDOWS_URL}" -O "${DownloadDir}/cni-plugins-windows.tgz"
  mkdir -p "${DownloadDir}/cni-plugins-windows"
  tar zxf "${DownloadDir}/cni-plugins-windows.tgz" -C "${DownloadDir}/cni-plugins-windows"
  cp "${DownloadDir}/cni-plugins-windows/host-local.exe" "${PUBLISH_DIR}/windows/bin/host-local.exe"

  cp hack/windows/Helper.psm1 "${PUBLISH_DIR}/windows/Helper.psm1"
  cp hack/windows/Start-AntreaAgent.ps1 "${PUBLISH_DIR}/windows/Start-AntreaAgent.ps1"
  cp hack/windows/Stop-AntreaAgent.ps1 "${PUBLISH_DIR}/windows/Stop-AntreaAgent.ps1"
  cp hack/windows/Install-OVS.ps1 "${PUBLISH_DIR}/windows/Install-OVS.ps1"
  cp hack/windows/Clean-AntreaNetwork.ps1 "${PUBLISH_DIR}/windows/Clean-AntreaNetwork.ps1"

  # If the NSX OVS is unsigned, set false here.
  if true; then
    sed -i 's|$ImportCertificate = $true|$ImportCertificate = $false|g' "${PUBLISH_DIR}/windows/Install-OVS.ps1"
  fi

  echo "==== NSX OVS build ===="
  if [ "$2" = "signed" ]; then
    find_pattern="openvswitch*-win64.zip"
  else
    find_pattern="openvswitch*-win64-unsigned.zip "
  fi
  NSXOVS_PATH=$(find "${GOBUILD_NSX_OVS_BUILD_ROOT}/windows_x64" -name ${find_pattern})
  TempDir="${REPO_ROOT}/nsx-ovs-temp"
  rm -rf "${TempDir}"
  mkdir -p "${TempDir}"

  cp "${NSXOVS_PATH}" "${DownloadDir}/nsx-ovs.zip"
  docker run --rm --user $(id -u):$(id -g) -v "${REPO_ROOT}":/tmp/windows -w /tmp/windows nsx-ujo-docker-local.artifactory.eng.vmware.com/interworking/busybox /bin/sh -c "unzip -q download/nsx-ovs.zip -d nsx-ovs-temp"
  OVSDir="${TempDir}/openvswitch"
  OVSDriverDir="${OVSDir}/driver"
  cp -r "${TempDir}/include" "${OVSDir}"
  cp -r "${TempDir}/lib" "${OVSDir}"
  cp -r "${TempDir}/scripts" "${OVSDir}"
  cp -r "${TempDir}/ovsext/win10_x64" "${OVSDriverDir}"

  # Copy VC redistributable file
  MSVC_REDISTS_PATH=${GOBUILD_CAYMAN_MSVC_REDISTS_ROOT}/win/exe/1033
  VCRedistDir="${OVSDir}/redist"
  rm -rf "${VCRedistDir}" && mkdir -p ${VCRedistDir}
  cp ${MSVC_REDISTS_PATH}/vcredist_x64.exe "${VCRedistDir}/"

  pushd "${TempDir}"
  zip --verbose -r "${PUBLISH_DIR}/windows/ovs-win64.zip" openvswitch
  popd

  antrea_windows_deliverables="antrea-windows-${antrea_deliverable_kind}-${ANTREA_VERSION_DIGIT}"
  antrea_windows_deliverables_tkg="antrea-windows-${antrea_deliverable_kind}"
  echo "{BUILD_NUMBER}" > "${PUBLISH_DIR}/windows/build_number.txt"
  pushd "${PUBLISH_DIR}/windows"
  zip --verbose -r "${PUBLISH_DIR}/${antrea_windows_deliverables}.zip" *
  popd
  cp "${PUBLISH_DIR}/${antrea_windows_deliverables}.zip" "${PUBLISH_DIR}/windows/${antrea_windows_deliverables_tkg}.zip"
  mv "${PUBLISH_DIR}/windows" "${PUBLISH_DIR}/windows-${antrea_deliverable_kind}"

  # Prepare Windows images
  if [ -n "$image_version" ]; then
    build_and_sign_windows_image "${antrea_deliverable_kind}" "${image_version}" "${OVSDir}"
    # Publish scripts
    publish_windows_scripts "${antrea_deliverable_kind}"
  fi

  rm -rf bin "${DownloadDir}" "${TempDir}"
}

function prepare_windows_image_files() {
  antrea_deliverable_kind=$1
  container_files_path=$2
  ovs_dir=$3

  cp -r ${ovs_dir} ${container_files_path}/openvswitch
  antrea_dir=${container_files_path}/antrea
  container_bin_dir=${antrea_dir}/bin
  mkdir -p ${container_bin_dir}
  container_cni_dir=${antrea_dir}/cni
  mkdir -p ${container_cni_dir}
  windows_publish_dir="${PUBLISH_DIR}/windows-${antrea_deliverable_kind}"
  windows_bins="${windows_publish_dir}/bin"
  cp "${windows_bins}/antctl.exe" "${container_bin_dir}/antctl.exe"
  cp "${windows_bins}/antrea-agent.exe" "${container_bin_dir}/antrea-agent.exe"
  cp "${windows_bins}/antrea-cni.exe" "${container_cni_dir}/antrea.exe"
  cp "${windows_bins}/host-local.exe" "${container_cni_dir}/host-local.exe"
  cp hack/windows/Install-OVS.ps1 "${antrea_dir}/Install-OVS.ps1"
}

function build_and_sign_windows_image() {
  antrea_deliverable_kind=$1
  image_version=$2
  ovs_dir=$3

  container_files_path="windows_container_files"
  rm -rf ${container_files_path} && mkdir -p ${container_files_path}
  prepare_windows_image_files "${antrea_deliverable_kind}" "${container_files_path}" "${ovs_dir}"

  image_dir="${PUBLISH_DIR}/windows-${antrea_deliverable_kind}/images"
  rm -rf "${image_dir}" && mkdir -p "${image_dir}"
  ${REPO_ROOT}/build/images/build-windows.sh --dockerfile build/images/Dockerfile.build.windows.tkg --local-dir ${container_files_path} --agent-tag ${image_version}
  image_id=$(tar -xOf antrea-windows.tar manifest.json | jq -r '.[0].Config' | sed "s/^blobs\///; s/\//:/g")
  gzip -9 -f antrea-windows.tar
  mv antrea-windows.tar.gz "${image_dir}/antrea-${antrea_deliverable_kind}-windows-${image_version}.tar.gz"
  image_digest_filename="antrea-${antrea_deliverable_kind}-windows-${image_version}-image-digests.txt"
  echo "antrea-${antrea_deliverable_kind}-windows@${image_id}" > "${image_dir}/${image_digest_filename}"
  checksum_filename="antrea-${antrea_deliverable_kind}-windows-${image_version}-image-checksums.txt"
  pushd "${image_dir}"
  sha256sum -- * > ${checksum_filename}
  gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename}.asc" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
  popd
  rm -rf ${container_files_path}
}

function publish_windows_scripts() {
  antrea_deliverable_kind=$1
  scripts_dir="${PUBLISH_DIR}/windows-${antrea_deliverable_kind}/scripts"
  rm -rf "${scripts_dir}" && mkdir -p "${scripts_dir}"
  cp hack/windows/Clean-AntreaNetwork.ps1 "${scripts_dir}/Clean-AntreaNetwork.ps1"
  # Updates "container" as the default OVSRunMode in Clean-AntreaNetwork.ps1.
  sed -i 's|$OVSRunMode = "service"|$OVSRunMode = "container"|g' "${scripts_dir}/Clean-AntreaNetwork.ps1"
}

function generate_windows_manifests() {
    antrea_deliverable_kind=$1
    image_version=$2
    binary_version=$3
    manifests_dir="${PUBLISH_DIR}/windows-${antrea_deliverable_kind}/manifests"
    rm -rf "${manifests_dir}" && mkdir -p "${manifests_dir}"
    IMG_NAME=antrea/antrea-windows IMG_TAG=${image_version} ${REPO_ROOT}/hack/generate-manifest-windows.sh --include-ovs --mode release > "${manifests_dir}/antrea-windows-${binary_version}.yml"
}

function prepare_whereabouts_tgz() {
  # docker volume source cannot be just "."
  local dest_dir="$(readlink -f $1)"
  local whereabouts_basename="$(basename "$(ls ${GOBUILD_CAYMAN_WHEREABOUTS_ROOT}/lin64/whereabouts/images/whereabouts-*.tar.gz | head -1)" .tar.gz)"
  local whereabouts_version="$(echo "${whereabouts_basename}" | awk -F- '{print $2}')"

  cp "${GOBUILD_CAYMAN_WHEREABOUTS_ROOT}/lin64/whereabouts/executables/whereabouts" "${dest_dir}"
  chmod +x "${dest_dir}/whereabouts"
  cd ${dest_dir}
  tar -zcf "whereabouts-${whereabouts_version}.tgz" whereabouts
  rm -f whereabouts
}

function prepare_local_yum_repo() {
  echo "====== Preparing local Photon Yum Repo ======"
  mkdir -p /tmp/photo-iso
  sudo mount -o loop "${GOBUILD_CSC_PHOTON_ROOT}/csc-photon-5.0.0-x86_64.iso" /tmp/photo-iso
  pushd "/tmp/photo-iso"
  run_python -m http.server 8080 &
  popd
  local public_ip_addr=$(ip -f inet -o address show scope global | head -n 1| cut -f 7 -d ' ' | cut -f 1 -d '/')
  export LOCAL_YUM_REPO_URL="http://${public_ip_addr}:8080/RPMS"
}

function stop_local_yum_repo() {
  jobs -l
  ps aux | grep python
  pgrep -P $(jobs -p %?http.server)
  pkill -SIGTERM -P $(jobs -p %?http.server)
  wait %?http.server || echo wait returns error $? as expected
}

function save_image_and_digest() {
  local image_name=$1
  local image_version=$2
  local output_dir=$3
  local image_id="$(docker inspect -f '{{.ID}}' "${image_name}:${image_version}")"
  docker save "${image_name}:${image_version}" | gzip -9 > "${output_dir}/${image_name##*/}-${image_version}.tar.gz"
  digest_filename="${image_name##*/}-${image_version}-image-digests.txt"
  echo "${image_name}@${image_id}" > "${output_dir}/${digest_filename}"
}

function sign_binaries() {
  local checksum_filename=$1
  local output_dir=$2
  local checksum_filename_asc="${checksum_filename}.asc"
  pushd "${output_dir}"
  sha256sum -- * > ${checksum_filename}
  # See other alternative keys in /build/toolchain/noarch/vmware/gpgsign/officialkey/
  gpgsignc textsign -i ${checksum_filename} -o "${checksum_filename_asc}" --hash=sha256 --keyid=${GPG_KEY_ID} ${GPGSIGNC_OPTS}
  popd
}

function generate_flow_visibility_e2e_manifests() {
  local antrea_repo_root=$1
  local binary_version=$2
  local output_dir=$3
  source "$antrea_repo_root/hack/verify-helm.sh"
  if [ -z "${HELM-}" ]; then
    HELM="$(verify_helm $GOBUILD_HELM_BIN_PATH)"
  elif ! $HELM version > /dev/null 2>&1; then
    echo "$HELM does not appear to be a valid helm binary"
    return 1
  fi
  FLOW_VISIBILITY_CHART="$antrea_repo_root/test/e2e/charts/flow-visibility"
  $HELM template "$FLOW_VISIBILITY_CHART"  > "${output_dir}/flow-visibility-e2e-${binary_version}.yml"
  $HELM template "$FLOW_VISIBILITY_CHART" --set "secureConnection.enable=true" > "${output_dir}/flow-visibility-tls-e2e-${binary_version}.yml"
}
