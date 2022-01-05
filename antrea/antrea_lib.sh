#### Build utility functions ####

function fips_make() {
  chmod +x ${GOBUILD_CAYMAN_GO_ROOT}/lin64/bin/go
  chmod +x -R ${GOBUILD_CAYMAN_GO_ROOT}/lin64/pkg/tool/linux_amd64
  mkdir -p "${REPO_ROOT}/gopath"
  mkdir -p "${REPO_ROOT}/gocache"
  mkdir -p "${REPO_ROOT}/goenv"
  GIT_SHA="$(git rev-parse --short HEAD)"
  ANTREA_VER=$(head -n 1 VERSION)

  if [ $# -eq 0 ]; then
    cmd="mkdir -p bin; go env -w CC='x86_64-linux-gnu-gcc'; GOOS=linux go build -o bin -ldflags ' -X ${ANTREA_DOMAIN}/pkg/version.Version=${ANTREA_VER} -X ${ANTREA_DOMAIN}/pkg/version.GitSHA=${GIT_SHA} -X ${ANTREA_DOMAIN}/pkg/version.GitTreeState=clean -X ${ANTREA_DOMAIN}/pkg/version.ReleaseStatus=unreleased' ${ANTREA_DOMAIN}/cmd/..."
  else
    cmd="mkdir -p bin; go env -w CC='x86_64-linux-gnu-gcc'; GOOS=linux $1"
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
		-v ${REPO_ROOT}:/usr/src/${ANTREA_DOMAIN} \
		golang:1.15 /bin/bash -c "${cmd}"
  chmod -R 0755 bin
}


function update_docker() {
  # Update Docker to a version that supports multi-stage builds
  echo  "====== Updating Docker ======"
  chmod a+x install_docker.sh
  sudo ./install_docker.sh
}


function run_python {
  PYTHON="${GOBUILD_CAYMAN_PYTHON_ROOT}/lin64/bin/python"
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
    git reset --hard remotes/origin/topic/${ANTREA_VERSION_DIGIT}-${test_image}-release
    if [ ${ipsec} = 'ipsec' ]; then
      image_name="${test_image}-ipsec"
    else
      rm -f test/e2e/ipsec_test.go
    fi
    fips_make "go test -c -v -x -o bin/e2e-${image_name}-${ANTREA_VERSION} ${ANTREA_DOMAIN}/test/e2e"
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
  rm -rf "${PUBLISH_DIR}/windows"
  mkdir -p "${PUBLISH_DIR}/windows"
  mkdir -p "${PUBLISH_DIR}/windows/etc"
  cp build/yamls/windows/base/conf/antrea-agent.conf "${PUBLISH_DIR}/windows/etc/antrea-agent.conf"
  cp build/yamls/windows/base/conf/antrea-cni.conflist "${PUBLISH_DIR}/windows/etc/antrea-cni.conflist"

  mkdir -p "${PUBLISH_DIR}/windows/bin"
  make docker-windows-bin
  cp bin/antrea-agent.exe "${PUBLISH_DIR}/windows/bin/antrea-agent.exe"
  cp bin/antrea-cni.exe "${PUBLISH_DIR}/windows/bin/antrea-cni.exe"

  DownloadDir="${REPO_ROOT}/download"
  rm -rf "${DownloadDir}"
  mkdir -p "${DownloadDir}"
  CNI_WINDOWS_URL="https://github.com/containernetworking/plugins/releases/download/v0.8.1/cni-plugins-windows-amd64-v0.8.1.tgz"
  wget -q "${CNI_WINDOWS_URL}" -O "${DownloadDir}/cni-plugins-windows.tgz"
  mkdir -p "${DownloadDir}/cni-plugins-windows"
  tar zxf "${DownloadDir}/cni-plugins-windows.tgz" -C "${DownloadDir}/cni-plugins-windows"
  cp "${DownloadDir}/cni-plugins-windows/host-local.exe" "${PUBLISH_DIR}/windows/bin/host-local.exe"

  cp hack/windows/Helper.psm1 "${PUBLISH_DIR}/windows/Helper.psm1"
  cp hack/windows/Start.ps1 "${PUBLISH_DIR}/windows/Start.ps1"
  cp hack/windows/Stop.ps1 "${PUBLISH_DIR}/windows/Stop.ps1"
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
  VCRedistUrl="http://build-artifactory.eng.vmware.com/artifactory/nsbu-windows-local/vcredists.zip"
  TempDir="${REPO_ROOT}/nsx-ovs-temp"
  rm -rf "${TempDir}"
  mkdir -p "${TempDir}"

  cp "${NSXOVS_PATH}" "${DownloadDir}/nsx-ovs.zip"
  wget -q "${VCRedistUrl}" -O "${DownloadDir}/vcredists.zip"
  docker run --rm --user $(id -u):$(id -g) -v "${REPO_ROOT}":/tmp/windows -w /tmp/windows projects.registry.vmware.com/library/busybox /bin/sh -c "unzip -q download/nsx-ovs.zip -d nsx-ovs-temp ; unzip -q download/vcredists.zip -d nsx-ovs-temp"
  OVSDir="${TempDir}/openvswitch"
  OVSDriverDir="${OVSDir}/driver"
  VCRedistDir="${OVSDir}/redist"
  cp -r "${TempDir}/include" "${OVSDir}"
  cp -r "${TempDir}/lib" "${OVSDir}"
  cp -r "${TempDir}/scripts" "${OVSDir}"
  cp -r "${TempDir}/vcredist2017" "${VCRedistDir}"
  cp -r "${TempDir}/ovsext/win10_x64" "${OVSDriverDir}"

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
  rm -rf bin
}
