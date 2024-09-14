package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	packagebundle "gitlab.eng.vmware.com/core-build/tanzu-release-machinery/carvel-package/pkg/sdk/package-bundle"
)

/*
Maintainer Notes:
  - These environment variables define absolute paths to image files
    and must be set before invoking build.go
*/
const (
	antreaAgentImageFilePath        = "IMAGE_FILEPATH_ANTREA_AGENT"
	antreaControllerImageFilePath   = "IMAGE_FILEPATH_ANTREA_CONTROLLER"
	antreaInterworkingImageFilePath = "IMAGE_FILEPATH_ANTREA_INTERWORKING"
	antreaWindowsImageFilePath      = "IMAGE_FILEPATH_ANTREA_WINDOWS"
)

/*
Maintainer Notes:
- This call to init will validate required env variables are set
*/
func init() {
	for _, envKey := range []string{antreaAgentImageFilePath, antreaControllerImageFilePath, antreaInterworkingImageFilePath, antreaWindowsImageFilePath} {
		if _, ok := os.LookupEnv(envKey); !ok {
			panic(fmt.Errorf("imageFilePath not set in env as key: %s", envKey))
		}
	}
}

// Cayman project specific build options that needs to be configured
var _packageBundleOptions []packagebundle.PackageBundleOptionFunc

func appendPackageBundleOptions(options []packagebundle.PackageBundleOptionFunc) {
	_packageBundleOptions = append(_packageBundleOptions, options...)
}

func getPackageBundleOptions() []packagebundle.PackageBundleOptionFunc {
	return _packageBundleOptions
}

/*
Maintainer Notes:
- Customize packaging options
*/
func configurePackageBundleOptions() {
	log.Println("package bundle: configurePackageBundleOptions")

	// package versions.
	version, suffix := getAntreaVersion()
	registry := "nsx-ujo-docker-local.artifactory.vcfd.broadcom.net/"

	appendPackageBundleOptions(
		[]packagebundle.PackageBundleOptionFunc{
			packagebundle.WithPackageName("antrea"),
			packagebundle.WithVersion(version),
			packagebundle.WithSubVersion(suffix + "-" + os.Getenv("PACKAGE_VERSION_SUFFIX")),
			packagebundle.WithLocalRegistry(),
			packagebundle.WithImages(
				packagebundle.WithImageOverride(
					packagebundle.WithImageAsRef(registry+"antrea/antrea-advanced-controller-debian:"+os.Getenv("ANTREA_IMAGE_VERSION")),
					packagebundle.WithImageAsFile(os.Getenv(antreaControllerImageFilePath)),
				),
				packagebundle.WithImageOverride(
					packagebundle.WithImageAsRef(registry+"antrea/antrea-advanced-agent-debian:"+os.Getenv("ANTREA_IMAGE_VERSION")),
					packagebundle.WithImageAsFile(os.Getenv(antreaAgentImageFilePath)),
				),
				packagebundle.WithImageOverride(
					packagebundle.WithImageAsRef(registry+"antrea/interworking-debian:"+os.Getenv("INTERWORKING_IMAGE_VERSION")),
					packagebundle.WithImageAsFile(os.Getenv(antreaInterworkingImageFilePath)),
				),
				packagebundle.WithImageOverride(
					packagebundle.WithImageAsRef(registry+"antrea/antrea-windows:"+os.Getenv("ANTREA_IMAGE_VERSION")),
					packagebundle.WithImageAsFile(os.Getenv(antreaWindowsImageFilePath)),
				),
			),
		})
}

func getAntreaVersion() (string, string) {
	version := os.Getenv("ANTREA_VERSION_DIGIT")
	splits := strings.Split(version, "+")
	return splits[0], splits[1]
}

func run() (exitCode int) {
	log.Println("package bundle: build started")
	configurePackageBundleOptions()

	if err := packagebundle.CreatePackageBundle(getPackageBundleOptions()...); err != nil {
		log.Println("package bundle: Failed to create bundle package error:", err.Error())
		return 1
	}
	log.Println("package bundle: build completed successfully")
	return 0
}

func main() {
	os.Exit(run())
}
