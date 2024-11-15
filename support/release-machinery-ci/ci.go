package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/joho/godotenv"

	"gitlab-vmw.devops.broadcom.net/core-build/tanzu-release-machinery/artifact/pkg/bwartifact"
	"gitlab-vmw.devops.broadcom.net/core-build/tanzu-release-machinery/ci-tooling/pkg/sdk/buildweb"
	"gitlab-vmw.devops.broadcom.net/core-build/tanzu-release-machinery/gitlab/pkg/sdk/gitlab"
)

const (
	relMachLogCtx = "release-machinery-ci"
	buildLogCtx   = "build cayman_antrea_package"
	publishLogCtx = "publish cayman_antrea_package"
)

const (
	AntreaVersionPath = "antrea_version.config"
)

func downloadFile(url string, filepath string) error {
	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

func downloadVersionFiles(buildIDDesc string) error {
	antreaPath := "publish/lin64/antrea/ANTREA_VERSIONS"
	url := bwartifact.GetBuildsquidPath(buildIDDesc, antreaPath)

	err := downloadFile(url, AntreaVersionPath)
	if err != nil {
		return err
	}
	return nil
}

/*
  Maintainer Notes:
  # Logging
  - There are helper functions for logging and commenting to gitlab MR: logInfo, logErr, logInfoAndPost, postToGitlab
  - Try to minimize information posted to gitlab MR, as
    - mutliple retries substantially increase the number of comments
	- it becomes difficult to navigate between review and CI comments.
  - Avoid sending error messages to gitlab, rather make the CI console logs verbose.

  # Publishing
  - Artifacts from buildweb official builds uploaded to staging artifactory
  - Artifacts from buildweb sandbox builds uploaded to dev artifactory
*/

func main() {
	buildStartTime := time.Now()
	logInfo(buildLogCtx, "started")
	gobuildTarget := "cayman_antrea_package" // same as the one found at support/gobuild/__init__.py
	timeout := time.Hour * 2

	bw, makeErr := buildweb.MakeBuildwebBuildOptions()
	if makeErr != nil {
		logErr(buildLogCtx, makeErr)
		os.Exit(1)
	}

	/*
	  Step 1: Trigger the build
	*/
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Trigger the build
	triggerBuildResult, triggerErr := bw.TriggerBuildwebBuild(ctx, gobuildTarget)
	logInfo(buildLogCtx, "trigger result", triggerBuildResult.String())
	if triggerErr != nil {
		logErr(buildLogCtx, triggerErr)
		os.Exit(1)
	}
	logInfoAndPost(buildLogCtx, "triggered successfully", triggerBuildResult.URL)

	buildResult, buildErr := bw.WatchBuildwebBuild(ctx, triggerBuildResult)
	// log build results irrespective of success or failures
	logInfo(buildLogCtx, "build result", buildResult.String())
	if buildErr != nil {
		logErr(buildLogCtx, buildErr)
		os.Exit(1)
	}

	logInfoAndPost(buildLogCtx, "completed successfully", time.Since(buildStartTime).String(), buildResult.BuildURL)

	// parse image versions
	err := downloadVersionFiles(buildResult.BuildIDDesc)
	if err != nil {
		logErr(buildLogCtx, triggerErr)
		os.Exit(1)
	}
	if err := godotenv.Load(AntreaVersionPath, "../../antrea/versions.config"); err != nil {
		logErr(buildLogCtx, err)
		os.Exit(1)
	}

	// comp versions
	interworkingVersion := "v" + os.Getenv("INTERWORKING_VERSION")
	antreaVersion := os.Getenv("ANTREA_VERSION")
	// package versions
	tkgVersionSuffix := os.Getenv("PACKAGE_VERSION_SUFFIX")          // eg: tkg.1
	bundleTag := os.Getenv("IMAGE_VERSION") + "-" + tkgVersionSuffix // eg: v2.1.0_vmware.1-tkg.1

	/*
	  Step 2: Publish the build artifacts
	*/
	logInfo(publishLogCtx, "build-squid artifact(s)", "started")

	artifactoryBasePath := fmt.Sprintf("antrea/%s", buildResult.BuildIDDesc)
	bwImagesBaseDir := "publish/lin64/antrea/images/"
	bwBundlesBaseDir := "publish/lin64/antrea/package-bundles/"
	bwCrsBaseDir := "publish/lin64/antrea/package-crs/"
	artifactoryImagesBase := "images"
	artifactoryPackagesBase := "packages"
	artifactoryCrsBase := "custom-resources"

	imageTagMap := map[string]string{
		"antrea-advanced-agent-debian":      antreaVersion, // eg: v1.13.3_vmware.4
		"antrea-advanced-controller-debian": antreaVersion,
		"antrea-advanced-windows":           antreaVersion,
		"interworking-debian":               interworkingVersion,
	}

	publishOpts := []bwartifact.OptionFunc{}   // declare at the beginning
	images := []bwartifact.ImagePublishSpecs{} // debugging purposes

	for compName, imageTag := range imageTagMap {
		imageTarball := fmt.Sprintf("%s-%s.tar.gz", compName, imageTag) // image tarball name: kube-controllers-v3.27.3_vmware.1.tar.gz
		if strings.Contains(compName, "interworking") {
			imageTarball = fmt.Sprintf("%s-%s.tar", compName, imageTag)
		}
		filePath := filepath.Join(bwImagesBaseDir, imageTarball)
		imgPath := filepath.Join(artifactoryImagesBase, compName)
		imgPubSpecs := bwartifact.ImagePublishSpecs{ // initialize; hence a new memory address
			PublishIdentifier: compName,
			BuildwebFilePath:  filePath,
			TargetImagePath:   imgPath,
			TargetImageTag:    imageTag,
		}
		imgFn := bwartifact.WithImagesForPublish(imgPubSpecs) // initialize; hence a new memory address
		publishOpts = append(publishOpts, imgFn)
		images = append(images, imgPubSpecs) // debugging purposes
	}

	log.Println(fmt.Sprintf("IMAGES: %+v", images)) // debugging purposes

	// bundle => antrea-v2.1.0_vmware.1-tkg.1-thick.tar
	bundleTar := fmt.Sprintf("antrea-%s-thick.tar", bundleTag)
	// Package CR => 2.1.0+vmware.1-tkg.1.yaml (1MB)
	pkgCrFilename := fmt.Sprintf("%s+%s-%s.yml", os.Getenv("ANTREA_SEMVER"), os.Getenv("VMWARE_VERSION_SUFFIX"), tkgVersionSuffix)
	pkgMetaCrFilename := "metadata.yml"

	bundlePubSpec := bwartifact.BundlePublishSpecs{
		PublishIdentifier: "antrea-package",
		BuildwebFilePath:  filepath.Join(bwBundlesBaseDir, bundleTar),
		TargetBundlePath:  filepath.Join(artifactoryPackagesBase, "antrea"), // packages/antrea
		BundleLookupTag:   bundleTag,
		PackageCR: &bwartifact.PackageCRPublishSpecs{
			PublishIdentifier: "antrea-package-cr",                              // a unique identifier
			BuildwebFilePath:  fmt.Sprintf("%s%s", bwCrsBaseDir, pkgCrFilename), // download path
			TargetFilePath:    filepath.Join(artifactoryCrsBase, pkgCrFilename), // upload path
		},
		PackageMetadataCR: &bwartifact.FilePublishSpecs{
			PublishIdentifier: "antrea-packageMetadata-cr",
			BuildwebFilePath:  filepath.Join(bwCrsBaseDir, pkgMetaCrFilename),
			TargetFilePath:    filepath.Join(artifactoryCrsBase, pkgMetaCrFilename), // custom-resources/metadata.yml
		},
	}

	bundleFn := bwartifact.WithBundlesForPublish(bundlePubSpec) // initialize; hence a new memory address
	publishOpts = append(publishOpts, bundleFn)
	publishOpts = append(publishOpts, bwartifact.WithIsOfficialBuildEnabled(buildResult.IsOfficialBuild))
	publishOpts = append(publishOpts, bwartifact.WithBuildIDDesc(buildResult.BuildIDDesc))

	if buildResult.IsOfficialBuild {
		publishOpts = append(publishOpts, bwartifact.WithDevArtifactory2(artifactoryBasePath))
	} else {
		publishOpts = append(publishOpts, bwartifact.WithDevArtifactory2(artifactoryBasePath))
	}

	pubResult, pubErr := bwartifact.Publish(publishOpts...)

	// Since publish results can be huge, we log to the CI runner machine only
	// Publish results must be logged irrespective of success or failures; they
	// help in debugging root cause of failure(s)
	logInfo(publishLogCtx, "publish results", pubResult.String())
	if pubErr != nil {
		logErr(publishLogCtx, pubErr)
		os.Exit(1)
	}

	// Same artifact can be published to multiple targets
	// minimalPublishResults maps PublishIdentifier to list of publish URLs
	//
	// usage: for posting to gitlab MR
	// TODO: replace with buildinfo file url when buildinfo generation is integrated
	minimalPublishResults := make(map[string][]string)
	for _, artifactPubRes := range pubResult {
		minimalPublishResults[artifactPubRes.PublishIdentifier] = append(minimalPublishResults[artifactPubRes.PublishIdentifier], artifactPubRes.DestinationURL)
	}

	minimalPublishJson, jsonMarshalErr := json.Marshal(minimalPublishResults)
	if jsonMarshalErr != nil {
		log.Println("WARN: publish: build-squid artifact(s): error converting minimalPublishResults to json:", jsonMarshalErr)
	} else {
		logInfoAndPost(publishLogCtx, "build-squid artifact(s)", "published", string(minimalPublishJson))
	}

	// Post to Gitlab on successful publish
	var pubComment strings.Builder
	for _, item := range pubResult {
		pubComment.WriteString(fmt.Sprintf("\n- id = %s", item.PublishIdentifier))
		pubComment.WriteString(fmt.Sprintf("\n  status = %s", item.Status))
		if item.DestinationURL != "" {
			pubComment.WriteString(fmt.Sprintf("\n  url = %s", item.DestinationURL))
		}
		if item.DestinationURLDigest != "" {
			pubComment.WriteString(fmt.Sprintf("\n  digest = %s", item.DestinationURLDigest))
		}
	}
	postToGitlab(fmt.Sprintf("publish succeeded: %s", pubComment.String()))
	log.Println(buildResult.String())

	os.Exit(0)
}

// logInfoAndPost formulates message with
// additional context details and logs to:
//
// - console
// - gitlab MR comment
func logInfoAndPost(msgCtx, msg string, info ...string) {
	logMsg := logInfo(msgCtx, msg, info...)
	if logMsg == "" {
		return
	}
	postToGitlab(logMsg)
}

// logInfo formulates message with additional
// context details and logs to console.
//
// Returns the formulated log message.
func logInfo(msgCtx, msg string, info ...string) string {
	if msg == "" {
		return ""
	}
	var additionalInfo string
	if len(info) != 0 {
		additionalInfo = strings.Join(info, ", ")
	}
	if additionalInfo != "" {
		msg = fmt.Sprintf("%s: %s", msg, additionalInfo)
	}
	logMsg := fmt.Sprintf("INFO: %s: %s: %s", relMachLogCtx, msgCtx, msg)
	log.Println(logMsg)
	return logMsg
}

// logErr formulates error message with additional
// context details and logs to console.
//
// Returns the formulated log message.
func logErr(msgCtx string, err error) string {
	logMsg := fmt.Sprintf("ERROR: %s: %s: %s", relMachLogCtx, msgCtx, err)
	log.Println(logMsg)
	postToGitlab("ci failed: check jenkins logs to find the error(s)")
	return logMsg
}

// postToGitlab adds a comment to the gitlab MR
// using provided message string.
func postToGitlab(msg string) {
	_, gitlabErr := gitlab.AddMRComment(
		gitlab.WithMRCommentParameters(
			gitlab.WithProjectIDFromEnvKey(gitlab.EnvKeyGitlabMergeRequestTargetProjectId),
			gitlab.WithMRIidFromEnvKey(gitlab.EnvKeyGitlabMergeRequestIid),
			gitlab.WithAccessTokenFromRMCIEnvKey(),
			gitlab.WithBaseURL(gitlab.GitlabVMWBroadcomNet),
		),
		gitlab.WithComment(msg),
	)
	if gitlabErr != nil {
		log.Printf("WARN: %s: add comment to merge request: (%+v)", relMachLogCtx, gitlabErr)
	}
}
