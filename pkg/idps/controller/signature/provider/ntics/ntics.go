// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package ntics

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/utils/inotify"

	tanzucrd "antrea.io/antrea/pkg/apis/tanzucrd/v1alpha1"
	crdclientset "antrea.io/antrea/pkg/client/clientset/versioned"
	tanzucrdinformers "antrea.io/antrea/pkg/client/informers/externalversions/tanzucrd/v1alpha1"
	tanzucrdlisters "antrea.io/antrea/pkg/client/listers/tanzucrd/v1alpha1"
	controllerconfig "antrea.io/antrea/pkg/idps/config/controller"
	"antrea.io/antrea/pkg/idps/controller/registration"
	bytesutil "antrea.io/antrea/pkg/idps/util/bytes"
)

const (
	SignatureProviderName = "ntics"

	nsxLicenseFile = "/var/run/antrea/idps/licenses/nsx-license"

	pathRegister             = "1.0/auth/register"
	pathAuthenticate         = "1.0/auth/authenticate"
	pathGetSignatureVersions = "2.0/intrusion-services/signatures/versions"
	pathGetSignatureInfo     = "2.0/intrusion-services/signatures"

	paramSignatureVersion = "signature_version"

	contentType         = "application/json;charset=utf-8"
	headerAuthorization = "Authorization"
	tokenType           = "bearer"
	tokenIDPSScope      = "idps_scope"

	// The bundled signature file downloaded from NTICS is compressed in zip. Here is the hierarchy of files in the
	// compressed zip file:
	// - IDSSignaturesVersion.txt
	// - nsx-ids-bundle.tar.gz.gpg
	//   - nsx-ids-bundle.tar.gz
	//     - antimalware-signatures.json
	//     - antimalware-signatures.rules.gz
	//     - classification.config
	//     - ids-signatures.json
	//     - ids-signatures.tar.gz
	//       - nsx-idps.addrs.yaml
	//       - nsx-idps.ports.yaml
	//       - rules
	//         - nsx-idps.rules
	//         - ET_LICENSE_5_0.txt
	//         - lua
	//     - changelog.json
	filenameNSXIDSBundleTarGzGPG = "nsx-ids-bundle.tar.gz.gpg"
	filenameNSXIDSBundleTarGz    = "nsx-ids-bundle.tar.gz"
	filenameIDSSignaturesTarGz   = "ids-signatures.tar.gz"

	// The files and directories that Suricata needs are listed in the follows:
	fileClassificationConfig     = "classification.config" // nsx-ids-bundle.tar.gz/classification.config
	fileNSXIDPSAddrsYaml         = "nsx-idps.addrs.yaml"   // nsx-ids-bundle.tar.gz/ids-signatures.tar.gz/nsx-idps.addrs.yaml
	fileNSXIDPSPortsYaml         = "nsx-idps.ports.yaml"   // nsx-ids-bundle.tar.gz/ids-signatures.tar.gz/nsx-idps.ports.yaml
	fileNSXIDPSRulesNSXIDPSRules = "rules/nsx-idps.rules"  // nsx-ids-bundle.tar.gz/ids-signatures.tar.gz/rules/nsx-idps.rules
	directoryRulesLua            = "rules/lua"             // nsx-ids-bundle.tar.gz/ids-signatures.tar.gz/rules/lua

	// The desired files and directories will be repacked and some files will be renamed when being repacked.
	renamedFileClassificationConfig     = "idps-suricata.ntics.classification.config"
	renamedFileNSXIDPSAddrsYaml         = "idps-suricata.ntics.addrs.yaml"
	renamedFileNSXIDPSPortsYaml         = "idps-suricata.ntics.ports.yaml"
	renamedFileNSXIDPSRulesNSXIDPSRules = "rules/ntics.rules"
)

var repackFilenameMapping = map[string]string{
	fileClassificationConfig:     renamedFileClassificationConfig,
	fileNSXIDPSAddrsYaml:         renamedFileNSXIDPSAddrsYaml,
	fileNSXIDPSPortsYaml:         renamedFileNSXIDPSPortsYaml,
	fileNSXIDPSRulesNSXIDPSRules: renamedFileNSXIDPSRulesNSXIDPSRules,
}

type registerInfo struct {
	LicenseKeys []string `json:"license_keys"`
	DeviceType  string   `json:"device_type"`
	ClientID    string   `json:"client_id,omitempty"`
}

type authenticateInfo struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

type tokenInfo struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	Scope       string `json:"scope"`
	ExpiresIn   int64  `json:"expires_in"`
}

type signatureVersion struct {
	Version     string `json:"version"`
	LastUpdated string `json:"last_updated"`
	VersionName string `json:"version_name"`
}

type signatureVersions struct {
	SignatureVersions []signatureVersion `json:"signature_versions"`
}

type signatureInfo struct {
	SignatureURL   string `json:"signatures_url"`
	Sha256Checksum string `json:"sha256_checksum"`
}

type SignatureProvider struct {
	signatureProviderInfoInformer     cache.SharedIndexInformer
	signatureProviderInfoLister       tanzucrdlisters.IDPSSignatureProviderInfoLister
	signatureProviderInfoListerSynced cache.InformerSynced
	crdClient                         crdclientset.Interface

	// nsxRegistrationStateController is used to validate the registration to NSX.
	nsxRegistrationStateController *registration.NSXRegistrationStateController

	// apiBaseURL is the base URL of NTICS APIs.
	apiBaseURL string
	// updatePeriod is the period to update the signature.
	updatePeriod time.Duration

	// nsxLicenseKey and deviceType are used to register a device to NTICS, and a client ID and client secret will be returned.
	// deviceType is used to identify current device.
	nsxLicenseKey           string
	nsxLicenseKeyUpdateChan chan struct{}
	nsxLicenseKeyUpdateLock sync.Mutex
	deviceType              string

	// clientID and clientSecret are used to authenticate to NITCS, and an API token will be returned.
	clientID     string
	clientSecret string

	// apiToken is used to authenticate when getting signature information from NTICS.
	apiToken string
	// apiTokenExpiration is the timeout of the apiToken. After timeout, a new apiToken can be obtained with clientID
	// and clientSecret.
	apiTokenExpiration time.Time

	// signatureVersion is the current version of NITCS signature.
	signatureVersion uint32
	// signatureData is a byte slice storing NTICS signature data.
	signatureData       []byte
	signatureUpdateLock sync.RWMutex
}

func NewSignatureProvider(signatureProviderInfoInformer tanzucrdinformers.IDPSSignatureProviderInfoInformer,
	crdClient crdclientset.Interface,
	nsxRegistrationStateController *registration.NSXRegistrationStateController,
	signatureProviderNTICSConfig *controllerconfig.SignatureProviderConfig) *SignatureProvider {
	return &SignatureProvider{
		signatureProviderInfoInformer:     signatureProviderInfoInformer.Informer(),
		signatureProviderInfoLister:       signatureProviderInfoInformer.Lister(),
		signatureProviderInfoListerSynced: signatureProviderInfoInformer.Informer().HasSynced,
		nsxRegistrationStateController:    nsxRegistrationStateController,
		crdClient:                         crdClient,
		apiBaseURL:                        signatureProviderNTICSConfig.APIBaseURL,
		updatePeriod:                      time.Second * time.Duration(signatureProviderNTICSConfig.SyncInterval),
		nsxLicenseKeyUpdateChan:           make(chan struct{}, 1),
		deviceType:                        signatureProviderNTICSConfig.DeviceType,
	}
}

func (n *SignatureProvider) Run(stopCh <-chan struct{}) {
	klog.InfoS("Starting Signature NTICS provider")
	defer klog.InfoS("Shutting down Signature NTICS provider")

	if !cache.WaitForNamedCacheSync("SignatureNTICSProvider", stopCh, n.signatureProviderInfoListerSynced) {
		return
	}

	// TODO: after validating the registration state, unconditionally sync the signature data.
	// Validate the registration state to NSX.
	wait.PollImmediateUntil(5*time.Second, func() (bool, error) {
		if err := n.nsxRegistrationStateController.ValidateRegistrationState(); err != nil {
			klog.ErrorS(err, "failed to validate NSX registration state, retry in 5s")
			return false, nil
		}
		return true, nil
	}, stopCh)

	// Load NSX license on startup.
	if err := n.updateLicense(); err != nil {
		klog.ErrorS(err, "Failed to load NSX license on startup")
	}

	// Start the goroutine to watch the NSX license file.
	go n.nsxLicenseWatcher(stopCh)

	ticker := time.NewTicker(n.updatePeriod)
	defer ticker.Stop()

	for {
		select {
		// Sync the signature data periodically.
		case <-ticker.C:
			if err := n.waitForSyncedSignatureData(); err != nil {
				klog.ErrorS(err, "Failed to sync signature data")
			}

		// After NSX license has been updated, unconditionally sync the signature data.
		case <-n.nsxLicenseKeyUpdateChan:
			if err := n.waitForSyncedSignatureData(); err != nil {
				klog.ErrorS(err, "Failed to sync signature data")
			}

		case <-stopCh:
			break
		}
	}
}

func (n *SignatureProvider) updateLicense() error {
	license, err := ioutil.ReadFile(nsxLicenseFile)
	if err != nil {
		return err
	}
	if len(license) == 0 {
		return fmt.Errorf("license cannot be empty")
	}

	n.nsxLicenseKeyUpdateLock.Lock()
	defer n.nsxLicenseKeyUpdateLock.Unlock()
	n.nsxLicenseKey = strings.TrimSpace(string(license))
	n.nsxLicenseKeyUpdateChan <- struct{}{}

	return nil
}

func (n *SignatureProvider) waitForSyncedSignatureData() error {
	if err := n.nsxRegistrationStateController.ValidateRegistrationState(); err != nil {
		return fmt.Errorf("failed to validate NSX registration state: %w", err)
	}

	return wait.PollImmediate(2*time.Second, 10*time.Second, func() (bool, error) {
		if err := n.syncSignatureData(); err != nil {
			klog.ErrorS(err, "Failed to sync signature data, retry in 2 seconds")
			return false, nil
		}
		return true, nil
	})
}

func (n *SignatureProvider) nsxLicenseWatcher(stopCh <-chan struct{}) {
	klog.InfoS("Starting NSX license watcher")
	defer klog.InfoS("Shutting down NSX license watcher")

	watcher, _ := inotify.NewWatcher()
	defer watcher.Close()
	if err := watcher.AddWatch(nsxLicenseFile, inotify.InCloseWrite); err != nil {
		klog.ErrorS(err, "Failed to start NSX license file inotify watcher", "NSXLicenseFile", nsxLicenseFile)
		return
	}

	for {
		select {
		case <-watcher.Event:
			if err := n.updateLicense(); err != nil {
				klog.ErrorS(err, "Failed to update NSX license")
			}
			klog.InfoS("Updated NSX license successfully")
		case <-stopCh:
			break
		}
	}
}

func (n *SignatureProvider) GetSignatureData() ([]byte, error) {
	n.signatureUpdateLock.RLock()
	defer n.signatureUpdateLock.RUnlock()

	if len(n.signatureData) == 0 {
		return nil, fmt.Errorf("empty signature data")
	}
	return n.signatureData, nil
}

func (n *SignatureProvider) syncSignatureData() error {
	n.signatureUpdateLock.Lock()
	defer n.signatureUpdateLock.Unlock()

	// Get the latest version number of NTICS signature.
	versions, err := n.getSignatureVersions()
	if err != nil {
		return err
	}
	versionVal, _ := strconv.Atoi(versions[0].Version)
	klog.V(4).InfoS("Got the latest version number of the signature", "IDPSSignatureProviderInfo", SignatureProviderName, "Version", versionVal)

	if uint32(versionVal) > n.signatureVersion || len(n.signatureData) == 0 {
		// Get the signature information with the given version number.
		signatureInfo, err := n.getSignatureInfo(versionVal)
		if err != nil {
			return err
		}
		// Get the signature data.
		rawSignatureData, err := n.getSignatureData(signatureInfo)
		if err != nil {
			return err
		}
		// Repack the signature data.
		repackedSignatureData, err := n.repackSignatureData(rawSignatureData)
		if err != nil {
			return err
		}
		// Create or update the IDPSSignatureProviderInfo object for NTICS which stores the signature version and sha256
		// checksum of the repacked signature data.
		err = n.createOrUpdateSignatureProviderObject(uint32(versionVal), repackedSignatureData)
		if err != nil {
			return err
		}

		n.signatureData = repackedSignatureData
		n.signatureVersion = uint32(versionVal)
		klog.InfoS("Synced signature data successfully", "IDPSSignatureProviderInfo", SignatureProviderName, "Version", versionVal)
	}

	return nil
}

// getSignatureData downloads the bundled signature file with given signature information.
func (n *SignatureProvider) getSignatureData(signatureInfo *signatureInfo) ([]byte, error) {
	response, err := http.Get(signatureInfo.SignatureURL)
	if err != nil {
		return nil, fmt.Errorf("failed to get response from URL %s: %w", signatureInfo.SignatureURL, err)
	}
	defer response.Body.Close()

	var signatureData []byte
	// The signature data on the response body is compressed with zip.
	if signatureData, err = ioutil.ReadAll(response.Body); err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	// Check the sha256 checksum.
	checksum := fmt.Sprintf("%x", sha256.Sum256(signatureData))
	if checksum != signatureInfo.Sha256Checksum {
		return nil, fmt.Errorf("got unexpected sha256 checksum value: %s, expected value: %s", checksum, signatureInfo.Sha256Checksum)
	}

	return signatureData, nil
}

// repackSignatureData extracts and repacks the desired files and directories from the downloaded data.
func (n *SignatureProvider) repackSignatureData(rawSignatureData []byte) ([]byte, error) {
	// Extract the data of file nsx-ids-bundle.tar.gz.gpg from the downloaded zip file.
	nsxIDSBundleTarGzGPGBytes, err := bytesutil.ExactTargetFileFromZipBytes(rawSignatureData, filenameNSXIDSBundleTarGzGPG)
	if err != nil {
		return nil, fmt.Errorf("failed to extract data of file %s from bytes: %w", filenameNSXIDSBundleTarGzGPG, err)
	}

	// Extract the data of file nsx-ids-bundle.tar.gz from nsx-ids-bundle.tar.gz.gpg.
	nsxIDSBundleTarGzBytes, err := bytesutil.DecryptGPGBytes(nsxIDSBundleTarGzGPGBytes, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to extract data of file %s: %w", filenameNSXIDSBundleTarGz, err)
	}

	repackedSignatureData := &bytes.Buffer{}
	gzWriter := gzip.NewWriter(repackedSignatureData)
	tarGzWriter := tar.NewWriter(gzWriter)

	// Repack file classification.config in nsx-ids-bundle.tar.gz.
	if err = repackFile(nsxIDSBundleTarGzBytes, fileClassificationConfig, tarGzWriter); err != nil {
		return nil, fmt.Errorf("failed to repack file %s: %w", fileClassificationConfig, err)
	}

	// Extract the data of file ids-signatures.tar.gz from nsx-ids-bundle.tar.gz
	idsSignaturesTarGzBytes, _, err := bytesutil.ExactTargetFileFromTarGzBytes(nsxIDSBundleTarGzBytes, filenameIDSSignaturesTarGz)
	if err != nil {
		return nil, fmt.Errorf("failed to extract data of file %s: %w", filenameIDSSignaturesTarGz, err)
	}

	// Repack file nsx-idps.addrs.yaml, nsx-idps.ports.yaml and rules/nsx-idps.rules in ids-signatures.tar.gz.
	filesToBeRepacked := []string{fileNSXIDPSAddrsYaml, fileNSXIDPSPortsYaml, fileNSXIDPSRulesNSXIDPSRules}
	for _, filename := range filesToBeRepacked {
		if err = repackFile(idsSignaturesTarGzBytes, filename, tarGzWriter); err != nil {
			return nil, fmt.Errorf("failed to repack file %s: %w", filename, err)
		}
	}

	// Repack directory rules/lua in ids-signatures.tar.gz.
	if err = repackDir(idsSignaturesTarGzBytes, directoryRulesLua, tarGzWriter); err != nil {
		return nil, fmt.Errorf("failed to repack directory %s: %w", directoryRulesLua, err)
	}

	tarGzWriter.Close()
	gzWriter.Close()

	return repackedSignatureData.Bytes(), nil
}

func repackFile(sourceTarGzBytes []byte, targetFile string, tarGzWriter *tar.Writer) error {
	// Extract target file bytes.
	targetFileBytes, header, err := bytesutil.ExactTargetFileFromTarGzBytes(sourceTarGzBytes, targetFile)
	if err != nil {
		return fmt.Errorf("failed to extract data of file %s: %w", targetFile, err)
	}
	// If the target filename needs to be renamed, update the name.
	if _, exists := repackFilenameMapping[targetFile]; exists {
		header.Name = repackFilenameMapping[targetFile]
	}
	// Write the file header to the writer.
	if err = tarGzWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write file header %s: %w", header.Name, err)
	}
	// Write the file data to the writer.
	if _, err = tarGzWriter.Write(targetFileBytes); err != nil {
		return fmt.Errorf("failed to write file data %s: %w", header.Name, err)
	}
	return nil
}

func repackDir(sourceTarGzBytes []byte, targetDir string, tarGzWriter *tar.Writer) error {
	// Extract target file bytes.
	targetDirBytes, err := bytesutil.ExactTargetDirFromTarGzBytes(sourceTarGzBytes, targetDir)
	if err != nil {
		return fmt.Errorf("failed to extract data of directory %s: %w", targetDir, err)
	}
	// Write every header and data of files in the directory to writer.
	for header, data := range targetDirBytes {
		if err = tarGzWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("failed to write file header %s: %w", header.Name, err)
		}

		if _, err = tarGzWriter.Write(data); err != nil {
			return fmt.Errorf("failed to write file data %s: %w", header.Name, err)
		}
	}
	return nil
}

// createOrUpdateSignatureProviderObject creates or updates the SignatureProvider object which stores the version and sha256 checksum values.
func (n *SignatureProvider) createOrUpdateSignatureProviderObject(version uint32, signatureData []byte) error {
	signatureProviderObj, err := n.signatureProviderInfoLister.Get(SignatureProviderName)
	if err != nil {
		signatureProviderObj = &tanzucrd.IDPSSignatureProviderInfo{}
		signatureProviderObj.SetName(SignatureProviderName)
		signatureProviderObj.SignatureBundle.Version = version
		signatureProviderObj.SignatureBundle.Sha256Checksum = bytesutil.Sha256Checksum(signatureData)
		if _, err = n.crdClient.TanzuCrdV1alpha1().IDPSSignatureProviderInfos().Create(context.TODO(), signatureProviderObj, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create IDPSSignatureProviderInfo object: %w", err)
		}
		klog.V(4).InfoS("Created IDPSSignatureProviderInfo object successfully", "IDPSSignatureProviderInfo", SignatureProviderName, "Version", version)
		return nil
	}

	// Update the IDPSSignatureProviderInfo object.
	copiedSignatureProviderObj := signatureProviderObj.DeepCopy()
	copiedSignatureProviderObj.SignatureBundle.Version = version
	copiedSignatureProviderObj.SignatureBundle.Sha256Checksum = fmt.Sprintf("%x", sha256.Sum256(signatureData))
	if _, err = n.crdClient.TanzuCrdV1alpha1().IDPSSignatureProviderInfos().Update(context.TODO(), copiedSignatureProviderObj, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update IDPSSignatureProviderInfo object: %w", err)
	}
	klog.V(4).InfoS("Updated IDPSSignatureProviderInfo object successfully", "IDPSSignatureProviderInfo", SignatureProviderName, "Version", version)
	return nil
}

// getSignatureInfo gets the information of signature with given version.
func (n *SignatureProvider) getSignatureInfo(version int) (*signatureInfo, error) {
	if !n.hasValidToken() {
		if err := n.authenticate(); err != nil {
			return nil, fmt.Errorf("failed to authenticate: %w", err)
		}
	}

	params := map[string]string{paramSignatureVersion: strconv.Itoa(version)}
	url := genURL(n.apiBaseURL, pathGetSignatureInfo, params)
	request, _ := http.NewRequest(http.MethodGet, url, nil)
	request.Header.Set(headerAuthorization, n.apiToken)
	response, err := (&http.Client{}).Do(request)
	if err != nil {
		return nil, fmt.Errorf("failed to get response from URL %s: %w", url, err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, handleErrorResponse(response)
	}

	var responseObj signatureInfo
	if err = decodeBody(response.Body, &responseObj); err != nil {
		return nil, fmt.Errorf("failed to decode response from URL %s: %w", url, err)
	}

	return &responseObj, nil
}

// getSignatureVersions gets the versions of signature.
func (n *SignatureProvider) getSignatureVersions() ([]signatureVersion, error) {
	if !n.hasValidToken() {
		if err := n.authenticate(); err != nil {
			return nil, fmt.Errorf("failed to authenticate: %w", err)
		}
	}

	url := genURL(n.apiBaseURL, pathGetSignatureVersions, nil)
	request, _ := http.NewRequest(http.MethodGet, url, nil)
	request.Header.Set(headerAuthorization, n.apiToken)
	response, err := (&http.Client{}).Do(request)
	if err != nil {
		return nil, fmt.Errorf("failed to get response from URL %s: %w", url, err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, handleErrorResponse(response)
	}

	responseObj := signatureVersions{}
	if err = decodeBody(response.Body, &responseObj); err != nil {
		return nil, fmt.Errorf("failed to decode response from URL %s: %w", url, err)
	}

	if len(responseObj.SignatureVersions) == 0 {
		return nil, fmt.Errorf("no signature version is found")
	}

	return responseObj.SignatureVersions, nil
}

func (n *SignatureProvider) authenticate() error {
	// Check if the client secret is valid. If not, register the client to obtain a client secret.
	if n.clientSecret == "" {
		if err := n.register(); err != nil {
			return fmt.Errorf("failed to register the client: %w", err)
		}
	}

	url := genURL(n.apiBaseURL, pathAuthenticate, nil)
	requestObj := authenticateInfo{
		ClientID:     n.clientID,
		ClientSecret: n.clientSecret,
	}
	response, err := http.Post(url, contentType, encodeBody(requestObj)) //nolint
	if err != nil {
		return fmt.Errorf("failed to get response from URL %s: %w", url, err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return handleErrorResponse(response)
	}

	var responseObj tokenInfo
	if err = decodeBody(response.Body, &responseObj); err != nil {
		return fmt.Errorf("failed to decode response from URL %s: %w", url, err)
	}

	if responseObj.TokenType != tokenType {
		return fmt.Errorf("unsupported token type %s", responseObj.TokenType)
	}

	if !strings.Contains(responseObj.Scope, tokenIDPSScope) {
		return fmt.Errorf("the token scope doesn't include %s", tokenIDPSScope)
	}

	n.apiToken = responseObj.AccessToken
	n.apiTokenExpiration = time.Now().Add(time.Second * time.Duration(responseObj.ExpiresIn))

	return nil
}

func (n *SignatureProvider) register() error {
	nsxLicenseKey := func() string {
		n.nsxLicenseKeyUpdateLock.Lock()
		defer n.nsxLicenseKeyUpdateLock.Unlock()
		return n.nsxLicenseKey
	}()

	if nsxLicenseKey == "" {
		return fmt.Errorf("license is not set")
	}

	url := genURL(n.apiBaseURL, pathRegister, nil)
	requestObj := registerInfo{
		LicenseKeys: []string{n.nsxLicenseKey},
		DeviceType:  n.deviceType,
	}
	if n.clientID != "" {
		// Include an existing client ID in the request. If client ID is not specified, a random client ID will be returned
		// in response.
		requestObj.ClientID = n.clientID
	}

	response, err := http.Post(url, contentType, encodeBody(requestObj)) //nolint
	if err != nil {
		return fmt.Errorf("failed to get response from URL %s: %w", url, err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return handleErrorResponse(response)
	}

	var responseObj authenticateInfo
	if err = decodeBody(response.Body, &responseObj); err != nil {
		return fmt.Errorf("failed to decode response from URL %s: %w", url, err)
	}
	n.clientID = responseObj.ClientID
	n.clientSecret = responseObj.ClientSecret

	return nil
}

func (n *SignatureProvider) hasValidToken() bool {
	// To avoid using expired token, treat the token that expires after 60s as expired.
	return n.apiToken != "" && n.apiTokenExpiration.After(time.Now().Add(time.Second*60))
}

func handleErrorResponse(response *http.Response) error {
	message, _ := ioutil.ReadAll(response.Body)
	return fmt.Errorf("got %d response from URL %s: %s", response.StatusCode, response.Request.URL, string(message))
}

func decodeBody(body io.Reader, v interface{}) error {
	if err := json.NewDecoder(body).Decode(v); err != nil {
		return err
	}
	return nil
}

func encodeBody(in interface{}) io.Reader {
	return bytes.NewBuffer(marshal(in))
}

func marshal(in interface{}) []byte {
	data, _ := json.Marshal(in)
	return data
}

func genURL(baseURL, path string, params map[string]string) string {
	url := fmt.Sprintf("%s/%s", baseURL, path)
	if params != nil {
		var paramSlice []string
		for k, v := range params {
			paramSlice = append(paramSlice, fmt.Sprintf("%s=%s", k, v))
		}
		url = fmt.Sprintf("%s?%s", url, strings.Join(paramSlice, "&"))
	}

	return url
}
