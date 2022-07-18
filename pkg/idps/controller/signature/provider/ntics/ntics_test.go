// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package ntics

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	validTestLicense   = "450J0-D0316-088N8-07AAP-AMRP7"
	invalidTestLicense = "00000-00000-00000-00000-00000"

	validTestSecret   = "abcdefghijklmnopqrstuvwxyz0123456789"
	invalidTestSecret = "000000000000000000000000000000000000"

	validTestClientID   = "1234-5678-9012-3456-7890"
	invalidTestClientID = "0000-0000-0000-0000-0000"

	// #nosec G101: false positive triggered by variable name which includes "token"
	validTestToken = "eyJhbGciOiJIUzUxMiJ9"
	// #nosec G101: false positive triggered by variable name which includes "token"
	expiredTestToken = "eyJhbGciOiJIUzUxMiJ0"
	// #nosec G101: false positive triggered by variable name which includes "token"
	invalidTestToken = "00000000000000000000"

	errorCodeInvalidLicense           = 100003
	errorCodeInvalidClientCredentials = 100002
	errorCodeExpiredToken             = 100012
	errorCodeInvalidToken             = 100013

	errorMessageInvalidLicense           = "The license is invalid or expired."
	errorMessageInvalidClientCredentials = "The client credentials are invalid."
	errorMessageInvalidToken             = "The access token is invalid."
	errorMessageExpiredToken             = "The access token is expired."
	errorMessageMissingToken             = "Missing Authentication Token."
	errorMessageRequireCredentials       = "Authorization header requires 'Credential' parameter."

	pathGetSignatureData = "download_signature"
)

type errorResponse struct {
	Code    int    `json:"error_code,omitempty"`
	Message string `json:"error_message"`
}

var (
	invalidLicenseResponse           = errorResponse{errorCodeInvalidLicense, errorMessageInvalidLicense}
	invalidClientCredentialsResponse = errorResponse{errorCodeInvalidClientCredentials, errorMessageInvalidClientCredentials}
	invalidTokenResponse             = errorResponse{errorCodeInvalidToken, errorMessageInvalidToken}
	expiredTokenResponse             = errorResponse{errorCodeExpiredToken, errorMessageExpiredToken}
	missingTokenResponse             = errorResponse{Message: errorMessageMissingToken}
	requireCredentialsResponse       = errorResponse{Message: errorMessageRequireCredentials}

	testBaseURL string
)

func mockRouteRegister(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusForbidden)
		w.Write(marshal(missingTokenResponse))
		return
	}

	var requestObj registerInfo
	decodeBody(r.Body, &requestObj)

	if requestObj.LicenseKeys[0] == validTestLicense && requestObj.DeviceType == deviceType {
		w.WriteHeader(http.StatusOK)
		responseObj := authenticateInfo{
			ClientID:     validTestClientID,
			ClientSecret: validTestSecret,
		}
		w.Write(marshal(responseObj))
	} else {
		w.WriteHeader(http.StatusForbidden)
		w.Write(marshal(invalidLicenseResponse))
	}
}

func mockRouteAuthenticate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusForbidden)
		responseObj := errorResponse{
			Message: errorMessageMissingToken,
		}
		w.Write(marshal(responseObj))
		return
	}

	var requestObj authenticateInfo
	decodeBody(r.Body, &requestObj)

	if requestObj.ClientID == validTestClientID && requestObj.ClientSecret == validTestSecret {
		w.WriteHeader(http.StatusOK)
		responseObj := tokenInfo{
			AccessToken: validTestToken,
			TokenType:   tokenType,
			ExpiresIn:   3600,
			Scope:       "app_id_scope,waf_ruleset_scope,url_reputation_scope,idps_scope,file_reputation_scope",
		}
		w.Write(marshal(responseObj))
	} else {
		w.WriteHeader(http.StatusForbidden)
		w.Write(marshal(invalidClientCredentialsResponse))
	}
}

func mockRouteGetSignatureVersions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusForbidden)
		w.Write(marshal(requireCredentialsResponse))
		return
	}

	token := r.Header.Get("Authorization")
	if token == validTestToken {
		w.WriteHeader(http.StatusOK)
		responseObj := signatureVersions{
			SignatureVersions: []signatureVersion{
				{
					Version:     "1119",
					LastUpdated: "2022-08-11T23:43:28Z",
					VersionName: "IDPSSignatures.1119.2022-08-11T23:41:58Z",
				},
				{
					Version:     "1118",
					LastUpdated: "2022-08-11T16:41:29Z",
					VersionName: "IDPSSignatures.1118.2022-08-11T16:40:19Z",
				},
			},
		}
		w.Write(marshal(responseObj))
	} else if token == expiredTestToken {
		w.WriteHeader(http.StatusForbidden)
		w.Write(marshal(expiredTokenResponse))
	} else {
		w.WriteHeader(http.StatusForbidden)
		w.Write(marshal(invalidTokenResponse))
	}
}

func mockRouteGetSignatureInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusForbidden)
		w.Write(marshal(requireCredentialsResponse))
		return
	}

	token := r.Header.Get("Authorization")
	if token == validTestToken {
		w.WriteHeader(http.StatusOK)
		responseObj := signatureInfo{
			SignatureURL:   fmt.Sprintf("%s/%s", testBaseURL, pathGetSignatureData),
			Sha256Checksum: "1",
		}
		w.Write(marshal(responseObj))
	} else if token == expiredTestToken {
		w.WriteHeader(http.StatusForbidden)
		w.Write(marshal(expiredTokenResponse))
	} else {
		w.WriteHeader(http.StatusForbidden)
		w.Write(marshal(invalidTokenResponse))
	}
}

func mockRouteGetSignatureData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	contents := "alert http any any -> any any (msg:\"Block-index-test1\"; content:\"index1.php\"; http_uri; sid:1;)"
	w.Write([]byte(contents))
}

func mockNTICSAPIServer() *httptest.Server {
	f := func(w http.ResponseWriter, r *http.Request) {
		uri := strings.TrimLeft(r.RequestURI, "/")
		uri = strings.Split(uri, "?")[0]
		switch uri {
		case pathRegister:
			mockRouteRegister(w, r)

		case pathAuthenticate:
			mockRouteAuthenticate(w, r)

		case pathGetSignatureVersions:
			mockRouteGetSignatureVersions(w, r)

		case pathGetSignatureInfo:
			mockRouteGetSignatureInfo(w, r)

		case pathGetSignatureData:
			mockRouteGetSignatureData(w, r)
		}
	}
	return httptest.NewServer(http.HandlerFunc(f))
}

func TestNTICSAPIs(t *testing.T) {
	server := mockNTICSAPIServer()
	defer server.Close()
	testBaseURL = server.URL

	t.Run("Register", func(t *testing.T) { testRegister(t, testBaseURL) })
	t.Run("Authenticate", func(t *testing.T) { testAuthenticate(t, testBaseURL) })
	t.Run("GetSignatureVersions", func(t *testing.T) { testGetSignatureVersions(t, testBaseURL) })
	t.Run("GetSignatureInfo", func(t *testing.T) { testGetSignatureInfo(t, testBaseURL) })
	t.Run("GetSignatureData", func(t *testing.T) { testGetSignatureData(t, testBaseURL) })
}

func testRegister(t *testing.T, baseURL string) {
	testCases := []struct {
		name                 string
		license              string
		expectedError        error
		expectedClientID     string
		expectedClientSecret string
	}{
		{
			name:                 "Valid license",
			license:              validTestLicense,
			expectedError:        nil,
			expectedClientID:     validTestClientID,
			expectedClientSecret: validTestSecret,
		},
		{
			name:                 "Invalid license",
			license:              invalidTestLicense,
			expectedError:        fmt.Errorf("got 403 response from URL %s/%s: %s", baseURL, pathRegister, marshal(invalidLicenseResponse)),
			expectedClientID:     "",
			expectedClientSecret: "",
		},
	}

	for _, tt := range testCases {
		n := &SignatureProvider{
			nsxLicenseKey: tt.license,
			apiBaseURL:    baseURL,
		}
		assert.Equal(t, tt.expectedError, n.register())
		assert.Equal(t, tt.expectedClientID, n.clientID)
		assert.Equal(t, tt.expectedClientSecret, n.clientSecret)
	}
}

func testAuthenticate(t *testing.T, baseURL string) {
	testCases := []struct {
		name          string
		clientID      string
		clientSecret  string
		expectedError error
		expectedToken string
	}{
		{
			name:          "Registered client ID and secret",
			clientID:      validTestClientID,
			clientSecret:  validTestSecret,
			expectedError: nil,
			expectedToken: validTestToken,
		},
		{
			name:          "Unregistered client ID and secret",
			clientID:      invalidTestClientID,
			clientSecret:  invalidTestSecret,
			expectedError: fmt.Errorf("got 403 response from URL %s/%s: %s", baseURL, pathAuthenticate, marshal(invalidClientCredentialsResponse)),
			expectedToken: "",
		},
	}
	for _, tt := range testCases {
		n := &SignatureProvider{
			clientID:     tt.clientID,
			clientSecret: tt.clientSecret,
			apiBaseURL:   baseURL,
		}
		assert.Equal(t, tt.expectedError, n.authenticate())
		assert.Equal(t, tt.expectedToken, n.apiToken)
	}
}

func testGetSignatureVersions(t *testing.T, baseURL string) {
	testCases := []struct {
		name          string
		apiToken      string
		expectedError error
	}{
		{
			name:          "Valid API token",
			apiToken:      validTestToken,
			expectedError: nil,
		},
		{
			name:          "Expired API token",
			apiToken:      expiredTestToken,
			expectedError: fmt.Errorf("got 403 response from URL %s/%s: %s", baseURL, pathGetSignatureVersions, marshal(expiredTokenResponse)),
		},
		{
			name:          "Invalid API token",
			apiToken:      invalidTestToken,
			expectedError: fmt.Errorf("got 403 response from URL %s/%s: %s", baseURL, pathGetSignatureVersions, marshal(invalidTokenResponse)),
		},
	}

	for _, tt := range testCases {
		n := &SignatureProvider{
			nsxLicenseKey:      tt.apiToken,
			apiBaseURL:         baseURL,
			apiToken:           tt.apiToken,
			apiTokenExpiration: time.Now().Add(time.Second * time.Duration(3600)),
		}
		versions, err := n.getSignatureVersions()
		assert.Equal(t, tt.expectedError, err)
		if err == nil {
			assert.GreaterOrEqual(t, len(versions), 1)
		}
	}
}

func testGetSignatureInfo(t *testing.T, baseURL string) {
	testCases := []struct {
		name          string
		apiToken      string
		expectedError error
	}{
		{
			name:          "Valid API token",
			apiToken:      validTestToken,
			expectedError: nil,
		},
		{
			name:          "Expired API token",
			apiToken:      expiredTestToken,
			expectedError: fmt.Errorf("got 403 response from URL %s/%s?signature_version=1119: %s", baseURL, pathGetSignatureInfo, marshal(expiredTokenResponse)),
		},
		{
			name:          "Invalid API token",
			apiToken:      invalidTestToken,
			expectedError: fmt.Errorf("got 403 response from URL %s/%s?signature_version=1119: %s", baseURL, pathGetSignatureInfo, marshal(invalidTokenResponse)),
		},
	}

	for _, tt := range testCases {
		n := &SignatureProvider{
			nsxLicenseKey:      validTestLicense,
			apiBaseURL:         baseURL,
			apiToken:           tt.apiToken,
			apiTokenExpiration: time.Now().Add(time.Second * time.Duration(3600)),
		}
		sigInfo, err := n.getSignatureInfo(1119)
		assert.Equal(t, tt.expectedError, err)
		if err == nil {
			assert.NotEmpty(t, sigInfo.SignatureURL)
			assert.NotEmpty(t, sigInfo.Sha256Checksum)
		}
	}
}

func testGetSignatureData(t *testing.T, baseURL string) {
	n := &SignatureProvider{
		apiBaseURL: baseURL,
	}
	sigInfo := &signatureInfo{SignatureURL: fmt.Sprintf("%s/%s", baseURL, pathGetSignatureData), Sha256Checksum: "6888e7579b8b08a1fd6e56f2d11f300c1f108ff582618c73e2530a6235e69bec"}
	_, err := n.getSignatureData(sigInfo)
	assert.NoError(t, err)
}
