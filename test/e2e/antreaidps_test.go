// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package e2e

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"antrea.io/antrea/pkg/apis/crd/v1alpha2"
	tanzucrd "antrea.io/antrea/pkg/apis/tanzucrd/v1alpha1"
	agentconfig "antrea.io/antrea/pkg/config/agent"
	"antrea.io/antrea/pkg/idps/controller/registration"
)

const (
	timeFormat = "2006-01-02 15:04:05"
)

func TestAntreaIDPS(t *testing.T) {
	skipIfNotAntreaIDPSTest(t)
	skipIfHasWindowsNodes(t)

	data, err := setupTestForAntreaIDPS(t)
	if err != nil {
		t.Fatalf("Error when setting up test: %v", err)
	}
	defer func() {
		teardownTest(t, data)
		teardownAntreaIDPS(t, data)
	}()

	ac := func(config *agentconfig.AgentConfig) {
		config.FeatureGates["TrafficControl"] = true
	}

	if err = data.mutateAntreaConfigMap(nil, ac, true, true); err != nil {
		t.Fatalf("Failed to enable TrafficControl feature: %v", err)
	}

	createMockNSXRegistration(t, data)

	t.Run("testCheckNSXRegistrationState", func(t *testing.T) {
		testCheckNSXRegistrationState(t, data)
	})
	t.Run("testSignatureSync", func(t *testing.T) {
		t.Run("triggeredByNSXLicenseUpdate", func(t *testing.T) {
			testSignatureSyncTriggeredByNSXLicenseUpdate(t, data)
		})
		t.Run("triggeredBySignatureUpdate", func(t *testing.T) {
			testSignatureSyncTriggeredBySignatureUpdate(t, data)
		})
	})
	t.Run("testSuricataAlertEvent", func(t *testing.T) {
		testSuricataAlertEvent(t, data)
	})
}

func createMockNSXRegistration(t *testing.T, data *TestData) {
	encryptedStr, _ := registration.NewEncryptString(fmt.Sprintf("%d", time.Now().Unix()))
	registrationObj := &tanzucrd.NSXRegistration{
		ObjectMeta: metav1.ObjectMeta{
			Name: registration.RegistrationInfoName,
		},
		Timestamp: string(encryptedStr),
	}
	// Clear the resource if existing.
	data.crdClient.TanzuCrdV1alpha1().NSXRegistrations().Delete(context.TODO(), registration.RegistrationInfoName, metav1.DeleteOptions{})
	// Create the resource.
	_, err := data.crdClient.TanzuCrdV1alpha1().NSXRegistrations().Create(context.TODO(), registrationObj, metav1.CreateOptions{})
	assert.NoError(t, err)
}

func testCheckNSXRegistrationState(t *testing.T, data *TestData) {
	err := wait.PollImmediate(5*time.Second, defaultTimeout, func() (bool, error) {
		registrationObj, err := data.crdClient.TanzuCrdV1alpha1().NSXRegistrations().Get(context.TODO(), registration.RegistrationInfoName, metav1.GetOptions{})
		if err != nil {
			t.Logf("Failed to get NSXRegistration object: %v", err)
			return false, nil
		}

		encryptedTimestamp := registration.EncryptedString(registrationObj.Timestamp)
		decryptedTimestamp, err := encryptedTimestamp.Decrypt()
		if err != nil {
			t.Logf("Failed to decrypt registration timestamp string: %v", err)
			return false, nil
		}

		timestamp, err := strconv.ParseInt(decryptedTimestamp, 10, 64)
		if err != nil {
			t.Logf("Invalid registration timestamp format: %v", err)
			return false, nil
		}

		lastRegistrationTimestamp := time.Unix(timestamp, 0)
		nowTimestamp := time.Now()

		allowedTimeDiff := 10 * time.Minute
		registrationExpireTime := 10 * time.Minute

		if lastRegistrationTimestamp.After(nowTimestamp.Add(allowedTimeDiff)) {
			return false, fmt.Errorf("future registration timestamp: %s, now timestamp: %s, allowed time diff %s",
				lastRegistrationTimestamp.Format(timeFormat),
				nowTimestamp.Format(timeFormat),
				allowedTimeDiff.String())
		}

		if nowTimestamp.Add(-allowedTimeDiff).After(lastRegistrationTimestamp.Add(registrationExpireTime)) {
			return false, fmt.Errorf("expired registration timestamp %s, now timestamp: %s, expire time: %s, allowed time diff: %s",
				lastRegistrationTimestamp.Format(timeFormat),
				nowTimestamp.Format(timeFormat),
				registrationExpireTime.String(),
				allowedTimeDiff.String())
		}

		t.Logf("NSX registration state is ok")
		return true, nil
	})
	assert.NoError(t, err)
}

func testSignatureSyncTriggeredByNSXLicenseUpdate(t *testing.T, data *TestData) {
	// Update the Secret which stores the NSX license.
	nsxLicense := testOptions.nsxLicense
	cmd := fmt.Sprintf("kubectl -n kube-system create secret generic antrea-idps-licenses --save-config --dry-run=client --from-literal=nsx-license=%s -o yaml | kubectl apply -f -", nsxLicense)
	_, _, stderr, err := data.provider.RunCommandOnNode(controlPlaneNodeName(), cmd)
	assert.NoError(t, err)
	if stderr != "" {
		t.Fatalf(stderr)
	}

	idpsControllerPod, err := data.getAntreaIDPSController()
	assert.NoError(t, err)

	// Check the NSX license file in the IDPS controller container.
	err = wait.PollImmediate(5*time.Second, defaultTimeout, func() (done bool, err error) {
		stdout, stderr, err := data.RunCommandFromPod(idpsControllerPod.Namespace, idpsControllerPod.Name, idpsControllerContainerName, []string{"sh", "-c", "cat /var/run/antrea/idps/licenses/nsx-license"})
		if err != nil || stderr != "" {
			t.Logf("Failed to read NSX license file, err: %v, stderr: %v, retry in 5s", err, stderr)
			return false, nil
		}
		if nsxLicense != strings.TrimSpace(stdout) {
			t.Log("Not found expected NSX license, retry in 5s")
			return false, nil
		}
		t.Log("Found expected NSX license")
		return true, nil
	})
	require.NoError(t, err, "Timeout validating NSX license")

	idpsAgentPod, err := data.getAntreaIDPSAgentOnNode(controlPlaneNodeName())
	assert.NoError(t, err)

	// Check if the IDPSSignatureProviderInfo object is created.
	err = wait.PollImmediate(5*time.Second, defaultTimeout, func() (done bool, err error) {
		_, err = data.crdClient.TanzuCrdV1alpha1().IDPSSignatureProviderInfos().Get(context.TODO(), "ntics", metav1.GetOptions{})
		if err != nil {
			t.Log("Failed to get the IDPSSignatureProviderInfo object for NTICS, retry in 5s")
			return false, nil
		}
		t.Log("Found the IDPSSignatureProviderInfo object for NTICS")
		return true, nil
	})
	assert.NoError(t, err)

	verifySignatureFile(t, data, idpsAgentPod.Namespace, idpsAgentPod.Name)
}

func testSignatureSyncTriggeredBySignatureUpdate(t *testing.T, data *TestData) {
	time.Sleep(time.Second)
	// Modify the version number of IDPSSignatureProviderInfo object directly to mock the signature update, then IDPS
	// agent will sync the signature data from IDPS controller since IDPSSignatureProviderInfo is updated.
	idpsSignatureProviderInfoObj, err := data.crdClient.TanzuCrdV1alpha1().IDPSSignatureProviderInfos().Get(context.TODO(), "ntics", metav1.GetOptions{})
	assert.NoError(t, err)

	copiedIDPSSignatureProviderInfoObj := idpsSignatureProviderInfoObj.DeepCopy()
	copiedIDPSSignatureProviderInfoObj.SignatureBundle.Version++

	_, err = data.crdClient.TanzuCrdV1alpha1().IDPSSignatureProviderInfos().Update(context.TODO(), copiedIDPSSignatureProviderInfoObj, metav1.UpdateOptions{})
	assert.NoError(t, err)

	idpsAgentPod, err := data.getAntreaIDPSAgentOnNode(controlPlaneNodeName())
	assert.NoError(t, err)

	verifySignatureFile(t, data, idpsAgentPod.Namespace, idpsAgentPod.Name)
}

func verifySignatureFile(t *testing.T, data *TestData, podNamespace, podName string) {
	// Check the file change timestamp of /etc/suricata/rules/ntics.rules which stores the NTICS signature data.
	signatureFile := "/etc/suricata/rules/ntics.rules"
	cmd := fmt.Sprintf("stat %s | grep Change | awk '{print $2,$3}'", signatureFile)
	err := wait.PollImmediate(5*time.Second, defaultTimeout, func() (done bool, err error) {
		stdout, stderr, err := data.RunCommandFromPod(podNamespace, podName, idpsAgentContainerName, []string{"sh", "-c", cmd})
		if err != nil {
			t.Logf("Failed to run command %s on Pod %s", cmd, podName)
			return false, nil
		}
		if stderr != "" {
			t.Logf("Failed to get the change time of file %s: %v", signatureFile, stderr)
			return false, nil
		}
		fileChangeTimestamp, err := time.ParseInLocation(timeFormat, strings.TrimSpace(stdout), time.Local)
		if err != nil {
			t.Logf("Failed to parse the change time of file %s", signatureFile)
			return false, nil
		}

		nowTimestamp := time.Now()
		if fileChangeTimestamp.After(nowTimestamp.Add(time.Second*10)) || fileChangeTimestamp.Before(nowTimestamp.Add(-time.Second*10)) {
			t.Logf("The change timestamp of the file %s is not recent, found change timestamp: %v, now timestamp: %v",
				signatureFile, fileChangeTimestamp.Format(timeFormat), nowTimestamp.Format(timeFormat))
			return false, nil
		}

		t.Logf("The change timestamp of the file %s is recent, found change timestamp: %v, now timestamp: %v",
			signatureFile, fileChangeTimestamp.Format(timeFormat), nowTimestamp.Format(timeFormat))
		return true, nil
	})
	require.NoError(t, err, "Timeout validating signature rules file")
}

func testSuricataAlertEvent(t *testing.T, data *TestData) {
	testAgnhostPodName := "test-idps-agnhost-pod"
	testLabels := map[string]string{"antrea-e2e": testAgnhostPodName}

	name := "test-idps-policy"
	idpsPolicyObj := &tanzucrd.IDPSPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: tanzucrd.IDPSPolicySpec{
			AppliedTo: v1alpha2.AppliedTo{
				PodSelector: &metav1.LabelSelector{MatchLabels: testLabels},
			},
		},
	}

	_, err := data.crdClient.TanzuCrdV1alpha1().IDPSPolicies().Create(context.TODO(), idpsPolicyObj, metav1.CreateOptions{})
	defer data.crdClient.TanzuCrdV1alpha1().IDPSPolicies().Delete(context.TODO(), name, metav1.DeleteOptions{})
	assert.NoError(t, err)
	t.Log("Created IDPS policy successfully")

	var tcObj *v1alpha2.TrafficControl
	err = wait.Poll(2*time.Second, 15*time.Second, func() (bool, error) {
		tcObj, err = data.crdClient.CrdV1alpha2().TrafficControls().Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	assert.NoError(t, err, "Timeout waiting for expected TrafficControl object")
	assert.Equal(t, idpsPolicyObj.Spec.AppliedTo, tcObj.Spec.AppliedTo)
	t.Log("Mirrored TrafficControl is created successfully")

	assert.NoError(t, data.createAgnhostPodOnNode(testAgnhostPodName, data.testNamespace, controlPlaneNodeName(), false))
	err = testData.podWaitForRunning(defaultTimeout, testAgnhostPodName, data.testNamespace)
	assert.NoError(t, err, "Error when waiting for Pod %s to be running", testAgnhostPodName)
	t.Log("Created test Pod successfully")

	idpsAgentPod, err := data.getAntreaIDPSAgentOnNode(controlPlaneNodeName())
	assert.NoError(t, err)

	// Clear the event log file.
	cmd := "truncate -s 0 /log/eve.alert.$(date +%Y-%m-%d).json"
	_, stderr, err := data.RunCommandFromPod(idpsAgentPod.Namespace, idpsAgentPod.Name, idpsSuricataContainerName, []string{"sh", "-c", cmd})
	assert.NoError(t, err)
	assert.Equal(t, "", stderr)

	err = wait.PollImmediate(5*time.Second, defaultTimeout, func() (bool, error) {
		nowTimestamp := time.Now().Format("2006-01-02T15:04")
		_, stderr, err = data.RunCommandFromPod(data.testNamespace, testAgnhostPodName, agnhostContainerName, []string{"curl", "-s", "http://testmynids.org/uid/index.html"})
		assert.NoError(t, err)
		assert.Equal(t, "", stderr)

		cmd = "tail -n1 /log/eve.alert.$(date +%Y-%m-%d).json"
		stdout, stderr, err := data.RunCommandFromPod(idpsAgentPod.Namespace, idpsAgentPod.Name, idpsSuricataContainerName, []string{"sh", "-c", cmd})
		if err != nil || stderr != "" {
			return false, nil
		}
		
		if strings.Contains(stdout, "testmyids.com") && strings.Contains(stdout, nowTimestamp) {
			t.Logf("Found expected Suricata event log")
			return true, nil
		}

		t.Logf("Failed to found expected Suricata event log, retry in 5s")
		return false, nil
	})
	assert.NoError(t, err, "Timeout waiting for expected Suricata event log")
}
