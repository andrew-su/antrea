package e2e

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	crdv1beta1 "antrea.io/antrea/pkg/apis/crd/v1beta1"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	. "antrea.io/antrea/test/e2e/utils"
)

func TestNetworkPolicyDryRun(t *testing.T) {
	skipIfHasWindowsNodes(t)
	skipIfAntreaPolicyDisabled(t)

	data, err := setupTest(t)
	if err != nil {
		t.Fatalf("Error when setting up test: %v", err)
	}
	//defer teardownTest(t, data)

	initialize(t, data, nil)

	// tests which need default deny
	t.Run("testDryRunANPAllow", func(t *testing.T) {
		//t.Cleanup(exportLogsForSubtest(t, data))
		applyDefaultDenyToAllNamespaces(k8sUtils, namespaces)
		testDryRunANPAllow(t, data)
		// testDryRunKNPAllow(t, data)
		cleanupDefaultDenyNPs(k8sUtils, namespaces)
	})
	// t.Run("testDryRunANPDrop", func(t *testing.T) { testDryRunANPDrop(t, data) })
	// t.Run("testDryRunANPReject", func(t *testing.T) { testDryRunANPReject(t, data) })
	// t.Run("testDryRunANPPass", func(t *testing.T) { testDryRunANPPass(t, data) })
	// t.Run("testDryRunANPUpdate", func(t *testing.T) { testDryRunANPUpdate(t, data) })
}

type dryRunTestCaseInfo struct {
	// all abbreviated i.e. a and b instead of a-uuid and b-uuid
	sourcePod            string
	sourceNamespace      string
	destinationPod       string
	destinationNamespace string
	port                 string
	expectedOutput       func(string) string
	expectedError        func() string
}

func successOutput(ip string) string {
	return fmt.Sprintf("Connection to %s 80 port [tcp/http] succeeded!\n", ip)
}
func failureProgressOutput(ip string) string {
	return fmt.Sprintf("nc: connect to %s port 80 (tcp) timed out: Operation in progress\n", ip)
}
func failureRefusedOutput(ip string) string {
	return fmt.Sprintf("nc: connect to %s port 80 (tcp) failed: Connection refused\n", ip)
}
func expectedError() string {
	return "command terminated with exit code 1"
}

func testDryRunANPAllow(t *testing.T, data *TestData) {
	dryRunANP := &AntreaNetworkPolicySpecBuilder{}
	dryRunANP = dryRunANP.SetName(getNS("x"), "dryrun-allow").
		SetPriority(1.0).
		SetDryRun(true).
		SetAppliedToGroup([]ANNPAppliedToSpec{{PodSelector: map[string]string{"pod": "a"}}}).
		AddEgress(ProtocolTCP, &p80, nil, nil, nil, nil, nil, nil, nil, nil, map[string]string{"pod": "b"}, map[string]string{"ns": getNS("y")}, nil,
			nil, nil, nil, nil, crdv1beta1.RuleActionAllow, "", "")

	allowANP := &AntreaNetworkPolicySpecBuilder{}
	allowANP = allowANP.SetName(getNS("y"), "non-dryrun-allow").
		SetPriority(1.0).
		SetAppliedToGroup([]ANNPAppliedToSpec{{PodSelector: map[string]string{"pod": "b"}}}).
		AddIngress(ProtocolTCP, &p80, nil, nil, nil, nil, nil, nil, nil, nil, map[string]string{"pod": "a"}, map[string]string{"ns": getNS("x")}, nil,
			nil, nil, nil, nil, crdv1beta1.RuleActionAllow, "", "")

	testInfo := getDryRunANPTestCaseInfo(dryRunANP.Get(), false)
	testInfo.expectedOutput = successOutput

	testStep := []*TestStep{
		{
			Name:  "dryrun-allow",
			Ports: []int32{80},
			TestResources: []metav1.Object{
				dryRunANP.Get(),
				allowANP.Get(),
			},
			Protocol: ProtocolTCP,
		},
	}
	testCase := []*TestCase{
		{"ANNP dry run Allow X/A to Y/B", testStep},
	}
	executeDryRunTests(t, testCase, []dryRunTestCaseInfo{testInfo}, data)
}

func testDryRunANPDrop(t *testing.T, data *TestData) {
	dryRunANP := &AntreaNetworkPolicySpecBuilder{}
	dryRunANP = dryRunANP.SetName(getNS("x"), "dryrun-drop").
		SetPriority(1.0).
		SetDryRun(true).
		SetAppliedToGroup([]ANNPAppliedToSpec{{PodSelector: map[string]string{"pod": "a"}}}).
		AddEgress(ProtocolTCP, &p80, nil, nil, nil, nil, nil, nil, nil, nil, map[string]string{"pod": "b"}, map[string]string{"ns": getNS("y")}, nil, nil, nil, nil, nil, crdv1beta1.RuleActionDrop, "", "")

	dryRunPolicy := dryRunANP.Get()
	testInfo := getDryRunANPTestCaseInfo(dryRunPolicy, false)
	testInfo.expectedOutput = failureProgressOutput
	testInfo.expectedError = expectedError
	fmt.Printf("DBUG: testInfo: %+v\n", testInfo)

	testStep := []*TestStep{
		{
			Name:  "dryrun-drop",
			Ports: []int32{80},
			TestResources: []metav1.Object{
				dryRunPolicy,
			},
			Protocol: ProtocolTCP,
		},
	}
	testCase := []*TestCase{
		{"ANNP dry run Drop X/A to Y/B", testStep},
	}
	executeDryRunTests(t, testCase, []dryRunTestCaseInfo{testInfo}, data)
}

func testDryRunANPReject(t *testing.T, data *TestData) {
	dryRunANP := &AntreaNetworkPolicySpecBuilder{}
	dryRunANP = dryRunANP.SetName(getNS("x"), "dryrun-reject").
		SetPriority(1.0).
		SetDryRun(true).
		SetAppliedToGroup([]ANNPAppliedToSpec{{PodSelector: map[string]string{"pod": "a"}}}).
		AddEgress(ProtocolTCP, &p80, nil, nil, nil, nil, nil, nil, nil, nil, map[string]string{"pod": "b"}, map[string]string{"ns": getNS("y")}, nil,
			nil, nil, nil, nil, crdv1beta1.RuleActionReject, "", "")

	testInfo := getDryRunANPTestCaseInfo(dryRunANP.Get(), false)
	testInfo.expectedOutput = failureRefusedOutput
	testInfo.expectedError = expectedError
	fmt.Printf("DBUG: testInfo: %+v\n", testInfo)

	testStep := []*TestStep{
		{
			Name:  "dryrun-reject",
			Ports: []int32{80},
			TestResources: []metav1.Object{
				dryRunANP.Get(),
			},
			Protocol: ProtocolTCP,
		},
	}
	testCase := []*TestCase{
		{"ANNP dry run Reject X/A to Y/B", testStep},
	}
	executeDryRunTests(t, testCase, []dryRunTestCaseInfo{testInfo}, data)
}

func testDryRunANPPass(t *testing.T, data *TestData) {
	rejectANP := &AntreaNetworkPolicySpecBuilder{}
	rejectANP = rejectANP.SetName(getNS("x"), "non-dryrun-reject").
		SetPriority(2.0).
		SetAppliedToGroup([]ANNPAppliedToSpec{{PodSelector: map[string]string{"pod": "a"}}})
	rejectANP.AddEgress(ProtocolTCP, &p80, nil, nil, nil, nil, nil, nil, nil, nil, map[string]string{"pod": "b"}, map[string]string{"ns": getNS("y")}, nil,
		nil, nil, nil, nil, crdv1beta1.RuleActionReject, "", "")

	dryRunBuilderPolicy := &AntreaNetworkPolicySpecBuilder{}
	dryRunBuilderPolicy = dryRunBuilderPolicy.SetName(getNS("y"), "dryrun-pass").
		SetPriority(1.0).
		SetDryRun(true).
		SetAppliedToGroup([]ANNPAppliedToSpec{{PodSelector: map[string]string{"pod": "b"}}}).
		AddEgress(ProtocolTCP, &p80, nil, nil, nil, nil, nil, nil, nil, nil, map[string]string{"pod": "a"}, map[string]string{"ns": getNS("x")}, nil,
			nil, nil, nil, nil, crdv1beta1.RuleActionPass, "", "")

	testInfo1 := getDryRunANPTestCaseInfo(dryRunBuilderPolicy.Get(), false)
	testInfo1.expectedOutput = failureProgressOutput
	testInfo1.expectedError = expectedError

	testStep1 := []*TestStep{
		{
			Name:  "dryrun-pass",
			Ports: []int32{80},
			TestResources: []metav1.Object{
				rejectANP.Get(),
				dryRunBuilderPolicy.Get(),
			},
			Protocol: ProtocolTCP,
		},
	}
	testCases := []*TestCase{
		{"ANNP dry run Pass X/A to Y/B", testStep1},
	}

	spec := &networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{
			MatchLabels: map[string]string{
				"pod": "b",
			},
		},
		Egress: []networkingv1.NetworkPolicyEgressRule{
			{
				To: []networkingv1.NetworkPolicyPeer{
					{
						PodSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"pod": "c",
							},
						},
					},
				},
			},
		},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
	}
	np, err := k8sUtils.createNetworkPolicyWithDryRun("deny-all", getNS("y"), false, spec)
	assert.NoError(t, err)
	defer func() {
		if err = data.deleteNetworkpolicy(np); err != nil {
			t.Fatalf("Error when deleting network policy: %v", err)
		}
	}()

	testInfo2 := getDryRunANPTestCaseInfo(dryRunBuilderPolicy.Get(), false)
	testInfo2.expectedOutput = failureProgressOutput
	testInfo2.expectedError = expectedError

	testStep2 := []*TestStep{
		{
			Name:  "dryrun-pass",
			Ports: []int32{80},
			TestResources: []metav1.Object{
				np,
				dryRunBuilderPolicy.Get(),
			},
			Protocol: ProtocolTCP,
		},
	}
	testCases = append(testCases,
		&TestCase{"ANNP dry run Pass X/A to Y/B", testStep2},
	)
	executeDryRunTests(t, testCases, []dryRunTestCaseInfo{testInfo1, testInfo2}, data)
}

func testDryRunANPUpdate(t *testing.T, data *TestData) {
	dryRunANP := &AntreaNetworkPolicySpecBuilder{}
	dryRunANP = dryRunANP.SetName(getNS("x"), "dryrun-allow").
		SetPriority(1.0).
		SetDryRun(true).
		SetAppliedToGroup([]ANNPAppliedToSpec{{PodSelector: map[string]string{"pod": "a"}}}).
		AddEgress(ProtocolTCP, &p80, nil, nil, nil, nil, nil, nil, nil, nil, map[string]string{"pod": "b"}, map[string]string{"ns": getNS("y")}, nil,
			nil, nil, nil, nil, crdv1beta1.RuleActionAllow, "", "")

	allowANP := &AntreaNetworkPolicySpecBuilder{}
	allowANP = allowANP.SetName(getNS("y"), "non-dryrun-allow").
		SetPriority(1.0).
		SetAppliedToGroup([]ANNPAppliedToSpec{{PodSelector: map[string]string{"pod": "b"}}})
	allowANP.AddIngress(ProtocolTCP, &p80, nil, nil, nil, nil, nil, nil, nil, nil, map[string]string{"pod": "a"}, map[string]string{"ns": getNS("x")}, nil,
		nil, nil, nil, nil, crdv1beta1.RuleActionAllow, "", "")

	testInfo := getDryRunANPTestCaseInfo(dryRunANP.Get(), false)
	testInfo.expectedOutput = successOutput

	testStep := []*TestStep{
		{
			Name:  "dryrun-allow",
			Ports: []int32{80},
			CustomSetup: func() {
				k8sUtils.CreateOrUpdateANNP(dryRunANP.Get())
			},
			TestResources: []metav1.Object{
				dryRunANP.Get(),
				allowANP.Get(),
			},
			Protocol: ProtocolTCP,
		},
	}
	testCase := []*TestCase{
		{"ANNP dry run Allow X/A to Y/B", testStep},
	}
	executeDryRunTests(t, testCase, []dryRunTestCaseInfo{testInfo}, data)
}

func testDryRunKNPAllow(t *testing.T, data *TestData) {
	spec := &networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{
			MatchLabels: map[string]string{
				"pod": "b",
			},
		},
		Egress: []networkingv1.NetworkPolicyEgressRule{
			{
				To: []networkingv1.NetworkPolicyPeer{
					{
						PodSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"pod": "c",
							},
						},
					},
				},
			},
		},
		PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
	}
	np, err := k8sUtils.createNetworkPolicyWithDryRun("deny-all", getNS("y"), false, spec)
	assert.NoError(t, err)
	defer func() {
		if err = data.deleteNetworkpolicy(np); err != nil {
			t.Fatalf("Error when deleting network policy: %v", err)
		}
	}()

	testStep1 := []*TestStep{
		{
			Name:  "dryrun-pass",
			Ports: []int32{80},
			TestResources: []metav1.Object{
				np,
				np,
			},
			Protocol: ProtocolTCP,
		},
	}
	testCases := []*TestCase{
		{"ANNP dry run Pass X/A to Y/B", testStep1},
	}
	testInfo := getDryRunNPTestCaseInfo(np, false)
	testInfo.expectedOutput = failureProgressOutput
	testInfo.expectedError = expectedError

	executeDryRunTests(t, testCases, []dryRunTestCaseInfo{testInfo}, data)
}

// executeTests runs all the tests in testList and prints results
func executeDryRunTests(t *testing.T, testList []*TestCase, testInfo []dryRunTestCaseInfo, data *TestData) {
	executeDryRunTestsWithData(t, testList, testInfo, data)
}

// custom func based on antreapolicy_test to run dryrun specific tests
func executeDryRunTestsWithData(t *testing.T, testList []*TestCase, testInfo []dryRunTestCaseInfo, data *TestData) {
	// one testInfo for every case->step
	testInfoIndex := 0
	for _, testCase := range testList {
		log.Infof("Running test case %s", testCase.Name)
		for _, step := range testCase.Steps {
			log.Infof("Running step %s of test case %s", step.Name, testCase.Name)
			applyTestStepResources(t, step)
			if step.CustomSetup != nil {
				step.CustomSetup()
			}

			// send packet from pod A to B
			fmt.Printf("DBUG: post-testInfoIndex: %d \n", testInfoIndex)
			sourcePodNamespace, err := sendPacket(data, testInfo[testInfoIndex])
			fmt.Printf("DBUG: pre-testInfoIndex: %d \n", testInfoIndex)
			testInfoIndex++
			assert.NoError(t, err)

			// sleep for a bit to let the stats refresh
			time.Sleep(time.Second * 60)

			// check NetworkPolicyStats
			if err := wait.PollUntilContextTimeout(context.Background(), 5*time.Second, defaultTimeout, false, func(ctx context.Context) (bool, error) {
				// fmt.Printf("DBUG: stats from namespace(%s) for anp name(%s)\n", sourcePodNamespace, step.Name)
				stats, err := data.crdClient.StatsV1alpha1().AntreaNetworkPolicyStats(sourcePodNamespace).Get(context.TODO(), step.Name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				// can't check for packetCount on a Pass rule, as it doesn't generate a metric table rule
				if !strings.Contains(step.Name, "pass") {
					assert.True(t, stats.TrafficStats.Packets > 0)
				}
				return true, nil
			}); err != nil {
				t.Fatalf("Error when waiting for NetworkPolicy stats: %v", err)
			}

			if step.CustomTeardown != nil {
				step.CustomTeardown()
			}
		}
		log.Debug("Cleaning-up all policies and groups created by this Testcase")
		cleanupTestCaseResources(t, testCase)
	}
	allTestList = append(allTestList, testList...)
}

func getDryRunANPTestCaseInfo(dryRunPolicy *crdv1beta1.NetworkPolicy, ingress bool) dryRunTestCaseInfo {
	if ingress {
		return dryRunTestCaseInfo{
			sourcePod:            dryRunPolicy.Spec.AppliedTo[0].PodSelector.MatchLabels["pod"],
			sourceNamespace:      strings.Split(dryRunPolicy.Namespace, "-")[0],
			destinationPod:       dryRunPolicy.Spec.Ingress[0].From[0].PodSelector.MatchLabels["pod"],
			destinationNamespace: strings.Split(dryRunPolicy.Spec.Ingress[0].From[0].NamespaceSelector.MatchLabels["ns"], "-")[0],
			port:                 dryRunPolicy.Spec.Ingress[0].Ports[0].Port.StrVal,
		}
	}
	return dryRunTestCaseInfo{
		sourcePod:            dryRunPolicy.Spec.AppliedTo[0].PodSelector.MatchLabels["pod"],
		sourceNamespace:      strings.Split(dryRunPolicy.Namespace, "-")[0],
		destinationPod:       dryRunPolicy.Spec.Egress[0].To[0].PodSelector.MatchLabels["pod"],
		destinationNamespace: strings.Split(dryRunPolicy.Spec.Egress[0].To[0].NamespaceSelector.MatchLabels["ns"], "-")[0],
		port:                 dryRunPolicy.Spec.Egress[0].Ports[0].Port.StrVal,
	}
}

func getDryRunNPTestCaseInfo(dryRunPolicy *networkingv1.NetworkPolicy, ingress bool) dryRunTestCaseInfo {
	if ingress {
		return dryRunTestCaseInfo{
			sourcePod:            dryRunPolicy.Spec.PodSelector.MatchLabels["pod"],
			sourceNamespace:      strings.Split(dryRunPolicy.Namespace, "-")[0],
			destinationPod:       dryRunPolicy.Spec.Ingress[0].From[0].PodSelector.MatchLabels["pod"],
			destinationNamespace: strings.Split(dryRunPolicy.Spec.Ingress[0].From[0].NamespaceSelector.MatchLabels["ns"], "-")[0],
			port:                 dryRunPolicy.Spec.Ingress[0].Ports[0].Port.StrVal,
		}
	}
	return dryRunTestCaseInfo{
		sourcePod:            dryRunPolicy.Spec.PodSelector.MatchLabels["pod"],
		sourceNamespace:      strings.Split(dryRunPolicy.Namespace, "-")[0],
		destinationPod:       dryRunPolicy.Spec.Egress[0].To[0].PodSelector.MatchLabels["pod"],
		destinationNamespace: strings.Split(dryRunPolicy.Spec.Egress[0].To[0].NamespaceSelector.MatchLabels["ns"], "-")[0],
		port:                 dryRunPolicy.Spec.Egress[0].Ports[0].Port.StrVal,
	}
}

// sends packets based on abbreviated pod names (a, b, c) and namespace names (x, y, z) and
// checks for the expected response, either showing successful or unsuccessful connection
// returns the source pod's namespace (non abbreviated)
func sendPacket(data *TestData, info dryRunTestCaseInfo) (string, error) {
	fmt.Printf("DBUG: info: %+v\n", info)
	sourcePodName, sourcePodNamespace, dstPodIP, err := getPodsAndIPs(info.sourcePod, info.sourceNamespace, info.destinationPod, info.destinationNamespace)
	if err != nil {
		return "", err
	}

	ncCmd := []string{"nc", "-vz", "-w", "4", dstPodIP, "80"}
	fmt.Printf("DBUG: kubectl exec -n %s %s -- %s\n", sourcePodNamespace, sourcePodName, strings.Join(ncCmd, " "))
	_, testErr, err := data.RunCommandFromPod(sourcePodNamespace, sourcePodName, "c80", ncCmd)
	if err != nil && info.expectedError != nil && info.expectedError() != err.Error() {
		return "", errors.New("error - unexpected error received: " + err.Error())
	}
	if info.expectedOutput(dstPodIP) != testErr {
		fmt.Printf("DBUG: info.expected:\n[%s]\nerr:\n[%s]\n", info.expectedOutput(dstPodIP), testErr)
		return "", errors.New("error - unexpected result of sending packet: " + testErr)
	}
	return sourcePodNamespace, nil
}

func getPodsAndIPs(sourcePodPrefix, sourceNamespacePrefix, dstPodPrefix, dstNamespacePrefix string) (string, string, string, error) {
	sourcePodNamespace := getNS(sourceNamespacePrefix)
	for _, pod1 := range allPods {
		if pod1.PodName() == sourcePodPrefix {
			for _, pod2 := range allPods {
				if pod2.PodName() == dstPodPrefix && pod2.Namespace() == getNS(dstNamespacePrefix) {
					dstPod, err := k8sUtils.GetPodsByLabel(pod2.Namespace(), "pod", dstPodPrefix)
					if err != nil {
						return "", "", "", err
					}
					srcPod, err := k8sUtils.GetPodsByLabel(sourcePodNamespace, "pod", sourcePodPrefix)
					if err != nil {
						return "", "", "", err
					}
					return srcPod[0].Name, sourcePodNamespace, dstPod[0].Status.PodIPs[0].IP, nil
				}
			}
		}
	}
	return "", "", "", errors.New("Error - could not find specified pods")
}
