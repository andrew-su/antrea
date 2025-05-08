package e2e

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	crdv1beta1 "antrea.io/antrea/pkg/apis/crd/v1beta1"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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
	// TODO continue to setup
	t.Run("testDryRunANPAllow", func(t *testing.T) {
		//t.Cleanup(exportLogsForSubtest(t, data))
		applyDefaultDenyToAllNamespaces(k8sUtils, namespaces)
		testDryRunANPAllow(t, data)
		cleanupDefaultDenyNPs(k8sUtils, namespaces)
	})
}

func testDryRunANPAllow(t *testing.T, data *TestData) {
	builder1 := &AntreaNetworkPolicySpecBuilder{}
	builder1 = builder1.SetName(getNS("x"), "dryrun-allow").
		SetPriority(1.0).
		SetAppliedToGroup([]ANNPAppliedToSpec{{PodSelector: map[string]string{"pod": "a"}}})
	builder1.AddEgress(ProtocolTCP, &p80, nil, nil, nil, nil, nil, nil, nil, nil, map[string]string{"pod": "b"}, map[string]string{"ns": getNS("y")}, nil,
		nil, nil, nil, nil, crdv1beta1.RuleActionAllow, "", "")

	builder2 := &AntreaNetworkPolicySpecBuilder{}
	builder2 = builder2.SetName(getNS("y"), "non-dryrun-allow").
		SetPriority(1.0).
		SetAppliedToGroup([]ANNPAppliedToSpec{{PodSelector: map[string]string{"pod": "b"}}})
	builder2.AddIngress(ProtocolTCP, &p80, nil, nil, nil, nil, nil, nil, nil, nil, map[string]string{"pod": "a"}, map[string]string{"ns": getNS("x")}, nil,
		nil, nil, nil, nil, crdv1beta1.RuleActionAllow, "", "")

	testStep := []*TestStep{
		{
			Name:  "dryrun-allow",
			Ports: []int32{80},
			TestResources: []metav1.Object{
				builder1.GetWithDryRun(true),
				builder2.Get(),
			},
			Protocol: ProtocolTCP,
		},
	}
	testCase := []*TestCase{
		{"ANNP dry run Allow X/A to Y/B", testStep},
	}
	executeDryRunTests(t, testCase, data)
}

// executeTests runs all the tests in testList and prints results
func executeDryRunTests(t *testing.T, testList []*TestCase, data *TestData) {
	executeDryRunTestsWithData(t, testList, data)
}

// custom func based on antreapolicy_test to run dryrun specific tests
func executeDryRunTestsWithData(t *testing.T, testList []*TestCase, data *TestData) {
	for _, testCase := range testList {
		log.Infof("Running test case %s", testCase.Name)
		for _, step := range testCase.Steps {
			log.Infof("Running step %s of test case %s", step.Name, testCase.Name)
			applyTestStepResources(t, step)
			if step.CustomSetup != nil {
				step.CustomSetup()
			}

			// send packet from pod A to B
			sourcePodNamespace, err := sendPacket(data, "a", "x", "b", "y")
			assert.NoError(t, err)

			// sleep for a bit to let the stats refresh
			time.Sleep(time.Second * 60)

			// check NetworkPolicyStats
			if err := wait.PollUntilContextTimeout(context.Background(), 5*time.Second, defaultTimeout, false, func(ctx context.Context) (bool, error) {
				stats, err := data.crdClient.StatsV1alpha1().AntreaNetworkPolicyStats(sourcePodNamespace).Get(context.TODO(), step.Name, metav1.GetOptions{})
				if err != nil {
					return false, err
				}
				assert.True(t, stats.TrafficStats.Packets > 0)
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

// sends packets based on abbreviated pod names (a, b, c) and namespace names (x, y, z)
// returns the source pod's namespace (non abbreviated)
func sendPacket(data *TestData, srcPod, srcNs, dstPod, dstNs string) (string, error) {
	sourcePodName, sourcePodNamespace, dstPodIP, err := getPodsAndIPs(srcPod, srcNs, dstPod, dstNs)
	if err != nil {
		return "", err
	}

	ncCmd := []string{"nc", "-vz", "-w", "4", dstPodIP, "80"}
	fmt.Printf("DBUG: applying from pod(%s) in namespace(%s) with command: %s\n", sourcePodName, sourcePodNamespace, ncCmd)
	_, testErr, err := data.RunCommandFromPod(sourcePodNamespace, sourcePodName, "c80", ncCmd)
	if err != nil {
		return "", err
	}
	if fmt.Sprintf("Connection to %s 80 port [tcp/http] succeeded!\n", dstPodIP) != testErr {
		return "", errors.New("error sending packets to pod")
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
