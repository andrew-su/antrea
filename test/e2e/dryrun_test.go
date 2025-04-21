package e2e

import (
	"fmt"
	"testing"

	crdv1beta1 "antrea.io/antrea/pkg/apis/crd/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "antrea.io/antrea/test/e2e/utils"
)

func TestDryRunNetworkPolicies(t *testing.T) {
	skipIfHasWindowsNodes(t)
	skipIfAntreaPolicyDisabled(t)

	data, err := setupTest(t)
	if err != nil {
		t.Fatalf("Error when setting up test: %v", err)
	}
	defer teardownTest(t, data)

	initialize(t, data, nil)
	defer k8sUtils.Cleanup(namespaces)

	t.Run("TestGroupDefaultDENY", func(t *testing.T) {
		// 	// testcases below require default-deny k8s NetworkPolicies to work
		applyDefaultDenyToAllNamespaces(k8sUtils, namespaces)
		defer cleanupDefaultDenyNPs(k8sUtils, namespaces)

		t.Run("Case=ACNPAllowXBtoA", func(t *testing.T) { testDryRunACNPAllowXBtoA(t, data) })
		t.Run("Case=NPAllowAnytoA", func(t *testing.T) { testDryRunK8sNPAllowAnytoA(t, data) })
	})

	t.Run("Case=ACNPPass", func(t *testing.T) { testDryRunACNPPass(t, data) })
	// t.Run("TestGroupK8sNP", func(t *testing.T) {})
	t.Run("Case=ACNPDenyAnytoA", func(t *testing.T) { testDryRunACNPDenyAnytoA(t, data) })
	t.Run("Case=K8sNPDenyAll", func(t *testing.T) { testDryRunK8sNPDenyAnyToA(t, data) })
}

func dryRunResult(nps *NetworkPolicyStatExpectation) func() {
	return func() {
		passed, failed := nps.GetSummary()
		fmt.Printf("Summary: passed %d, failed %d\n", passed, failed)
		if failed > 0 {
			for _, ex := range nps.Expectations {
				fmt.Printf("expectation %#v\n", ex)
				ex.PrintSummary()
			}
		}
	}
}

// testDryRunACNPAllowXBtoA tests traffic from X/B to pods with label A, after applying the default deny
// k8s NetworkPolicies in all namespaces and a dry-run ACNP to allow X/B to A. Traffic should remain denied.
// Additionally it tests for switching off dry-run for the policy and it should update appropriately
func testDryRunACNPAllowXBtoA(t *testing.T, data *TestData) {
	builder := &ClusterNetworkPolicySpecBuilder{}
	builder = builder.SetName("acnp-allow-xb-to-a").
		SetDryRun(true).
		SetPriority(1.0).
		SetAppliedToGroup([]ACNPAppliedToSpec{{PodSelector: map[string]string{"pod": "a"}}})
	builder.AddIngress(ProtocolTCP, &p80, nil, nil, nil, nil, nil, nil, nil, map[string]string{"pod": "b"}, nil, map[string]string{"ns": getNS("x")},
		nil, nil, nil, nil, nil, crdv1beta1.RuleActionAllow, "", "", nil)

	dryRunReachability := NewReachability(allPods, Dropped)
	dryRunReachability.ExpectSelf(allPods, Connected)
	dryRunPolicy := builder.Get()

	updateReachability := NewReachability(allPods, Dropped)
	updateReachability.Expect(getPod("x", "b"), getPod("x", "a"), Connected)
	updateReachability.Expect(getPod("x", "b"), getPod("y", "a"), Connected)
	updateReachability.Expect(getPod("x", "b"), getPod("z", "a"), Connected)
	updateReachability.ExpectSelf(allPods, Connected)
	builder.SetDryRun(false)
	updatedPolicy := builder.Get()

	nps := &NetworkPolicyStatExpectation{}
	nps.WithExpectation(&AntreaClusterNetworkPolicyStatExpectation{
		PolicyStatExpectation: &PolicyStatExpectation{
			Name: dryRunPolicy.Name,
			TrafficExpectations: []*TrafficExpectation{
				{
					operation: func(want int64, got int64) bool {
						return got >= want
					},
					field: TrafficPackets,
					want:  int64(1),
				},
			},
		},
	})

	testStep := []*TestStep{
		{
			Name:                   "Port 80",
			Reachability:           dryRunReachability,
			NetworkStatExpectation: nps,
			TestResources:          []metav1.Object{dryRunPolicy},
			Ports:                  []int32{80},
			Protocol:               ProtocolTCP,
		}, {
			Name:           "Remove Dry-Run",
			Reachability:   updateReachability,
			TestResources:  []metav1.Object{updatedPolicy},
			Ports:          []int32{80},
			Protocol:       ProtocolTCP,
			CustomTeardown: dryRunResult(nps),
		},
	}
	testCase := []*TestCase{
		{"ACNP Allow X/B to A then Update", testStep},
	}
	executeTestsWithData(t, testCase, data)
}

// testACNPAllowAnytoA tests traffic from any pod to pods with label A, after applying the default deny
// k8s NetworkPolicies in all namespaces and ACNP to allow Any to A.
func testDryRunK8sNPAllowAnytoA(t *testing.T, data *TestData) {
	builder := &NetworkPolicySpecBuilder{}
	builder = builder.SetName(getNS("x"), "k8snp-allow-any-to-a").
		SetDryRun(true).
		SetPodSelector(map[string]string{"pod": "a"}).
		SetTypeIngress().
		AddIngress("", nil, nil, nil, nil, map[string]string{}, nil, nil, nil)

	reachability := NewReachability(allPods, Dropped)
	reachability.ExpectSelf(allPods, Connected)

	policy := builder.Get()

	nps := &NetworkPolicyStatExpectation{}
	nps.WithExpectation(&K8sNetworkPolicyStatExpectation{
		PolicyStatExpectation: &PolicyStatExpectation{
			Name:      policy.Name,
			Namespace: policy.Namespace,
			TrafficExpectations: []*TrafficExpectation{
				{
					operation: func(want int64, got int64) bool {
						return got >= want
					},
					field: TrafficPackets,
					want:  int64(1),
				},
			},
		},
	})

	testStep := []*TestStep{
		{
			Name:                   "Port 80",
			Reachability:           reachability,
			NetworkStatExpectation: nps,
			TestResources:          []metav1.Object{builder.Get()},
			Ports:                  []int32{80},
			Protocol:               ProtocolTCP,
			CustomTeardown:         dryRunResult(nps),
		},
	}
	testCase := []*TestCase{
		{"K8sNP Allow Any to A", testStep},
	}
	executeTestsWithData(t, testCase, data)
}

// testDryRunACNPDenyAnytoA tests traffic from Any pod to pods with label A. With dry-run drop
// rules, the traffic should connect.
func testDryRunACNPDenyAnytoA(t *testing.T, data *TestData) {
	builder := &ClusterNetworkPolicySpecBuilder{}
	builder = builder.SetName("acnp-deny-any-to-a").
		SetDryRun(true).
		SetPriority(1.0).
		SetAppliedToGroup([]ACNPAppliedToSpec{{PodSelector: map[string]string{"pod": "a"}}})
	builder.AddIngress(ProtocolTCP, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, crdv1beta1.RuleActionDrop, "", "", nil)

	reachability := NewReachability(allPods, Connected)
	reachability.ExpectSelf(allPods, Connected)

	policy := builder.Get()

	nps := &NetworkPolicyStatExpectation{}
	nps.WithExpectation(&AntreaClusterNetworkPolicyStatExpectation{
		PolicyStatExpectation: &PolicyStatExpectation{
			Name: policy.Name,
			TrafficExpectations: []*TrafficExpectation{
				{
					operation: func(want int64, got int64) bool {
						return got >= want
					},
					field: TrafficPackets,
					want:  int64(1),
				},
			},
		},
	})

	testStep := []*TestStep{
		{
			Name:                   "Port 80",
			Reachability:           reachability,
			NetworkStatExpectation: nps,
			TestResources:          []metav1.Object{policy},
			Ports:                  []int32{80},
			Protocol:               ProtocolTCP,
			CustomTeardown:         dryRunResult(nps),
		},
	}
	testCase := []*TestCase{
		{"ACNP Deny any to A", testStep},
	}
	executeTestsWithData(t, testCase, data)
}

// testDryRunK8sNPDenyAnyToA tests traffic from any pod to X/A. The dry-run policy does not trigger
// default drop rules and everything can connect
func testDryRunK8sNPDenyAnyToA(t *testing.T, data *TestData) {
	builder := &NetworkPolicySpecBuilder{}
	builder = builder.SetName(getNS("x"), "k8snp-deny-any-to-a").
		SetDryRun(true).
		SetPodSelector(map[string]string{"pod": "a"}).
		SetTypeIngress()

	reachability := NewReachability(allPods, Connected)
	reachability.ExpectSelf(allPods, Connected)

	policy := builder.Get()

	// Drop rules for k8s network policies do not log any stats.

	testStep := []*TestStep{
		{
			Name:          "Port 80",
			Reachability:  reachability,
			TestResources: []metav1.Object{policy},
			Ports:         []int32{80},
			Protocol:      ProtocolTCP,
		},
	}
	testCase := []*TestCase{
		{"K8sNP Deny Any to A", testStep},
	}
	executeTestsWithData(t, testCase, data)
}

// testDryRunACNPPass tests traffic from pods with label B to pod X/A.
// d a dry-run ACNP to allow X/B to A. Traffic should remain denied.
func testDryRunACNPPass(t *testing.T, data *TestData) {
	appliedToGroup := []ACNPAppliedToSpec{{PodSelector: map[string]string{"pod": "a"}, NSSelector: map[string]string{"ns": getNS("x")}}}
	acnpPolicyPassBuilder := &ClusterNetworkPolicySpecBuilder{}
	acnpPolicyPassBuilder = acnpPolicyPassBuilder.SetName("acnp-pass").
		SetPriority(5).
		SetDryRun(true).
		SetAppliedToGroup(appliedToGroup).
		AddIngress(ProtocolTCP, &p80, nil, nil, nil, nil, nil, nil, nil, map[string]string{"pod": "b"}, nil, nil,
			nil, nil, nil, nil, nil, crdv1beta1.RuleActionPass, "", "", nil)

	acnpPolicyBuilder := &ClusterNetworkPolicySpecBuilder{}
	acnpPolicyBuilder = acnpPolicyBuilder.SetName("acnp-policy-to-hit").
		SetPriority(1).
		SetAppliedToGroup(appliedToGroup).
		AddIngress(ProtocolTCP, &p80, nil, nil, nil, nil, nil, nil, nil, map[string]string{"pod": "b"}, nil, nil,
			nil, nil, nil, nil, nil, crdv1beta1.RuleActionDrop, "", "", nil)

	skipK8sNP := &NetworkPolicySpecBuilder{}
	skipK8sNP = skipK8sNP.SetName(getNS("x"), "k8snp-allow-b-to-a").
		SetPodSelector(map[string]string{"pod": "a"}).
		SetTypeIngress().
		AddIngress("", nil, nil, nil, nil, map[string]string{"pod": "b"}, nil, nil, nil)

	target := getPod("x", "a")
	reachability := NewReachability(allPods, Connected)

	reachability.Expect(getPod("x", "b"), target, Dropped)
	reachability.Expect(getPod("y", "b"), target, Dropped)
	reachability.Expect(getPod("z", "b"), target, Dropped)

	reachability.Expect(getPod("y", "a"), target, Dropped)
	reachability.Expect(getPod("z", "a"), target, Dropped)

	reachability.Expect(getPod("x", "c"), target, Dropped)
	reachability.Expect(getPod("y", "c"), target, Dropped)
	reachability.Expect(getPod("z", "c"), target, Dropped)
	reachability.ExpectSelf(allPods, Connected)

	passPolicy := acnpPolicyPassBuilder.Get()
	hitPolicy := acnpPolicyBuilder.Get()
	skipPolicy := skipK8sNP.Get()

	nps := &NetworkPolicyStatExpectation{}
	nps.WithExpectation(&AntreaClusterNetworkPolicyStatExpectation{
		PolicyStatExpectation: &PolicyStatExpectation{
			Name: passPolicy.Name,
			TrafficExpectations: []*TrafficExpectation{
				{
					operation: func(want int64, got int64) bool {
						return got == want
					},
					field: TrafficPackets,
					want:  int64(0),
				},
			},
		},
	}).WithExpectation(&AntreaClusterNetworkPolicyStatExpectation{
		PolicyStatExpectation: &PolicyStatExpectation{
			Name: hitPolicy.Name,
			TrafficExpectations: []*TrafficExpectation{
				{
					operation: func(want int64, got int64) bool {
						return got >= want
					},
					field: TrafficPackets,
					want:  int64(9),
				},
			},
		},
	}).WithExpectation(&K8sNetworkPolicyStatExpectation{
		PolicyStatExpectation: &PolicyStatExpectation{
			Name:      skipPolicy.Name,
			Namespace: skipPolicy.Namespace,
			TrafficExpectations: []*TrafficExpectation{
				{
					operation: func(want int64, got int64) bool {
						return got == want
					},
					field: TrafficPackets,
					want:  int64(0),
				},
			},
		},
	})

	testStep := []*TestStep{
		{
			Name:                   "Port 80",
			Reachability:           reachability,
			NetworkStatExpectation: nps,
			TestResources: []metav1.Object{
				passPolicy,
				hitPolicy,
				skipPolicy,
			},
			Ports:          []int32{80},
			Protocol:       ProtocolTCP,
			CustomTeardown: dryRunResult(nps),
		},
	}
	testCase := []*TestCase{
		{"ACNP pass any to A, K8sNP Allow XB to A", testStep},
	}
	executeTestsWithData(t, testCase, data)
}
