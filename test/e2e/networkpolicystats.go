package e2e

import (
	"context"
	"errors"
	"time"

	"antrea.io/antrea/pkg/client/clientset/versioned/typed/stats/v1alpha1"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

type TrafficField string

type PolicyStatType string

const (
	TrafficPackets  TrafficField = "packets"
	TrafficBytes    TrafficField = "bytes"
	TrafficSessions TrafficField = "sessions"

	// testANNPNetworkPolicyStatsWithDropAction
)

type TrafficExpectation struct {
	operation func(want, got int64) bool
	field     TrafficField
	want      int64

	got    int64
	failed bool
}

type PolicyStatExpectation struct {
	Name      string
	Namespace string

	TrafficExpectations []*TrafficExpectation
	err                 error
}

type AntreaNetworkPolicyStatExpectation struct {
	*PolicyStatExpectation
}

type AntreaClusterNetworkPolicyStatExpectation struct {
	*PolicyStatExpectation
}

type K8sNetworkPolicyStatExpectation struct {
	*PolicyStatExpectation
}

type NetworkPolicyStatExpectation struct {
	Expectations []NetworkPolicyStatExpectationChecker
}

func (e NetworkPolicyStatExpectation) GetSummary() (int, int) {
	var numPassed, numFailed int
	for _, exp := range e.Expectations {
		pass, fail := exp.Summary()
		numFailed += fail
		numPassed += pass
	}
	return numPassed, numFailed
}

type NetworkPolicyStatExpectationChecker interface {
	Check(ctx context.Context, client v1alpha1.StatsV1alpha1Interface) bool
	Summary() (int, int)
	PrintSummary()
	Error() error
}

func (sc *NetworkPolicyStatExpectation) WithExpectation(expectation NetworkPolicyStatExpectationChecker) *NetworkPolicyStatExpectation {
	if expectation != nil {
		sc.Expectations = append(sc.Expectations, expectation)
	}
	return sc
}

func (sc *NetworkPolicyStatExpectation) Check(client v1alpha1.StatsV1alpha1Interface) error {
	var errs error
	for _, exp := range sc.Expectations {
		wait.PollUntilContextTimeout(context.Background(), 5*time.Second, defaultTimeout, true, func(ctx context.Context) (bool, error) {
			return exp.Check(ctx, client), nil
		})
		errs = errors.Join(errs, exp.Error())
	}

	return errs
}

func (e *PolicyStatExpectation) Error() error {
	return e.err
}

func (e *PolicyStatExpectation) Summary() (int, int) {
	var fail int
	for _, exp := range e.TrafficExpectations {
		if exp.failed {
			fail++
		}
	}

	return len(e.TrafficExpectations) - fail, fail
}

func (e *PolicyStatExpectation) PrintSummary() {
	for _, exp := range e.TrafficExpectations {
		if exp.failed {
			log.Infof("Expectation: %#v", exp)
		}
	}
}

func (exp *AntreaNetworkPolicyStatExpectation) Check(context context.Context, client v1alpha1.StatsV1alpha1Interface) bool {
	res, err := client.AntreaNetworkPolicyStats(exp.Namespace).Get(context, exp.Name, metav1.GetOptions{})
	if err != nil {
		exp.err = err
		log.Infof("fetching Antrea network policy %q in namespace %q failed with error: %v", exp.Name, exp.Namespace, err)
		return false
	}

	exp.err = nil
	hasFailedExpectations := false
	for _, exp := range exp.TrafficExpectations {
		var got int64
		switch exp.field {
		case TrafficBytes:
			got = res.TrafficStats.Bytes
		case TrafficPackets:
			got = res.TrafficStats.Packets
		case TrafficSessions:
			got = res.TrafficStats.Sessions
		default:
			// Log here
			continue
		}

		exp.got = got
		if !exp.operation(exp.want, got) {
			exp.failed = true
			hasFailedExpectations = true
		} else {
			exp.failed = false
		}
	}

	return !hasFailedExpectations
}

func (exp *K8sNetworkPolicyStatExpectation) Check(context context.Context, client v1alpha1.StatsV1alpha1Interface) bool {
	res, err := client.NetworkPolicyStats(exp.Namespace).Get(context, exp.Name, metav1.GetOptions{})
	if err != nil {
		exp.err = err
		log.Infof("fetching k8s network policy %q in namespace %q failed with error: %v", exp.Name, exp.Namespace, err)
		return false
	}

	exp.err = nil
	hasFailedExpectations := false
	for _, exp := range exp.TrafficExpectations {
		var got int64
		switch exp.field {
		case TrafficBytes:
			got = res.TrafficStats.Bytes
		case TrafficPackets:
			got = res.TrafficStats.Packets
		case TrafficSessions:
			got = res.TrafficStats.Sessions
		default:
			// Log here
			continue
		}

		exp.got = got
		if !exp.operation(exp.want, got) {
			exp.failed = true
			hasFailedExpectations = true
		} else {
			exp.failed = false
		}
	}

	return !hasFailedExpectations
}

func (exp *AntreaClusterNetworkPolicyStatExpectation) Check(context context.Context, client v1alpha1.StatsV1alpha1Interface) bool {
	res, err := client.AntreaClusterNetworkPolicyStats().Get(context, exp.Name, metav1.GetOptions{})
	if err != nil {
		exp.err = err
		log.Infof("fetching antrea cluster network policy %q failed with error: %v", exp.Name, err)
		return false
	}

	exp.err = nil
	hasFailedExpectations := false
	for _, exp := range exp.TrafficExpectations {
		var got int64
		switch exp.field {
		case TrafficBytes:
			got = res.TrafficStats.Bytes
		case TrafficPackets:
			got = res.TrafficStats.Packets
		case TrafficSessions:
			got = res.TrafficStats.Sessions
		default:
			// Log here
			continue
		}

		exp.got = got
		if !exp.operation(exp.want, got) {
			exp.failed = true
			hasFailedExpectations = true
		} else {
			exp.failed = false
		}
	}

	return !hasFailedExpectations
}
