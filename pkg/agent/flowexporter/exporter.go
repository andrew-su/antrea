// Copyright 2025 Antrea Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package flowexporter

import (
	"context"
	"fmt"
	"hash/fnv"
	"net"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"antrea.io/antrea/pkg/agent/config"
	"antrea.io/antrea/pkg/agent/controller/noderoute"
	"antrea.io/antrea/pkg/agent/flowexporter/broadcaster"
	"antrea.io/antrea/pkg/agent/flowexporter/connections"
	"antrea.io/antrea/pkg/agent/flowexporter/exporter"
	"antrea.io/antrea/pkg/agent/flowexporter/filter"
	"antrea.io/antrea/pkg/agent/flowexporter/options"
	"antrea.io/antrea/pkg/agent/proxy"
	"antrea.io/antrea/pkg/features"
	"antrea.io/antrea/pkg/ovs/ovsconfig"
	"antrea.io/antrea/pkg/querier"
	"antrea.io/antrea/pkg/util/env"
	k8sutil "antrea.io/antrea/pkg/util/k8s"
	"antrea.io/antrea/pkg/util/objectstore"
	utilwait "antrea.io/antrea/pkg/util/wait"
)

// When initializing flowExporter, a slice is allocated with a fixed size to
// store expired connections. The advantage is every time we export, the connection
// store lock will only be held for a bounded time. The disadvantages are: 1. the
// constant is independent of actual number of expired connections 2. when the
// number of expired connections goes over the constant, the export can not be
// finished in a single round. It could be delayed by conntrack connections polling
// routine, which also acquires the connection store lock. The possible solution
// can be taking a fraction of the size of connection store to approximate the
// number of expired connections, while having a min and a max to handle edge cases,
// e.g. min(50 + 0.1 * connectionStore.size(), 200)
const maxConnsToExport = 64

type FlowExporter struct {
	collectorProto      string
	collectorAddr       string
	exporter            exporter.Interface
	v4Enabled           bool
	v6Enabled           bool
	k8sClient           kubernetes.Interface
	nodeRouteController *noderoute.Controller
	isNetworkPolicyOnly bool
	egressQuerier       querier.EgressQuerier
	podStore            objectstore.PodStore
	l7Listener          *connections.L7Listener

	poller      *connections.Poller
	broadcaster broadcaster.Broadcaster
}

func NewFlowExporter(podStore objectstore.PodStore, proxier proxy.ProxyQuerier, k8sClient kubernetes.Interface, nodeRouteController *noderoute.Controller,
	trafficEncapMode config.TrafficEncapModeType, nodeConfig *config.NodeConfig, v4Enabled, v6Enabled bool, serviceCIDRNet, serviceCIDRNetv6 *net.IPNet,
	ovsDatapathType ovsconfig.OVSDatapathType, proxyEnabled bool, npQuerier querier.AgentNetworkPolicyInfoQuerier, o *options.FlowExporterOptions,
	egressQuerier querier.EgressQuerier, podNetworkWait *utilwait.Group,
	podL7FlowExporterAttrGetter connections.PodL7FlowExporterAttrGetter, l7FlowExporterEnabled bool,
) (*FlowExporter, error) {
	var l7Listener *connections.L7Listener
	var eventMapGetter connections.L7EventMapGetter
	if l7FlowExporterEnabled {
		l7Listener = connections.NewL7Listener(podL7FlowExporterAttrGetter, podStore)
		eventMapGetter = l7Listener
	}
	connBroadcaster := broadcaster.New()
	connTrackDumper := connections.InitializeConnTrackDumper(nodeConfig, serviceCIDRNet, serviceCIDRNetv6, ovsDatapathType, proxyEnabled, filter.NewProtocolFilter(nil)) // Use nil filter because the filter will happen per destination
	poller := connections.NewPoller(connTrackDumper, connBroadcaster, eventMapGetter, connections.PollerConfig{
		PollInterval:          o.PollInterval,
		V4Enabled:             v4Enabled,
		V6Enabled:             v6Enabled,
		ConnectUplinkToBridge: o.ConnectUplinkToBridge,
	})

	if nodeRouteController == nil {
		klog.InfoS("NodeRouteController is nil, will not be able to determine flow type for connections")
	}

	nodeName, err := env.GetNodeName()
	if err != nil {
		return nil, err
	}
	obsDomainID := genObservationID(nodeName)

	klog.InfoS("Retrieveing this Node's UID from K8s", "nodeName", nodeName)
	node, err := k8sClient.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get Node with name %s from K8s: %w", nodeName, err)
	}
	nodeUID := string(node.UID)
	klog.InfoS("Retrieved this Node's UID from K8s", "nodeName", nodeName, "nodeUID", nodeUID)

	var exp exporter.Interface
	if o.FlowCollectorProto == "grpc" {
		exp = exporter.NewGRPCExporter(nodeName, nodeUID, obsDomainID)
	} else {
		var collectorProto string
		if o.FlowCollectorProto == "tls" {
			collectorProto = "tcp"
		} else {
			collectorProto = o.FlowCollectorProto
		}
		exp = exporter.NewIPFIXExporter(collectorProto, nodeName, obsDomainID, v4Enabled, v6Enabled)
	}

	return &FlowExporter{
		collectorProto:      o.FlowCollectorProto,
		collectorAddr:       o.FlowCollectorAddr,
		exporter:            exp,
		v4Enabled:           v4Enabled,
		v6Enabled:           v6Enabled,
		k8sClient:           k8sClient,
		isNetworkPolicyOnly: trafficEncapMode.IsNetworkPolicyOnly(),
		nodeRouteController: nodeRouteController,
		egressQuerier:       egressQuerier,
		podStore:            podStore,
		l7Listener:          l7Listener,

		poller:      poller,
		broadcaster: connBroadcaster,
	}, nil
}

func genObservationID(nodeName string) uint32 {
	h := fnv.New32()
	h.Write([]byte(nodeName))
	return h.Sum32()
}

func (exp *FlowExporter) GetDenyConnPublisher() broadcaster.Publisher {
	return exp.broadcaster
}

func (exp *FlowExporter) Run(stopCh <-chan struct{}) {
	// Start L7 connection flow socket
	if features.DefaultFeatureGate.Enabled(features.L7FlowExporter) {
		go exp.l7Listener.Run(stopCh)
	}

	if exp.nodeRouteController != nil {
		// Wait for NodeRouteController to have processed the initial list of Nodes so that
		// the list of Pod subnets is up-to-date.
		if !cache.WaitForCacheSync(stopCh, exp.nodeRouteController.HasSynced) {
			return
		}
	}

	go exp.poller.Run(stopCh)

	go exp.broadcaster.Start(stopCh)

	for {
		select {
		case <-stopCh:
			return
		}
	}
}

// resolveCollectorAddress resolves the collector address provided in the config to an IP address or
// DNS name. The collector address can be a namespaced reference to a K8s Service, and hence needs
// resolution (to the Service's ClusterIP). The function also returns a server name to be used in
// the TLS handshake (when TLS is enabled).
func (exp *FlowExporter) resolveCollectorAddress(ctx context.Context) (string, string, error) {
	host, port, err := net.SplitHostPort(exp.collectorAddr)
	if err != nil {
		return "", "", err
	}
	ns, name := k8sutil.SplitNamespacedName(host)
	if ns == "" {
		return exp.collectorAddr, "", nil
	}
	svc, err := exp.k8sClient.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve Service: %s/%s", ns, name)
	}
	if svc.Spec.ClusterIP == "" {
		return "", "", fmt.Errorf("ClusterIP is not available for Service: %s/%s", ns, name)
	}
	addr := net.JoinHostPort(svc.Spec.ClusterIP, port)
	dns := fmt.Sprintf("%s.%s.svc", name, ns)
	klog.V(2).InfoS("Resolved Service address", "address", addr)
	return addr, dns, nil
}
