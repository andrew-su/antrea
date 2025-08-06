package flowexporter

import (
	"context"
	"fmt"
	"net"
	"time"

	"antrea.io/antrea/pkg/agent/config"
	"antrea.io/antrea/pkg/agent/controller/noderoute"
	"antrea.io/antrea/pkg/agent/flowexporter/connections"
	"antrea.io/antrea/pkg/agent/flowexporter/exporter"
	"antrea.io/antrea/pkg/agent/flowexporter/filter"
	"antrea.io/antrea/pkg/agent/proxy"
	api "antrea.io/antrea/pkg/apis/crd/v1beta1"
	"antrea.io/antrea/pkg/client/informers/externalversions/crd/v1beta1"
	"antrea.io/antrea/pkg/ovs/ovsconfig"
	"antrea.io/antrea/pkg/querier"
	"antrea.io/antrea/pkg/util/env"
	"antrea.io/antrea/pkg/util/objectstore"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

var logLevel klog.Level = 0

type FlowExporter2 struct {
	nodeName    string
	nodeUID     string
	obsDomainID uint32

	v4Enabled bool
	v6Enabled bool

	pollInterval   time.Duration
	targetInformer v1beta1.FlowExporterTargetInformer
	consumers      []exporter.Interface

	dumper connections.ConnTrackDumper
}

type FlowExporterConfig struct {
	// Exporter Settings
	PollInterval     time.Duration
	IPV4Enabled      bool
	IPV6Enabled      bool
	TrafficEncapMode config.TrafficEncapModeType

	K8sClient           kubernetes.Interface
	Proxier             proxy.Proxier
	NodeRouteController *noderoute.Controller

	// Used by ConnTrackDumper constructor
	NodeConfig       *config.NodeConfig
	ServiceCIDRNet   *net.IPNet
	ServiceCIDRNetv6 *net.IPNet
	OVSDatapathType  ovsconfig.OVSDatapathType
	ProxyEnabled     bool

	// Used by L7 Listener
	L7FlowExporterEnabled       bool
	PodL7FlowExporterAttrGetter connections.PodL7FlowExporterAttrGetter
}

func ConnTrackDumperFromConfig(config *FlowExporterConfig) (connections.ConnTrackDumper, error) {
	// TODO: Validate configs are set
	protocolFilter := filter.NewProtocolFilter(nil)
	return connections.InitializeConnTrackDumper(config.NodeConfig, config.ServiceCIDRNet, config.ServiceCIDRNetv6, config.OVSDatapathType, config.ProxyEnabled, protocolFilter), nil
}

func NewFlowExporter2(config *FlowExporterConfig, targetInformer v1beta1.FlowExporterTargetInformer, podStore objectstore.PodStore, npQuerier querier.AgentNetworkPolicyInfoQuerier, egressQuerier querier.EgressQuerier) (*FlowExporter2, error) {
	dumper, err := ConnTrackDumperFromConfig(config)
	if err != nil {
		return nil, fmt.Errorf("error creating conntrack dumper from config: %w", err)
	}

	nodeName, err := env.GetNodeName()
	if err != nil {
		return nil, err
	}
	obsDomainID := genObservationID(nodeName)

	klog.InfoS("Retrieving this Node's UID from K8s", "nodeName", nodeName)
	node, err := config.K8sClient.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get Node with name %s from K8s: %w", nodeName, err)
	}
	nodeUID := string(node.UID)
	klog.InfoS("Retrieved this Node's UID from K8s", "nodeName", nodeName, "nodeUID", nodeUID)

	fe := &FlowExporter2{
		nodeName:    nodeName,
		obsDomainID: obsDomainID,
		nodeUID:     nodeUID,

		pollInterval:   config.PollInterval,
		targetInformer: targetInformer,

		dumper: dumper,
	}

	targetInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    fe.onNewTarget,
		UpdateFunc: fe.onTargetUpdate,
		DeleteFunc: fe.onTargetDelete,
	})

	return fe, nil
}

func (fe *FlowExporter2) onNewTarget(obj interface{}) {
	targetRes := obj.(*api.FlowExporterTarget)
	klog.V(logLevel).InfoS("Received new FlowExporterTarget", "resource", klog.KObj(targetRes))

	// TODO: Validate

	var exp exporter.Interface
	switch targetRes.Spec.Protocol {
	case api.ProtoGRPC:
		// TODO: Validate gRPC config
		exp = exporter.NewGRPCExporter(fe.nodeName, fe.nodeUID, fe.obsDomainID)
	case api.ProtoIPFix:
		// TODO: Validate ipfix config
		var collectorProto api.TransportProtocol
		if targetRes.Spec.IPFixConfig.Transport == api.ProtoTLS {
			collectorProto = api.ProtoTCP
		} else {
			collectorProto = targetRes.Spec.IPFixConfig.Transport
		}
		exp = exporter.NewIPFIXExporter(string(collectorProto), fe.nodeName, fe.obsDomainID, fe.v4Enabled, fe.v6Enabled)
	default:
		klog.V(logLevel).InfoS("invalid protocol for FlowExporterTarget", "resource", klog.KObj(targetRes))
		return
	}

	fe.consumers = append(fe.consumers, exp)

}

func (fe *FlowExporter2) onTargetUpdate(oldObj interface{}, newObj interface{}) {
	targetRes := newObj.(*api.FlowExporterTarget)
	oldTargetRes := newObj.(*api.FlowExporterTarget)
	klog.V(logLevel).InfoS("FlowExporterTarget updated", "old", klog.KObj(oldTargetRes), "new", klog.KObj(targetRes))
}

func (fe *FlowExporter2) onTargetDelete(obj interface{}) {
	targetRes := obj.(*api.FlowExporterTarget)
	klog.V(logLevel).InfoS("FlowExporterTarget deleted", "resource", klog.KObj(targetRes))
}

// Run starts the NodeLatencyMonitor.
func (m *FlowExporter2) Run(stopCh <-chan struct{}) {
	klog.V(logLevel).Info("FlowExporter 2 Running")
	for range stopCh {
		return
	}
}
