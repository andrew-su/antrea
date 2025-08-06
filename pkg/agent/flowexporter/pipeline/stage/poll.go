package stage

import (
	"context"
	"net"
	"time"

	"antrea.io/antrea/pkg/agent/config"
	"antrea.io/antrea/pkg/agent/flowexporter/connection"
	"antrea.io/antrea/pkg/agent/flowexporter/connections"
	"antrea.io/antrea/pkg/agent/openflow"
	"antrea.io/antrea/pkg/ovs/ovsconfig"
	"k8s.io/klog/v2"
)

type Poller struct {
	pollInterval          time.Duration
	v4Enabled             bool
	v6Enabled             bool
	connectUplinkToBridge bool

	NodeConfig       *config.NodeConfig
	ServiceCIDRNet   *net.IPNet
	ServiceCIDRNetv6 *net.IPNet
	OVSDatapathType  ovsconfig.OVSDatapathType
	ProxyEnabled     bool

	connDumper connections.ConnTrackDumper
}

func (p *Poller) Run(ctx context.Context, input <-chan *connection.Connection) <-chan *connection.Connection {
	out := make(chan *connection.Connection)
	go func() {
		ticker := time.NewTicker(p.pollInterval)
		defer ticker.Stop()
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				conns, err := p.poll()
				if err != nil {
					// Not failing here as errors can be transient and could be resolved in future poll cycles.
					// TODO: Come up with a backoff/retry mechanism by increasing poll interval and adding retry timeout
					klog.Errorf("Error during conntrack poll cycle: %v", err)
				}

				for _, conn := range conns {
					out <- conn
				}
			}
		}
	}()
	return out
}

func (p *Poller) poll() ([]*connection.Connection, error) {
	var zones []uint16
	var connsLens []int
	if p.v4Enabled {
		if p.connectUplinkToBridge {
			zones = append(zones, uint16(openflow.IPCtZoneTypeRegMark.GetValue()<<12))
		} else {
			zones = append(zones, openflow.CtZone)
		}
	}
	if p.v6Enabled {
		if p.connectUplinkToBridge {
			zones = append(zones, uint16(openflow.IPv6CtZoneTypeRegMark.GetValue()<<12))
		} else {
			zones = append(zones, openflow.CtZoneV6)
		}
	}
	var totalConns int
	var filteredConnsList []*connection.Connection
	for _, zone := range zones {
		filteredConnsPerZone, totalConnsPerZone, err := p.connDumper.DumpFlows(zone)
		if err != nil {
			return nil, err
		}
		totalConns += totalConnsPerZone
		filteredConnsList = append(filteredConnsList, filteredConnsPerZone...)
		connsLens = append(connsLens, len(filteredConnsList))
	}

	return filteredConnsList, nil
}

func NewPoller(pollInterval time.Duration, connDumper connections.ConnTrackDumper, v4Enabled, v6Enabled, connectUplinkToBridge bool) Stage {
	// // TODO: Remove filter from connTrackDumper.
	// protocolFilter := filter.NewProtocolFilter(nil)

	p := &Poller{
		pollInterval: pollInterval,
		v4Enabled:    v4Enabled,
		v6Enabled:    v6Enabled,
		connDumper:   connDumper,

		// NodeConfig:       nodeConfig,
		// ServiceCIDRNet:   serviceCIDRNet,
		// ServiceCIDRNetv6: serviceCIDRNetv6,
		// OVSDatapathType:  ovsDatapathType,
		// ProxyEnabled:     proxyEnabled,
		// connTrackDumper:  connections.InitializeConnTrackDumper(nodeConfig, serviceCIDRNet, serviceCIDRNetv6, ovsDatapathType, proxyEnabled, protocolFilter),
	}

	return p
}
