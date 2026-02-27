// Copyright 2025 Antrea Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connections

import (
	"container/heap"
	"sync"
	"time"

	"k8s.io/klog/v2"
	clockutils "k8s.io/utils/clock"

	"antrea.io/antrea/pkg/agent/controller/noderoute"
	"antrea.io/antrea/pkg/agent/flowexporter/connection"
	"antrea.io/antrea/pkg/agent/flowexporter/options"
	"antrea.io/antrea/pkg/agent/flowexporter/utils"
	"antrea.io/antrea/pkg/agent/metrics"
	"antrea.io/antrea/pkg/agent/proxy"
	"antrea.io/antrea/pkg/querier"
	"antrea.io/antrea/pkg/util/objectstore"
	utilwait "antrea.io/antrea/pkg/util/wait"
)

type AugmentFn func(conn *connection.Connection) *connection.Connection

type MergeFunc func(existing, incoming *connection.Connection) *connection.Connection

func NewConnStore(
	connTrackDumper ConnTrackDumper,
	v4Enabled bool,
	v6Enabled bool,
	podStore objectstore.PodStore,
	proxier proxy.ProxyQuerier,
	npQuerier querier.AgentNetworkPolicyInfoQuerier,
	egressQuerier querier.EgressQuerier,
	podNetworkWait *utilwait.Group,
	nodeRouteController *noderoute.Controller,
	isNetworkPolicyOnly bool,
	o *options.FlowExporterOptions,
) *ConnStore {
	store := &ConnStore{
		entries: make(map[connection.ConnectionKey]*connection.Connection, 100),
		gc: gcHeap{
			keyToItem: make(map[connection.ConnectionKey]*gcItem),
		},
		clock: clockutils.RealClock{},

		podStore:             podStore,
		antreaProxier:        proxier,
		networkPolicyQuerier: npQuerier,
		egressQuerier:        egressQuerier,
		nodeRouteController:  nodeRouteController,
		isNetworkPolicyOnly:  isNetworkPolicyOnly,
	}

	return store
}

type ConnStore struct {
	staleConnectionTimeout time.Duration

	entries      map[connection.ConnectionKey]*connection.Connection
	entriesMutex sync.RWMutex
	gc           gcHeap

	podStore             objectstore.PodStore
	antreaProxier        proxy.ProxyQuerier
	networkPolicyQuerier querier.AgentNetworkPolicyInfoQuerier
	egressQuerier        querier.EgressQuerier
	nodeRouteController  *noderoute.Controller
	isNetworkPolicyOnly  bool

	// networkPolicyWait is used to determine when NetworkPolicy flows have been installed and
	// when the mapping from flow ID to NetworkPolicy rule is available. We will ignore
	// connections which started prior to that time to avoid reporting invalid NetworkPolicy
	// metadata in flow records. This is because the mapping is not "stable" and is expected to
	// change when the Agent restarts.
	networkPolicyWait *utilwait.Group
	// networkPolicyReadyTime is set to the current time when we are done waiting on networkPolicyWait.
	networkPolicyReadyTime time.Time

	clock clockutils.WithTicker
}

func (s *ConnStore) HasConn(key connection.ConnectionKey) bool {
	s.entriesMutex.RLock()
	defer s.entriesMutex.RUnlock()
	_, ok := s.entries[key]
	return ok
}

func (s *ConnStore) HandleDenyConnection(conn any) {
	if conn == nil {
		return
	}

	denyConn, ok := conn.(*connection.Connection)
	if !ok {
		return
	}

	s.updateConns([]*connection.Connection{denyConn}, s.denyConnectionAugment, denyConnMerge)
}

func (s *ConnStore) HandleCTConnection(conns any) {
	if conns == nil {
		return
	}

	ctConns, ok := conns.([]*connection.Connection)
	if !ok {
		return
	}

	s.updateConns(ctConns, s.ctConnectionAugment, ctConnMerge)
}

func (s *ConnStore) StartPeriodicStaleConnCleanup(stopCh <-chan struct{}) {
	staleConnTicker := s.clock.NewTicker(s.staleConnectionTimeout)
	defer staleConnTicker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-staleConnTicker.C():
			s.removeStaleConnections()
		}
	}
}

func (s *ConnStore) ctConnectionAugment(conn *connection.Connection) *connection.Connection {
	conn = s.fillPodInfo(conn)
	conn = s.fillServiceInfo(conn)
	conn = s.fillNetworkPolicyMetadataInfo(conn)
	conn = s.fillFlowType(conn)
	conn = s.fillEgressInfo(conn)

	return conn
}

func (s *ConnStore) denyConnectionAugment(conn *connection.Connection) *connection.Connection {
	if conn.StartTime.IsZero() {
		conn.StartTime = s.clock.Now()
		conn.StopTime = conn.StartTime
	}

	conn = s.fillPodInfo(conn)
	conn = s.fillServiceInfo(conn)
	conn = s.fillFlowType(conn)
	conn = s.fillNetworkPolicyMetadataInfo(conn)
	return conn
}

func (s *ConnStore) updateConns(conns []*connection.Connection, augment AugmentFn, mergeFn MergeFunc) {
	if len(conns) == 0 {
		return
	}

	now := s.clock.Now()
	for _, conn := range conns {
		if conn == nil {
			continue
		}

		isDeny := isDenyFlow(conn)

		key := connection.NewConnectionKey(conn)
		existing, connExists := s.entries[key]

		// This is only needed because of how "IsConnectionDying" works. We should rely on either
		// stale connections cleanup or updated connection id in the case of conntrack flows instead.
		// TODO: Remove usage of IsPresent
		conn.IsPresent = !isDeny
		conn.LastUpdateTime = now

		// Handle a special case where the keys are the same (same src/dst address, port and protocol)
		// however the connection itself is new. This can happen for example when doing the following:
		// curl --local-port 55555 <target>
		// When this case applies, we want to re-populate the connection again since a network policy
		// or target service may have changed while ending up with the same resulting destination pod.
		// This applies only to CT flows; denied connections will increment the stats until it has
		// been removed (stale) since denied connections do not specify ID.
		// Should we consider whether the new connection is started after the existing connection?
		if connExists && conn.ID != existing.ID {
			existing = nil
			connExists = false
		}

		if !connExists {
			conn = augment(conn)
			if !acceptConnection(conn) {
				continue
			}

			if isDeny {
				metrics.TotalDenyConnections.Inc()
			} else {
				metrics.TotalAntreaConnectionsInConnTrackTable.Inc()
			}
		} else if !isDeny && utils.IsConnectionDying(existing) {
			// Dying CT connection
			continue
		}

		updatedConn := mergeFn(existing, conn)
		s.entries[key] = updatedConn

		heap.Push(&s.gc, &gcItem{
			conn:       conn,
			expiryNano: updatedConn.LastUpdateTime.UnixNano() + s.staleConnectionTimeout.Nanoseconds(),
		})
	}
}

func (s *ConnStore) removeStaleConnections() {
	klog.V(5).InfoS("Removing stale connections from store")
	now := s.clock.Now().UnixNano()
	conns := []*connection.Connection{}
	for s.gc.Len() > 0 {
		top := s.gc.items[0]
		if top.expiryNano > now {
			// The top connection is not ready to be deleted, since the connections are sorted
			// by expiry time we can exit here.
			break
		}
		heap.Pop(&s.gc)

		key := connection.NewConnectionKey(top.conn)
		delete(s.entries, key)

		if isDenyFlow(top.conn) {
			metrics.TotalDenyConnections.Dec()
		} else {
			metrics.TotalAntreaConnectionsInConnTrackTable.Dec()
		}
		klog.V(5).InfoS("Removed stale connection", "expiredAt", top.expiryNano, "startTime", top.conn.StartTime, "key", key)
		conns = append(conns, top.conn)
	}
}

func (cs *ConnStore) NumConnections() int {
	cs.entriesMutex.RLock()
	defer cs.entriesMutex.RUnlock()
	return len(cs.entries)
}

func (cs *ConnStore) DeleteAllConnections() int {
	cs.entriesMutex.RLock()
	defer cs.entriesMutex.RUnlock()
	num := len(cs.entries)
	clear(cs.entries)
	return num
}

func acceptConnection(conn *connection.Connection) bool {
	if conn == nil {
		return false
	}

	if conn.SourcePodName == "" && conn.DestinationPodName == "" {
		klog.V(5).InfoS("Skip this connection as we cannot map any of the connection IPs to a local Pod", "srcIP", conn.FlowKey.SourceAddress.String(), "dstIP", conn.FlowKey.DestinationAddress.String())
		return false
	}

	if conn.FlowType == utils.FlowTypeUnsupported {
		klog.V(6).InfoS("Skip this connection flow type unsupported", "flowType", conn.FlowType)
		return false
	}
	return true
}

func ctConnMerge(existing, incoming *connection.Connection) *connection.Connection {
	incoming.IsPresent = true
	if existing == nil {
		return incoming
	}

	if incoming.OriginalPackets > existing.OriginalPackets ||
		incoming.ReversePackets > existing.ReversePackets ||
		incoming.TCPState != existing.TCPState {
		existing.LastUpdateTime = incoming.LastUpdateTime
	}

	existing.StopTime = incoming.StopTime
	existing.OriginalBytes = incoming.OriginalBytes
	existing.OriginalPackets = incoming.OriginalPackets
	existing.ReverseBytes = incoming.ReverseBytes
	existing.ReversePackets = incoming.ReversePackets
	existing.TCPState = incoming.TCPState

	return existing
}

func denyConnMerge(existing, incoming *connection.Connection) *connection.Connection {
	if existing == nil {
		return incoming
	}

	existing.StopTime = incoming.StopTime
	existing.OriginalBytes += incoming.OriginalBytes
	existing.OriginalPackets += incoming.OriginalPackets
	existing.LastUpdateTime = incoming.LastUpdateTime

	return existing
}

type gcItem struct {
	conn       *connection.Connection
	expiryNano int64
	index      int
}
type gcHeap struct {
	items     []*gcItem
	keyToItem map[connection.ConnectionKey]*gcItem
}

func (h gcHeap) Len() int           { return len(h.items) }
func (h gcHeap) Less(i, j int) bool { return h.items[i].expiryNano < h.items[j].expiryNano }
func (h gcHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}
func (h *gcHeap) Push(x any) {
	item := x.(*gcItem)
	key := connection.NewConnectionKey(item.conn)
	if item, ok := h.keyToItem[key]; ok {
		heap.Remove(h, item.index)
	}
	h.items = append(h.items, item)
	h.keyToItem[key] = item
}
func (h *gcHeap) Pop() any {
	old := h.items
	n := len(old)
	it := old[n-1]
	h.items = old[:n-1]

	key := connection.NewConnectionKey(it.conn)
	delete(h.keyToItem, key)
	return it
}

func isDenyFlow(conn *connection.Connection) bool {
	return conn != nil && conn.ID == 0
}
