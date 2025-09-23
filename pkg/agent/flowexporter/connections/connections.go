// Copyright 2020 Antrea Authors
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

package connections

import (
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"antrea.io/antrea/pkg/agent/flowexporter/connection"
	"antrea.io/antrea/pkg/agent/flowexporter/options"
	"antrea.io/antrea/pkg/agent/flowexporter/priorityqueue"
	"antrea.io/antrea/pkg/agent/flowexporter/utils"
	"antrea.io/antrea/pkg/agent/proxy"
	"antrea.io/antrea/pkg/util/objectstore"
)

const (
	periodicDeleteInterval = time.Minute
)

type connectionStore struct {
	connections            map[connection.ConnectionKey]*connection.Connection
	podStore               objectstore.PodStore
	antreaProxier          proxy.Proxier
	expirePriorityQueue    *priorityqueue.ExpirePriorityQueue
	staleConnectionTimeout time.Duration
	mutex                  sync.Mutex
}

func NewConnectionStore(
	podStore objectstore.PodStore,
	proxier proxy.Proxier,
	o *options.FlowExporterOptions) connectionStore {
	return connectionStore{
		connections:            make(map[connection.ConnectionKey]*connection.Connection),
		podStore:               podStore,
		antreaProxier:          proxier,
		expirePriorityQueue:    priorityqueue.NewExpirePriorityQueue(o.ActiveFlowTimeout, o.IdleFlowTimeout),
		staleConnectionTimeout: o.StaleConnectionTimeout,
	}
}

// GetConnByKey gets the connection in connection map given the connection key.
func (cs *connectionStore) GetConnByKey(connKey connection.ConnectionKey) (*connection.Connection, bool) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	conn, found := cs.connections[connKey]
	return conn, found
}

// ForAllConnectionsDo execute the callback for each connection in connection map.
func (cs *connectionStore) ForAllConnectionsDo(callback connection.ConnectionMapCallBack) error {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	return cs.ForAllConnectionsDoWithoutLock(callback)
}

// ForAllConnectionsDoWithoutLock execute the callback for each connection in connection
// map, without grabbing the lock. Caller is expected to grab lock.
func (cs *connectionStore) ForAllConnectionsDoWithoutLock(callback connection.ConnectionMapCallBack) error {
	for k, v := range cs.connections {
		err := callback(k, v)
		if err != nil {
			klog.ErrorS(err, "Callback execution failed for flow", "key", k, "conn", v)
			return err
		}
	}
	return nil
}

func (cs *connectionStore) RemoveFromMap(connKey connection.ConnectionKey) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	delete(cs.connections, connKey)
}

// AddConnToMap adds the connection to connections map given connection key.
// This is used only for unit tests.
func (cs *connectionStore) AddConnToMap(connKey *connection.ConnectionKey, conn *connection.Connection) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.connections[*connKey] = conn
}

// LookupServiceProtocol returns the corresponding Service protocol string for a given protocol identifier
func lookupServiceProtocol(protoID uint8) (corev1.Protocol, error) {
	serviceProto, found := serviceProtocolMap[protoID]
	if !found {
		return "", fmt.Errorf("unknown protocol identifier: %d", protoID)
	}
	return serviceProto, nil
}

func (cs *connectionStore) AcquireConnStoreLock() {
	cs.mutex.Lock()
}

func (cs *connectionStore) ReleaseConnStoreLock() {
	cs.mutex.Unlock()
}

// UpdateConnAndQueue deletes the inactive connection from keyToItem map,
// without adding it back to the PQ. In this way, we can avoid to reset the
// item's expire time every time we encounter it in the PQ. The method also
// updates active connection's stats fields and adds it back to the PQ. Layer 7
// fields should be set to default to prevent from re-exporting same values.
func (cs *connectionStore) UpdateConnAndQueue(pqItem *priorityqueue.ItemToExpire, currTime time.Time) {
	conn := pqItem.Conn
	conn.LastExportTime = currTime
	conn.AppProtocolName = ""
	conn.HttpVals = ""
	if conn.ReadyToDelete || !conn.IsActive {
		cs.expirePriorityQueue.RemoveItemFromMap(conn)
	} else {
		// For active connections, we update their "prev" stats fields,
		// reset active expire time and push back into the PQ.
		conn.PrevBytes = conn.OriginalBytes
		conn.PrevPackets = conn.OriginalPackets
		conn.PrevTCPState = conn.TCPState
		conn.PrevReverseBytes = conn.ReverseBytes
		conn.PrevReversePackets = conn.ReversePackets
		cs.expirePriorityQueue.ResetActiveExpireTimeAndPush(pqItem, currTime)
	}
}

func (cs *connectionStore) UpsertConn(conn *connection.Connection) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.UpsertConnWithoutLock(conn)
}

func (cs *connectionStore) UpsertConnWithoutLock(conn *connection.Connection) {
	connKey := connection.NewConnectionKey(conn)
	existingConn, ok := cs.connections[connKey]
	if !ok {
		cs.connections[connKey] = conn
	} else {
		existingConn.StopTime = conn.StopTime
		existingConn.OriginalBytes = conn.OriginalBytes
		existingConn.OriginalPackets = conn.OriginalPackets
		existingConn.ReverseBytes = conn.ReverseBytes
		existingConn.ReversePackets = conn.ReversePackets
		existingConn.TCPState = conn.TCPState
		existingConn.IsActive = utils.CheckConntrackConnActive(existingConn)
	}
}
