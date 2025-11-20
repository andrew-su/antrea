package flowexporter

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-base/metrics/legacyregistry"

	"antrea.io/antrea/pkg/agent/flowexporter/connection"
	"antrea.io/antrea/pkg/agent/flowexporter/connections"
	exportertesting "antrea.io/antrea/pkg/agent/flowexporter/exporter/testing"
	"antrea.io/antrea/pkg/agent/flowexporter/priorityqueue"
	flowexportertesting "antrea.io/antrea/pkg/agent/flowexporter/testing"
	"antrea.io/antrea/pkg/agent/metrics"
	agenttypes "antrea.io/antrea/pkg/agent/types"
	queriertest "antrea.io/antrea/pkg/querier/testing"
)

const (
	testActiveFlowTimeout = 3 * time.Second
	testIdleFlowTimeout   = 1 * time.Second
)

// TODO: This test needs to be fixed
// - It used subtests but they depend on each other, which is not a good pattern
// - The subtests should not share a gomock Controller
// - The expectation for Export should not use gomock.Any()
func runSendFlowRecordTests(t *testing.T, destination *Destination, isIPv6 bool) {
	ctrl := gomock.NewController(t)
	mockExporter := exportertesting.NewMockInterface(ctrl)
	destination.exp = mockExporter
	startTime := time.Now()

	tests := []struct {
		name               string
		isDenyConn         bool
		isConnPresent      bool
		tcpState           string
		statusFlag         uint32
		protoID            uint8
		originalPackets    uint64
		reversePackets     uint64
		prevPackets        uint64
		prevReversePackets uint64
		activeExpireTime   time.Time
		idleExpireTime     time.Time
	}{
		{
			name:               "conntrack connection being active time out",
			isDenyConn:         false,
			isConnPresent:      true,
			tcpState:           "SYN_SENT",
			statusFlag:         4,
			protoID:            6,
			originalPackets:    1,
			reversePackets:     1,
			prevPackets:        0,
			prevReversePackets: 0,
			activeExpireTime:   startTime.Add(-testActiveFlowTimeout),
			idleExpireTime:     startTime.Add(10 * testIdleFlowTimeout),
		},
		{
			name:               "conntrack connection being idle time out and becoming inactive",
			isDenyConn:         false,
			isConnPresent:      true,
			tcpState:           "SYN_SENT",
			statusFlag:         4,
			protoID:            6,
			originalPackets:    0,
			reversePackets:     0,
			prevPackets:        0,
			prevReversePackets: 0,
			activeExpireTime:   startTime.Add(10 * testActiveFlowTimeout),
			idleExpireTime:     startTime.Add(-testIdleFlowTimeout),
		},
		{
			name:               "conntrack connection with deleted connection",
			isDenyConn:         false,
			isConnPresent:      false,
			tcpState:           "TIME_WAIT",
			statusFlag:         204,
			protoID:            6,
			originalPackets:    0,
			reversePackets:     0,
			prevPackets:        0,
			prevReversePackets: 0,
			activeExpireTime:   startTime.Add(-testActiveFlowTimeout),
			idleExpireTime:     startTime.Add(-testIdleFlowTimeout),
		},
		{
			name:               "deny connection being active time out",
			isDenyConn:         true,
			isConnPresent:      false,
			tcpState:           "TIME_WAIT",
			statusFlag:         204,
			protoID:            6,
			originalPackets:    1,
			reversePackets:     0,
			prevPackets:        0,
			prevReversePackets: 0,
			activeExpireTime:   startTime.Add(-testActiveFlowTimeout),
			idleExpireTime:     startTime.Add(10 * testIdleFlowTimeout),
		},
		{
			name:               "deny connection being active time out and becoming inactive",
			isDenyConn:         true,
			isConnPresent:      false,
			tcpState:           "TIME_WAIT",
			statusFlag:         204,
			protoID:            6,
			originalPackets:    1,
			reversePackets:     0,
			prevPackets:        1,
			prevReversePackets: 0,
			activeExpireTime:   startTime.Add(-testActiveFlowTimeout),
			idleExpireTime:     startTime.Add(10 * testIdleFlowTimeout),
		},
		{
			name:               "deny connection being idle time out",
			isDenyConn:         true,
			isConnPresent:      false,
			tcpState:           "TIME_WAIT",
			statusFlag:         204,
			protoID:            6,
			originalPackets:    0,
			reversePackets:     0,
			prevPackets:        0,
			prevReversePackets: 0,
			activeExpireTime:   startTime.Add(10 * testActiveFlowTimeout),
			idleExpireTime:     startTime.Add(-testIdleFlowTimeout),
		},
	}
	for id, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := connections.ConnectionStoreConfig{
				ActiveFlowTimeout:      testActiveFlowTimeout,
				IdleFlowTimeout:        testIdleFlowTimeout,
				StaleConnectionTimeout: 1,
			}

			destination.conntrackConnStore = connections.NewConntrackConnectionStore(nil /* TODO */, nil, nil, nil, nil, config)
			destination.denyConnStore = connections.NewDenyConnectionStore(nil, nil, nil, config)
			destination.conntrackPriorityQueue = destination.conntrackConnStore.GetPriorityQueue()
			destination.denyPriorityQueue = destination.denyConnStore.GetPriorityQueue()
			destination.numConnsExported = 0
			var conn, denyConn *connection.Connection
			var pqItem *priorityqueue.ItemToExpire

			if !tt.isDenyConn {
				// Prepare connection map
				conn = flowexportertesting.GetConnection(isIPv6, tt.isConnPresent, tt.statusFlag, tt.protoID, tt.tcpState)
				connKey := connection.NewConnectionKey(conn)
				conn.OriginalPackets = tt.originalPackets
				conn.ReversePackets = tt.reversePackets
				destination.conntrackConnStore.AddOrUpdateConn(conn)
				assert.Equalf(t, getNumOfConntrackConns(destination.conntrackConnStore), 1, "connection is expected to be in the connection map")
				assert.Equalf(t, destination.conntrackPriorityQueue.Len(), 1, "pqItem is expected to be in the expire priority queue")
				conn.PrevPackets = tt.prevPackets
				conn.PrevReversePackets = tt.prevReversePackets
				pqItem = destination.conntrackPriorityQueue.KeyToItem[connKey]
				pqItem.ActiveExpireTime = tt.activeExpireTime
				pqItem.IdleExpireTime = tt.idleExpireTime
			} else {
				// Prepare deny connection map
				denyConn = flowexportertesting.GetDenyConnection(isIPv6, tt.protoID)
				connKey := connection.NewConnectionKey(denyConn)
				destination.denyConnStore.AddOrUpdateConn(denyConn)
				assert.Equalf(t, getNumOfDenyConns(destination.denyConnStore), 1, "deny connection is expected to be in the connection map")
				assert.Equalf(t, destination.denyPriorityQueue.Len(), 1, "pqItem is expected to be in the expire priority queue")
				denyConn.PrevPackets = tt.prevPackets
				pqItem = destination.denyPriorityQueue.KeyToItem[connKey]
				pqItem.ActiveExpireTime = tt.activeExpireTime
				pqItem.IdleExpireTime = tt.idleExpireTime
			}

			mockExporter.EXPECT().Export(gomock.Any())

			_, err := destination.sendFlowRecords()
			assert.NoError(t, err)
			assert.Equalf(t, uint64(1), destination.numConnsExported, "1 data set should have been sent.")

			switch id {
			case 0: // conntrack connection being active time out
				assert.True(t, pqItem.ActiveExpireTime.After(startTime))
				assert.Equal(t, conn.OriginalPackets, conn.PrevPackets)
				assert.Equalf(t, 1, destination.conntrackPriorityQueue.Len(), "Length of expire priority queue should be 1")
			case 1: // conntrack connection being idle time out and becoming inactive
				assert.False(t, conn.IsActive)
				assert.Equalf(t, 0, destination.conntrackPriorityQueue.Len(), "Length of expire priority queue should be 0")
			case 2: // conntrack connection with deleted connection
				assert.True(t, conn.ReadyToDelete)
				assert.Equalf(t, 0, destination.conntrackPriorityQueue.Len(), "Length of expire priority queue should be 0")
			case 3: // deny connection being active time out
				assert.True(t, pqItem.ActiveExpireTime.After(startTime))
				assert.Equal(t, denyConn.OriginalPackets, denyConn.PrevPackets)
				assert.Equalf(t, 1, destination.denyPriorityQueue.Len(), "Length of expire priority queue should be 1")
			case 4: // deny connection being active time out and becoming inactive
				assert.False(t, denyConn.IsActive)
				assert.Equalf(t, 0, destination.denyPriorityQueue.Len(), "Length of expire priority queue should be 0")
			case 5: // deny connection being idle time out
				assert.Equal(t, true, denyConn.ReadyToDelete)
				assert.Equalf(t, 0, destination.denyPriorityQueue.Len(), "Length of expire priority queue should be 0")
			}
		})
	}
}

func TestDestination_sendFlowRecords(t *testing.T) {
	for _, tc := range []struct {
		v4Enabled bool
		v6Enabled bool
	}{
		{true, false},
		{false, true},
		{true, true},
	} {
		testSendFlowRecords(t, tc.v4Enabled, tc.v6Enabled)
	}
}

func TestDestination_fillEgressInfo(t *testing.T) {
	testCases := []struct {
		name                   string
		sourcePodNamespace     string
		sourcePodName          string
		expectedEgressName     string
		expectedEgressUID      string
		expectedEgressIP       string
		expectedEgressNodeName string
		expectedErr            string
	}{
		{
			name:                   "EgressName, EgressIP and EgressNodeName filled",
			sourcePodNamespace:     "namespaceA",
			sourcePodName:          "podA",
			expectedEgressName:     "test-egress",
			expectedEgressUID:      "test-egress-uid",
			expectedEgressIP:       "172.18.0.1",
			expectedEgressNodeName: "test-egress-node",
		},
		{
			name:               "No Egress Information filled",
			sourcePodNamespace: "namespaceA",
			sourcePodName:      "podC",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			egressQuerier := queriertest.NewMockEgressQuerier(ctrl)
			exp := &Destination{
				egressQuerier: egressQuerier,
			}
			conn := connection.Connection{
				SourcePodNamespace: tc.sourcePodNamespace,
				SourcePodName:      tc.sourcePodName,
			}
			if tc.expectedEgressName != "" {
				egressQuerier.EXPECT().GetEgress(conn.SourcePodNamespace, conn.SourcePodName).Return(agenttypes.EgressConfig{
					Name:       tc.expectedEgressName,
					UID:        types.UID(tc.expectedEgressUID),
					EgressIP:   tc.expectedEgressIP,
					EgressNode: tc.expectedEgressNodeName,
				}, nil)
			} else {
				egressQuerier.EXPECT().GetEgress(conn.SourcePodNamespace, conn.SourcePodName).Return(agenttypes.EgressConfig{}, fmt.Errorf("no Egress applied to Pod %s", conn.SourcePodName))
			}
			exp.fillEgressInfo(&conn)
			assert.Equal(t, tc.expectedEgressName, conn.EgressName)
			assert.Equal(t, tc.expectedEgressUID, conn.EgressUID)
			assert.Equal(t, tc.expectedEgressIP, conn.EgressIP)
			assert.Equal(t, tc.expectedEgressNodeName, conn.EgressNodeName)
		})
	}
}

func testSendFlowRecords(t *testing.T, v4Enabled bool, v6Enabled bool) {
	destination := &Destination{}

	if v4Enabled {
		runSendFlowRecordTests(t, destination, false)
	}
	if v6Enabled {
		runSendFlowRecordTests(t, destination, true)
	}
}

func TestDestination_findFlowType(t *testing.T) {
	conn1 := connection.Connection{SourcePodName: "podA", DestinationPodName: "podB"}
	conn2 := connection.Connection{SourcePodName: "podA", DestinationPodName: ""}
	for _, tc := range []struct {
		isNetworkPolicyOnly bool
		conn                connection.Connection
		expectedFlowType    uint8
	}{
		{true, conn1, 1},
		{true, conn2, 2},
		{false, conn1, 0},
	} {
		flowExp := &Destination{
			DestinationConfig: DestinationConfig{
				isNetworkPolicyOnly: tc.isNetworkPolicyOnly,
			},
		}
		flowType := flowExp.findFlowType(tc.conn)
		assert.Equal(t, tc.expectedFlowType, flowType)
	}
}

func TestDestination_Connect(t *testing.T) {
	metrics.InitializeConnectionMetrics()
	ctrl := gomock.NewController(t)
	mockExporter := exportertesting.NewMockInterface(ctrl)
	collectorAddr := "127.0.0.1:4739"
	exp := &Destination{
		DestinationConfig: DestinationConfig{
			address: collectorAddr,
		},
		exp: mockExporter,
	}
	// TODO: test the TLS case (requires certificates)
	mockExporter.EXPECT().ConnectToCollector(collectorAddr, nil)
	require.NoError(t, exp.Connect(context.Background()))
	assert.True(t, exp.connected)
	checkTotalReconnectionsMetric(t)
	metrics.ReconnectionsToFlowCollector.Dec()
}

func checkTotalReconnectionsMetric(t *testing.T) {
	expected := `
	# HELP antrea_agent_flow_collector_reconnection_count [ALPHA] Number of re-connections between Flow Exporter and flow collector. This metric gets updated whenever the connection is re-established between the Flow Exporter and the flow collector (e.g. the Flow Aggregator).
	# TYPE antrea_agent_flow_collector_reconnection_count gauge
	antrea_agent_flow_collector_reconnection_count 1
	`
	err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected), "antrea_agent_flow_collector_reconnection_count")
	assert.NoError(t, err)
}

func getNumOfConntrackConns(connStore *connections.ConntrackConnectionStore) int {
	count := 0
	countNumOfConns := func(key connection.ConnectionKey, conn *connection.Connection) error {
		count++
		return nil
	}
	connStore.ForAllConnectionsDo(countNumOfConns)
	return count
}

func getNumOfDenyConns(connStore *connections.DenyConnectionStore) int {
	count := 0
	countNumOfConns := func(key connection.ConnectionKey, conn *connection.Connection) error {
		count++
		return nil
	}
	connStore.ForAllConnectionsDo(countNumOfConns)
	return count
}
