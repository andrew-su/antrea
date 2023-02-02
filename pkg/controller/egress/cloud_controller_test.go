// Copyright 2023 Antrea Authors
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

package egress

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	agenttypes "antrea.io/antrea/pkg/agent/types"
	egressv1beta1 "antrea.io/antrea/pkg/apis/crd/v1beta1"
	"antrea.io/antrea/pkg/client/clientset/versioned"
	fakeversioned "antrea.io/antrea/pkg/client/clientset/versioned/fake"
	crdinformers "antrea.io/antrea/pkg/client/informers/externalversions"
	cloudprovidertest "antrea.io/antrea/pkg/cloudprovider/testing"
)

const (
	fakeEgressIP1 = "192.168.1.1"
	fakeEgressIP2 = "192.168.1.2"
	fakeEgressIP3 = "192.168.1.3"
	fakeNode      = "ip-192-168-1-1.us-west-2.compute.internal"
)

type fakeEgressCloudController struct {
	*EgressCloudController
	client             kubernetes.Interface
	crdClient          versioned.Interface
	informerFactory    informers.SharedInformerFactory
	crdInformerFactory crdinformers.SharedInformerFactory
	cloudProvider      *cloudprovidertest.MockInterface
}

func newFakeEgressCloudController(ctrl *gomock.Controller, objects, crdObjects []runtime.Object) *fakeEgressCloudController {
	client := fake.NewSimpleClientset(objects...)
	crdClient := fakeversioned.NewSimpleClientset(crdObjects...)
	informerFactory := informers.NewSharedInformerFactory(client, resyncPeriod)
	crdInformerFactory := crdinformers.NewSharedInformerFactory(crdClient, resyncPeriod)
	egressInformer := crdInformerFactory.Crd().V1beta1().Egresses()
	nodeInformer := informerFactory.Core().V1().Nodes()
	cloudProvider := cloudprovidertest.NewMockInterface(ctrl)
	egressCloudController, _ := NewEgressCloudController(client, egressInformer, nodeInformer, cloudProvider)
	return &fakeEgressCloudController{
		EgressCloudController: egressCloudController,
		client:                client,
		crdClient:             crdClient,
		informerFactory:       informerFactory,
		crdInformerFactory:    crdInformerFactory,
		cloudProvider:         cloudProvider,
	}
}

func TestEgressCloudControllerSyncNode(t *testing.T) {
	tests := []struct {
		name                       string
		node                       *corev1.Node
		expectedCloudProviderCalls func(recorder *cloudprovidertest.MockInterfaceMockRecorder)
		expectedErr                string
		expectedMaxEgressIPs       string
	}{
		{
			name: "regular",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "node",
					Labels: map[string]string{corev1.LabelInstanceTypeStable: "t3a.large"},
				},
			},
			expectedCloudProviderCalls: func(recorder *cloudprovidertest.MockInterfaceMockRecorder) {
				recorder.GetMaxIPsByInstanceType("t3a.large").Return(11, true, nil)
			},
			expectedMaxEgressIPs: "11",
		},
		{
			name: "error getting max IPs",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "node",
					Labels: map[string]string{corev1.LabelInstanceTypeStable: "t3a.large"},
				},
			},
			expectedCloudProviderCalls: func(recorder *cloudprovidertest.MockInterfaceMockRecorder) {
				recorder.GetMaxIPsByInstanceType("t3a.large").Return(0, false, fmt.Errorf("server error"))
			},
			expectedErr:          "server error",
			expectedMaxEgressIPs: "",
		},
		{
			name: "no instance type label",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node",
				},
			},
			expectedCloudProviderCalls: func(recorder *cloudprovidertest.MockInterfaceMockRecorder) {},
			expectedMaxEgressIPs:       "",
		},
		{
			name: "max-egress-ips annotation exists",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node",
					Annotations: map[string]string{agenttypes.NodeMaxEgressIPsAnnotationKey: "2"},
				},
			},
			expectedCloudProviderCalls: func(recorder *cloudprovidertest.MockInterfaceMockRecorder) {},
			expectedMaxEgressIPs:       "2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			stopCh := make(chan struct{})
			defer close(stopCh)
			c := newFakeEgressCloudController(ctrl, []runtime.Object{tt.node}, nil)
			c.informerFactory.Start(stopCh)
			c.crdInformerFactory.Start(stopCh)
			c.informerFactory.WaitForCacheSync(stopCh)
			c.crdInformerFactory.WaitForCacheSync(stopCh)

			tt.expectedCloudProviderCalls(c.cloudProvider.EXPECT())
			err := c.syncNode(tt.node.Name)
			if tt.expectedErr != "" {
				assert.ErrorContains(t, err, tt.expectedErr)
			} else {
				assert.NoError(t, err)
			}
			node, err := c.client.CoreV1().Nodes().Get(context.Background(), tt.node.Name, metav1.GetOptions{})
			require.NoError(t, err)
			assert.Equal(t, tt.expectedMaxEgressIPs, node.Annotations[agenttypes.NodeMaxEgressIPsAnnotationKey])
		})
	}
}

func TestNodeEvents(t *testing.T) {
	tests := []struct {
		name             string
		node             *corev1.Node
		expectedQueueLen int
	}{
		{
			name: "regular",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "node",
					Labels: map[string]string{corev1.LabelInstanceTypeStable: "t3a.large"},
				},
			},
			expectedQueueLen: 1,
		},
		{
			name: "no instance type label",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node",
				},
			},
			expectedQueueLen: 0,
		},
		{
			name: "max-egress-ips annotation exists",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "node",
					Annotations: map[string]string{agenttypes.NodeMaxEgressIPsAnnotationKey: "2"},
				},
			},
			expectedQueueLen: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			stopCh := make(chan struct{})
			defer close(stopCh)
			c := newFakeEgressCloudController(ctrl, nil, nil)
			c.enqueueNode(tt.node)
			assert.Equal(t, tt.expectedQueueLen, c.nodeQueue.Len())
		})
	}
}

func TestEgressAddEvents(t *testing.T) {
	tests := []struct {
		name             string
		egress           *egressv1beta1.Egress
		expectedQueueLen int
	}{
		{
			name: "regular",
			egress: &egressv1beta1.Egress{
				ObjectMeta: metav1.ObjectMeta{Name: "egressA", UID: "uidA"},
				Spec:       egressv1beta1.EgressSpec{EgressIP: fakeEgressIP1},
				Status:     egressv1beta1.EgressStatus{EgressIP: fakeEgressIP1, EgressNode: fakeNode},
			},
			expectedQueueLen: 1,
		},
		{
			name: "no spec and status values",
			egress: &egressv1beta1.Egress{
				ObjectMeta: metav1.ObjectMeta{Name: "egressB", UID: "uidA"},
				Spec:       egressv1beta1.EgressSpec{EgressIP: ""},
				Status:     egressv1beta1.EgressStatus{EgressIP: "", EgressNode: ""},
			},
			expectedQueueLen: 0,
		},
		{
			name: "no status values",
			egress: &egressv1beta1.Egress{
				ObjectMeta: metav1.ObjectMeta{Name: "egressC", UID: "uidA"},
				Spec:       egressv1beta1.EgressSpec{EgressIP: fakeEgressIP3},
				Status:     egressv1beta1.EgressStatus{EgressIP: "", EgressNode: ""},
			},
			expectedQueueLen: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			stopCh := make(chan struct{})
			defer close(stopCh)
			c := newFakeEgressCloudController(ctrl, nil, nil)
			c.addEgress(tt.egress)
			assert.Equal(t, tt.expectedQueueLen, c.egressQueue.Len())
		})
	}
}

func TestEgressUpdateEvents(t *testing.T) {
	tests := []struct {
		name             string
		oldEgress        *egressv1beta1.Egress
		curEgress        *egressv1beta1.Egress
		expectedQueueLen int
	}{
		{
			name: "regular",
			oldEgress: &egressv1beta1.Egress{
				ObjectMeta: metav1.ObjectMeta{Name: "egressA", UID: "uidA"},
				Spec:       egressv1beta1.EgressSpec{EgressIP: fakeEgressIP1},
				Status:     egressv1beta1.EgressStatus{EgressIP: fakeEgressIP1, EgressNode: fakeNode},
			},
			curEgress: &egressv1beta1.Egress{
				ObjectMeta: metav1.ObjectMeta{Name: "egressA", UID: "uidA"},
				Spec:       egressv1beta1.EgressSpec{EgressIP: fakeEgressIP2},
				Status:     egressv1beta1.EgressStatus{EgressIP: fakeEgressIP2, EgressNode: fakeNode},
			},
			expectedQueueLen: 1,
		},
		{
			name: "same event status",
			oldEgress: &egressv1beta1.Egress{
				ObjectMeta: metav1.ObjectMeta{Name: "egressB", UID: "uidA"},
				Spec:       egressv1beta1.EgressSpec{EgressIP: fakeEgressIP1},
				Status:     egressv1beta1.EgressStatus{EgressIP: fakeEgressIP1, EgressNode: fakeNode},
			},
			curEgress: &egressv1beta1.Egress{
				ObjectMeta: metav1.ObjectMeta{Name: "egressB", UID: "uidA"},
				Spec:       egressv1beta1.EgressSpec{EgressIP: fakeEgressIP1},
				Status:     egressv1beta1.EgressStatus{EgressIP: fakeEgressIP1, EgressNode: fakeNode},
			},
			expectedQueueLen: 0,
		},
		{
			name: "new status is empty",
			oldEgress: &egressv1beta1.Egress{
				ObjectMeta: metav1.ObjectMeta{Name: "egressC", UID: "uidA"},
				Spec:       egressv1beta1.EgressSpec{EgressIP: fakeEgressIP1},
				Status:     egressv1beta1.EgressStatus{EgressIP: fakeEgressIP1, EgressNode: fakeNode},
			},
			curEgress: &egressv1beta1.Egress{
				ObjectMeta: metav1.ObjectMeta{Name: "egressC", UID: "uidA"},
				Spec:       egressv1beta1.EgressSpec{EgressIP: ""},
				Status:     egressv1beta1.EgressStatus{EgressIP: "", EgressNode: ""},
			},
			expectedQueueLen: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			stopCh := make(chan struct{})
			defer close(stopCh)
			c := newFakeEgressCloudController(ctrl, nil, nil)
			c.updateEgress(tt.oldEgress, tt.curEgress)
			assert.Equal(t, tt.expectedQueueLen, c.egressQueue.Len())
		})
	}
}

func TestEgressDeleteEvents(t *testing.T) {
	tests := []struct {
		name             string
		egress           *egressv1beta1.Egress
		expectedQueueLen int
	}{
		{
			name: "regular1",
			egress: &egressv1beta1.Egress{
				ObjectMeta: metav1.ObjectMeta{Name: "egressA"},
			},
			expectedQueueLen: 1,
		},
		{
			name: "regular2",
			egress: &egressv1beta1.Egress{
				ObjectMeta: metav1.ObjectMeta{Name: "egressB"},
			},
			expectedQueueLen: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			stopCh := make(chan struct{})
			defer close(stopCh)
			c := newFakeEgressCloudController(ctrl, nil, nil)
			c.deleteEgress(tt.egress)
			assert.Equal(t, tt.expectedQueueLen, c.egressQueue.Len())
		})
	}
}

func TestEgressCloudControllerSyncEgress(t *testing.T) {
	tests := []struct {
		name                       string
		egress                     *egressv1beta1.Egress
		expectedCloudProviderCalls func(recorder *cloudprovidertest.MockInterfaceMockRecorder)
		expectedErr                string
	}{
		{
			name: "regular add Egress",
			egress: &egressv1beta1.Egress{
				ObjectMeta: metav1.ObjectMeta{Name: "egressA", UID: "uidA"},
				Spec:       egressv1beta1.EgressSpec{EgressIP: fakeEgressIP1},
				Status:     egressv1beta1.EgressStatus{EgressIP: fakeEgressIP1, EgressNode: fakeNode},
			},
			expectedCloudProviderCalls: func(recorder *cloudprovidertest.MockInterfaceMockRecorder) {
				recorder.AssignIPToNode(fakeEgressIP1, fakeNode).Return(nil)
			},
		},
		{
			name: "regular delete Egress",
			egress: &egressv1beta1.Egress{
				ObjectMeta: metav1.ObjectMeta{Name: "egressA"},
			},
			expectedCloudProviderCalls: func(recorder *cloudprovidertest.MockInterfaceMockRecorder) {},
		},
		{
			name: "fail to assign IP",
			egress: &egressv1beta1.Egress{
				ObjectMeta: metav1.ObjectMeta{Name: "egressA", UID: "uidA"},
				Spec:       egressv1beta1.EgressSpec{EgressIP: fakeEgressIP1},
				Status:     egressv1beta1.EgressStatus{EgressIP: fakeEgressIP1, EgressNode: fakeNode},
			},
			expectedCloudProviderCalls: func(recorder *cloudprovidertest.MockInterfaceMockRecorder) {
				recorder.AssignIPToNode(fakeEgressIP1, fakeNode).Return(fmt.Errorf("failed to assign IP"))
			},
			expectedErr: "failed to assign IP",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			stopCh := make(chan struct{})
			defer close(stopCh)
			var fakeObjects []runtime.Object
			fakeObjects = append(fakeObjects, tt.egress)
			c := newFakeEgressCloudController(ctrl, nil, fakeObjects)
			c.informerFactory.Start(stopCh)
			c.crdInformerFactory.Start(stopCh)
			c.informerFactory.WaitForCacheSync(stopCh)
			c.crdInformerFactory.WaitForCacheSync(stopCh)

			tt.expectedCloudProviderCalls(c.cloudProvider.EXPECT())
			err := c.syncEgress(tt.egress.Name)
			if tt.expectedErr != "" {
				assert.ErrorContains(t, err, tt.expectedErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
