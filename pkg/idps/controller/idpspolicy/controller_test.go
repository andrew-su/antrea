// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package idpspolicy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	crd "antrea.io/antrea/pkg/apis/crd/v1alpha2"
	tanzucrd "antrea.io/antrea/pkg/apis/tanzucrd/v1alpha1"
	fakeversioned "antrea.io/antrea/pkg/client/clientset/versioned/fake"
	crdinformers "antrea.io/antrea/pkg/client/informers/externalversions"
)

var (
	labels1 = map[string]string{"app1": "foo1"}
	labels2 = map[string]string{"app2": "foo2"}
)

type fakeController struct {
	*Controller
	mockController     *gomock.Controller
	crdClient          *fakeversioned.Clientset
	crdInformerFactory crdinformers.SharedInformerFactory
}

func newFakeController(t *testing.T, objects []runtime.Object) *fakeController {
	controller := gomock.NewController(t)
	crdClient := fakeversioned.NewSimpleClientset(objects...)
	crdInformerFactory := crdinformers.NewSharedInformerFactory(crdClient, 0)
	idpsPolicyInformer := crdInformerFactory.TanzuCrd().V1alpha1().IDPSPolicies()
	trafficControlInformer := crdInformerFactory.Crd().V1alpha2().TrafficControls()

	idpsPolicyController := NewIDPSPolicyController(trafficControlInformer, idpsPolicyInformer, crdClient)

	return &fakeController{
		Controller:         idpsPolicyController,
		mockController:     controller,
		crdClient:          crdClient,
		crdInformerFactory: crdInformerFactory,
	}
}

func TestIDPSPolicyAdd(t *testing.T) {
	testCases := []struct {
		name                       string
		idpsPolicy                 *tanzucrd.IDPSPolicy
		expectedTrafficControlSpec crd.TrafficControlSpec
	}{
		{
			name: "test IDPSPolicy",
			idpsPolicy: &tanzucrd.IDPSPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name: "testIDS",
				},
				Spec: tanzucrd.IDPSPolicySpec{
					AppliedTo: crd.AppliedTo{
						PodSelector: &metav1.LabelSelector{MatchLabels: labels1},
					},
				},
			},
			expectedTrafficControlSpec: crd.TrafficControlSpec{
				AppliedTo: crd.AppliedTo{
					PodSelector: &metav1.LabelSelector{MatchLabels: labels1},
				},
				Direction:  crd.DirectionBoth,
				Action:     crd.ActionMirror,
				TargetPort: crd.TrafficControlPort{Device: &crd.NetworkDevice{Name: idsTargetPortName}},
			},
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			idpsPolicy := tt.idpsPolicy.Name
			c := newFakeController(t, []runtime.Object{tt.idpsPolicy})
			defer c.mockController.Finish()

			stopCh := make(chan struct{})
			defer close(stopCh)

			c.crdInformerFactory.Start(stopCh)
			c.crdInformerFactory.WaitForCacheSync(stopCh)

			require.NoError(t, c.syncIDPSPolicy(idpsPolicy))
			time.Sleep(time.Second)

			tc, err := c.crdInformerFactory.Crd().V1alpha2().TrafficControls().Lister().Get(idpsPolicy)
			require.NoError(t, err)
			require.Equal(t, tc.Spec.AppliedTo, tt.idpsPolicy.Spec.AppliedTo)
			require.Equal(t, tc.Spec, tt.expectedTrafficControlSpec)
			require.Equal(t, tc.GetAnnotations(), map[string]string{managedBy: controllerName})
		})
	}
}

func TestIDPSPolicyUpdate(t *testing.T) {
	idpsPolicyName := "testIDPSPolicy"
	oldIDPSPolicyName := &tanzucrd.IDPSPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: idpsPolicyName,
		},
		Spec: tanzucrd.IDPSPolicySpec{
			AppliedTo: crd.AppliedTo{
				PodSelector: &metav1.LabelSelector{MatchLabels: labels1},
			},
		},
	}
	oldTrafficControl := &crd.TrafficControl{
		ObjectMeta: metav1.ObjectMeta{
			Name: idpsPolicyName,
		},
		Spec: crd.TrafficControlSpec{
			AppliedTo: crd.AppliedTo{
				PodSelector: &metav1.LabelSelector{MatchLabels: labels1},
			},
			Direction:  crd.DirectionBoth,
			Action:     crd.ActionMirror,
			TargetPort: crd.TrafficControlPort{Device: &crd.NetworkDevice{Name: idsTargetPortName}},
		},
	}

	c := newFakeController(t, []runtime.Object{oldIDPSPolicyName, oldTrafficControl})
	defer c.mockController.Finish()

	stopCh := make(chan struct{})
	defer close(stopCh)

	c.crdInformerFactory.Start(stopCh)
	c.crdInformerFactory.WaitForCacheSync(stopCh)

	item, _ := c.queue.Get()
	c.queue.Done(item)

	newIDPSPolicy := &tanzucrd.IDPSPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: idpsPolicyName,
		},
		Spec: tanzucrd.IDPSPolicySpec{
			AppliedTo: crd.AppliedTo{
				PodSelector: &metav1.LabelSelector{MatchLabels: labels2},
			},
		},
	}

	expectedTrafficControlSpec := crd.TrafficControlSpec{
		AppliedTo: crd.AppliedTo{
			PodSelector: &metav1.LabelSelector{MatchLabels: labels2},
		},
		Direction:  crd.DirectionBoth,
		Action:     crd.ActionMirror,
		TargetPort: crd.TrafficControlPort{Device: &crd.NetworkDevice{Name: idsTargetPortName}},
	}

	_, err := c.crdClient.TanzuCrdV1alpha1().IDPSPolicies().Update(context.TODO(), newIDPSPolicy, metav1.UpdateOptions{})
	require.NoError(t, err)
	time.Sleep(time.Second)

	require.NoError(t, c.syncIDPSPolicy(idpsPolicyName))
	time.Sleep(time.Second)

	trafficControl, err := c.crdInformerFactory.Crd().V1alpha2().TrafficControls().Lister().Get(idpsPolicyName)
	require.NoError(t, err)
	require.Equal(t, expectedTrafficControlSpec.AppliedTo, trafficControl.Spec.AppliedTo)
	require.Equal(t, expectedTrafficControlSpec, trafficControl.Spec)
}

func TestIDPSPolicyDelete(t *testing.T) {
	idpsPolicyName := "testIDPSPolicy"
	idpsPolicy := &tanzucrd.IDPSPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: idpsPolicyName,
		},
		Spec: tanzucrd.IDPSPolicySpec{
			AppliedTo: crd.AppliedTo{
				PodSelector: &metav1.LabelSelector{MatchLabels: labels1},
			},
		},
	}
	trafficControl := &crd.TrafficControl{
		ObjectMeta: metav1.ObjectMeta{
			Name: idpsPolicyName,
		},
		Spec: crd.TrafficControlSpec{
			AppliedTo: crd.AppliedTo{
				PodSelector: &metav1.LabelSelector{MatchLabels: labels1},
			},
			Direction:  crd.DirectionBoth,
			Action:     crd.ActionMirror,
			TargetPort: crd.TrafficControlPort{Device: &crd.NetworkDevice{Name: idsTargetPortName}},
		},
	}

	c := newFakeController(t, []runtime.Object{idpsPolicy, trafficControl})
	defer c.mockController.Finish()

	stopCh := make(chan struct{})
	defer close(stopCh)

	c.crdInformerFactory.Start(stopCh)
	c.crdInformerFactory.WaitForCacheSync(stopCh)

	item, _ := c.queue.Get()
	c.queue.Done(item)

	require.NoError(t, c.crdClient.TanzuCrdV1alpha1().IDPSPolicies().Delete(context.TODO(), idpsPolicyName, metav1.DeleteOptions{}))
	time.Sleep(time.Second)

	require.NoError(t, c.syncIDPSPolicy(idpsPolicyName))
	time.Sleep(time.Second)

	_, err := c.crdInformerFactory.Crd().V1alpha2().TrafficControls().Lister().Get(idpsPolicyName)
	require.Equal(t, true, apierrors.IsNotFound(err))
}
