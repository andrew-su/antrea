// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package idpspolicy

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	crd "antrea.io/antrea/pkg/apis/crd/v1alpha2"
	tanzucrd "antrea.io/antrea/pkg/apis/tanzucrd/v1alpha1"
	crdclientset "antrea.io/antrea/pkg/client/clientset/versioned"
	crdinformers "antrea.io/antrea/pkg/client/informers/externalversions/crd/v1alpha2"
	tanzucrdinformers "antrea.io/antrea/pkg/client/informers/externalversions/tanzucrd/v1alpha1"
	crdlisters "antrea.io/antrea/pkg/client/listers/crd/v1alpha2"
	tanzucrdlisters "antrea.io/antrea/pkg/client/listers/tanzucrd/v1alpha1"
)

const (
	managedBy      = "crd.antrea.tanzu.vmware.com/managed-by"
	controllerName = "IDPSPolicyController"

	maxRetries     = 15
	minRetryDelay  = 5 * time.Second
	maxRetryDelay  = 300 * time.Second
	defaultWorkers = 4

	resyncPeriod time.Duration = 0

	idsTargetPortName = "antrea-tap0"
)

type Controller struct {
	idpsPolicyInformer     cache.SharedIndexInformer
	idpsPolicyLister       tanzucrdlisters.IDPSPolicyLister
	idpsPolicyListerSynced cache.InformerSynced

	trafficControlInformer     cache.SharedIndexInformer
	trafficControlLister       crdlisters.TrafficControlLister
	trafficControlListerSynced cache.InformerSynced

	queue     workqueue.RateLimitingInterface
	crdClient crdclientset.Interface
}

func NewIDPSPolicyController(
	trafficControlInformer crdinformers.TrafficControlInformer,
	idpsPolicyInformer tanzucrdinformers.IDPSPolicyInformer,
	crdClient crdclientset.Interface,
) *Controller {
	c := &Controller{
		idpsPolicyInformer:         idpsPolicyInformer.Informer(),
		idpsPolicyLister:           idpsPolicyInformer.Lister(),
		idpsPolicyListerSynced:     idpsPolicyInformer.Informer().HasSynced,
		trafficControlInformer:     trafficControlInformer.Informer(),
		trafficControlLister:       trafficControlInformer.Lister(),
		trafficControlListerSynced: trafficControlInformer.Informer().HasSynced,
		crdClient:                  crdClient,
		queue:                      workqueue.NewNamedRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(minRetryDelay, maxRetryDelay), "idpsPolicies"),
	}
	c.idpsPolicyInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.addIDPSPolicy,
			UpdateFunc: c.updateIDPSPolicy,
			DeleteFunc: c.deleteIDPSPolicy,
		},
		resyncPeriod,
	)
	return c
}

func (c *Controller) addIDPSPolicy(obj interface{}) {
	idpsPolicy := obj.(*tanzucrd.IDPSPolicy)
	klog.V(2).InfoS("Processing IDPSPolicy ADD event", "IDPSPolicy", klog.KObj(idpsPolicy))
	c.queue.Add(idpsPolicy.Name)
}

func (c *Controller) updateIDPSPolicy(oldObj, obj interface{}) {
	oldIDPSPolicy := oldObj.(*tanzucrd.IDPSPolicy)
	idpsPolicy := obj.(*tanzucrd.IDPSPolicy)
	if oldIDPSPolicy.GetGeneration() != idpsPolicy.GetGeneration() {
		klog.V(2).InfoS("Processing IDPSPolicy UPDATE event", "IDPSPolicy", klog.KObj(idpsPolicy))
		c.queue.Add(idpsPolicy.Name)
	}
}

func (c *Controller) deleteIDPSPolicy(obj interface{}) {
	idpsPolicy := obj.(*tanzucrd.IDPSPolicy)
	klog.V(2).InfoS("Processing IDPSPolicy DELETE event", "IDPSPolicy", klog.KObj(idpsPolicy))
	c.queue.Add(idpsPolicy.Name)
}

func (c *Controller) Run(stopCh <-chan struct{}) {
	defer c.queue.ShutDown()

	klog.InfoS("Starting", "controllerName", controllerName)
	defer klog.InfoS("Shutting down", "controllerName", controllerName)

	if !cache.WaitForNamedCacheSync(controllerName, stopCh, c.idpsPolicyListerSynced, c.trafficControlListerSynced) {
		return
	}

	for i := 0; i < defaultWorkers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (c *Controller) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *Controller) processNextWorkItem() bool {
	cKey, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(cKey)

	err := c.syncIDPSPolicy(cKey.(string))
	c.handleErr(err, cKey)

	return true
}

func (c *Controller) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	if c.queue.NumRequeues(key) < maxRetries {
		klog.Warningf("Error mirroring object for %q resource, retrying. Error: %v", key, err)
		c.queue.AddRateLimited(key)
		return
	}

	klog.Warningf("Retry budget exceeded, dropping %q resource out of the queue: %v", key, err)
	c.queue.Forget(key)
	utilruntime.HandleError(err)
}

func (c *Controller) syncIDPSPolicy(idpsPolicyName string) error {
	startTime := time.Now()
	defer func() {
		klog.V(2).InfoS("Finished syncing IDPSPolicy", "IDPSPolicy", idpsPolicyName, "durationTime", time.Since(startTime))
	}()

	idpsPolicyExists := true
	idpsPolicy, err := c.idpsPolicyLister.Get(idpsPolicyName)
	if err != nil {
		idpsPolicyExists = false
	}

	trafficControlExists := true
	trafficControlName := idpsPolicyName
	trafficControl, err := c.trafficControlLister.Get(trafficControlName)
	if err != nil {
		trafficControlExists = false
	}

	if !idpsPolicyExists {
		if !trafficControlExists {
			return nil
		}
		return c.crdClient.CrdV1alpha2().TrafficControls().Delete(context.TODO(), trafficControlName, metav1.DeleteOptions{})
	}

	trafficControl = c.generateOrUpdateTrafficControl(trafficControl, idpsPolicy)
	if !trafficControlExists {
		if _, err = c.crdClient.CrdV1alpha2().TrafficControls().Create(context.TODO(), trafficControl, metav1.CreateOptions{}); err != nil {
			return err
		}
	} else {
		if _, err = c.crdClient.CrdV1alpha2().TrafficControls().Update(context.TODO(), trafficControl, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}

	return nil
}

func (c *Controller) generateOrUpdateTrafficControl(oldTrafficControl *crd.TrafficControl, idpsPolicy *tanzucrd.IDPSPolicy) *crd.TrafficControl {
	var trafficControl *crd.TrafficControl
	if oldTrafficControl == nil {
		ptrBool := func(value bool) *bool {
			return &value
		}
		ownerRef := metav1.OwnerReference{
			APIVersion:         "crd.antrea.tanzu.vmware.com/v1alpha1",
			Kind:               "IDPSPolicy",
			Name:               idpsPolicy.Name,
			UID:                idpsPolicy.UID,
			Controller:         ptrBool(true),
			BlockOwnerDeletion: ptrBool(true),
		}
		trafficControl = &crd.TrafficControl{
			ObjectMeta: metav1.ObjectMeta{
				Name:            idpsPolicy.Name,
				Annotations:     map[string]string{managedBy: controllerName},
				OwnerReferences: []metav1.OwnerReference{ownerRef},
			},
		}
	} else {
		trafficControl = oldTrafficControl.DeepCopy()
	}

	trafficControl.Spec = crd.TrafficControlSpec{
		AppliedTo:  *idpsPolicy.Spec.AppliedTo.DeepCopy(),
		Action:     crd.ActionMirror,
		Direction:  crd.DirectionBoth,
		TargetPort: crd.TrafficControlPort{Device: &crd.NetworkDevice{Name: idsTargetPortName}},
	}
	return trafficControl
}
