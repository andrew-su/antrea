// Copyright 2021 Antrea Authors
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
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"antrea.io/antrea/pkg/apis/controlplane"
	"antrea.io/antrea/pkg/apis/crd/v1beta1"
	egressv1beta1 "antrea.io/antrea/pkg/apis/crd/v1beta1"
	"antrea.io/antrea/pkg/apiserver/storage"
	clientset "antrea.io/antrea/pkg/client/clientset/versioned"
	egressinformers "antrea.io/antrea/pkg/client/informers/externalversions/crd/v1beta1"
	egresslisters "antrea.io/antrea/pkg/client/listers/crd/v1beta1"
	"antrea.io/antrea/pkg/controller/externalippool"
	"antrea.io/antrea/pkg/controller/grouping"
	antreatypes "antrea.io/antrea/pkg/controller/types"
	"antrea.io/antrea/pkg/features"
	"antrea.io/antrea/pkg/util/k8s"
)

const (
	controllerName = "EgressController"
	// Set resyncPeriod to 0 to disable resyncing.
	resyncPeriod time.Duration = 0
	// How long to wait before retrying the processing of an Egress change.
	minRetryDelay = 5 * time.Second
	maxRetryDelay = 300 * time.Second
	// Default number of workers processing an Egress change.
	defaultWorkers = 4
	// egressGroupType is the type used when registering EgressGroups to the grouping interface.
	egressGroupType grouping.GroupType = "egressGroup"

	externalIPPoolIndex = "externalIPPool"

	subjectUserIndex  = "subjectUser"
	subjectGroupIndex = "subjectGroup"
)

// ipAllocation contains a map of IPPool and IP string: <the IP Pool which allocates the IP>:<the IP>.
type ipAllocation map[string]string

// EgressController is responsible for synchronizing the EgressGroups selected by Egresses.
type EgressController struct {
	crdClient clientset.Interface

	externalIPAllocator externalippool.ExternalIPAllocator

	// ipAllocationMap is a map from Egress name to ipAllocation, which is used to check whether the Egress's IP has
	// changed and to release the IP after the Egress is removed.
	ipAllocationMap   map[string]ipAllocation
	ipAllocationMutex sync.RWMutex

	egressInformer egressinformers.EgressInformer
	egressLister   egresslisters.EgressLister
	egressIndexer  cache.Indexer
	// egressListerSynced is a function which returns true if the Egresses shared informer has been synced at least once.
	egressListerSynced cache.InformerSynced
	// egressGroupStore is the storage where the EgressGroups are stored.
	egressGroupStore storage.Interface
	// queue maintains the EgressGroup objects that need to be synced.
	queue workqueue.TypedRateLimitingInterface[string]
	// groupingInterface knows Pods that a given group selects.
	groupingInterface grouping.Interface
	// Added as a member to the struct to allow injection for testing.
	groupingInterfaceSynced func() bool

	egressEntitlementInformer     egressinformers.EgressEntitlementInformer
	egressEntitlementLister       egresslisters.EgressEntitlementLister
	egressEntitlementListerSynced cache.InformerSynced

	egressEntitlementBindingInformer     egressinformers.EgressEntitlementBindingInformer
	egressEntitlementBindingLister       egresslisters.EgressEntitlementBindingLister
	egressEntitlementBindingListerSynced cache.InformerSynced

	isEnterpriseAntrea bool
}

// NewEgressController returns a new *EgressController.
func NewEgressController(crdClient clientset.Interface,
	groupingInterface grouping.Interface,
	egressInformer egressinformers.EgressInformer,
	externalIPAllocator externalippool.ExternalIPAllocator,
	egressGroupStore storage.Interface,
	egressEntitlementInformer egressinformers.EgressEntitlementInformer,
	egressEntitlementBindingInformer egressinformers.EgressEntitlementBindingInformer,
	enterpriseAntrea bool) *EgressController {
	c := &EgressController{
		crdClient:          crdClient,
		egressInformer:     egressInformer,
		egressLister:       egressInformer.Lister(),
		egressListerSynced: egressInformer.Informer().HasSynced,
		egressIndexer:      egressInformer.Informer().GetIndexer(),
		egressGroupStore:   egressGroupStore,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](minRetryDelay, maxRetryDelay),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "egress",
			},
		),
		groupingInterface:       groupingInterface,
		groupingInterfaceSynced: groupingInterface.HasSynced,
		ipAllocationMap:         make(map[string]ipAllocation),
		externalIPAllocator:     externalIPAllocator,
		isEnterpriseAntrea:      enterpriseAntrea,
	}
	// Add handlers for Group events and Egress events.
	c.groupingInterface.AddEventHandler(egressGroupType, c.enqueueEgressGroup)
	egressInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.addEgress,
			UpdateFunc: c.updateEgress,
			DeleteFunc: c.deleteEgress,
		},
		resyncPeriod,
	)
	// externalIPPoolIndex will be used to get all Egresses associated with a given ExternalIPPool.
	egressInformer.Informer().AddIndexers(cache.Indexers{externalIPPoolIndex: func(obj interface{}) (strings []string, e error) {
		egress, ok := obj.(*egressv1beta1.Egress)
		if !ok {
			return nil, fmt.Errorf("obj is not Egress: %+v", obj)
		}
		var externalIPPools []string
		if egress.Spec.ExternalIPPool != "" {
			externalIPPools = append(externalIPPools, egress.Spec.ExternalIPPool)
		}
		for _, externalIPPool := range egress.Spec.ExternalIPPools {
			if externalIPPool != "" {
				externalIPPools = append(externalIPPools, externalIPPool)
			}
		}
		return externalIPPools, nil
	}})
	c.externalIPAllocator.AddEventHandler(func(ipPool string) {
		c.enqueueEgresses(ipPool)
	})

	if features.DefaultFeatureGate.Enabled(features.EgressRBAC) && enterpriseAntrea {
		c.egressEntitlementInformer = egressEntitlementInformer
		c.egressEntitlementLister = egressEntitlementInformer.Lister()
		c.egressEntitlementListerSynced = egressEntitlementInformer.Informer().HasSynced
		c.egressEntitlementBindingInformer = egressEntitlementBindingInformer
		c.egressEntitlementBindingLister = egressEntitlementBindingInformer.Lister()
		c.egressEntitlementBindingListerSynced = egressEntitlementBindingInformer.Informer().HasSynced

		egressEntitlementBindingInformer.Informer().AddIndexers(
			cache.Indexers{
				subjectUserIndex: func(obj interface{}) ([]string, error) {
					eetb, ok := obj.(*egressv1beta1.EgressEntitlementBinding)
					if !ok || len(eetb.Spec.Subjects) == 0 {
						return []string{}, nil
					}
					var subjects []string
					for _, s := range eetb.Spec.Subjects {
						switch s.Kind {
						case rbacv1.UserKind:
							subjects = append(subjects, s.Name)
						case rbacv1.ServiceAccountKind:
							subjects = append(subjects, fmt.Sprintf("%s:%s", s.Namespace, s.Name))
						}
					}
					return subjects, nil
				},
				subjectGroupIndex: func(obj interface{}) ([]string, error) {
					eetb, ok := obj.(*egressv1beta1.EgressEntitlementBinding)
					if !ok || len(eetb.Spec.Subjects) == 0 {
						return []string{}, nil
					}
					var subjects []string
					for _, s := range eetb.Spec.Subjects {
						if s.Kind == rbacv1.GroupKind {
							subjects = append(subjects, s.Name)
						}
					}
					return subjects, nil
				},
			},
		)
	}
	return c
}

// Run begins watching and syncing of the EgressController.
func (c *EgressController) Run(stopCh <-chan struct{}) {
	defer c.queue.ShutDown()

	klog.InfoS("Starting", "controller", controllerName)
	defer klog.InfoS("Shutting down", "controller", controllerName)

	cacheSyncs := []cache.InformerSynced{c.egressListerSynced, c.groupingInterfaceSynced, c.externalIPAllocator.HasSynced}
	if !cache.WaitForNamedCacheSync(controllerName, stopCh, cacheSyncs...) {
		return
	}
	egresses, _ := c.egressLister.List(labels.Everything())
	c.restoreIPAllocations(egresses)
	for i := 0; i < defaultWorkers; i++ {
		go wait.Until(c.egressGroupWorker, time.Second, stopCh)
	}
	<-stopCh
}

// restoreIPAllocations restores the existing EgressIPs of Egresses and records the successful ones in ipAllocationMap.
func (c *EgressController) restoreIPAllocations(egresses []*egressv1beta1.Egress) {
	var previousIPAllocations []externalippool.IPAllocation
	for _, egress := range egresses {
		restorePoolIPs := make(map[string]string)
		if egress.Spec.ExternalIPPool != "" {
			restorePoolIPs[egress.Spec.ExternalIPPool] = egress.Spec.EgressIP
		} else {
			for i, pool := range egress.Spec.ExternalIPPools {
				if len(egress.Spec.EgressIPs) <= i {
					break
				}
				egressIP := egress.Spec.EgressIPs[i]
				restorePoolIPs[pool] = egressIP
			}
		}
		for pool, ipStr := range restorePoolIPs {
			// Ignore Egress that is not associated to ExternalIPPool or doesn't have EgressIP assigned.
			if ipStr == "" {
				continue
			}
			ip := net.ParseIP(ipStr)
			allocation := externalippool.IPAllocation{
				ObjectReference: v1.ObjectReference{
					Name: egress.Name,
					Kind: egress.Kind,
				},
				IPPoolName: pool,
				IP:         ip,
			}
			previousIPAllocations = append(previousIPAllocations, allocation)
		}
	}
	succeededAllocations := c.externalIPAllocator.RestoreIPAllocations(previousIPAllocations)
	for _, alloc := range succeededAllocations {
		c.setIPAllocation(alloc.ObjectReference.Name, alloc.IP, alloc.IPPoolName)
		klog.InfoS("Restored EgressIP", "egress", alloc.ObjectReference.Name, "ip", alloc.IP, "pool", alloc.IPPoolName)
	}
}

func (c *EgressController) egressGroupWorker() {
	for c.processNextEgressGroupWorkItem() {
	}
}

func (c *EgressController) processNextEgressGroupWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	if err := c.syncEgress(key); err != nil {
		// Put the item back on the workqueue to handle any transient errors.
		c.queue.AddRateLimited(key)
		klog.ErrorS(err, "Failed to sync EgressGroup", "key", key)
		return true
	}
	// If no error occurs we Forget this item so it does not get queued again until
	// another change happens.
	c.queue.Forget(key)
	return true
}

func (c *EgressController) getIPAllocation(egressName string) ipAllocation {
	c.ipAllocationMutex.RLock()
	defer c.ipAllocationMutex.RUnlock()
	return c.ipAllocationMap[egressName]
}

func (c *EgressController) deleteIPAllocation(egressName, poolName string) {
	c.ipAllocationMutex.Lock()
	defer c.ipAllocationMutex.Unlock()
	delete(c.ipAllocationMap[egressName], poolName)
	if len(c.ipAllocationMap[egressName]) == 0 {
		delete(c.ipAllocationMap, egressName)
	}
}

func (c *EgressController) setIPAllocation(egressName string, ip net.IP, poolName string) {
	c.ipAllocationMutex.Lock()
	defer c.ipAllocationMutex.Unlock()
	ipAllocations := c.ipAllocationMap[egressName]
	if ipAllocations == nil {
		ipAllocations = make(map[string]string)
		c.ipAllocationMap[egressName] = ipAllocations
	}
	ipAllocations[poolName] = ip.String()
}

func (c *EgressController) syncEgressIP(egress *egressv1beta1.Egress) (*egressv1beta1.Egress, error) {
	// specEgressIPs tracks EgressIPs that should be updated to the Egress's spec.
	specEgressIPs := make(map[string]string)
	if egress.Spec.ExternalIPPool != "" {
		specEgressIPs[egress.Spec.ExternalIPPool] = egress.Spec.EgressIP
	} else {
		for i, eip := range egress.Spec.ExternalIPPools {
			if len(egress.Spec.EgressIPs) > i {
				specEgressIPs[eip] = egress.Spec.EgressIPs[i]
			} else {
				specEgressIPs[eip] = ""
			}
		}
	}

	poolsToAllocate := sets.StringKeySet(specEgressIPs)
	for prevIPPool, prevIP := range c.getIPAllocation(egress.Name) {
		specEgressIP, exists := specEgressIPs[prevIPPool]
		if exists && (specEgressIP == "" || specEgressIP == prevIP) {
			if c.externalIPAllocator.IPPoolHasIP(prevIPPool, net.ParseIP(prevIP)) {
				poolsToAllocate.Delete(prevIPPool)
				// Reuse previously allocated IP.
				specEgressIPs[prevIPPool] = prevIP
				continue
			}
			// The ExternalIPPool may no longer exist, or the IP is not in range.
			specEgressIPs[prevIPPool] = ""
		}
		// The ExternalIPPool may no longer exist, or the IP is not in range.
		// Release the previous allocation if any of the following happens:
		// 1. The pool is not in the spec of the Egress.
		// 2. The desired EgressIP is set and doesn't match the previous IP.
		// 3. The pool no longer exists.
		// 4. The IP is not in the range of the pool.
		c.releaseEgressIP(egress.Name, net.ParseIP(prevIP), prevIPPool)
	}

	// No need to update the Egress if no pool is set.
	if egress.Spec.ExternalIPPool == "" && len(egress.Spec.ExternalIPPools) == 0 {
		return egress, nil
	}

	succeed := false
	for eip := range poolsToAllocate {
		specEgressIP := specEgressIPs[eip]
		var ip net.IP
		if specEgressIP != "" {
			ip = net.ParseIP(specEgressIP)
			if err := c.externalIPAllocator.UpdateIPAllocation(eip, ip); err != nil {
				klog.ErrorS(err, "Error when allocating specific IP for Egress From ExternalIPPool", "externalIPPool", eip, "egressIP", specEgressIP, "egress", klog.KObj(egress))
				if err == externalippool.ErrExternalIPPoolNotFound {
					// Reclaim the IP from the Egress API.
					specEgressIPs[eip] = ""
				}
				continue
			}
		} else {
			var err error
			// User doesn't specify the Egress IP, allocate one.
			if ip, err = c.externalIPAllocator.AllocateIPFromPool(eip); err != nil {
				klog.ErrorS(err, "Error when allocating an IP for Egress from ExternalIPPool", "externalIPPool", eip, "egress", klog.KObj(egress))
				continue
			}
			// Claim the IP from the Egress API.
			specEgressIPs[eip] = ip.String()
		}
		c.setIPAllocation(egress.Name, ip, eip)
		defer func() {
			if !succeed {
				c.releaseEgressIP(egress.Name, ip, eip)
			}
		}()
	}

	updatedEgress, err := c.updateEgressIP(egress, specEgressIPs)
	if err != nil {
		return egress, err
	} else {
		egress = updatedEgress
	}
	succeed = true
	return egress, nil
}

// updateEgressIP updates the Egress's EgressIP/EgressIPs in Kubernetes API.
func (c *EgressController) updateEgressIP(egress *egressv1beta1.Egress, ips map[string]string) (*v1beta1.Egress, error) {
	var egressIPPtr *string
	var egressIP string
	var egressIPs []string
	if egress.Spec.ExternalIPPool != "" {
		egressIP = ips[egress.Spec.ExternalIPPool]
		if egressIP != "" {
			egressIPPtr = &egressIP
		}
	} else {
		for _, eip := range egress.Spec.ExternalIPPools {
			ip := ips[eip]
			egressIPs = append(egressIPs, ip)
		}
	}
	if egressIP == egress.Spec.EgressIP && reflect.DeepEqual(egressIPs, egress.Spec.EgressIPs) {
		return egress, nil
	}
	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"egressIP":  egressIPPtr,
			"egressIPs": egressIPs,
		},
	}
	patchBytes, _ := json.Marshal(patch)
	if updatedEgress, err := c.crdClient.CrdV1beta1().Egresses().Patch(context.TODO(), egress.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{}); err != nil {
		return nil, fmt.Errorf("error when updating EgressIP for Egress %s: %v", egress.Name, err)
	} else {
		return updatedEgress, nil
	}
}

// releaseEgressIP removes the Egress's ipAllocation in the cache and releases the IP to the pool.
func (c *EgressController) releaseEgressIP(egressName string, egressIP net.IP, poolName string) {
	if err := c.externalIPAllocator.ReleaseIP(poolName, egressIP); err != nil {
		if err == externalippool.ErrExternalIPPoolNotFound {
			// Ignore the error since the external IP Pool could be deleted.
			klog.InfoS("Failed to release EgressIP because IP Pool does not exist", "egress", egressName, "ip", egressIP, "pool", poolName)
		} else {
			// It is possible for the external IP Pool to have been deleted and
			// recreated immediately with a different range, which would trigger this
			// case. Transient errors in ReleaseIP are not possible, so there is no
			// point in retrying. We should still delete our own state by calling
			// deleteIPAllocation.
			klog.ErrorS(err, "Failed to release IP", "ip", egressIP, "pool", poolName)
		}
	} else {
		klog.InfoS("Released EgressIP", "egress", egressName, "ip", egressIP, "pool", poolName)
	}
	c.deleteIPAllocation(egressName, poolName)
}

func (c *EgressController) syncEgress(key string) error {
	startTime := time.Now()
	defer func() {
		d := time.Since(startTime)
		klog.V(2).InfoS("Finished syncing Egress", "egress", key, "duration", d)
	}()

	egress, err := c.egressLister.Get(key)
	if err != nil {
		// The Egress has been deleted, release its EgressIP if there was one.
		for pool, ip := range c.getIPAllocation(key) {
			c.releaseEgressIP(key, net.ParseIP(ip), pool)
		}
		return nil
	}

	egress, err = c.syncEgressIP(egress)
	c.updateEgressAllocatedCondition(egress, err)
	if err != nil {
		return err
	}

	egressGroupObj, found, _ := c.egressGroupStore.Get(key)
	if !found {
		klog.V(2).InfoS("EgressGroup %s not found", "name", key)
		return nil
	}

	nodeNames := sets.Set[string]{}
	podNum := 0
	memberSetByNode := make(map[string]controlplane.GroupMemberSet)
	egressGroup := egressGroupObj.(*antreatypes.EgressGroup)
	pods, _ := c.groupingInterface.GetEntities(egressGroupType, key)
	for _, pod := range pods {
		// Ignore Pod if it's not scheduled or is already terminated. And Egress does not support HostNetwork Pods, so also ignore
		// Pod if it's HostNetwork Pod.
		if pod.Spec.NodeName == "" || pod.Spec.HostNetwork || k8s.IsPodTerminated(pod) {
			continue
		}
		podNum++
		podSet := memberSetByNode[pod.Spec.NodeName]
		if podSet == nil {
			podSet = controlplane.GroupMemberSet{}
			memberSetByNode[pod.Spec.NodeName] = podSet
		}
		groupMember := &controlplane.GroupMember{
			Pod: &controlplane.PodReference{
				Name:      pod.Name,
				Namespace: pod.Namespace,
			},
		}
		podSet.Insert(groupMember)
		// Update the NodeNames in order to set the SpanMeta for EgressGroup.
		nodeNames.Insert(pod.Spec.NodeName)
	}
	updatedEgressGroup := &antreatypes.EgressGroup{
		UID:               egressGroup.UID,
		Name:              egressGroup.Name,
		GroupMemberByNode: memberSetByNode,
		SpanMeta:          antreatypes.SpanMeta{NodeNames: nodeNames},
	}
	klog.V(2).InfoS("Updating existing EgressGroup", "name", key, "podNum", podNum, "nodeNum", nodeNames.Len())
	c.egressGroupStore.Update(updatedEgressGroup)
	return nil
}

func (c *EgressController) enqueueEgressGroup(key string) {
	klog.V(4).InfoS("Adding new key to EgressGroup queue", "key", key)
	c.queue.Add(key)
}

// addEgress processes Egress ADD events and creates corresponding EgressGroup.
func (c *EgressController) addEgress(obj interface{}) {
	egress := obj.(*egressv1beta1.Egress)
	klog.InfoS("Processing Egress ADD event", "egress", egress.Name, "selector", egress.Spec.AppliedTo)
	// Create an EgressGroup object corresponding to this Egress and enqueue task to the workqueue.
	egressGroup := &antreatypes.EgressGroup{
		Name: egress.Name,
		UID:  egress.UID,
	}
	c.egressGroupStore.Create(egressGroup)
	// Register the group to the grouping interface.
	groupSelector := antreatypes.NewGroupSelector("", egress.Spec.AppliedTo.PodSelector, egress.Spec.AppliedTo.NamespaceSelector, nil, nil)
	c.groupingInterface.AddGroup(egressGroupType, egress.Name, groupSelector)
	c.queue.Add(egress.Name)
}

// updateEgress processes Egress UPDATE events and updates corresponding EgressGroup.
func (c *EgressController) updateEgress(old, cur interface{}) {
	oldEgress := old.(*egressv1beta1.Egress)
	curEgress := cur.(*egressv1beta1.Egress)
	klog.InfoS("Processing Egress UPDATE event", "egress", curEgress.Name, "selector", curEgress.Spec.AppliedTo)
	// TODO: Define custom Equal function to be more efficient.
	if !reflect.DeepEqual(oldEgress.Spec.AppliedTo, curEgress.Spec.AppliedTo) {
		// Update the group's selector in the grouping interface.
		groupSelector := antreatypes.NewGroupSelector("", curEgress.Spec.AppliedTo.PodSelector, curEgress.Spec.AppliedTo.NamespaceSelector, nil, nil)
		c.groupingInterface.AddGroup(egressGroupType, curEgress.Name, groupSelector)
	}
	if oldEgress.GetGeneration() != curEgress.GetGeneration() {
		c.queue.Add(curEgress.Name)
	}
}

// deleteEgress processes Egress DELETE events and deletes corresponding EgressGroup.
func (c *EgressController) deleteEgress(obj interface{}) {
	egress := obj.(*egressv1beta1.Egress)
	klog.InfoS("Processing Egress DELETE event", "egress", egress.Name)
	c.egressGroupStore.Delete(egress.Name)
	// Unregister the group from the grouping interface.
	c.groupingInterface.DeleteGroup(egressGroupType, egress.Name)
	c.queue.Add(egress.Name)
}

// enqueueEgresses enqueues all Egresses that refer to the provided ExternalIPPool.
func (c *EgressController) enqueueEgresses(poolName string) {
	objects, _ := c.egressIndexer.ByIndex(externalIPPoolIndex, poolName)
	for _, object := range objects {
		egress := object.(*egressv1beta1.Egress)
		c.queue.Add(egress.Name)
	}
}

func (c *EgressController) updateEgressAllocatedCondition(egress *egressv1beta1.Egress, err error) {
	var desiredCondition *egressv1beta1.EgressCondition
	if egress.Spec.ExternalIPPool != "" {
		if err == nil {
			desiredCondition = &egressv1beta1.EgressCondition{
				Type:               egressv1beta1.IPAllocated,
				Status:             v1.ConditionTrue,
				Reason:             "Allocated",
				Message:            "EgressIP is successfully allocated",
				LastTransitionTime: metav1.Now(),
			}
		} else {
			desiredCondition = &egressv1beta1.EgressCondition{
				Type:               egressv1beta1.IPAllocated,
				Status:             v1.ConditionFalse,
				Reason:             "AllocationError",
				Message:            fmt.Sprintf("Cannot allocate EgressIP from ExternalIPPool: %v", err),
				LastTransitionTime: metav1.Now(),
			}
		}
	}

	toUpdate := egress.DeepCopy()
	var updateErr, getErr error
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		actualCondition := egressv1beta1.GetEgressCondition(toUpdate.Status.Conditions, egressv1beta1.IPAllocated)
		if compareConditionIgnoringTimestamp(actualCondition, desiredCondition) {
			return nil
		}
		var newConditions []egressv1beta1.EgressCondition
		for _, c := range toUpdate.Status.Conditions {
			if c.Type != egressv1beta1.IPAllocated {
				newConditions = append(newConditions, c)
			}
		}
		if desiredCondition != nil {
			newConditions = append(newConditions, *desiredCondition)
		}
		toUpdate.Status.Conditions = newConditions
		_, updateErr = c.crdClient.CrdV1beta1().Egresses().UpdateStatus(context.TODO(), toUpdate, metav1.UpdateOptions{})
		if updateErr != nil && errors.IsConflict(updateErr) {
			if toUpdate, getErr = c.crdClient.CrdV1beta1().Egresses().Get(context.TODO(), egress.Name, metav1.GetOptions{}); getErr != nil {
				return getErr
			}
		}
		return updateErr
	}); err != nil {
		klog.ErrorS(err, "Error updating Egress Status")
	}
}

// compareConditionIgnoringTimestamp compares two conditions ignoring the timestamp
func compareConditionIgnoringTimestamp(condition1, condition2 *egressv1beta1.EgressCondition) bool {
	if condition1 == nil && condition2 == nil {
		return true
	}
	if condition1 == nil || condition2 == nil {
		return false
	}
	return condition1.Message == condition2.Message && condition1.Reason == condition2.Reason && condition1.Status == condition2.Status
}
