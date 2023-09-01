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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	agenttypes "antrea.io/antrea/pkg/agent/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	egressv1beta1 "antrea.io/antrea/pkg/apis/crd/v1beta1"
	egressinformers "antrea.io/antrea/pkg/client/informers/externalversions/crd/v1beta1"
	egresslisters "antrea.io/antrea/pkg/client/listers/crd/v1beta1"
	"antrea.io/antrea/pkg/cloudprovider"
	coreinformers "k8s.io/client-go/informers/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
)

const (
	cloudControllerName = "EgressCloudController"

	egressNodeIndex = "egressNode"
)

// cloudState keeps the actual state of the Egress that has been realized on cloud.
type cloudState struct {
	// Private IP for Egress on cloud interface.
	secondPrivateIP string
	// The last known Node Object which can be used to unassign the Egress IP.
	node *corev1.Node
}

// EgressCloudController is responsible for synchronizing the Egress status on cloud nodes selected by Egresses.
type EgressCloudController struct {
	k8sClient clientset.Interface

	egressInformer egressinformers.EgressInformer
	egressLister   egresslisters.EgressLister
	egressIndexer  cache.Indexer
	// egressListerSynced is a function which returns true if the Egresses shared informer has been synced at least once.
	egressListerSynced cache.InformerSynced

	nodeInformer     coreinformers.NodeInformer
	nodeLister       corelisters.NodeLister
	nodeListerSynced cache.InformerSynced

	// egressQueue maintains the Egresses that need to be synced.
	egressQueue workqueue.RateLimitingInterface
	// nodeQueue maintains the Nodes that need to be synced.
	nodeQueue workqueue.RateLimitingInterface

	// cloudProvider interface implements different public cloud api calls
	cloudProvider cloudprovider.Interface
	cloudStates   map[string]*cloudState
	// cloudStatesMutex avoids race condition during updating cloudStates items
	cloudStatesMutex sync.RWMutex
}

// NewEgressCloudController returns a new *EgressCloudController.
func NewEgressCloudController(k8sClient clientset.Interface,
	egressInformer egressinformers.EgressInformer,
	nodeInformer coreinformers.NodeInformer,
	cloudProvider cloudprovider.Interface) (*EgressCloudController, error) {
	c := &EgressCloudController{
		k8sClient:          k8sClient,
		egressInformer:     egressInformer,
		egressLister:       egressInformer.Lister(),
		egressListerSynced: egressInformer.Informer().HasSynced,
		egressIndexer:      egressInformer.Informer().GetIndexer(),
		nodeInformer:       nodeInformer,
		nodeLister:         nodeInformer.Lister(),
		nodeListerSynced:   nodeInformer.Informer().HasSynced,
		egressQueue:        workqueue.NewNamedRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(minRetryDelay, maxRetryDelay), "egress"),
		nodeQueue:          workqueue.NewNamedRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(minRetryDelay, maxRetryDelay), "node"),
		cloudStates:        map[string]*cloudState{},
		cloudProvider:      cloudProvider,
	}

	egressInformer.Informer().AddIndexers(
		cache.Indexers{
			// egressNodeIndex will be used to get all Egresses assigned to a given Node.
			egressNodeIndex: func(obj interface{}) ([]string, error) {
				egress, ok := obj.(*egressv1beta1.Egress)
				if !ok {
					return nil, fmt.Errorf("obj is not Egress: %+v", obj)
				}
				if egress.Status.EgressNode == "" {
					return nil, nil
				}
				return []string{egress.Status.EgressNode}, nil
			},
		})

	egressInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.addEgress,
			UpdateFunc: c.updateEgress,
			DeleteFunc: c.deleteEgress,
		},
		resyncPeriod,
	)

	nodeInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc: c.enqueueNode,
			UpdateFunc: func(_, obj interface{}) {
				c.enqueueNode(obj)
			},
		},
		resyncPeriod,
	)
	return c, nil
}

// addEgress processes Egress Status ADD events.
func (c *EgressCloudController) addEgress(obj interface{}) {
	egress := obj.(*egressv1beta1.Egress)
	if egress.Status.EgressIP == "" || egress.Status.EgressNode == "" {
		klog.V(4).InfoS("Skip Egress ADD event", "EgressName", egress.Name, "EgressIP", egress.Status.EgressIP, "EgressNode", egress.Status.EgressNode)
		return
	}
	klog.V(2).InfoS("Processing Egress %s ADD event", egress.Name)
	c.egressQueue.Add(egress.Name)
}

// updateEgress processes Egress Status UPDATE events.
func (c *EgressCloudController) updateEgress(old, cur interface{}) {
	oldEgress := old.(*egressv1beta1.Egress)
	curEgress := cur.(*egressv1beta1.Egress)
	// Ignore handling UPDATE event if there is no Egress status update.
	if curEgress.Status.EgressNode == oldEgress.Status.EgressNode && curEgress.Status.EgressIP == oldEgress.Status.EgressIP {
		klog.V(4).InfoS("Skip Egress UPDATE event", "EgressName", curEgress.Name, "curEgressIP", curEgress.Status.EgressIP, "curEgressNode", curEgress.Status.EgressNode)
		return
	}
	c.egressQueue.Add(curEgress.Name)
	klog.V(2).InfoS("Processed Egress Status UPDATE event", "egress", klog.KObj(curEgress), "curEgressIP", curEgress.Status.EgressIP, "curEgressNode", curEgress.Status.EgressNode, "oldEgressIP", oldEgress.Status.EgressIP, "oldEgressNode", oldEgress.Status.EgressNode)
}

// deleteEgress processes Egress DELETE events.
func (c *EgressCloudController) deleteEgress(obj interface{}) {
	egress := obj.(*egressv1beta1.Egress)
	klog.V(2).InfoS("Processing Egress %s DELETE event", egress.Name)
	c.egressQueue.Add(egress.Name)
}

func (c *EgressCloudController) enqueueNode(obj interface{}) {
	node := obj.(*corev1.Node)
	// Skip if the Node is already annotated.
	_, exists := node.Annotations[agenttypes.NodeMaxEgressIPsAnnotationKey]
	if exists {
		return
	}
	// Skip if the instance type is unknown.
	_, exists = node.Labels[corev1.LabelInstanceTypeStable]
	if !exists {
		return
	}
	c.nodeQueue.Add(node.Name)
}

// Run begins watching and syncing of the cloud controller.
func (c *EgressCloudController) Run(stopCh <-chan struct{}) {
	defer c.egressQueue.ShutDown()

	klog.Infof("Starting %s", cloudControllerName)
	defer klog.Infof("Shutting down %s", cloudControllerName)

	cacheSyncs := []cache.InformerSynced{c.egressListerSynced, c.nodeListerSynced}
	if !cache.WaitForNamedCacheSync(cloudControllerName, stopCh, cacheSyncs...) {
		return
	}
	c.restoreCloudAssignments()
	for i := 0; i < defaultWorkers; i++ {
		go wait.Until(c.egressWorker, time.Second, stopCh)
		go wait.Until(c.nodeWorker, time.Second, stopCh)
	}
	<-stopCh
}

// restoreCloudAssignments restores existing cloud IPs of Egresses.
func (c *EgressCloudController) restoreCloudAssignments() {
	nodes, _ := c.nodeLister.List(labels.Everything())
	for i := range nodes {
		node := nodes[i]
		currentIPs, err := c.cloudProvider.GetIPsByNode(node)
		if err != nil {
			klog.ErrorS(err, "Failed to get IPs from cloud Node", "node", node.Name)
			continue
		}
		// Restore IPs to cloud nodes
		egressesOnNode, _ := c.egressInformer.Informer().GetIndexer().ByIndex(egressNodeIndex, node.Name)
		expectIPs := []string{}
		expectKeys := make(map[string]string)
		for _, obj := range egressesOnNode {
			egressOnNode := obj.(*egressv1beta1.Egress)
			// Ignore Egress that doesn't have effective EgressIP. This should only happen in upgrade case.
			if egressOnNode.Status.EgressIP == "" {
				continue
			}
			expectIPs = append(expectIPs, egressOnNode.Status.EgressIP)
			expectKeys[egressOnNode.Status.EgressIP] = egressOnNode.Name
		}
		ipsToAssign, ipsToUnassign := getIPDiffs(expectIPs, currentIPs)
		// Unassign unused IPs first, then assign expect IPs.
		for ipToUnassign := range ipsToUnassign {
			if err := c.cloudProvider.UnassignIPToNode(ipToUnassign, node); err != nil {
				klog.ErrorS(err, "Failed to unassign Egress IP to cloud Node", "ip", ipToUnassign, "node", node.Name)
			}
		}
		for ipToAssign := range ipsToAssign {
			if err := c.cloudProvider.AssignIPToNode(ipToAssign, node); err != nil {
				klog.ErrorS(err, "Failed to assign Egress IP to cloud Node", "ip", ipToAssign, "node", node.Name)
			} else {
				_ = c.newCloudState(expectKeys[ipToAssign], ipToAssign, node)
			}
			// Delete IPs to assign from expectKeys to keep assigned IPs.
			delete(expectKeys, ipToAssign)
		}
		for assignedIP := range expectKeys {
			_ = c.newCloudState(expectKeys[assignedIP], assignedIP, node)
		}
		klog.V(4).InfoS("Restored Egress IP assignments of Node on cloud", "nodeName", node.Name)
	}
	klog.InfoS("Restored Egress IP assignments of Nodes on cloud")
}

func (c *EgressCloudController) egressWorker() {
	for c.processNextEgressWorkItem() {
	}
}

func (c *EgressCloudController) processNextEgressWorkItem() bool {
	key, quit := c.egressQueue.Get()
	if quit {
		return false
	}
	defer c.egressQueue.Done(key)

	err := c.syncEgress(key.(string))
	if err != nil {
		// Put the item back on the workqueue to handle any transient errors.
		c.egressQueue.AddRateLimited(key)
		klog.ErrorS(err, "Failed to sync Egress", "key", key)
		return true
	}
	// If no error occurs we forget this item so it does not get queued again until
	// another change happens.
	c.egressQueue.Forget(key)
	return true
}

func (c *EgressCloudController) syncEgress(key string) error {
	startTime := time.Now()
	defer func() {
		d := time.Since(startTime)
		klog.V(2).Infof("Finished syncing Egress %s. (%v)", key, d)
	}()

	egress, err := c.egressLister.Get(key)
	if err != nil {
		if errors.IsNotFound(err) {
			// The Egress has been deleted, unassign its EgressIP to cloud node if there was one.
			state, exists := c.getCloudState(key)
			if !exists {
				klog.V(4).InfoS("Neither CloudState nor Egress exist")
				return nil
			}
			if err := c.unassignIPWithCloudState(key, state); err != nil {
				return err
			}
			return nil
		}
		return err
	}
	egressIP := egress.Status.EgressIP
	egressNode := egress.Status.EgressNode
	cState, exist := c.getCloudState(key)
	if exist {
		if egressIP == cState.secondPrivateIP && egressNode == cState.node.Name {
			klog.V(4).InfoS("CloudState has no update", "ip", egressIP, "nodeName", egressNode)
			return nil
		}
		if err := c.unassignIPWithCloudState(key, cState); err != nil {
			return err
		}
	}
	if egressIP == "" || egressNode == "" {
		// Never assign any IP or create new cloud state for Egress without IP or node status.
		return nil
	}
	node, err := c.nodeLister.Get(egressNode)
	if err != nil {
		return fmt.Errorf("failed to get Node %s: %w", egressNode, err)
	}
	if err := c.cloudProvider.AssignIPToNode(egressIP, node); err != nil {
		return fmt.Errorf("failed to assign IP %s to cloud node %s: %w", egressIP, egressNode, err)
	}
	klog.InfoS("Assigned Egress IP to cloud Node", "egress", egress.Name, "ip", egressIP, "nodeName", egressNode)
	_ = c.newCloudState(key, egressIP, node)
	return nil
}

func (c *EgressCloudController) unassignIPWithCloudState(egressName string, state *cloudState) error {
	if err := c.cloudProvider.UnassignIPToNode(state.secondPrivateIP, state.node); err != nil {
		return fmt.Errorf("failed to unassign Egress IP %s on cloud node %s: %w", state.secondPrivateIP, state.node.Name, err)
	}
	klog.InfoS("Unassigned Egress IP to cloud node", "egress", egressName, "ip", state.secondPrivateIP, "nodeName", state.node.Name)
	c.deleteCloudState(egressName)
	return nil
}

func (c *EgressCloudController) getCloudState(egressName string) (*cloudState, bool) {
	c.cloudStatesMutex.RLock()
	defer c.cloudStatesMutex.RUnlock()
	state, exists := c.cloudStates[egressName]
	return state, exists
}

func (c *EgressCloudController) deleteCloudState(egressName string) {
	c.cloudStatesMutex.Lock()
	defer c.cloudStatesMutex.Unlock()
	delete(c.cloudStates, egressName)
}

func (c *EgressCloudController) newCloudState(egressName, ip string, node *corev1.Node) *cloudState {
	c.cloudStatesMutex.Lock()
	defer c.cloudStatesMutex.Unlock()
	state := &cloudState{
		secondPrivateIP: ip,
		node:            node,
	}
	c.cloudStates[egressName] = state
	return state
}

func (c *EgressCloudController) nodeWorker() {
	for c.processNextNodeWorkItem() {
	}
}

func (c *EgressCloudController) processNextNodeWorkItem() bool {
	key, quit := c.nodeQueue.Get()
	if quit {
		return false
	}
	defer c.nodeQueue.Done(key)

	err := c.syncNode(key.(string))
	if err != nil {
		// Put the item back on the workqueue to handle any transient errors.
		c.nodeQueue.AddRateLimited(key)
		klog.ErrorS(err, "Failed to sync Node", "key", key)
		return true
	}
	// If no error occurs we forget this item so it does not get queued again until
	// another change happens.
	c.nodeQueue.Forget(key)
	return true
}

func (c *EgressCloudController) syncNode(key string) error {
	startTime := time.Now()
	defer func() {
		d := time.Since(startTime)
		klog.V(2).Infof("Finished syncing Node %s. (%v)", key, d)
	}()

	node, err := c.nodeLister.Get(key)
	if err != nil {
		// The Node has been deleted, do nothing.
		return nil
	}

	// Skip if the Node is already annotated.
	_, exists := node.Annotations[agenttypes.NodeMaxEgressIPsAnnotationKey]
	if exists {
		return nil
	}
	// Skip if the instance type is unknown.
	instanceType, exists := node.Labels[corev1.LabelInstanceTypeStable]
	if !exists {
		return nil
	}
	maxIPsPerNode, ok, err := c.cloudProvider.GetMaxIPsByInstanceType(instanceType)
	if err != nil {
		return fmt.Errorf("error getting maximum IPs of instance type %s: %v", instanceType, err)
	}
	// maxIPsPerNode cannot be retrieved, just return and do not retry.
	if !ok {
		return nil
	}
	patch, _ := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				agenttypes.NodeMaxEgressIPsAnnotationKey: fmt.Sprint(maxIPsPerNode),
			},
		},
	})
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := c.k8sClient.CoreV1().Nodes().Patch(context.TODO(), node.Name, apitypes.MergePatchType, patch, metav1.PatchOptions{})
		return err
	}); err != nil {
		return fmt.Errorf("error annotating maximum IPs for Node %s: %v", node.Name, err)
	}
	return nil
}

func getIPDiffs(expectIPs []string, currentIPs []string) (sets.String, sets.String) {
	set1 := sets.NewString(expectIPs...)
	set2 := sets.NewString(currentIPs...)

	return set1.Difference(set2), set2.Difference(set1)
}
