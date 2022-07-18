// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package signature

import (
	"context"
	"fmt"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	tanzucrd "antrea.io/antrea/pkg/apis/tanzucrd/v1alpha1"
	tanzucrdinformers "antrea.io/antrea/pkg/client/informers/externalversions/tanzucrd/v1alpha1"
	tanzucrdlisters "antrea.io/antrea/pkg/client/listers/tanzucrd/v1alpha1"
	"antrea.io/antrea/pkg/idps/agent"
	"antrea.io/antrea/pkg/idps/agent/suricata"
	bytesutil "antrea.io/antrea/pkg/idps/util/bytes"
)

const (
	controllerName = "SignatureController"

	apiPrefix = "/signatures"

	maxRetries     = 15
	minRetryDelay  = 5 * time.Second
	maxRetryDelay  = 300 * time.Second
	defaultWorkers = 4

	resyncPeriod time.Duration = 0
)

type Controller struct {
	signatureProviderInfoInformer     cache.SharedIndexInformer
	signatureProviderInfoLister       tanzucrdlisters.IDPSSignatureProviderInfoLister
	signatureProviderInfoListerSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface

	// idpsClientProvider is used to get an Antrea IDPS client.
	idpsClientProvider agent.IDPSClientProvider
	// suricata is used to load signature data to Suricata.
	suricata suricata.Interface
}

func NewController(signatureProviderInfoInformer tanzucrdinformers.IDPSSignatureProviderInfoInformer,
	idpsClientProvider agent.IDPSClientProvider,
	suricata suricata.Interface) *Controller {
	d := &Controller{
		signatureProviderInfoInformer:     signatureProviderInfoInformer.Informer(),
		signatureProviderInfoLister:       signatureProviderInfoInformer.Lister(),
		signatureProviderInfoListerSynced: signatureProviderInfoInformer.Informer().HasSynced,
		queue:                             workqueue.NewNamedRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(minRetryDelay, maxRetryDelay), "signatures"),
		idpsClientProvider:                idpsClientProvider,
		suricata:                          suricata,
	}

	d.signatureProviderInfoInformer.AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    d.addSignatureProviderInfo,
			UpdateFunc: d.updateSignatureProviderInfo,
			DeleteFunc: nil,
		},
		resyncPeriod,
	)

	return d
}

func (c *Controller) addSignatureProviderInfo(obj interface{}) {
	signatureProviderInfo := obj.(*tanzucrd.IDPSSignatureProviderInfo)
	klog.V(2).InfoS("Processing IDPSSignatureProviderInfo ADD event", "IDPSSignatureProviderInfo", klog.KObj(signatureProviderInfo))
	c.queue.Add(signatureProviderInfo.Name)
}

func (c *Controller) updateSignatureProviderInfo(oldObj, obj interface{}) {
	oldSignatureProvideInfo := oldObj.(*tanzucrd.IDPSSignatureProviderInfo)
	signatureProviderInfo := obj.(*tanzucrd.IDPSSignatureProviderInfo)
	if oldSignatureProvideInfo.GetGeneration() != signatureProviderInfo.GetGeneration() {
		klog.V(2).InfoS("Processing IDPSSignatureProviderInfo UPDATE event", "IDPSSignatureProviderInfo", klog.KObj(signatureProviderInfo))
		c.queue.Add(signatureProviderInfo.Name)
	}
}

func (c *Controller) Run(stopCh <-chan struct{}) {
	defer c.queue.ShutDown()

	klog.InfoS("Starting", "controllerName", controllerName)
	defer klog.InfoS("Shutting down", "controllerName", controllerName)

	if !cache.WaitForNamedCacheSync(controllerName, stopCh, c.signatureProviderInfoListerSynced) {
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

	err := c.syncSignatureProviderInfo(cKey.(string))
	c.handleErr(err, cKey)

	return true
}

func (c *Controller) syncSignatureProviderInfo(signatureProviderName string) error {
	startTime := time.Now()
	defer func() {
		klog.V(2).InfoS("Finished syncing IDPSSignatureProviderInfo", "IDPSSignatureProviderInfo", signatureProviderName, "durationTime", time.Since(startTime))
	}()

	// Get the IDPSSignatureProviderInfo object.
	signatureProviderInfoObj, err := c.signatureProviderInfoLister.Get(signatureProviderName)
	if err != nil {
		return err
	}
	// Get the signature data.
	signatureData, err := c.fetchSignatureData(signatureProviderInfoObj)
	if err != nil {
		return err
	}
	// Load the signature data to Suricata.
	if err = c.suricata.LoadSignature(signatureProviderName, signatureData); err != nil {
		return err
	}

	klog.InfoS("Synced signature data successfully", "IDPSSignatureProviderInfo", signatureProviderName, "Version", signatureProviderInfoObj.SignatureBundle.Version)
	return nil
}

func (c *Controller) fetchSignatureData(signatureProviderInfo *tanzucrd.IDPSSignatureProviderInfo) ([]byte, error) {
	// Get an Antrea IDPS client.
	client, err := c.idpsClientProvider.GetIDPSClient()
	if err != nil {
		return nil, fmt.Errorf("failed to get IDPS client: %w", err)
	}
	// Download signature data from Antrea IDPS Controller.
	uri := fmt.Sprintf("%s/%s", apiPrefix, signatureProviderInfo.Name)
	signatureData, err := client.Get().RequestURI(uri).DoRaw(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to download signature data: %w", err)
	}
	// Verify the downloaded signature data.
	sha256Checksum := bytesutil.Sha256Checksum(signatureData)
	if sha256Checksum != signatureProviderInfo.SignatureBundle.Sha256Checksum {
		return nil, fmt.Errorf("got unexpected sha256 checksum value: %s, expected value: %s", sha256Checksum, signatureProviderInfo.SignatureBundle.Sha256Checksum)
	}
	return signatureData, nil
}

func (c *Controller) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	if c.queue.NumRequeues(key) < maxRetries {
		klog.Warningf("Error syncing object for %q resource, retrying. Error: %v", key, err)
		c.queue.AddRateLimited(key)
		return
	}

	klog.Warningf("Retry budget exceeded, dropping %q resource out of the queue: %v", key, err)
	c.queue.Forget(key)
	utilruntime.HandleError(err)
}
