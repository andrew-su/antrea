// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package main

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/klog/v2"

	crdinformers "antrea.io/antrea/pkg/client/informers/externalversions"
	"antrea.io/antrea/pkg/idps/agent"
	"antrea.io/antrea/pkg/idps/agent/signature"
	"antrea.io/antrea/pkg/idps/agent/suricata"
	"antrea.io/antrea/pkg/log"
	"antrea.io/antrea/pkg/signals"
	"antrea.io/antrea/pkg/util/k8s"
	"antrea.io/antrea/pkg/version"
)

const (
	// informerDefaultResync is the default resync period if a handler doesn't specify one.
	// Use the same default value as kube-controller-manager:
	// https://github.com/kubernetes/kubernetes/blob/release-1.17/pkg/controller/apis/config/v1alpha1/defaults.go#L120
	informerDefaultResync = 12 * time.Hour
)

// run starts IDPS Agent with the given options and waits for termination signal.
func run(o *Options) error {
	klog.Infof("Starting Antrea IDPS Agent (version %s)", version.GetFullVersion())
	// Create K8s Clientset, CRD Clientset and SharedInformerFactory for the given config.
	k8sClient, _, crdClient, _, _, _, err := k8s.CreateClients(o.config.ClientConnection, "")
	if err != nil {
		return fmt.Errorf("error creating K8s clients: %v", err)
	}
	informerFactory := informers.NewSharedInformerFactory(k8sClient, informerDefaultResync)
	crdInformerFactory := crdinformers.NewSharedInformerFactory(crdClient, informerDefaultResync)
	idpsSignatureProviderInfoInformer := crdInformerFactory.TanzuCrd().V1alpha1().IDPSSignatureProviderInfos()

	// Create IDPS Clientset for the given config.
	idpsClientProvider := agent.NewIDPSClientProvider(o.config.IDPSClientConnection, k8sClient)
	if err = idpsClientProvider.RunOnce(); err != nil {
		return err
	}

	signatureController := signature.NewController(idpsSignatureProviderInfoInformer, idpsClientProvider, suricata.NewSuricata())

	// Set up signal capture: the first SIGTERM / SIGINT signal is handled gracefully and will
	// cause the stopCh channel to be closed; if another signal is received before the program
	// exits, we will force exit.
	stopCh := signals.RegisterSignalHandlers()
	// Generate a context for functions which require one (instead of stopCh).
	// We cancel the context when the function returns, which in the normal case will be when
	// stopCh is closed.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	informerFactory.Start(stopCh)
	crdInformerFactory.Start(stopCh)

	go idpsClientProvider.Run(ctx)

	log.StartLogFileNumberMonitor(stopCh)

	go signatureController.Run(stopCh)

	<-stopCh
	klog.Info("Stopping Antrea IDPS Agent")
	return nil
}
