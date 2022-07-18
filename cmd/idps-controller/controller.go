// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"time"

	apiextensionclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	genericopenapi "k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	aggregatorclientset "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"

	"antrea.io/antrea/pkg/apiserver/certificate"
	"antrea.io/antrea/pkg/apiserver/openapi"
	crdinformers "antrea.io/antrea/pkg/client/informers/externalversions"
	"antrea.io/antrea/pkg/idps/apiserver"
	idpscertificate "antrea.io/antrea/pkg/idps/apiserver/certificate"
	"antrea.io/antrea/pkg/idps/controller/idpspolicy"
	"antrea.io/antrea/pkg/idps/controller/registration"
	"antrea.io/antrea/pkg/idps/controller/signature"
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

	// serverMinWatchTimeout determines the timeout allocated to watch from Antrea IDPS
	// clients. Each watch will be allocated a random timeout between this value and twice this
	// value, to help randomly distribute reconnections over time.
	// This parameter corresponds to the MinRequestTimeout server config parameter in
	// https://godoc.org/k8s.io/apiserver/pkg/server#Config.
	serverMinWatchTimeout = 2 * time.Hour
)

var allowedPaths = []string{
	"/healthz",
	"/livez",
	"/readyz",
}

// run starts IDPS Controller with the given options and waits for termination signal.
func run(o *Options) error {
	klog.Infof("Starting Antrea IDPS Controller (version %s)", version.GetFullVersion())
	// Create K8s Clientset, Aggregator Clientset, CRD Clientset and SharedInformerFactory for the given config.
	// Aggregator Clientset is used to update the CABundle of the APIServices backed by antrea-controller so that
	// the aggregator can verify its serving certificate.
	k8sClient, aggregatorClient, crdClient, apiExtensionClient, _, _, err := k8s.CreateClients(o.config.ClientConnection, "")
	if err != nil {
		return fmt.Errorf("error creating K8s clients: %v", err)
	}
	informerFactory := informers.NewSharedInformerFactory(k8sClient, informerDefaultResync)
	crdInformerFactory := crdinformers.NewSharedInformerFactory(crdClient, informerDefaultResync)
	trafficControlInformer := crdInformerFactory.Crd().V1alpha2().TrafficControls()
	idpsPolicyInformer := crdInformerFactory.TanzuCrd().V1alpha1().IDPSPolicies()
	idpsSignatureProviderInfoInformer := crdInformerFactory.TanzuCrd().V1alpha1().IDPSSignatureProviderInfos()
	registrationInformer := crdInformerFactory.TanzuCrd().V1alpha1().NSXRegistrations()

	registrationController := registration.NewRegistrationController(registrationInformer)

	idpsPolicyController := idpspolicy.NewIDPSPolicyController(trafficControlInformer, idpsPolicyInformer, crdClient)

	signatureManager, err := signature.NewSignatureManager(idpsSignatureProviderInfoInformer,
		crdClient,
		registrationController,
		&o.config.SignatureProviderNTICS)
	if err != nil {
		return err
	}

	apiServerConfig, err := createAPIServerConfig(o.config.ClientConnection.Kubeconfig,
		k8sClient,
		aggregatorClient,
		apiExtensionClient,
		signatureManager,
		o.config.APIPort)
	if err != nil {
		return fmt.Errorf("error creating API server config: %v", err)
	}
	apiServer, err := apiServerConfig.Complete(informerFactory).New()
	if err != nil {
		return fmt.Errorf("error creating API server: %v", err)
	}

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

	log.StartLogFileNumberMonitor(stopCh)

	go apiServer.Run(ctx)

	go signatureManager.Run(stopCh)

	go idpsPolicyController.Run(stopCh)

	<-stopCh
	klog.Info("Stopping Antrea IDPS Controller")
	return nil
}

func createAPIServerConfig(kubeconfig string,
	client clientset.Interface,
	aggregatorClient aggregatorclientset.Interface,
	apiExtensionClient apiextensionclientset.Interface,
	signatureManager *signature.Manager,
	bindPort int) (*apiserver.Config, error) {
	secureServing := genericoptions.NewSecureServingOptions().WithLoopback()
	authentication := genericoptions.NewDelegatingAuthenticationOptions()
	authorization := genericoptions.NewDelegatingAuthorizationOptions().WithAlwaysAllowPaths(allowedPaths...)

	caCertController, err := certificate.ApplyServerCert(true,
		client,
		aggregatorClient,
		apiExtensionClient,
		secureServing,
		idpscertificate.DefaultCAConfig())
	if err != nil {
		return nil, fmt.Errorf("error applying server cert: %v", err)
	}

	secureServing.BindPort = bindPort
	secureServing.BindAddress = net.IPv4zero
	if len(kubeconfig) > 0 {
		authentication.RemoteKubeConfigFile = kubeconfig
		authorization.RemoteKubeConfigFile = kubeconfig
	}

	serverConfig := genericapiserver.NewConfig(apiserver.Codecs)
	if err = secureServing.ApplyTo(&serverConfig.SecureServing, &serverConfig.LoopbackClientConfig); err != nil {
		return nil, err
	}
	if err = authentication.ApplyTo(&serverConfig.Authentication, serverConfig.SecureServing, nil); err != nil {
		return nil, err
	}
	if err = authorization.ApplyTo(&serverConfig.Authorization); err != nil {
		return nil, err
	}
	if err = os.MkdirAll(path.Dir(apiserver.TokenPath), os.ModeDir); err != nil {
		return nil, fmt.Errorf("error when creating dirs of token file: %v", err)
	}
	if err = ioutil.WriteFile(apiserver.TokenPath, []byte(serverConfig.LoopbackClientConfig.BearerToken), 0600); err != nil {
		return nil, fmt.Errorf("error when writing loopback access token to file: %v", err)
	}
	serverConfig.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(
		openapi.GetOpenAPIDefinitions,
		genericopenapi.NewDefinitionNamer(apiserver.Scheme))
	serverConfig.OpenAPIConfig.Info.Title = "AntreaIDPSController"
	serverConfig.MinRequestTimeout = int(serverMinWatchTimeout.Seconds())

	return apiserver.NewConfig(
		serverConfig,
		caCertController,
		signatureManager), nil
}
