// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package agent

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"sync"

	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/component-base/config"
	"k8s.io/klog/v2"

	"antrea.io/antrea/pkg/client/clientset/versioned/scheme"
	idpscertificate "antrea.io/antrea/pkg/idps/apiserver/certificate"
)

// IDPSClientProvider provides a method to get IDPS client.
type IDPSClientProvider interface {
	GetIDPSClient() (rest.Interface, error)
}

// idpsClientProvider provides an IDPSClientProvider that can dynamically react to ConfigMap changes.
type idpsClientProvider struct {
	config config.ClientConnectionConfiguration
	// mutex protects client.
	mutex sync.RWMutex
	// client is the IDPS client that will be returned. It will be updated when caBundle is updated.
	client rest.Interface
	// caContentProvider provides the very latest content of the ca bundle.
	caContentProvider *dynamiccertificates.ConfigMapCAController
}

var _ dynamiccertificates.Listener = &idpsClientProvider{}

func NewIDPSClientProvider(config config.ClientConnectionConfiguration, kubeClient kubernetes.Interface) *idpsClientProvider {
	// The key "ca.crt" may not exist at the beginning, no need to fail as the CA provider will watch the ConfigMap
	// and notify idpsClientProvider of any update. The consumers of idpsClientProvider are supposed to always
	// call GetIDPSClient() to get a client and not cache it.
	idpsCAProvider, _ := dynamiccertificates.NewDynamicCAFromConfigMapController(
		"antrea-idps-ca",
		idpscertificate.GetCAConfigMapNamespace(),
		idpscertificate.IDPSCAConfigMapName,
		idpscertificate.CAConfigMapKey,
		kubeClient)
	idpsClientProvider := &idpsClientProvider{
		config:            config,
		caContentProvider: idpsCAProvider,
	}

	idpsCAProvider.AddListener(idpsClientProvider)
	return idpsClientProvider
}

// RunOnce runs the task a single time synchronously, ensuring client is initialized if kubeconfig is specified.
func (p *idpsClientProvider) RunOnce() error {
	return p.updateIDPSClient()
}

// Run starts the caContentProvider, which watches the ConfigMap and notifies changes
// by calling Enqueue.
func (p *idpsClientProvider) Run(ctx context.Context) {
	p.caContentProvider.Run(ctx, 1)
}

// Enqueue implements dynamiccertificates.Listener. It will be called by caContentProvider
// when caBundle is updated.
func (p *idpsClientProvider) Enqueue() {
	if err := p.updateIDPSClient(); err != nil {
		klog.Errorf("Failed to update IDPS client: %v", err)
	}
}

// GetIDPSClient implements GetIDPSClient.
func (p *idpsClientProvider) GetIDPSClient() (rest.Interface, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	if p.client == nil {
		return nil, fmt.Errorf("IDPS client is not ready")
	}
	return p.client, nil
}

func (p *idpsClientProvider) updateIDPSClient() error {
	var kubeConfig *rest.Config
	var err error
	if len(p.config.Kubeconfig) == 0 {
		klog.Info("No IDPS kubeconfig file was specified. Falling back to in-cluster config")
		caBundle := p.caContentProvider.CurrentCABundleContent()
		if caBundle == nil {
			klog.Info("Didn't get CA certificate, skip updating IDPS Client")
			return nil
		}
		kubeConfig, err = inClusterConfig(caBundle)
	} else {
		kubeConfig, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: p.config.Kubeconfig},
			&clientcmd.ConfigOverrides{}).ClientConfig()
	}
	if err != nil {
		return err
	}

	// ContentType will be used to define the Accept header if AcceptContentTypes is not set.
	kubeConfig.ContentType = "application/vnd.kubernetes.protobuf"
	kubeConfig.QPS = p.config.QPS
	kubeConfig.Burst = int(p.config.Burst)
	kubeConfig.NegotiatedSerializer = scheme.Codecs

	client, err := rest.UnversionedRESTClientFor(kubeConfig)
	if err != nil {
		return fmt.Errorf("failed to create rest client: %w", err)
	}

	klog.Info("Updating IDPS client with the new CA bundle")
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.client = client

	return nil
}

// inClusterConfig returns a config object which uses the service account kubernetes gives to Pods. It's intended for
// clients that expect to be running inside a pod running on kubernetes. It will return error if called from a process
// not running in a kubernetes environment.
func inClusterConfig(caBundle []byte) (*rest.Config, error) {
	// #nosec G101: false positive triggered by variable name which includes "token"
	const tokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	host, port := os.Getenv("ANTREA_IDPS_SERVICE_HOST"), os.Getenv("ANTREA_IDPS_SERVICE_PORT")
	if len(host) == 0 || len(port) == 0 {
		return nil, fmt.Errorf("unable to load in-cluster configuration, ANTREA_IDPS_SERVICE_HOST and ANTREA_IDPS_SERVICE_PORT must be defined")
	}

	token, err := ioutil.ReadFile(tokenFile)
	if err != nil {
		return nil, err
	}

	tlsClientConfig := rest.TLSClientConfig{
		CAData:     caBundle,
		ServerName: idpscertificate.GetIDPSServerName(),
	}
	return &rest.Config{
		Host:            "https://" + net.JoinHostPort(host, port),
		TLSClientConfig: tlsClientConfig,
		BearerToken:     string(token),
		BearerTokenFile: tokenFile,
	}, nil
}
