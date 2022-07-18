// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package apiserver

import (
	"context"
	"fmt"
	"net/http"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/informers"
	"k8s.io/klog/v2"

	"antrea.io/antrea/pkg/apiserver/certificate"
	"antrea.io/antrea/pkg/idps/controller/signature"
	"antrea.io/antrea/pkg/idps/controller/signature/provider"
)

var (
	// Scheme defines methods for serializing and deserializing API objects.
	Scheme = runtime.NewScheme()
	// Codecs provides methods for retrieving codecs and serializers for specific
	// versions and content types.
	Codecs = serializer.NewCodecFactory(Scheme)
	// #nosec G101: false positive triggered by variable name which includes "token"
	TokenPath = "/var/run/antrea/apiserver/loopback-client-token"
)

func init() {
	// We need to add the options to empty v1, see sample-apiserver/pkg/apiserver/apiserver.go.
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})
}

// ExtraConfig holds custom apiserver config.
type ExtraConfig struct {
	signatureManager *signature.Manager
	caCertController *certificate.CACertController
}

// Config defines the config for Antrea IDPS apiserver.
type Config struct {
	genericConfig *genericapiserver.Config
	extraConfig   ExtraConfig
}

// APIServer contains state for a Kubernetes cluster apiserver.
type APIServer struct {
	GenericAPIServer *genericapiserver.GenericAPIServer
	caCertController *certificate.CACertController
}

func (s *APIServer) Run(ctx context.Context) error {
	// Make sure CACertController runs once to publish the CA cert before starting APIServer.
	if err := s.caCertController.RunOnce(ctx); err != nil {
		klog.Warningf("caCertController RunOnce failed: %v", err)
	}
	go s.caCertController.Run(ctx, 1)

	return s.GenericAPIServer.PrepareRun().Run(ctx.Done())
}

type completedConfig struct {
	genericConfig genericapiserver.CompletedConfig
	extraConfig   *ExtraConfig
}

func NewConfig(
	genericConfig *genericapiserver.Config,
	caCertController *certificate.CACertController,
	signatureManager *signature.Manager) *Config {
	return &Config{
		genericConfig: genericConfig,
		extraConfig: ExtraConfig{
			caCertController: caCertController,
			signatureManager: signatureManager,
		},
	}
}

func (c *Config) Complete(informers informers.SharedInformerFactory) completedConfig {
	return completedConfig{c.genericConfig.Complete(informers), &c.extraConfig}
}

func (c completedConfig) New() (*APIServer, error) {
	genericServer, err := c.genericConfig.New("idps-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, err
	}

	s := &APIServer{
		GenericAPIServer: genericServer,
		caCertController: c.extraConfig.caCertController,
	}
	installHandlers(c.extraConfig, s.GenericAPIServer)

	return s, nil
}

func installHandlers(c *ExtraConfig, s *genericapiserver.GenericAPIServer) {
	for name, sigProvider := range c.signatureManager.GetProviders() {
		uri := fmt.Sprintf("/signatures/%s", name)
		s.Handler.NonGoRestfulMux.HandleFunc(uri, signaturesHandleFunc(sigProvider))
	}
}

func signaturesHandleFunc(sigProvider provider.SignatureProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		signatureData, err := sigProvider.GetSignatureData()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err = w.Write(signatureData); err != nil {
			http.Error(w, "failed to write signature data", http.StatusInternalServerError)
		}
	}
}
