// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package signature

import (
	"k8s.io/klog/v2"

	crdclientset "antrea.io/antrea/pkg/client/clientset/versioned"
	tanzucrdinformers "antrea.io/antrea/pkg/client/informers/externalversions/tanzucrd/v1alpha1"
	controllerconfig "antrea.io/antrea/pkg/idps/config/controller"
	"antrea.io/antrea/pkg/idps/controller/registration"
	"antrea.io/antrea/pkg/idps/controller/signature/provider"
	"antrea.io/antrea/pkg/idps/controller/signature/provider/ntics"
)

type Manager struct {
	signatureProviders map[string]provider.SignatureProvider
}

func NewSignatureManager(signatureProviderInfoInformer tanzucrdinformers.IDPSSignatureProviderInfoInformer,
	crdClient crdclientset.Interface,
	registrationController *registration.NSXRegistrationStateController,
	signatureNTICSConfig *controllerconfig.SignatureProviderConfig) (*Manager, error) {
	signatureProviders := make(map[string]provider.SignatureProvider)

	// Currently, we have only signature NTICS. In the future, there might be more signature providers.
	signatureProviders[ntics.SignatureProviderName] = ntics.NewSignatureProvider(signatureProviderInfoInformer,
		crdClient,
		registrationController,
		signatureNTICSConfig)

	return &Manager{signatureProviders: signatureProviders}, nil
}

func (m *Manager) Run(stopCh <-chan struct{}) {
	klog.InfoS("Starting Signature Manager")
	defer klog.InfoS("Shutting down Signature Manager")

	for _, signatureProvider := range m.signatureProviders {
		go signatureProvider.Run(stopCh)
	}

	<-stopCh
}

func (m *Manager) GetProviders() map[string]provider.SignatureProvider {
	return m.signatureProviders
}
