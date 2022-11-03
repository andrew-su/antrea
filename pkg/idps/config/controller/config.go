// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package config

import (
	componentbaseconfig "k8s.io/component-base/config"
)

type ControllerConfig struct {
	// clientConnection specifies the kubeconfig file and client connection settings for the IDPS controller to communicate
	// with the Kubernetes apiserver.
	ClientConnection componentbaseconfig.ClientConnectionConfiguration `yaml:"clientConnection"`
	// APIPort is the port for the IDPS controller apiserver to serve on. Defaults to 20349.
	APIPort int `yaml:"apiPort,omitempty"`
	// SignatureProviderNTICS is the configuration for signature provider NTICS.
	SignatureProviderNTICS SignatureProviderConfig `yaml:"signatureProviderNTICS"`
}

type SignatureProviderConfig struct {
	// APIBaseURL is the base URL for the signature provider APIs.
	APIBaseURL string `yaml:"apiBaseURL,omitempty"`
	// SyncInterval is the period (seconds) to sync the signature data. Defaults to 600.
	SyncInterval int `yaml:"syncInterval,omitempty"`
	// DeviceType is used to identify current device when registering to signature provider NTICS.
	DeviceType string `yaml:"deviceType,omitempty"`
}
