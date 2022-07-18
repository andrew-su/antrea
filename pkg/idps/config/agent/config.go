// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package config

import componentbaseconfig "k8s.io/component-base/config"

type AgentConfig struct {
	// clientConnection specifies the kubeconfig file and client connection settings for the IDPS agent to communicate
	// with the Kubernetes apiserver.
	ClientConnection componentbaseconfig.ClientConnectionConfiguration `yaml:"clientConnection"`
	// IDPSClientConnection specifies the kubeconfig file and client connection settings for the IDPS agent to communicate
	// with the IDPS controller apiserver.
	IDPSClientConnection componentbaseconfig.ClientConnectionConfiguration `yaml:"idpsClientConnection"`
}
