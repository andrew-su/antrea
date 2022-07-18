// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package env

import (
	"os"

	"k8s.io/klog/v2"
)

const (
	podNamespaceEnvKey = "POD_NAMESPACE"

	defaultIDPSNamespace = "kube-system"
)

// GetPodNamespace returns Namespace of the Pod where the code executes.
func GetPodNamespace() string {
	podNamespace := os.Getenv(podNamespaceEnvKey)
	if podNamespace == "" {
		klog.InfoS("Environment variable not found", "EnvironmentVariable", podNamespaceEnvKey)
	}
	return podNamespace
}

func GetIDPSNamespace() string {
	namespace := GetPodNamespace()
	if namespace == "" {
		klog.InfoS("Failed to get Pod Namespace from environment. Using default namespace as the IDPS Service Namespace", "DefaultIDPSNamespace", defaultIDPSNamespace)
		namespace = defaultIDPSNamespace
	}
	return namespace
}
