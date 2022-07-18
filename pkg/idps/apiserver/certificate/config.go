// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package certificate

import (
	"time"

	"antrea.io/antrea/pkg/apiserver/certificate"
	"antrea.io/antrea/pkg/idps/util/env"
)

const (
	IDPSCAConfigMapName = "antrea-idps-ca"
	IDPSServiceName     = "antrea-idps"
	CAConfigMapKey      = "ca.crt"
)

func GetCAConfigMapNamespace() string {
	return env.GetIDPSNamespace()
}

// GetIDPSServerName returns the DNS names that the TLS certificate will be signed with.
func GetIDPSServerName() string {
	namespace := env.GetIDPSNamespace()
	idpsServerName := IDPSServiceName + "." + namespace + ".svc"
	return idpsServerName
}

func DefaultCAConfig() *certificate.CAConfig {
	return &certificate.CAConfig{
		CAConfigMapName:   IDPSCAConfigMapName,
		SelfSignedCertDir: "/var/run/antrea/idps/idps-controller-self-signed",
		CertReadyTimeout:  2 * time.Minute,
		MaxRotateDuration: time.Hour * (24 * 365),
		ServiceName:       IDPSServiceName,
		PairName:          "antrea-idps-controller",
	}
}
