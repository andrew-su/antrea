// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package provider

type SignatureProvider interface {
	Run(stopCh <-chan struct{})

	GetSignatureData() ([]byte, error)
}
