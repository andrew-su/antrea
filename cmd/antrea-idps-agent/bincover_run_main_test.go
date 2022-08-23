// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

//go:build testbincover
// +build testbincover

package main

import (
	"testing"

	"github.com/confluentinc/bincover"
)

func TestBincoverRunMain(t *testing.T) {
	bincover.RunTest(main)
}
