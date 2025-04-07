// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package e2e

import (
	"fmt"
	"testing"
)

func skipIfNotAntreaIDPSTest(tb testing.TB) {
	if !testOptions.enableAntreaIDPS {
		tb.Skipf("Skipping Antrea IDPS test: %s", tb.Name())
	}
}

func setupTestForAntreaIDPS(tb testing.TB) (*TestData, error) {
	testData, err := setupTest(tb)
	if err != nil {
		return testData, err
	}
	if err = testData.deployAntreaIDPS(); err != nil {
		return nil, err
	}
	return testData, nil
}

func teardownAntreaIDPS(t *testing.T, data *TestData) {
	rc, _, _, err := data.provider.RunCommandOnNode(controlPlaneNodeName(), fmt.Sprintf("kubectl delete -f %s", antreaIDPSYML))
	if err != nil || rc != 0 {
		t.Logf("error when deleting the Antrea IDPS")
	}
}
