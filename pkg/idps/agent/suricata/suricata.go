// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package suricata

import (
	"fmt"
	"os"
	"os/exec"
)

const (
	suricataConfigDir = "/etc/suricata"
)

type Interface interface {
	LoadSignature(signature string, data []byte) error
}

type Suricata struct{}

func NewSuricata() Interface {
	return &Suricata{}
}

func (p *Suricata) LoadSignature(name string, data []byte) error {
	// Save the signature data to a file.
	tempTarGzFile := fmt.Sprintf("/tmp/%s.tar.gz", name)
	file, err := os.OpenFile(tempTarGzFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open or create temp file %s to save data of signature %s: %w", tempTarGzFile, name, err)
	}
	defer file.Close()
	if _, err = file.Write(data); err != nil {
		return err
	}
	defer os.Remove(tempTarGzFile)

	// Extract compressed signature data.
	cmd := fmt.Sprintf("tar xvzf %s -C %s", tempTarGzFile, suricataConfigDir)
	command := exec.Command("bash", "-c", cmd)
	if err = command.Run(); err != nil {
		return fmt.Errorf("failed to extract compressed signature data: %w", err)
	}

	// Reload Suricata process to load signature data.
	command = exec.Command("bash", "-c", "kill -USR2 $(pidof suricata)")
	if err = command.Run(); err != nil {
		return fmt.Errorf("failed to reload Suricata process: %w", err)
	}

	return nil
}
