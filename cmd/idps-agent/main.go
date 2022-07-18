// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package main

import (
	"os"

	"github.com/spf13/cobra"
	"k8s.io/klog/v2"

	"antrea.io/antrea/pkg/log"
	"antrea.io/antrea/pkg/version"
)

func main() {
	command := newAgentCommand()
	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}

func newAgentCommand() *cobra.Command {
	opts := newOptions()

	cmd := &cobra.Command{
		Use:  "antrea-idps-agent",
		Long: "The Antrea IDPS agent.",
		Run: func(cmd *cobra.Command, args []string) {
			log.InitLogs(cmd.Flags())
			defer log.FlushLogs()
			if err := opts.complete(args); err != nil {
				klog.Fatalf("Failed to complete: %v", err)
			}
			if err := run(opts); err != nil {
				klog.Fatalf("Error running controller: %v", err)
			}
		},
		Version: version.GetFullVersionWithRuntimeInfo(),
	}

	flags := cmd.Flags()
	opts.addFlags(flags)
	log.AddFlags(flags)
	return cmd
}
