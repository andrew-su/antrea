// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package main

import (
	"io/ioutil"

	"github.com/spf13/pflag"
	"gopkg.in/yaml.v2"

	"antrea.io/antrea/pkg/apis"
	controllerconfig "antrea.io/antrea/pkg/idps/config/controller"
)

type Options struct {
	// The path of configuration file.
	configFile string
	// The configuration object
	config *controllerconfig.ControllerConfig
}

func newOptions() *Options {
	return &Options{
		config: &controllerconfig.ControllerConfig{},
	}
}

// addFlags adds flags to fs and binds them to options.
func (o *Options) addFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.configFile, "config", o.configFile, "The path to the configuration file")
}

func (o *Options) complete(_ []string) error {
	if len(o.configFile) > 0 {
		if err := o.loadConfigFromFile(); err != nil {
			return err
		}
	}
	o.setDefaults()
	return nil
}

func (o *Options) loadConfigFromFile() error {
	data, err := ioutil.ReadFile(o.configFile)
	if err != nil {
		return err
	}

	return yaml.UnmarshalStrict(data, &o.config)
}

func (o *Options) setDefaults() {
	if o.config.APIPort == 0 {
		o.config.APIPort = apis.IDPSControllerAPIPort
	}
	if o.config.SignatureProviderNTICS.APIBaseURL == "" {
		o.config.SignatureProviderNTICS.APIBaseURL = "https://api.prod.nsxti.vmware.com"
	}
	if o.config.SignatureProviderNTICS.SyncInterval == 0 {
		o.config.SignatureProviderNTICS.SyncInterval = 600
	}
}
