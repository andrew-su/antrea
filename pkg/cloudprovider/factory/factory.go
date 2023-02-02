// Copyright 2023 Antrea Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package factory

import (
	"fmt"

	"antrea.io/antrea/pkg/cloudprovider"
	"antrea.io/antrea/pkg/cloudprovider/aws"
)

const (
	ProviderAWS = "aws"
)

type Factory func() (cloudprovider.Interface, error)

var cloudProviders = make(map[string]Factory)

func RegisterCloudProvider(cloudProviderName string, factory Factory) {
	cloudProviders[cloudProviderName] = factory
}

func InitCloudProvider(cloudProviderName string) (cloudprovider.Interface, error) {
	factory, exists := cloudProviders[cloudProviderName]
	if !exists {
		return nil, fmt.Errorf("cloud provider %s is invalid", cloudProviderName)
	}
	provider, err := factory()
	if err != nil {
		return nil, fmt.Errorf("error when initializing cloud provider %s: %w", cloudProviderName, err)
	}
	return provider, nil
}

func GetSupportedCloudProviders() []string {
	providers := make([]string, 0, len(cloudProviders))
	for provider := range cloudProviders {
		providers = append(providers, provider)
	}
	return providers
}

func init() {
	RegisterCloudProvider(ProviderAWS, aws.NewClient)
}
