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

package aws

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"

	"antrea.io/antrea/pkg/cloudprovider"
)

// Client represents an AWS API client
type Client struct {
	ec2Client EC2Interface

	nodeToInterfaceID map[string]string
	nodeMutex         sync.RWMutex

	instanceTypeToMaxIPs map[string]int
	instanceTypeMutex    sync.RWMutex
}

// NewClient returns a cloud provider interface backed by AWS.
func NewClient() (cloudprovider.Interface, error) {
	// config.WithEC2IMDSRegion() will set the region from the EC2 IMDS metadata.
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithEC2IMDSRegion())
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS configuration: %w", err)
	}
	client := ec2.NewFromConfig(cfg)
	return &Client{
		ec2Client:            client,
		nodeToInterfaceID:    map[string]string{},
		instanceTypeToMaxIPs: map[string]int{},
	}, nil
}
