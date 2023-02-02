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
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"
)

func newFakeClient(ctrl *gomock.Controller) (*Client, *MockEC2Interface) {
	ec2Client := NewMockEC2Interface(ctrl)
	client := &Client{
		ec2Client:            ec2Client,
		nodeToInterfaceID:    map[string]string{},
		instanceTypeToMaxIPs: map[string]int{},
	}
	return client, ec2Client
}

func int32ToInt32Ptr(i int32) *int32 {
	return &i
}

func TestGetMaxIPsByInstanceType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client, ec2Client := newFakeClient(ctrl)

	ec2Client.EXPECT().DescribeInstanceTypes(context.TODO(), &ec2.DescribeInstanceTypesInput{
		InstanceTypes: []types.InstanceType{"t3a.large"},
	}).Return(&ec2.DescribeInstanceTypesOutput{InstanceTypes: []types.InstanceTypeInfo{
		{
			NetworkInfo: &types.NetworkInfo{
				Ipv4AddressesPerInterface: int32ToInt32Ptr(6),
			},
		},
	}}, nil)
	maxIPs, found, err := client.GetMaxIPsByInstanceType("t3a.large")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, 5, maxIPs)

	// Query the same instance type the second time, EC2 interface is not supposed to be called again.
	client.GetMaxIPsByInstanceType("t3a.large")

	ec2Client.EXPECT().DescribeInstanceTypes(context.TODO(), &ec2.DescribeInstanceTypesInput{
		InstanceTypes: []types.InstanceType{"m5.large"},
	}).Return(nil, fmt.Errorf("server error"))
	_, found, err = client.GetMaxIPsByInstanceType("m5.large")
	assert.ErrorContains(t, err, "server error")
	assert.False(t, found)

}

func TestGetIPsByNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client, ec2Client := newFakeClient(ctrl)

	ec2Client.EXPECT().DescribeInstances(context.TODO(), &ec2.DescribeInstancesInput{
		Filters: []types.Filter{
			{
				Name: aws.String("private-dns-name"),
				Values: []string{
					"ip-192-168-1-1.us-west-2.compute.internal",
				},
			},
		},
	}).Return(&ec2.DescribeInstancesOutput{Reservations: []types.Reservation{
		{
			Instances: []types.Instance{
				{
					PrivateIpAddress: aws.String("192.168.1.1"),
					NetworkInterfaces: []types.InstanceNetworkInterface{
						{
							NetworkInterfaceId: aws.String("id-abcdef"),
							PrivateIpAddress:   aws.String("192.168.1.1"),
							PrivateIpAddresses: []types.InstancePrivateIpAddress{
								{PrivateIpAddress: aws.String("192.168.1.1")},
								{PrivateIpAddress: aws.String("192.168.1.2")},
								{PrivateIpAddress: aws.String("192.168.1.3")},
							},
						},
					},
				},
			},
		},
	}}, nil)
	expectedIPs := []string{"192.168.1.2", "192.168.1.3"}
	gotIPs, err := client.GetIPsByNode("ip-192-168-1-1.us-west-2.compute.internal")
	assert.NoError(t, err)
	assert.Equal(t, expectedIPs, gotIPs)

	ec2Client.EXPECT().DescribeInstances(context.TODO(), &ec2.DescribeInstancesInput{
		Filters: []types.Filter{
			{
				Name: aws.String("private-dns-name"),
				Values: []string{
					"ip-192-168-64-1.us-west-2.compute.internal",
				},
			},
		},
	}).Return(nil, fmt.Errorf("server error"))
	gotIPs, err = client.GetIPsByNode("ip-192-168-64-1.us-west-2.compute.internal")
	assert.ErrorContains(t, err, "server error")
	assert.Equal(t, []string(nil), gotIPs)
}

func TestAssignIPToNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client, ec2Client := newFakeClient(ctrl)

	ec2Client.EXPECT().DescribeInstances(context.TODO(), &ec2.DescribeInstancesInput{
		Filters: []types.Filter{
			{
				Name: aws.String("private-dns-name"),
				Values: []string{
					"ip-192-168-128-1.us-west-2.compute.internal",
				},
			},
		},
	}).Return(&ec2.DescribeInstancesOutput{Reservations: []types.Reservation{
		{
			Instances: []types.Instance{
				{
					PrivateIpAddress: aws.String("192.168.128.1"),
					NetworkInterfaces: []types.InstanceNetworkInterface{
						{
							NetworkInterfaceId: aws.String("id-abcdef"),
							PrivateIpAddress:   aws.String("192.168.128.1"),
							PrivateIpAddresses: []types.InstancePrivateIpAddress{
								{PrivateIpAddress: aws.String("192.168.128.1")},
							},
						},
					},
				},
			},
		},
	}}, nil)

	ec2Client.EXPECT().AssignPrivateIpAddresses(context.TODO(), &ec2.AssignPrivateIpAddressesInput{
		AllowReassignment:  aws.Bool(true),
		NetworkInterfaceId: aws.String("id-abcdef"),
		PrivateIpAddresses: []string{"192.168.128.2"},
	}).Return(nil, nil)
	err := client.AssignIPToNode("192.168.128.2", "ip-192-168-128-1.us-west-2.compute.internal")
	assert.NoError(t, err)

	ec2Client.EXPECT().AssignPrivateIpAddresses(context.TODO(), &ec2.AssignPrivateIpAddressesInput{
		AllowReassignment:  aws.Bool(true),
		NetworkInterfaceId: aws.String("id-abcdef"),
		PrivateIpAddresses: []string{"192.168.192.1"},
	}).Return(nil, fmt.Errorf("failed to assign IP"))
	err = client.AssignIPToNode("192.168.192.1", "ip-192-168-128-1.us-west-2.compute.internal")
	assert.ErrorContains(t, err, "failed to assign IP")
}

func TestUnassignIPToNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client, ec2Client := newFakeClient(ctrl)

	ec2Client.EXPECT().DescribeInstances(context.TODO(), &ec2.DescribeInstancesInput{
		Filters: []types.Filter{
			{
				Name: aws.String("private-dns-name"),
				Values: []string{
					"ip-192-168-1-1.us-west-2.compute.internal",
				},
			},
		},
	}).Return(&ec2.DescribeInstancesOutput{Reservations: []types.Reservation{
		{
			Instances: []types.Instance{
				{
					PrivateIpAddress: aws.String("192.168.1.1"),
					NetworkInterfaces: []types.InstanceNetworkInterface{
						{
							NetworkInterfaceId: aws.String("id-abcdef"),
							PrivateIpAddress:   aws.String("192.168.1.1"),
							PrivateIpAddresses: []types.InstancePrivateIpAddress{
								{PrivateIpAddress: aws.String("192.168.1.1")},
								{PrivateIpAddress: aws.String("192.168.1.2")},
								{PrivateIpAddress: aws.String("192.168.1.3")},
								{PrivateIpAddress: aws.String("192.168.1.4")},
							},
						},
					},
				},
			},
		},
	}}, nil)

	ec2Client.EXPECT().UnassignPrivateIpAddresses(context.TODO(), &ec2.UnassignPrivateIpAddressesInput{
		NetworkInterfaceId: aws.String("id-abcdef"),
		PrivateIpAddresses: []string{"192.168.1.4"},
	}).Return(nil, nil)
	err := client.UnassignIPToNode("192.168.1.4", "ip-192-168-1-1.us-west-2.compute.internal")
	assert.NoError(t, err)

	// Unassign the same ip address the second time, EC2 interface will return an error.
	ec2Client.EXPECT().UnassignPrivateIpAddresses(context.TODO(), &ec2.UnassignPrivateIpAddressesInput{
		NetworkInterfaceId: aws.String("id-abcdef"),
		PrivateIpAddresses: []string{"192.168.1.4"},
	}).Return(nil, fmt.Errorf("IP has been unassigned"))
	err = client.UnassignIPToNode("192.168.1.4", "ip-192-168-1-1.us-west-2.compute.internal")
	assert.ErrorContains(t, err, "IP has been unassigned")
}
