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
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func fakeNode(name, providerID string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: corev1.NodeSpec{
			ProviderID: providerID,
		},
	}
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
	node := fakeNode("ip-192-168-1-1.us-west-2.compute.internal", "aws:///us-west-2a/i-1234567890abcdef0")
	client, ec2Client := newFakeClient(ctrl)
	ec2Client.EXPECT().DescribeInstances(context.TODO(), &ec2.DescribeInstancesInput{
		InstanceIds: []string{"i-1234567890abcdef0"},
	}).Return(&ec2.DescribeInstancesOutput{Reservations: []types.Reservation{
		{
			Instances: []types.Instance{
				{
					InstanceId:       aws.String("i-1234567890abcdef0"),
					PrivateIpAddress: aws.String("192.168.1.1"),
					NetworkInterfaces: []types.InstanceNetworkInterface{
						{
							NetworkInterfaceId: aws.String("eni-1234567890abcdef0"),
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
	gotIPs, err := client.GetIPsByNode(node)
	assert.NoError(t, err)
	assert.Equal(t, expectedIPs, gotIPs)

	ec2Client.EXPECT().DescribeInstances(context.TODO(), &ec2.DescribeInstancesInput{
		InstanceIds: []string{"i-1234567890abcdef0"},
	}).Return(nil, fmt.Errorf("server error"))
	gotIPs, err = client.GetIPsByNode(node)
	assert.ErrorContains(t, err, "server error")
	assert.Equal(t, []string(nil), gotIPs)
}

func TestAssignIPToNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client, ec2Client := newFakeClient(ctrl)
	node := fakeNode("ip-192-168-128-1.us-west-2.compute.internal", "aws:///us-west-2a/i-1234567890abcdef0")
	ec2Client.EXPECT().DescribeInstances(context.TODO(), &ec2.DescribeInstancesInput{
		InstanceIds: []string{"i-1234567890abcdef0"},
	}).Return(&ec2.DescribeInstancesOutput{Reservations: []types.Reservation{
		{
			Instances: []types.Instance{
				{
					PrivateIpAddress: aws.String("192.168.128.1"),
					NetworkInterfaces: []types.InstanceNetworkInterface{
						{
							NetworkInterfaceId: aws.String("eni-1234567890abcdef0"),
							PrivateIpAddress:   aws.String("192.168.128.1"),
							PrivateIpAddresses: []types.InstancePrivateIpAddress{
								{PrivateIpAddress: aws.String("192.168.128.1")},
							},
						},
					},
				},
			},
		},
	}}, nil).AnyTimes()

	ec2Client.EXPECT().AssignPrivateIpAddresses(context.TODO(), &ec2.AssignPrivateIpAddressesInput{
		AllowReassignment:  aws.Bool(true),
		NetworkInterfaceId: aws.String("eni-1234567890abcdef0"),
		PrivateIpAddresses: []string{"192.168.128.2"},
	}).Return(nil, nil)
	err := client.AssignIPToNode("192.168.128.2", node)
	assert.NoError(t, err)

	ec2Client.EXPECT().AssignPrivateIpAddresses(context.TODO(), &ec2.AssignPrivateIpAddressesInput{
		AllowReassignment:  aws.Bool(true),
		NetworkInterfaceId: aws.String("eni-1234567890abcdef0"),
		PrivateIpAddresses: []string{"192.168.192.1"},
	}).Return(nil, fmt.Errorf("failed to assign IP"))
	err = client.AssignIPToNode("192.168.192.1", node)
	assert.ErrorContains(t, err, "failed to assign IP")
}

func TestUnassignIPToNode(t *testing.T) {

	tests := []struct {
		name             string
		ip               string
		node             *corev1.Node
		expectedEC2Calls func(*MockEC2Interface)
		expectError      bool
	}{
		{
			name: "unassign successfully",
			ip:   "192.168.1.4",
			node: fakeNode("ip-192-168-1-1.us-west-2.compute.internal", "aws:///us-west-2a/i-1234567890abcdef0"),
			expectedEC2Calls: func(recorder *MockEC2Interface) {
				recorder.EXPECT().DescribeInstances(context.TODO(), &ec2.DescribeInstancesInput{
					InstanceIds: []string{"i-1234567890abcdef0"},
				}).Return(&ec2.DescribeInstancesOutput{Reservations: []types.Reservation{
					{
						Instances: []types.Instance{
							{
								PrivateIpAddress: aws.String("192.168.1.1"),
								NetworkInterfaces: []types.InstanceNetworkInterface{
									{
										NetworkInterfaceId: aws.String("eni-1234567890abcdef0"),
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
				recorder.EXPECT().UnassignPrivateIpAddresses(context.TODO(), &ec2.UnassignPrivateIpAddressesInput{
					NetworkInterfaceId: aws.String("eni-1234567890abcdef0"),
					PrivateIpAddresses: []string{"192.168.1.4"},
				}).Return(nil, nil)
			},
		},
		{
			name: "instance not found",
			ip:   "192.168.1.4",
			node: fakeNode("ip-192-168-1-1.us-west-2.compute.internal", "aws:///us-west-2a/i-1234567890abcdef0"),
			expectedEC2Calls: func(recorder *MockEC2Interface) {
				recorder.EXPECT().DescribeInstances(context.TODO(), &ec2.DescribeInstancesInput{
					InstanceIds: []string{"i-1234567890abcdef0"},
				}).Return(nil,
					newSmithyOperationError("DescribeInstances",
						"InvalidInstanceID.NotFound",
						"The instance ID 'i-1234567890abcdef0' does not exist",
						http.StatusBadRequest),
				)
			},
		},
		{
			name: "IP already unassigned",
			ip:   "192.168.1.4",
			node: fakeNode("ip-192-168-1-1.us-west-2.compute.internal", "aws:///us-west-2a/i-1234567890abcdef0"),
			expectedEC2Calls: func(recorder *MockEC2Interface) {
				recorder.EXPECT().DescribeInstances(context.TODO(), &ec2.DescribeInstancesInput{
					InstanceIds: []string{"i-1234567890abcdef0"},
				}).Return(&ec2.DescribeInstancesOutput{Reservations: []types.Reservation{
					{
						Instances: []types.Instance{
							{
								PrivateIpAddress: aws.String("192.168.1.1"),
								NetworkInterfaces: []types.InstanceNetworkInterface{
									{
										NetworkInterfaceId: aws.String("eni-1234567890abcdef0"),
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
				recorder.EXPECT().UnassignPrivateIpAddresses(context.TODO(), &ec2.UnassignPrivateIpAddressesInput{
					NetworkInterfaceId: aws.String("eni-1234567890abcdef0"),
					PrivateIpAddresses: []string{"192.168.1.4"},
				}).Return(nil, newSmithyOperationError("UnassignPrivateIpAddresses",
					"InvalidParameterValue",
					"Some of the specified private IP addresses are not assigned to the network interface 'eni-1234567890abcdef0'",
					http.StatusBadRequest))
			},
		},
		{
			name: "NetworkInterface not found",
			ip:   "192.168.1.4",
			node: fakeNode("ip-192-168-1-1.us-west-2.compute.internal", "aws:///us-west-2a/i-1234567890abcdef0"),
			expectedEC2Calls: func(recorder *MockEC2Interface) {
				recorder.EXPECT().DescribeInstances(context.TODO(), &ec2.DescribeInstancesInput{
					InstanceIds: []string{"i-1234567890abcdef0"},
				}).Return(&ec2.DescribeInstancesOutput{Reservations: []types.Reservation{
					{
						Instances: []types.Instance{
							{
								PrivateIpAddress: aws.String("192.168.1.1"),
								NetworkInterfaces: []types.InstanceNetworkInterface{
									{
										NetworkInterfaceId: aws.String("eni-1234567890abcdef0"),
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
				recorder.EXPECT().UnassignPrivateIpAddresses(context.TODO(), &ec2.UnassignPrivateIpAddressesInput{
					NetworkInterfaceId: aws.String("eni-1234567890abcdef0"),
					PrivateIpAddresses: []string{"192.168.1.4"},
				}).Return(nil, newSmithyOperationError("UnassignPrivateIpAddresses",
					"InvalidNetworkInterfaceID.NotFound",
					"The network interface ID 'eni-1234567890abcdef0' does not exist",
					http.StatusBadRequest))
			},
		},
		{
			name: "Unexpected error",
			ip:   "192.168.1.4",
			node: fakeNode("ip-192-168-1-1.us-west-2.compute.internal", "aws:///us-west-2a/i-1234567890abcdef0"),
			expectedEC2Calls: func(recorder *MockEC2Interface) {
				recorder.EXPECT().DescribeInstances(context.TODO(), &ec2.DescribeInstancesInput{
					InstanceIds: []string{"i-1234567890abcdef0"},
				}).Return(nil, newSmithyOperationError("DescribeInstances",
					"IncorrectInstanceState",
					"The instance ID 'i-1234567890abcdef0' is not in the correct state for the operation",
					http.StatusBadRequest))
			},
			expectError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			client, ec2Client := newFakeClient(ctrl)
			test.expectedEC2Calls(ec2Client)
			err := client.UnassignIPToNode(test.ip, test.node)
			assert.True(t, test.expectError == (err != nil))
		})
	}
}

func Test_parseInstanceID(t *testing.T) {
	tests := []struct {
		name       string
		providerID string
		want       string
		wantErr    bool
	}{
		{"valid", "aws:///us-west-2a/i-1234567890abcdef0", "i-1234567890abcdef0", false},
		{"invalid", "aws:///us-west-2a/1234567890abcdef0", "", true},
		{"empty", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseInstanceID(tt.providerID)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseInstanceID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseInstanceID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func newSmithyOperationError(operation, errCode, errMsg string, httpCode int) error {
	return &smithy.OperationError{
		ServiceID:     "EC2",
		OperationName: operation,
		Err: &awshttp.ResponseError{
			RequestID: uuid.New().String(),
			ResponseError: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{
					Response: &http.Response{
						StatusCode: httpCode,
					},
				},
				Err: &smithy.GenericAPIError{
					Code:    errCode,
					Message: errMsg,
				},
			},
		},
	}
}
