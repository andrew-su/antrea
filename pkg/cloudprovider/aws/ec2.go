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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"k8s.io/klog/v2"

	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

type EC2Interface interface {
	DescribeInstances(ctx context.Context, params *ec2.DescribeInstancesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
	AssignPrivateIpAddresses(ctx context.Context, params *ec2.AssignPrivateIpAddressesInput, optFns ...func(*ec2.Options)) (*ec2.AssignPrivateIpAddressesOutput, error)
	UnassignPrivateIpAddresses(ctx context.Context, params *ec2.UnassignPrivateIpAddressesInput, optFns ...func(*ec2.Options)) (*ec2.UnassignPrivateIpAddressesOutput, error)
	DescribeInstanceTypes(ctx context.Context, params *ec2.DescribeInstanceTypesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error)
}

// getIPsAndInterfaceIDOnCloudNode gets assigned IPs on node and returns the interface ID.
// Return error if any operation fails.
func (c *Client) getIPsAndInterfaceIDOnCloudNode(node string) (string, []string, error) {
	var interfaceID string = ""
	assignedIPs := []string{}
	privateDNSName := node
	instanceInput := &ec2.DescribeInstancesInput{
		Filters: []ec2types.Filter{
			{
				Name: aws.String("private-dns-name"),
				Values: []string{
					privateDNSName,
				},
			},
		},
	}
	instanceResult, err := c.ec2Client.DescribeInstances(context.TODO(), instanceInput)
	if err != nil {
		return "", nil, fmt.Errorf("unable to complete DescribeInstances API call: %w", err)
	}

	reservations := instanceResult.Reservations
	if len(reservations) == 0 {
		return "", nil, fmt.Errorf("reservation for Node %s not found", node)
	} else if len(reservations) > 1 {
		return "", nil, fmt.Errorf("found %d reservations for Node %s", len(reservations), node)
	}
	instances := instanceResult.Reservations[0].Instances
	if len(instances) == 0 {
		return "", nil, fmt.Errorf("instances for Node %s not found", node)
	} else if len(instances) > 1 {
		return "", nil, fmt.Errorf("found %d instances for Node %s", len(instances), node)
	}
	instance := instances[0]
	// Lookup the interface with the same private IP of the instance
	privateIPv4 := *instance.PrivateIpAddress
	for _, networkInterface := range instance.NetworkInterfaces {
		assignedIPs = assignedIPs[:0]
		for _, interfaceIP := range networkInterface.PrivateIpAddresses {
			if privateIPv4 == *interfaceIP.PrivateIpAddress {
				interfaceID = *networkInterface.NetworkInterfaceId
				continue
			}
			assignedIPs = append(assignedIPs, *interfaceIP.PrivateIpAddress)
		}
		if interfaceID != "" {
			return interfaceID, assignedIPs, nil
		}
	}

	return "", nil, fmt.Errorf("unable to find target IP %s from instances", privateIPv4)
}

// GetIPsOnCloudNode gets assigned IPs on node.
func (c *Client) GetIPsByNode(node string) ([]string, error) {
	interfaceID, assignedIPs, err := c.getIPsAndInterfaceIDOnCloudNode(node)
	if err != nil {
		return nil, err
	}
	c.setInterfaceID(node, interfaceID)
	return assignedIPs, nil
}

func (c *Client) setInterfaceID(node string, interfaceID string) error {
	c.nodeMutex.Lock()
	defer c.nodeMutex.Unlock()
	c.nodeToInterfaceID[node] = interfaceID
	return nil
}

func (c *Client) getInterfaceID(node string) (string, error) {
	c.nodeMutex.Lock()
	defer c.nodeMutex.Unlock()
	interfaceID, exists := c.nodeToInterfaceID[node]
	if !exists {
		var err error
		interfaceID, _, err = c.getIPsAndInterfaceIDOnCloudNode(node)
		if err != nil {
			return "", err
		}
		c.nodeToInterfaceID[node] = interfaceID
	}
	return interfaceID, nil
}

// AssignIPToCloudNode assigns an IP to AWS interface which is on the target node.
func (c *Client) AssignIPToNode(ip string, node string) error {
	interfaceID, err := c.getInterfaceID(node)
	if err != nil {
		return err
	}
	ipInput := &ec2.AssignPrivateIpAddressesInput{
		AllowReassignment:  aws.Bool(true),
		NetworkInterfaceId: aws.String(interfaceID),
		PrivateIpAddresses: []string{ip},
	}
	_, err = c.ec2Client.AssignPrivateIpAddresses(context.TODO(), ipInput)
	if err != nil {
		return fmt.Errorf("unable to assign IP %s to interface %s on node %s: %w", ip, interfaceID, node, err)
	}
	return nil
}

// UnassignIPToCloudNode unassigns an IP on AWS interface which is on the target node.
func (c *Client) UnassignIPToNode(ip string, node string) error {
	interfaceID, err := c.getInterfaceID(node)
	if err != nil {
		return err
	}
	ipInput := &ec2.UnassignPrivateIpAddressesInput{
		NetworkInterfaceId: aws.String(interfaceID),
		PrivateIpAddresses: []string{ip},
	}
	_, err = c.ec2Client.UnassignPrivateIpAddresses(context.TODO(), ipInput)
	if err != nil {
		return fmt.Errorf("unable to unassign IP %s to interface %s on node %s: %w", ip, interfaceID, node, err)
	}
	return nil
}

func (c *Client) GetMaxIPsByInstanceType(instanceType string) (int, bool, error) {
	maxIPs, exists := func() (int, bool) {
		c.instanceTypeMutex.RLock()
		defer c.instanceTypeMutex.RUnlock()
		maxIPs, exists := c.instanceTypeToMaxIPs[instanceType]
		return maxIPs, exists
	}()
	if exists {
		return maxIPs, true, nil
	}

	instanceTypesInput := &ec2.DescribeInstanceTypesInput{
		InstanceTypes: []ec2types.InstanceType{ec2types.InstanceType(instanceType)},
	}
	instanceTypesOutput, err := c.ec2Client.DescribeInstanceTypes(context.TODO(), instanceTypesInput)
	if err != nil {
		return 0, false, fmt.Errorf("error describing instance type %s: %v", instanceType, err)
	}
	if len(instanceTypesOutput.InstanceTypes) == 0 {
		return 0, false, fmt.Errorf("instance type %s not found", instanceType)
	}
	if len(instanceTypesOutput.InstanceTypes) > 1 {
		return 0, false, fmt.Errorf("found %d instance type info for %s", len(instanceTypesOutput.InstanceTypes), instanceType)
	}
	instanceTypeInfo := instanceTypesOutput.InstanceTypes[0]
	// It shouldn't happen. However, if it happens, there must be something wrong on AWS side, and there is no point to retry.
	if instanceTypeInfo.NetworkInfo.Ipv4AddressesPerInterface == nil {
		klog.InfoS("Ipv4AddressesPerInterface for instance type is unknown", "instanceType", instanceType)
		return 0, false, nil
	}

	// Must minus 1 (occupied by the primary private IP)
	maxIPs = int(*instanceTypeInfo.NetworkInfo.Ipv4AddressesPerInterface) - 1
	c.instanceTypeMutex.Lock()
	defer c.instanceTypeMutex.Unlock()
	c.instanceTypeToMaxIPs[instanceType] = maxIPs
	return maxIPs, true, nil
}
