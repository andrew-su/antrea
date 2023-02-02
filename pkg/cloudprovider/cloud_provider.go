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

package cloudprovider

type Interface interface {
	// GetMaxIPsByInstanceType returns maximum IPs that can be assigned to a given instance type.
	// It doesn't count the primary IP which cannot be assigned/unassigned.
	GetMaxIPsByInstanceType(instanceType string) (int, bool, error)
	// GetIPsByNode returns IPs that are assigned to a given Node.
	// It doesn't include the primary IP.
	GetIPsByNode(node string) ([]string, error)
	// AssignIPToNode assignes an IP to a given Node.
	AssignIPToNode(ip string, node string) error
	// UnassignIPToNode unassignes an IP to a given Node.
	UnassignIPToNode(ip string, node string) error
}
