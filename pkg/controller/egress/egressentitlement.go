// Copyright 2024 Antrea Authors
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

package egress

import (
	"context"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	crdv1beta1 "antrea.io/antrea/pkg/apis/crd/v1beta1"
)

var (
	systemGeneratedEntitlementBinding = crdv1beta1.EgressEntitlementBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "system-default",
		},
		Spec: crdv1beta1.EgressEntitlementBindingSpec{
			EgressEntitlement: "system-default",
			Subjects: []rbacv1.Subject{
				{
					Kind:     "Group",
					Name:     "system:authenticated",
					APIGroup: "rbac.authorization.k8s.io",
				},
			},
		},
	}

	systemGeneratedEntitlement = crdv1beta1.EgressEntitlement{
		ObjectMeta: metav1.ObjectMeta{
			Name: "system-default",
		},
		Spec: crdv1beta1.EgressEntitlementSpec{
			ExternalIPPools: []string{"*"},
			EgressIPs:       []string{"*"},
			AppliedToScope: crdv1beta1.AppliedToScope{
				NamespaceSelector: &metav1.LabelSelector{},
			},
		},
	}
)

// Initialize the default EgressEntitlement to allow all users open access to all the resources.
func (c *EgressController) InitializeEgressEntitlement() {
	// Initialize EgressEntitlements iff it is enterprise version of antrea.
	if c.isEnterpriseAntrea {
		// Check if EgressEntitlement is already present.
		if _, err := c.egressEntitlementLister.Get(systemGeneratedEntitlement.Name); err == nil {
			// EgressEntitlement is already present.
			klog.InfoS("EgressEntitlement already created", "egressEntitlement", klog.KObj(&systemGeneratedEntitlement))
			return
		}
		// Check if EgressEntitlementBinding is already present.
		if _, err := c.egressEntitlementBindingLister.Get(systemGeneratedEntitlementBinding.Name); err == nil {
			klog.InfoS("EgressEntitlementBinding already created", "egressEntitlementBinding", klog.KObj(&systemGeneratedEntitlementBinding))
		} else {
			// Create EgressEntitlementBinding.
			c.initEgressEntitlementBinding(systemGeneratedEntitlementBinding)
		}
		// Create EgressEntitlement.
		c.initEgressEntitlement(systemGeneratedEntitlement)
	}
}

// initEgressEntitlementBinding attempts to create system EgressEntitlementBindings until they are created using an
// exponential backoff period from 1 to max of 8secs.
func (c *EgressController) initEgressEntitlementBinding(eeb crdv1beta1.EgressEntitlementBinding) {
	var err error
	const maxBackoffTime = 8 * time.Second
	backoff := 1 * time.Second
	retryAttempt := 1

	for {
		klog.V(2).InfoS("Creating system EgressEntilementBinding", "egressEntitlementBinding", klog.KObj(&eeb))
		// Attempt to recreate EgressEntitlementBinding after a backoff only if it does not exist.
		if _, err = c.crdClient.CrdV1beta1().EgressEntitlementBindings().Create(context.TODO(), &eeb, metav1.CreateOptions{}); err != nil {
			if errors.IsAlreadyExists(err) {
				klog.InfoS("System EgressEntitlementBinding already exists", "egressEntitlementBinding", klog.KObj(&eeb))
				return
			}
			klog.ErrorS(err, "Failed to create system EgressEntitlementBinding on init, will retry", "egressEntitlementBinding", klog.KObj(&eeb), "attempts", retryAttempt)
			// EgressEntitlementBinding creation may fail because antrea APIService is not yet ready
			// to accept requests for validation. Retry fixed number of times
			// not exceeding 8s.
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoffTime {
				backoff = maxBackoffTime
			}
			retryAttempt += 1
			continue
		}
		klog.InfoS("Created system EgressEntitlementBinding", "egressEntitlementBinding", klog.KObj(&eeb))
		return
	}
}

// initEgressEntitlement attempts to create system EgressEntitlements until they are created using an
// exponential backoff period from 1 to max of 8secs.
func (c *EgressController) initEgressEntitlement(ee crdv1beta1.EgressEntitlement) {
	var err error
	const maxBackoffTime = 8 * time.Second
	backoff := 1 * time.Second
	retryAttempt := 1

	for {
		klog.V(2).InfoS("Creating system EgressEntilement", "egressEntitlement", klog.KObj(&ee))
		// Attempt to recreate EgressEntitlement after a backoff only if it does not exist.
		if _, err = c.crdClient.CrdV1beta1().EgressEntitlements().Create(context.TODO(), &ee, metav1.CreateOptions{}); err != nil {
			if errors.IsAlreadyExists(err) {
				klog.InfoS("System EgressEntitlement already exists", "egressEntitlement", klog.KObj(&ee))
				return
			}
			klog.ErrorS(err, "Failed to create system EgressEntitlement on init, will retry", "egressEntitlement", klog.KObj(&ee), "attempts", retryAttempt)
			// EgressEntitlement creation may fail because antrea APIService is not yet ready
			// to accept requests for validation. Retry fixed number of times
			// not exceeding 8s.
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoffTime {
				backoff = maxBackoffTime
			}
			retryAttempt += 1
			continue
		}
		klog.InfoS("Created system EgressEntitlement", "egressEntitlement", klog.KObj(&ee))
		return
	}
}
