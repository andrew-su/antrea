// Copyright 2021 Antrea Authors
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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	crdv1beta1 "antrea.io/antrea/pkg/apis/crd/v1beta1"
	"antrea.io/antrea/pkg/features"
)

func marshal(object runtime.Object) []byte {
	raw, _ := json.Marshal(object)
	return raw
}

func TestValidateLabelSelector(t *testing.T) {
	tests := []struct {
		name             string
		supersetSelector *metav1.LabelSelector
		subsetSelector   *metav1.LabelSelector
		expectedResponse string
	}{
		{
			name: "No namespace selected by superset",
			subsetSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"foo": "bar",
					"env": "staging",
					"app": "nginx",
				},
			},
			expectedResponse: "No Namespace is allowed by the EgressEntitlement",
		},
		{
			name: "Subset selects all namespaces",
			supersetSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"env": "staging",
					"foo": "bar",
				},
			},
			subsetSelector:   &metav1.LabelSelector{},
			expectedResponse: "Selecting all Namespaces is not permitted by the EgressEntitlement",
		},
		{
			name: "Valid Namespace selector with some extra labels",
			supersetSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"env": "staging",
					"foo": "bar",
				},
			},
			subsetSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"foo": "bar",
					"env": "staging",
					"app": "nginx",
				},
			},
		},
		{
			name: "Superset has matchExpression and subset provides matchLabel[Valid]",
			supersetSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"staging", "prod", "dev"},
					},
				},
			},
			subsetSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"env": "staging",
				},
			},
		},
		{
			name: "Superset has matchExpression and subset provides matchLabel[Invalid]",
			supersetSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"staging", "prod", "dev"},
					},
				},
			},
			subsetSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"env": "temp",
				},
			},
			expectedResponse: "Values provided for key \"env\" and operator \"In\" should be a subset of [dev prod staging], but the provided namespaceSelector uses [temp]",
		},
		{
			name: "MatchExpression does not match the required expression[MatchExpressions NotIn Operator]",
			supersetSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"staging", "prod", "dev"},
					},
				},
			},
			subsetSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"staging", "prod"},
					},
				},
			},
			expectedResponse: "Values provided for key \"env\" and operator \"NotIn\" should be a superset of [dev prod staging], but the provided namespaceSelector uses [prod staging]",
		},
		{
			name: "valid MatchExpression [MatchExpressions NotIn Operator]",
			supersetSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"staging", "prod"},
					},
				},
			},
			subsetSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpNotIn,
						Values:   []string{"staging", "prod", "dev"},
					},
				},
			},
		},
		{
			name: "MatchExpression does not match the required expression[MatchExpressions Exists Operator]",
			supersetSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpExists,
					},
					{
						Key:      "stage",
						Operator: metav1.LabelSelectorOpExists,
					},
				},
			},
			subsetSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpExists,
					},
				},
			},
			expectedResponse: "Key \"stage\" is required in the namespaceSelector field while creating the Egress resource, as restricted by the EgressEntitlement",
		},
		{
			name: "valid MatchExpression[MatchExpressions Exists Operator]",
			supersetSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpExists,
					},
				},
			},
			subsetSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpExists,
					},
					{
						Key:      "stage",
						Operator: metav1.LabelSelectorOpExists,
					},
				},
			},
		},
		{
			name: "MatchExpression does not match the required expression[MatchExpressions DoesNotExist Operator]",
			supersetSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
					{
						Key:      "stage",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
				},
			},
			subsetSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
				},
			},
			expectedResponse: "Key \"stage\" is required in the namespaceSelector field while creating the Egress resource, as restricted by the EgressEntitlement",
		},
		{
			name: "valid MatchExpressions[MatchExpressions DoesNotExist Operator]",
			supersetSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
				},
			},
			subsetSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "env",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
					{
						Key:      "stage",
						Operator: metav1.LabelSelectorOpDoesNotExist,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid, response := validateLabelSelector(tt.supersetSelector, tt.subsetSelector)
			if tt.expectedResponse == "" {
				assert.True(t, valid)
			} else {
				assert.False(t, valid)
				assert.Equal(t, tt.expectedResponse, response)
			}
		})
	}
}

func TestEgressControllerValidateEgress(t *testing.T) {
	var (
		bandwidth = crdv1beta1.Bandwidth{
			Rate:  "500k",
			Burst: "10M",
		}
		invalidBandwidthRate = crdv1beta1.Bandwidth{
			Rate:  "500A",
			Burst: "10G",
		}
		invalidBandwidthBurst = crdv1beta1.Bandwidth{
			Rate:  "1.5G",
			Burst: "10b",
		}
		egressEntitlementBinding = crdv1beta1.EgressEntitlementBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: "system-default1",
			},
			Spec: crdv1beta1.EgressEntitlementBindingSpec{
				EgressEntitlement: "system-default1",
				Subjects: []rbacv1.Subject{
					{
						Kind:     "Group",
						Name:     "system:authenticated",
						APIGroup: "rbac.authorization.k8s.io",
					},
				},
			},
		}
	)
	tests := []struct {
		name                      string
		existingExternalIPPools   []*crdv1beta1.ExternalIPPool
		request                   *admv1.AdmissionRequest
		expectedResponse          *admv1.AdmissionResponse
		egressEntitlements        []*crdv1beta1.EgressEntitlement
		egressEntitlementBindings []*crdv1beta1.EgressEntitlementBinding
	}{
		{
			name:                    "Requesting IP from non-existing ExternalIPPool should not be allowed",
			existingExternalIPPools: nil,
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.1", "nonExistingPool", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "ExternalIPPool nonExistingPool does not exist",
				},
			},
		},
		{
			name: "Requesting IP from non-existing ExternalIPPool should not be allowed[multi-Pools]",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{
				newExternalIPPool("bar", "10.10.10.0/24", "", ""),
			},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgressWithMultiExternalIPPools("foo", "", "", []string{"1.1.1.1", "10.10.10.1"}, []string{"nonExistingPool", "bar"}, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "ExternalIPPool nonExistingPool does not exist",
				},
			},
		},
		{
			name:                    "Requesting IP out of range should not be allowed",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{newExternalIPPool("bar", "10.10.10.0/24", "", "")},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.11.1", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "IP 10.10.11.1 is not within the IP range of ExternalIPPool bar",
				},
			},
		},
		{
			name: "Requesting IP out of range should not be allowed[multi-pools]",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{
				newExternalIPPool("bar", "10.10.10.0/24", "", ""),
				newExternalIPPool("bar1", "20.20.20.0/24", "", ""),
			},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgressWithMultiExternalIPPools("foo", "", "", []string{"10.10.11.1", "20.20.20.1"}, []string{"bar", "bar1"}, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "IP 10.10.11.1 is not within the IP range of ExternalIPPool bar",
				},
			},
		},
		{
			name:                    "Requesting normal IP should be allowed",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{newExternalIPPool("bar", "10.10.10.0/24", "", "")},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.1", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name:                    "Requesting EgressIPs nums larger than ExternalIPPools should not be allowed[multi-pools]",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{newExternalIPPool("bar", "10.10.10.0/24", "", "")},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgressWithMultiExternalIPPools("foo", "", "", []string{"10.10.10.1", "2.2.2.2"}, []string{"bar"}, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "The count of EgressIPs 2 must not be greater than the count of ExternalIPPools 1",
				},
			},
		},
		{
			name: "Requesting ExternalIPPools is empty and EgressIPs num larger than 1 should not be allowed[multi-pools]",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgressWithMultiExternalIPPools("foo", "", "", []string{"10.10.10.1", "2.2.2.2"}, nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "EgressIP, ExternalIPPool, and ExternalIPPools must not be empty at the same time",
				},
			},
		},
		{
			name: "Requesting normal Egress with multiple ExternalIPPools should be allowed[multi-pools]",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{
				newExternalIPPool("bar", "10.10.10.0/24", "", ""),
				newExternalIPPool("bar1", "20.20.20.0/24", "", ""),
			},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgressWithMultiExternalIPPools("foo", "", "", []string{"10.10.10.1", "20.20.20.1"}, []string{"bar", "bar1"}, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Requesting Egress with multiple ExternalIPPools num larger than EgressIPs num should be allowed[multi-pools]",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{
				newExternalIPPool("bar", "10.10.10.0/24", "", ""),
				newExternalIPPool("bar1", "20.20.20.0/24", "", ""),
			},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgressWithMultiExternalIPPools("foo", "", "", []string{"10.10.10.1"}, []string{"bar", "bar1"}, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Requesting Egress with multiple ExternalIPPools and nil EgressIPs should be allowed[multi-pools]",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{
				newExternalIPPool("bar", "10.10.10.0/24", "", ""),
				newExternalIPPool("bar1", "20.20.20.0/24", "", ""),
			},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgressWithMultiExternalIPPools("foo", "", "", nil, []string{"bar", "bar1"}, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Requesting Egress with multiple ExternalIPPools(with '' pool) and nil EgressIPs should not be allowed[multi-pools]",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{
				newExternalIPPool("bar", "10.10.10.0/24", "", ""),
				newExternalIPPool("bar1", "20.20.20.0/24", "", ""),
			},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgressWithMultiExternalIPPools("foo", "", "", nil, []string{"bar", "bar1", ""}, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "The items of ExternalIPPools must not be empty",
				},
			},
		},
		{
			name: "Requesting Egress with multiple ExternalIPPools(with duplicate pool name) and nil EgressIPs should not be allowed[multi-pools]",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{
				newExternalIPPool("bar", "10.10.10.0/24", "", ""),
			},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgressWithMultiExternalIPPools("foo", "", "", nil, []string{"bar", "bar"}, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "The items of ExternalIPPools must be unique",
				},
			},
		},
		{
			name:                    "Updating EgressIP to invalid one should not be allowed",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{newExternalIPPool("bar", "10.10.10.0/24", "", "")},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "UPDATE",
				OldObject: runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.1", "bar", nil, nil, nil))},
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.11.1", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "IP 10.10.11.1 is not within the IP range of ExternalIPPool bar",
				},
			},
		},
		{
			name:                    "Updating EgressIP to valid one should be allowed",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{newExternalIPPool("bar", "10.10.10.0/24", "", "")},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "UPDATE",
				OldObject: runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.1", "bar", nil, nil, nil))},
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name:                    "Updating podSelector should be allowed",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{newExternalIPPool("bar", "10.10.10.0/24", "", "")},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "UPDATE",
				OldObject: runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.1", "bar", nil, nil, nil))},
				Object: runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "bar", &metav1.LabelSelector{
					MatchLabels: map[string]string{"foo": "bar"},
				}, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "DELETE operation should be allowed",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "DELETE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Creating an Egress with bandwidth should be allowed",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.1", "", nil, nil, &bandwidth))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Update an Egress bandwidth config should be allowed",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "UPDATE",
				OldObject: runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.1", "", nil, nil, &bandwidth))},
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.1", "", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Create an Egress with invalid bandwidth rate",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.1", "", nil, nil, &invalidBandwidthRate))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "Rate 500A in Egress foo is invalid: quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'",
				},
			},
		},
		{
			name: "Create an Egress with invalid bandwidth burst",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.1", "", nil, nil, &invalidBandwidthBurst))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "Burst 10b in Egress foo is invalid: quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'",
				},
			},
		},
		{
			name: "Requesting to use ExternalIPPool in Egress for which user is not entitled",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{
				newExternalIPPool("bar", "10.10.10.0/24", "", ""),
			},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"exip"},
						EgressIPs:       []string{"*"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "None of the user entitlements allow the creation / update / deletion of this Egress resource for the following reasons:\nEntitlement \"system-default\": [Access to the ExternalIPPool \"bar\" is not permitted by the EgressEntitlement]\n",
				},
			},
		},
		{
			name: "Requesting to use static IP in Egress for which user is not entitled",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.1", "", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"*"},
						EgressIPs:       []string{"10.10.10.2"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "None of the user entitlements allow the creation / update / deletion of this Egress resource for the following reasons:\nEntitlement \"system-default\": [Static IP 10.10.10.1 used in Egress CRD, does not belong to the list of Egress IPs for which user is entitled]\n",
				},
			},
		},
		{
			name: "Requesting to use static IP in Egress for which user is entitled",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.1", "", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"*"},
						EgressIPs:       []string{"10.10.10.1"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Requesting to use ExternalIPPool in Egress for which user is entitled",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{
				newExternalIPPool("bar", "10.10.10.0/24", "", ""),
			},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"bar"},
						EgressIPs:       []string{"*"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "User is entitled to consume all resources[ExternalIPPool case]",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{
				newExternalIPPool("bar", "10.10.10.0/24", "", ""),
			},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "User is entitled to consume all resources[Static IP case]",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.1", "", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Requesting to delete Egress resource [ExternalIPPool unentitled case]",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "DELETE",
				OldObject: runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"exip"},
						EgressIPs:       []string{"*"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Requesting to delete Egress resource [Static IP unentitled case]",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "DELETE",
				OldObject: runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"*"},
						EgressIPs:       []string{"10.10.10.1"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Requesting to delete Egress resource [Static IP entitled case]",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "DELETE",
				OldObject: runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"*"},
						EgressIPs:       []string{"10.10.10.2"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Requesting to delete Egress resource [ExternalIPPool entitled case]",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "DELETE",
				OldObject: runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"bar"},
						EgressIPs:       []string{"*"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Requesting to delete Egress resource [All Allow case]",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "DELETE",
				OldObject: runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Requesting to create Egress without Namespace selector",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{
				newExternalIPPool("bar", "10.10.10.0/24", "", ""),
			},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"*"},
						EgressIPs:       []string{"*"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"env": "staging",
									"foo": "bar",
								},
							},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "None of the user entitlements allow the creation / update / deletion of this Egress resource for the following reasons:\nEntitlement \"system-default\": [NamespaceSelector must be specified as required by the EgressEntitlement]\n",
				},
			},
		},
		{
			name: "Requesting to create Egress with unentitled Namespace selector",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "", nil, &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}}, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"*"},
						EgressIPs:       []string{"*"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"env": "staging",
									"foo": "bar",
								},
							},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "None of the user entitlements allow the creation / update / deletion of this Egress resource for the following reasons:\nEntitlement \"system-default\": [Key \"env\" is required in the namespaceSelector field while creating the Egress resource, as restricted by the EgressEntitlement]\n",
				},
			},
		},
		{
			name: "Requesting to create Egress with valid Namespace selector",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "", nil, &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar", "env": "staging"}}, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"*"},
						EgressIPs:       []string{"*"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"env": "staging",
									"foo": "bar",
								},
							},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Requesting to create Egress with unentitled Namespace selector[MatchExpressions In Operator]",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "", nil, &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "env", Operator: metav1.LabelSelectorOpIn, Values: []string{"staging", "prod", "test"}}}}, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"*"},
						EgressIPs:       []string{"*"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "env",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{"staging", "prod"},
									},
								},
							},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "None of the user entitlements allow the creation / update / deletion of this Egress resource for the following reasons:\nEntitlement \"system-default\": [Values provided for key \"env\" and operator \"In\" should be a subset of [prod staging], but the provided namespaceSelector uses [prod staging test]]\n",
				},
			},
		},
		{
			name: "Requesting to create Egress with valid Namespace selector[MatchExpressions In Operator]",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "", nil, &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "env", Operator: metav1.LabelSelectorOpIn, Values: []string{"staging", "prod"}}}}, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"*"},
						EgressIPs:       []string{"*"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "env",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{"staging", "prod", "test"},
									},
								},
							},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name: "Requesting to create Egress with invalid operator for Namespace selector",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "", nil, &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "env", Operator: metav1.LabelSelectorOpDoesNotExist}}}, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"*"},
						EgressIPs:       []string{"*"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "env",
										Operator: metav1.LabelSelectorOpExists,
									},
								},
							},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "None of the user entitlements allow the creation / update / deletion of this Egress resource for the following reasons:\nEntitlement \"system-default\": [Operator \"Exists\" is not present for key \"env\" in provided namespaceSelector]\n",
				},
			},
		},
		{
			name: "Requesting to create Egress with all allow entitlement for Namespace selector",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "", nil, &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "env", Operator: metav1.LabelSelectorOpDoesNotExist}}}, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements:        []*crdv1beta1.EgressEntitlement{&systemGeneratedEntitlement},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name:                    "Requesting to create Egress allowed with IP out of entitled ExternalIPPool range",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{newExternalIPPool("bar", "10.10.10.0/24", "", "")},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"bar"},
						EgressIPs:       []string{},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
		{
			name:                    "Requesting to create Egress with IP out of entitled ExternalIPPool range",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{newExternalIPPool("bar", "10.10.10.0/24", "", "")},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "192.168.83.84", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"bar"},
						EgressIPs:       []string{},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "IP 192.168.83.84 is not within the IP range of ExternalIPPool bar",
				},
			},
		},
		{
			name:                    "Requesting to consume unentitled resources in Egress",
			existingExternalIPPools: []*crdv1beta1.ExternalIPPool{newExternalIPPool("bar1", "10.10.10.0/24", "", "")},
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "bar1", nil, &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}}, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"bar"},
						EgressIPs:       []string{},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"env": "staging",
									"foo": "bar",
								},
							},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "None of the user entitlements allow the creation / update / deletion of this Egress resource for the following reasons:\nEntitlement \"system-default\": [Key \"env\" is required in the namespaceSelector field while creating the Egress resource, as restricted by the EgressEntitlement; Access to the ExternalIPPool \"bar1\" is not permitted by the EgressEntitlement]\n",
				},
			},
		},
		{
			name: "Requesting to delete Egress resource [Namespace unentitled case]",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "DELETE",
				OldObject: runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "bar", nil, nil, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"exip"},
						EgressIPs:       []string{"*"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"env": "staging",
									"foo": "bar",
								},
							},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "None of the user entitlements allow the creation / update / deletion of this Egress resource for the following reasons:\nEntitlement \"system-default\": [NamespaceSelector must be specified as required by the EgressEntitlement]\n",
				},
			},
		},
		{
			name: "Multiple Egress Entitlement[Not allowed case]",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "", nil, &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "env", Operator: metav1.LabelSelectorOpIn, Values: []string{"staging", "prod"}}}}, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"*"},
						EgressIPs:       []string{"10.10.10.3"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "env",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{"staging", "prod", "test"},
									},
								},
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default1",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"*"},
						EgressIPs:       []string{"10.10.10.2"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "env",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{"staging"},
									},
								},
							},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding, &egressEntitlementBinding},
			expectedResponse: &admv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "None of the user entitlements allow the creation / update / deletion of this Egress resource for the following reasons:\nEntitlement \"system-default\": [Static IP 10.10.10.2 used in Egress CRD, does not belong to the list of Egress IPs for which user is entitled]\nEntitlement \"system-default1\": [Values provided for key \"env\" and operator \"In\" should be a subset of [staging], but the provided namespaceSelector uses [prod staging]]\n",
				},
			},
		},
		{
			name: "Multiple Egress Entitlement[Allowed case]",
			request: &admv1.AdmissionRequest{
				Name:      "foo",
				Operation: "CREATE",
				Object:    runtime.RawExtension{Raw: marshal(newEgress("foo", "10.10.10.2", "", nil, &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "env", Operator: metav1.LabelSelectorOpIn, Values: []string{"staging", "prod"}}}}, nil))},
				UserInfo: authenticationv1.UserInfo{
					Username: "test-user",
					Groups:   []string{"system:authenticated"},
				},
			},
			egressEntitlements: []*crdv1beta1.EgressEntitlement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"*"},
						EgressIPs:       []string{"10.10.10.2"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "env",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{"staging"},
									},
								},
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "system-default1",
					},
					Spec: crdv1beta1.EgressEntitlementSpec{
						ExternalIPPools: []string{"*"},
						EgressIPs:       []string{"10.10.10.2"},
						AppliedToScope: crdv1beta1.AppliedToScope{
							NamespaceSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "env",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{"staging", "prod", "test"},
									},
								},
							},
						},
					},
				},
			},
			egressEntitlementBindings: []*crdv1beta1.EgressEntitlementBinding{&systemGeneratedEntitlementBinding, &egressEntitlementBinding},
			expectedResponse:          &admv1.AdmissionResponse{Allowed: true},
		},
	}

	featuregatetesting.SetFeatureGateDuringTest(t, features.DefaultFeatureGate, features.EgressRBAC, true)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stopCh := make(chan struct{})
			defer close(stopCh)
			var objs []runtime.Object
			for _, pool := range tt.existingExternalIPPools {
				objs = append(objs, pool)
			}
			for _, egressEntitlement := range tt.egressEntitlements {
				objs = append(objs, egressEntitlement)
			}
			for _, egressEntitlementBinding := range tt.egressEntitlementBindings {
				objs = append(objs, egressEntitlementBinding)
			}
			controller := newController(nil, objs)
			controller.informerFactory.Start(stopCh)
			controller.crdInformerFactory.Start(stopCh)
			controller.informerFactory.WaitForCacheSync(stopCh)
			controller.crdInformerFactory.WaitForCacheSync(stopCh)
			go controller.externalIPAllocator.Run(stopCh)
			require.True(t, cache.WaitForCacheSync(stopCh, controller.externalIPAllocator.HasSynced))
			controller.externalIPAllocator.RestoreIPAllocations(nil)
			review := &admv1.AdmissionReview{
				Request: tt.request,
			}
			gotResponse := controller.ValidateEgress(review)
			assert.Equal(t, tt.expectedResponse, gotResponse)
		})
	}
}
