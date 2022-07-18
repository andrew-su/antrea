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

package v1alpha1

import (
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"antrea.io/antrea/pkg/apis/crd/v1alpha2"
)

const (
	// Permissionedit allows creating, updating and deleting permission for Tiers
	PermissionEdit = "edit"
)

// +genclient
// +genclient:nonNamespaced
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type TierEntitlement struct {
	metav1.TypeMeta `json:",inline"`
	// Standard metadata of the object.
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Specification of the desired behavior of TierEntitlement.
	Spec TierEntitlementSpec `json:"spec"`
}

// TierEntitlementSpec defines the desired state for TierEntitlement.
type TierEntitlementSpec struct {
	// Tier is a list of Tier names to which this entitlement belongs to.
	// TiersAll represents all Tiers.
	Tiers []string `json:"tiers"`
	// Permission defines the allowed actions to be performed on the Tiers
	// specified in Tiers. The only allowed permission is "edit". The "edit"
	// permission allows any authorized user to add/remove Tier references in
	// an Antrea-native policy.
	Permission string `json:"permission"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type TierEntitlementList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []TierEntitlement `json:"items"`
}

// +genclient
// +genclient:nonNamespaced
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type TierEntitlementBinding struct {
	metav1.TypeMeta `json:",inline"`
	// Standard metadata of the object.
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Specification of the desired behavior of TierEntitlementBinding.
	Spec TierEntitlementBindingSpec `json:"spec"`
}

// TierEntitlementBindingSpec defines the desired state for TierEntitlementBinding.
type TierEntitlementBindingSpec struct {
	// Subjects holds references to the objects the entitlement applies to.
	// +optional
	Subjects []rbacv1.Subject `json:"subjects,omitempty"`
	// TierEntitlement references a TierEntitlement in the global namespace.
	// If the TierEntitlement cannot be resolved, the Authorizer must return an
	// error.
	TierEntitlement string `json:"tierEntitlement"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type TierEntitlementBindingList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []TierEntitlementBinding `json:"items"`
}

// +genclient
// +genclient:nonNamespaced
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// IDPSPolicy allows selecting Pods with appliedTo and applying IDS/IPS to these Pods.
type IDPSPolicy struct {
	metav1.TypeMeta `json:",inline"`
	// Standard metadata of the object.
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Specification of the desired behavior of IDPSPolicy.
	Spec IDPSPolicySpec `json:"spec"`
}

// IDPSPolicySpec describes the spec of IDPSPolicy.
type IDPSPolicySpec struct {
	// AppliedTo is used to select Pods.
	AppliedTo v1alpha2.AppliedTo `json:"appliedTo"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type IDPSPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []IDPSPolicy `json:"items"`
}

// +genclient
// +genclient:nonNamespaced
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type IDPSSignatureProviderInfo struct {
	metav1.TypeMeta `json:",inline"`
	// Standard metadata of the object.
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Signature bundle information.
	SignatureBundle IDPSSignatureBundleInfo `json:"signatureBundle"`
}

type IDPSSignatureBundleInfo struct {
	// Version is the value of signature version.
	Version uint32 `json:"version"`

	// Sha256Checksum is the sha256 checksum value of signature data.
	Sha256Checksum string `json:"sha256CheckSum"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type IDPSSignatureProviderInfoList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []IDPSSignatureProviderInfo `json:"items"`
}

// +genclient
// +genclient:nonNamespaced
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type NSXRegistration struct {
	metav1.TypeMeta `json:",inline"`
	// Standard metadata of the object.
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Timestamp is an encrypted timestamp string, which records the time that NSX registration is last confirmed.
	Timestamp string `json:"timestamp,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type NSXRegistrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []NSXRegistration `json:"items"`
}
