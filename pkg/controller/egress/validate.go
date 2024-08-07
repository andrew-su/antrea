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
	"fmt"
	"net"
	"reflect"
	"sort"
	"strings"

	admv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	crdv1beta1 "antrea.io/antrea/pkg/apis/crd/v1beta1"
	"antrea.io/antrea/pkg/features"
	"antrea.io/antrea/pkg/util/env"
)

type expression struct {
	op     metav1.LabelSelectorOperator
	values sets.Set[string]
}

// Create a map of values corresponding to each operator from the argument expression object.
func requirementsByOperator(expressions []expression) map[metav1.LabelSelectorOperator]sets.Set[string] {
	requirements := make(map[metav1.LabelSelectorOperator]sets.Set[string])
	for _, expr := range expressions {
		switch expr.op {
		case metav1.LabelSelectorOpIn:
			if requirements[expr.op] == nil {
				requirements[expr.op] = expr.values
			} else {
				requirements[expr.op] = requirements[expr.op].Intersection(expr.values)
			}
		case metav1.LabelSelectorOpNotIn:
			requirements[expr.op] = requirements[expr.op].Union(expr.values)
		case metav1.LabelSelectorOpExists, metav1.LabelSelectorOpDoesNotExist:
			requirements[expr.op] = sets.New[string]()
		}
	}
	return requirements
}

// Canonicalize the LabelSelector object for further validation.
func canonicalize(selector *metav1.LabelSelector) map[string][]expression {
	effectiveSelector := make(map[string][]expression)
	if selector.MatchLabels != nil {
		for key, value := range selector.MatchLabels {
			effectiveSelector[key] = append(effectiveSelector[key], expression{
				op:     metav1.LabelSelectorOpIn,
				values: sets.New[string](value),
			})
		}
	}
	if selector.MatchExpressions != nil {
		for _, expr := range selector.MatchExpressions {
			effectiveSelector[expr.Key] = append(effectiveSelector[expr.Key], expression{
				op:     expr.Operator,
				values: sets.New[string](expr.Values...),
			})
		}
	}
	return effectiveSelector
}

// validateLabelSelector validates if everything selected by the subsetSelector is guaranteed to be selected by the supersetSelector.
func validateLabelSelector(supersetSelector, subsetSelector *metav1.LabelSelector) (bool, string) {
	// If no namespace is selected by entitlement then Egress would be rejected.
	if supersetSelector == nil {
		return false, fmt.Sprintf("No Namespace is allowed by the EgressEntitlement")
	}
	// Empty namespace selector means supersetSelector can select everything.
	if len(supersetSelector.MatchLabels)+len(supersetSelector.MatchExpressions) == 0 {
		return true, ""
	}
	// nil namespaceSelector in AppliedTo means all namespaces unless the whole AppliedTo is empty.
	// As the egress entitlement doesn't select everything, we required it to be specified.
	if subsetSelector == nil {
		return false, fmt.Sprintf("NamespaceSelector must be specified as required by the EgressEntitlement")
	}
	// If subsetSelector selects all the namespaces.
	if len(subsetSelector.MatchLabels)+len(subsetSelector.MatchExpressions) == 0 {
		return false, fmt.Sprintf("Selecting all Namespaces is not permitted by the EgressEntitlement")
	}

	effectiveSupersetSelector := canonicalize(supersetSelector)
	effectiveSubsetSelector := canonicalize(subsetSelector)

	for key, expressionSuperset := range effectiveSupersetSelector {
		expressionSubset, exists := effectiveSubsetSelector[key]
		if !exists {
			return false, fmt.Sprintf("Key %q is required in the namespaceSelector field while creating the Egress resource, as restricted by the EgressEntitlement", key)
		}
		supersetRequirements := requirementsByOperator(expressionSuperset)
		subsetRequirements := requirementsByOperator(expressionSubset)
		for op, requirementsSuper := range supersetRequirements {
			requirementsSub, exists := subsetRequirements[op]
			if !exists {
				return false, fmt.Sprintf("Operator %q is not present for key %q in provided namespaceSelector", op, key)
			}
			switch op {
			case metav1.LabelSelectorOpIn:
				if !requirementsSuper.IsSuperset(requirementsSub) {
					return false, fmt.Sprintf("Values provided for key %q and operator %q should be a subset of %v, but the provided namespaceSelector uses %v", key, op, sets.List(requirementsSuper), sets.List(requirementsSub))
				}
			case metav1.LabelSelectorOpNotIn:
				if !requirementsSub.IsSuperset(requirementsSuper) {
					return false, fmt.Sprintf("Values provided for key %q and operator %q should be a superset of %v, but the provided namespaceSelector uses %v", key, op, sets.List(requirementsSuper), sets.List(requirementsSub))
				}
			case metav1.LabelSelectorOpExists, metav1.LabelSelectorOpDoesNotExist:
				continue
			}
		}
	}
	return true, ""
}

// Validate whether the user is entitled to the ExternalIPPools selected by them.
func validateExternalIPPools(newEgress *crdv1beta1.Egress, entitlement *crdv1beta1.EgressEntitlement) (bool, string) {
	allowedExternalIPPools := sets.New[string]()
	for _, pool := range entitlement.Spec.ExternalIPPools {
		if pool == "*" {
			return true, ""
		}
		allowedExternalIPPools.Insert(pool)
	}

	if len(newEgress.Spec.ExternalIPPools) != 0 {
		if !allowedExternalIPPools.HasAll(newEgress.Spec.ExternalIPPools...) {
			return false, fmt.Sprintf("Access to the ExternalIPPools %q is not permitted by the EgressEntitlement", newEgress.Spec.ExternalIPPools)
		}
	} else if newEgress.Spec.ExternalIPPool != "" {
		if ok := allowedExternalIPPools.Has(newEgress.Spec.ExternalIPPool); !ok {
			return false, fmt.Sprintf("Access to the ExternalIPPool %q is not permitted by the EgressEntitlement", newEgress.Spec.ExternalIPPool)
		}
	}

	return true, ""
}

// Validate provided Static Egress IP should belong to the list of entitled externalIPPools.
func validateEgressIPs(newEgress *crdv1beta1.Egress, entitlement *crdv1beta1.EgressEntitlement) (bool, string) {
	allowedEgressIPs := sets.New[string]()
	for _, ip := range entitlement.Spec.EgressIPs {
		if ip == "*" {
			return true, ""
		}
		allowedEgressIPs.Insert(ip)
	}

	// Added a condition for ExternalIPPool because there may be a case that EgressIP is assigned from ExternalIPPool
	// and it will be added to the spec of the Egress but it may not be present in the entitlement so it will raise error.
	if newEgress.Spec.EgressIP != "" && newEgress.Spec.ExternalIPPool == "" && len(newEgress.Spec.ExternalIPPools) == 0 {
		if ok := allowedEgressIPs.Has(newEgress.Spec.EgressIP); !ok {
			return false, fmt.Sprintf("Static IP %s used in Egress CRD, does not belong to the list of Egress IPs for which user is entitled", newEgress.Spec.EgressIP)
		}
	}

	return true, ""
}

// Validate provided Namespaces should belong to the list of entitled Namespaces.
func validateNamespaces(oldEgress, newEgress *crdv1beta1.Egress, entitlement *crdv1beta1.EgressEntitlement) (bool, string) {
	if oldEgress.Name != "" {
		if allow, cause := validateLabelSelector(entitlement.Spec.AppliedToScope.NamespaceSelector, oldEgress.Spec.AppliedTo.NamespaceSelector); !allow {
			return allow, cause
		}
	}
	if newEgress.Name != "" {
		if allow, cause := validateLabelSelector(entitlement.Spec.AppliedToScope.NamespaceSelector, newEgress.Spec.AppliedTo.NamespaceSelector); !allow {
			return allow, cause
		}
	}
	return true, ""
}

func (c *EgressController) authorization(oldEgress, newEgress *crdv1beta1.Egress, user authenticationv1.UserInfo) (bool, string) {

	getAntreaControllerServiceAccount := func() string {
		return strings.Join([]string{
			"system", "serviceaccount", env.GetAntreaNamespace(), "antrea-controller",
		}, ":")
	}

	// antrea-controller user should have the privilege to edit all Egresses.
	antreaControllerServiceAccount := getAntreaControllerServiceAccount()
	if user.Username == antreaControllerServiceAccount {
		return true, ""
	}

	// By default cluster admins should have the privilege to edit all Egresses.
	for _, g := range user.Groups {
		// "system:masters" is the built-in super-powers / break-glass Group that can bypass RBAC.
		// "kubeadm:cluster-admins" is the default group of admin client since kubeadm 1.29.
		if g == "system:masters" || g == "kubeadm:cluster-admins" {
			return true, ""
		}
	}

	// Fetch restrictions(EgressEntitlement & EgressEntitlementBinding) imposed on the user.
	var entitlementNames []string
	// Fetch the bindings associated with the user.
	bindings, _ := c.egressEntitlementBindingInformer.Informer().GetIndexer().ByIndex(subjectUserIndex, user.Username)
	// Fetch the entitlements associated with the bindings on user.
	for _, binding := range bindings {
		bindingObj := binding.(*crdv1beta1.EgressEntitlementBinding)
		entitlementNames = append(entitlementNames, bindingObj.Spec.EgressEntitlement)
	}
	// Fetch the bindings associated with the user's groups.
	for _, g := range user.Groups {
		bindings, _ := c.egressEntitlementBindingInformer.Informer().GetIndexer().ByIndex(subjectGroupIndex, g)
		// Fetch entitlements associated with the bindings on the user group.
		for _, binding := range bindings {
			bindingObj := binding.(*crdv1beta1.EgressEntitlementBinding)
			entitlementNames = append(entitlementNames, bindingObj.Spec.EgressEntitlement)
		}
	}

	if len(entitlementNames) == 0 {
		return false, fmt.Sprintf("User %s has no associated entitlement", user.Username)
	}

	// Sort the entitlements based on their name to maintain the order in which aggregated error is displayed.
	sort.Strings(entitlementNames)
	aggregatedError := make(map[string][]string)
	for _, entitlementName := range entitlementNames {
		errorFlag := false
		entitlement, err := c.egressEntitlementLister.Get(entitlementName)
		if err != nil {
			aggregatedError[entitlementName] = append(aggregatedError[entitlementName], fmt.Sprintf("EgressEntitlement %s not found;", entitlementName))
			continue
		}
		// Validate the entitlement for workload / Namespace selection.
		// We validate old Egress object only for workload (Namespace) selection (AppliedToScope), because
		// the other two fields(EgressIP & ExternalIPPool) define which resources the user can consume and
		// there is no point in validating them again for updates / deletions, given that we already
		// allowed their consumption during the initial create or an earlier update.
		// On the other hand, we do need to make sure that the user is allowed to change existing Egress
		// policies for workloads selected by the old Egress version, hence why we need to validate
		// Namespace selection.
		if allow, reason := validateNamespaces(oldEgress, newEgress, entitlement); !allow {
			aggregatedError[entitlementName] = append(aggregatedError[entitlementName], reason)
			errorFlag = true
		}
		// Validate the entitlement for ExternalIPPools.
		if allow, reason := validateExternalIPPools(newEgress, entitlement); !allow {
			aggregatedError[entitlementName] = append(aggregatedError[entitlementName], reason)
			errorFlag = true
		}
		// Validate the Entitlement for EgressIPs.
		if allow, reason := validateEgressIPs(newEgress, entitlement); !allow {
			aggregatedError[entitlementName] = append(aggregatedError[entitlementName], reason)
			errorFlag = true
		}
		if !errorFlag {
			return true, ""
		}
	}

	result := "None of the user entitlements allow the creation / update / deletion of this Egress resource for the following reasons:\n"
	for _, entitlement := range entitlementNames {
		errors := "[" + strings.Join(aggregatedError[entitlement], "; ") + "]"
		result += fmt.Sprintf("Entitlement %q: %v\n", entitlement, errors)
	}
	return false, result
}

func (c *EgressController) ValidateEgress(review *admv1.AdmissionReview) *admv1.AdmissionResponse {
	var result *metav1.Status
	var msg string
	allowed := true
	ui := review.Request.UserInfo
	egressRBAC := features.DefaultFeatureGate.Enabled(features.EgressRBAC) && c.isEnterpriseAntrea

	klog.V(2).Info("Validating Egress", "request", review.Request)
	var newObj, oldObj crdv1beta1.Egress
	if review.Request.Object.Raw != nil {
		if err := json.Unmarshal(review.Request.Object.Raw, &newObj); err != nil {
			klog.ErrorS(err, "Error de-serializing current Egress")
			return newAdmissionResponseForErr(err)
		}
	}
	if review.Request.OldObject.Raw != nil {
		if err := json.Unmarshal(review.Request.OldObject.Raw, &oldObj); err != nil {
			klog.ErrorS(err, "Error de-serializing old Egress")
			return newAdmissionResponseForErr(err)
		}
	}

	shouldAllow := func(oldEgress, newEgress *crdv1beta1.Egress) (bool, string) {
		// Validate Egress trafficShaping
		if newEgress.Spec.Bandwidth != nil {
			_, err := resource.ParseQuantity(newEgress.Spec.Bandwidth.Rate)
			if err != nil {
				return false, fmt.Sprintf("Rate %s in Egress %s is invalid: %v", newEgress.Spec.Bandwidth.Rate, newEgress.Name, err)
			}
			_, err = resource.ParseQuantity(newEgress.Spec.Bandwidth.Burst)
			if err != nil {
				return false, fmt.Sprintf("Burst %s in Egress %s is invalid: %v", newEgress.Spec.Bandwidth.Burst, newEgress.Name, err)
			}
		}
		// Allow it if EgressIP and ExternalIPPool don't change.
		if newEgress.Spec.EgressIP == oldEgress.Spec.EgressIP &&
			newEgress.Spec.ExternalIPPool == oldEgress.Spec.ExternalIPPool &&
			reflect.DeepEqual(newEgress.Spec.EgressIPs, oldEgress.Spec.EgressIPs) &&
			reflect.DeepEqual(newEgress.Spec.ExternalIPPools, oldEgress.Spec.ExternalIPPools) {
			return true, ""
		}
		checkIPAndPool := func(ipStr, pool string) (bool, string) {
			ip := net.ParseIP(ipStr)
			if ip == nil {
				return false, fmt.Sprintf("IP %s is not valid", ipStr)
			}
			if !c.externalIPAllocator.IPPoolExists(pool) {
				return false, fmt.Sprintf("ExternalIPPool %s does not exist", pool)
			}
			if !c.externalIPAllocator.IPPoolHasIP(pool, ip) {
				return false, fmt.Sprintf("IP %s is not within the IP range of ExternalIPPool %s", ipStr, pool)
			}
			return true, ""
		}
		singleEgressIP := newEgress.Spec.EgressIP != "" || newEgress.Spec.ExternalIPPool != ""
		if singleEgressIP {
			// Only validate whether the specified Egress IP is in the Pool when they are both set.
			if newEgress.Spec.EgressIP == "" || newEgress.Spec.ExternalIPPool == "" {
				return true, ""
			}
			if allowed, message := checkIPAndPool(newEgress.Spec.EgressIP, newEgress.Spec.ExternalIPPool); !allowed {
				return false, message
			}
		} else {
			if len(newEgress.Spec.ExternalIPPools) == 0 {
				return false, fmt.Sprintf("EgressIP, ExternalIPPool, and ExternalIPPools must not be empty at the same time")
			}
			if len(newEgress.Spec.EgressIPs) > len(newEgress.Spec.ExternalIPPools) {
				return false, fmt.Sprintf("The count of EgressIPs %d must not be greater than the count of ExternalIPPools %d", len(newEgress.Spec.EgressIPs), len(newEgress.Spec.ExternalIPPools))
			}
			visitedPools := sets.NewString()
			for i, pool := range newEgress.Spec.ExternalIPPools {
				if pool == "" {
					return false, fmt.Sprintf("The items of ExternalIPPools must not be empty")
				}
				if visitedPools.Has(pool) {
					return false, fmt.Sprintf("The items of ExternalIPPools must be unique")
				}
				visitedPools.Insert(pool)
				if len(newEgress.Spec.EgressIPs) <= i {
					continue
				}
				ipStr := newEgress.Spec.EgressIPs[i]
				// Allow empty IP in EgressIPs as IP allocation may fail for some pools but succeed for other pools.
				if ipStr == "" {
					continue
				}
				if allowed, message := checkIPAndPool(ipStr, pool); !allowed {
					return false, message
				}
			}
		}
		return true, ""
	}

	switch review.Request.Operation {
	case admv1.Create:
		klog.V(2).Info("Validating CREATE request for Egress")
		if egressRBAC {
			if authorized, reason := c.authorization(&oldObj, &newObj, ui); !authorized {
				allowed = false
				msg = reason
				break
			}
		}
		allowed, msg = shouldAllow(&oldObj, &newObj)
	case admv1.Update:
		klog.V(2).Info("Validating UPDATE request for Egress")
		if egressRBAC {
			if authorized, reason := c.authorization(&oldObj, &newObj, ui); !authorized {
				allowed = false
				msg = reason
				break
			}
		}
		allowed, msg = shouldAllow(&oldObj, &newObj)
	case admv1.Delete:
		klog.V(2).Info("Validating DELETE request for Egress")
		if egressRBAC {
			if authorized, reason := c.authorization(&oldObj, &newObj, ui); !authorized {
				allowed = false
				msg = reason
			}
		}
	}

	if msg != "" {
		result = &metav1.Status{
			Message: msg,
		}
	}
	return &admv1.AdmissionResponse{
		Allowed: allowed,
		Result:  result,
	}
}

func newAdmissionResponseForErr(err error) *admv1.AdmissionResponse {
	return &admv1.AdmissionResponse{
		Result: &metav1.Status{
			Message: err.Error(),
		},
	}
}
