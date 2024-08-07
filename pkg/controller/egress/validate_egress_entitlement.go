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
	"encoding/json"
	"fmt"

	admv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	crdv1beta1 "antrea.io/antrea/pkg/apis/crd/v1beta1"
)

func (c *EgressController) ValidateEgressEntitlement(review *admv1.AdmissionReview) *admv1.AdmissionResponse {
	var result *metav1.Status
	var msg string
	allowed := true

	klog.V(2).InfoS("Validating EgressEntitlement", "request", review.Request)
	var oldObj crdv1beta1.EgressEntitlement
	if review.Request.OldObject.Raw != nil {
		if err := json.Unmarshal(review.Request.OldObject.Raw, &oldObj); err != nil {
			klog.ErrorS(err, "Error de-serializing old EgressEntitlement")
			return newAdmissionResponseForErr(err)
		}
	}

	shouldAllow := func(oldEgressEntitlement *crdv1beta1.EgressEntitlement) (bool, string) {
		if oldEgressEntitlement.Name == systemGeneratedEntitlement.Name {
			return false, fmt.Sprintf("You are not allowed to delete the default egress entitlement %s", oldEgressEntitlement.Name)
		}
		return true, ""
	}

	switch review.Request.Operation {
	case admv1.Delete:
		klog.V(2).InfoS("Validating DELETE request for EgressEntitlement", "egressEntitlement", oldObj.Name)
		if authorized, reason := shouldAllow(&oldObj); !authorized {
			allowed = false
			msg = reason
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
