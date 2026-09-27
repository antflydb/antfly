// Copyright 2026 Antfly, Inc.
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
	"context"

	antflyaiv1alpha1 "github.com/antflydb/antfly/go/pkg/operator/api/inference/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// InferenceProxyValidator implements admission.Validator for InferenceProxy.
type InferenceProxyValidator struct{}

var _ admission.Validator[*antflyaiv1alpha1.InferenceProxy] = &InferenceProxyValidator{}

func (v *InferenceProxyValidator) ValidateCreate(ctx context.Context, obj *antflyaiv1alpha1.InferenceProxy) (admission.Warnings, error) {
	return nil, obj.ValidateInferenceProxy()
}

func (v *InferenceProxyValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *antflyaiv1alpha1.InferenceProxy) (admission.Warnings, error) {
	return nil, newObj.ValidateInferenceProxy()
}

func (v *InferenceProxyValidator) ValidateDelete(ctx context.Context, obj *antflyaiv1alpha1.InferenceProxy) (admission.Warnings, error) {
	return nil, nil
}
