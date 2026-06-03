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
