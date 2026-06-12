package v1alpha1

import (
	"context"

	antflyaiv1alpha1 "github.com/antflydb/antfly/go/pkg/operator/api/inference/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// InferencePoolValidator implements admission.Validator for InferencePool.
type InferencePoolValidator struct{}

var _ admission.Validator[*antflyaiv1alpha1.InferencePool] = &InferencePoolValidator{}

func (v *InferencePoolValidator) ValidateCreate(ctx context.Context, obj *antflyaiv1alpha1.InferencePool) (admission.Warnings, error) {
	return nil, obj.ValidateInferencePool()
}

func (v *InferencePoolValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *antflyaiv1alpha1.InferencePool) (admission.Warnings, error) {
	if err := newObj.ValidateImmutability(oldObj); err != nil {
		return nil, err
	}
	return nil, newObj.ValidateInferencePool()
}

func (v *InferencePoolValidator) ValidateDelete(ctx context.Context, obj *antflyaiv1alpha1.InferencePool) (admission.Warnings, error) {
	return nil, nil
}
