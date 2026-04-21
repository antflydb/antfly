package v1alpha1

import (
	"context"

	antflyaiv1alpha1 "github.com/antflydb/antfly/pkg/termite-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// TermitePoolValidator implements admission.Validator for TermitePool.
type TermitePoolValidator struct{}

var _ admission.Validator[*antflyaiv1alpha1.TermitePool] = &TermitePoolValidator{}

func (v *TermitePoolValidator) ValidateCreate(ctx context.Context, obj *antflyaiv1alpha1.TermitePool) (admission.Warnings, error) {
	return nil, obj.ValidateTermitePool()
}

func (v *TermitePoolValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *antflyaiv1alpha1.TermitePool) (admission.Warnings, error) {
	if err := newObj.ValidateImmutability(oldObj); err != nil {
		return nil, err
	}
	return nil, newObj.ValidateTermitePool()
}

func (v *TermitePoolValidator) ValidateDelete(ctx context.Context, obj *antflyaiv1alpha1.TermitePool) (admission.Warnings, error) {
	return nil, nil
}
