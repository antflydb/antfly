package v1alpha1

import (
	"context"

	antflyaiv1alpha1 "github.com/antflydb/antfly/pkg/termite-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// TermiteRouteValidator implements admission.Validator for TermiteRoute.
type TermiteRouteValidator struct{}

var _ admission.Validator[*antflyaiv1alpha1.TermiteRoute] = &TermiteRouteValidator{}

func (v *TermiteRouteValidator) ValidateCreate(ctx context.Context, obj *antflyaiv1alpha1.TermiteRoute) (admission.Warnings, error) {
	return nil, obj.ValidateTermiteRoute()
}

func (v *TermiteRouteValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *antflyaiv1alpha1.TermiteRoute) (admission.Warnings, error) {
	return nil, newObj.ValidateTermiteRoute()
}

func (v *TermiteRouteValidator) ValidateDelete(ctx context.Context, obj *antflyaiv1alpha1.TermiteRoute) (admission.Warnings, error) {
	return nil, nil
}
