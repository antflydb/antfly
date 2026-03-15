package v1

import (
	"context"

	antflyv1 "github.com/antflydb/antfly/pkg/operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// AntflyRestoreValidator implements admission.Validator for AntflyRestore.
type AntflyRestoreValidator struct{}

var _ admission.Validator[*antflyv1.AntflyRestore] = &AntflyRestoreValidator{}

func (v *AntflyRestoreValidator) ValidateCreate(ctx context.Context, obj *antflyv1.AntflyRestore) (admission.Warnings, error) {
	return nil, obj.ValidateAntflyRestore()
}

func (v *AntflyRestoreValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *antflyv1.AntflyRestore) (admission.Warnings, error) {
	return nil, newObj.ValidateRestoreUpdate(oldObj)
}

func (v *AntflyRestoreValidator) ValidateDelete(ctx context.Context, obj *antflyv1.AntflyRestore) (admission.Warnings, error) {
	return nil, nil
}
