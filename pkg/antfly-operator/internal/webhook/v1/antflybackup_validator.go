package v1

import (
	"context"

	antflyv1 "github.com/antflydb/antfly/pkg/antfly-operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// AntflyBackupValidator implements admission.Validator for AntflyBackup.
type AntflyBackupValidator struct{}

var _ admission.Validator[*antflyv1.AntflyBackup] = &AntflyBackupValidator{}

func (v *AntflyBackupValidator) ValidateCreate(ctx context.Context, obj *antflyv1.AntflyBackup) (admission.Warnings, error) {
	return nil, obj.ValidateAntflyBackup()
}

func (v *AntflyBackupValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *antflyv1.AntflyBackup) (admission.Warnings, error) {
	if err := newObj.ValidateBackupImmutability(oldObj); err != nil {
		return nil, err
	}
	return nil, newObj.ValidateAntflyBackup()
}

func (v *AntflyBackupValidator) ValidateDelete(ctx context.Context, obj *antflyv1.AntflyBackup) (admission.Warnings, error) {
	return nil, nil
}
