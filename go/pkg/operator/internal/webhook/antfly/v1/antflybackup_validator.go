package v1

import (
	"context"
	"strings"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// AntflyBackupValidator implements admission.Validator for AntflyBackup.
type AntflyBackupValidator struct{}

var _ admission.Validator[*antflyv1.AntflyBackup] = &AntflyBackupValidator{}

func (v *AntflyBackupValidator) ValidateCreate(ctx context.Context, obj *antflyv1.AntflyBackup) (admission.Warnings, error) {
	return nil, obj.ValidateCreate()
}

func (v *AntflyBackupValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *antflyv1.AntflyBackup) (admission.Warnings, error) {
	if err := newObj.ValidateUpdate(oldObj); err != nil {
		return nil, err
	}
	if strings.TrimSpace(newObj.Spec.Destination.Connection) == "" {
		return admission.Warnings{"legacy AntflyBackup is suspended until spec.destination.connection is configured"}, nil
	}
	return nil, nil
}

func (v *AntflyBackupValidator) ValidateDelete(ctx context.Context, obj *antflyv1.AntflyBackup) (admission.Warnings, error) {
	return nil, nil
}
