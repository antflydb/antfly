package v1

import (
	"context"
	"reflect"
	"strings"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// AntflyRestoreValidator implements admission.Validator for AntflyRestore.
type AntflyRestoreValidator struct{}

var _ admission.Validator[*antflyv1.AntflyRestore] = &AntflyRestoreValidator{}

func (v *AntflyRestoreValidator) ValidateCreate(ctx context.Context, obj *antflyv1.AntflyRestore) (admission.Warnings, error) {
	return nil, obj.ValidateCreate()
}

func (v *AntflyRestoreValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *antflyv1.AntflyRestore) (admission.Warnings, error) {
	if strings.TrimSpace(newObj.Spec.Source.Connection) == "" &&
		strings.TrimSpace(oldObj.Spec.Source.Connection) == "" &&
		reflect.DeepEqual(newObj.Spec, oldObj.Spec) {
		// Status subresource writes for pre-upgrade objects must remain valid so
		// the controller can publish the migration condition.
		return admission.Warnings{"legacy AntflyRestore is pending until spec.source.connection is configured"}, nil
	}
	if err := newObj.ValidateRestoreUpdate(oldObj); err != nil {
		return nil, err
	}
	if strings.TrimSpace(newObj.Spec.Source.Connection) == "" {
		return admission.Warnings{"legacy AntflyRestore is pending until spec.source.connection is configured"}, nil
	}
	return nil, nil
}

func (v *AntflyRestoreValidator) ValidateDelete(ctx context.Context, obj *antflyv1.AntflyRestore) (admission.Warnings, error) {
	return nil, nil
}
