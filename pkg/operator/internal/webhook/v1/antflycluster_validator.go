package v1

import (
	"context"

	antflyv1 "github.com/antflydb/antfly/pkg/operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// AntflyClusterValidator implements admission.Validator for AntflyCluster.
type AntflyClusterValidator struct{}

var _ admission.Validator[*antflyv1.AntflyCluster] = &AntflyClusterValidator{}

func (v *AntflyClusterValidator) ValidateCreate(ctx context.Context, obj *antflyv1.AntflyCluster) (admission.Warnings, error) {
	return nil, obj.ValidateAntflyCluster()
}

func (v *AntflyClusterValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *antflyv1.AntflyCluster) (admission.Warnings, error) {
	if err := newObj.ValidateImmutability(oldObj); err != nil {
		return nil, err
	}
	return nil, newObj.ValidateAntflyCluster()
}

func (v *AntflyClusterValidator) ValidateDelete(ctx context.Context, obj *antflyv1.AntflyCluster) (admission.Warnings, error) {
	return nil, nil
}
