package v1

import (
	"context"
	"fmt"

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// AntflyClusterValidator implements admission.Validator for AntflyCluster.
type AntflyClusterValidator struct {
	EnableHotStandbyHA bool
}

var _ admission.Validator[*antflyv1.AntflyCluster] = &AntflyClusterValidator{}

func (v *AntflyClusterValidator) ValidateCreate(ctx context.Context, obj *antflyv1.AntflyCluster) (admission.Warnings, error) {
	if err := v.validateHotStandbyFeatureGate(obj); err != nil {
		return nil, err
	}
	return nil, obj.ValidateAntflyCluster()
}

func (v *AntflyClusterValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *antflyv1.AntflyCluster) (admission.Warnings, error) {
	if err := newObj.ValidateImmutability(oldObj); err != nil {
		return nil, err
	}
	if err := v.validateHotStandbyFeatureGate(newObj); err != nil {
		return nil, err
	}
	return nil, newObj.ValidateAntflyCluster()
}

func (v *AntflyClusterValidator) ValidateDelete(ctx context.Context, obj *antflyv1.AntflyCluster) (admission.Warnings, error) {
	return nil, nil
}

func (v *AntflyClusterValidator) validateHotStandbyFeatureGate(obj *antflyv1.AntflyCluster) error {
	if v.EnableHotStandbyHA || !hotStandbyHARequested(obj) {
		return nil
	}
	return fmt.Errorf("hot-standby HA is disabled by the operator feature gate; restart the operator with --enable-hot-standby-ha=true to admit spec.highAvailability.mode=HotStandby")
}

func hotStandbyHARequested(obj *antflyv1.AntflyCluster) bool {
	return obj != nil && obj.Spec.HighAvailability != nil &&
		obj.Spec.HighAvailability.Mode == antflyv1.HAModeHotStandby
}
