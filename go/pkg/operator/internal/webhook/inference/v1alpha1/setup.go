package v1alpha1

import (
	antflyaiv1alpha1 "github.com/antflydb/antfly/go/pkg/operator/api/inference/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
)

// SetupWithManager registers all admission webhooks with the manager.
func SetupWithManager(mgr ctrl.Manager) error {
	if err := builder.WebhookManagedBy(mgr, &antflyaiv1alpha1.InferencePool{}).
		WithValidator(&InferencePoolValidator{}).
		Complete(); err != nil {
		return err
	}

	if err := builder.WebhookManagedBy(mgr, &antflyaiv1alpha1.InferenceProxy{}).
		WithValidator(&InferenceProxyValidator{}).
		Complete(); err != nil {
		return err
	}

	return nil
}
