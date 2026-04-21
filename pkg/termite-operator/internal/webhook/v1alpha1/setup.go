package v1alpha1

import (
	antflyaiv1alpha1 "github.com/antflydb/antfly/pkg/termite-operator/api/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
)

// SetupWithManager registers all admission webhooks with the manager.
func SetupWithManager(mgr ctrl.Manager) error {
	if err := builder.WebhookManagedBy(mgr, &antflyaiv1alpha1.TermitePool{}).
		WithValidator(&TermitePoolValidator{}).
		Complete(); err != nil {
		return err
	}

	if err := builder.WebhookManagedBy(mgr, &antflyaiv1alpha1.TermiteRoute{}).
		WithValidator(&TermiteRouteValidator{}).
		Complete(); err != nil {
		return err
	}

	return nil
}
