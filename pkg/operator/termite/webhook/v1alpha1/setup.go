package v1alpha1

import (
	internalwebhook "github.com/antflydb/antfly/pkg/operator/termite/internal/webhook/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
)

// SetupWithManager registers Termite admission webhooks with the manager.
func SetupWithManager(mgr ctrl.Manager) error {
	return internalwebhook.SetupWithManager(mgr)
}
