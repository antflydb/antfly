package v1

import (
	internalwebhook "github.com/antflydb/antfly/pkg/operator/antfly/internal/webhook/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

// SetupWithManager registers Antfly admission webhooks with the manager.
func SetupWithManager(mgr ctrl.Manager) error {
	return internalwebhook.SetupWithManager(mgr)
}
