package v1

import (
	antflyv1 "github.com/antflydb/antfly/pkg/operator/api/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
)

// SetupWithManager registers all admission webhooks with the manager.
func SetupWithManager(mgr ctrl.Manager) error {
	if err := builder.WebhookManagedBy(mgr, &antflyv1.AntflyCluster{}).
		WithValidator(&AntflyClusterValidator{}).
		Complete(); err != nil {
		return err
	}

	if err := builder.WebhookManagedBy(mgr, &antflyv1.AntflyBackup{}).
		WithValidator(&AntflyBackupValidator{}).
		Complete(); err != nil {
		return err
	}

	if err := builder.WebhookManagedBy(mgr, &antflyv1.AntflyRestore{}).
		WithValidator(&AntflyRestoreValidator{}).
		Complete(); err != nil {
		return err
	}

	return nil
}
