// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1

import (
	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
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
