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
