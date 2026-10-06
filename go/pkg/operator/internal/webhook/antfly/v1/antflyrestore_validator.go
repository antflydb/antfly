// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
