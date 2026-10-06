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

	antflyv1 "github.com/antflydb/antfly/go/pkg/operator/api/antfly/v1"
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
