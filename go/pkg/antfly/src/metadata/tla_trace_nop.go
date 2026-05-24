// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//go:build !with_tla

package metadata

import (
	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/google/uuid"
)

// traceCheckPredicates is a no-op without the with_tla build tag.
func (ms *MetadataStore) traceCheckPredicates(_ uuid.UUID, _ map[types.ID]struct{}) {}
