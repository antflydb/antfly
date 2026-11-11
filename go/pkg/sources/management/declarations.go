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

package management

import "context"

// DeclarationBackend optionally serves configuration without querying native
// readiness. It must apply the same scope, authorization and private-field
// projection as GetSource, and include revision and reconciliation state.
type DeclarationBackend interface {
	GetSourceDeclaration(context.Context, string) (Source, error)
}

// ReservedSourceConfigFields returns the contract-owned fields that cannot be
// supplied in opaque connector config. Each call returns an independent list.
func ReservedSourceConfigFields() []string {
	return []string{"permissions", "permission_ttl_seconds", "publication", "streams", "search_version", "artifacts", "processing", "search_status", "knowledge", "knowledge_status"}
}
