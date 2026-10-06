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

package antflyevalaf

import (
	"github.com/antflydb/antfly/go/pkg/evalaf/rag"
)

// NewCitationEvaluator creates a citation evaluator configured for Antfly's format.
// Antfly uses the format: [resource_id 0] or [resource_id 0, 1, 2]
func NewCitationEvaluator(name string) *rag.CitationEvaluator {
	if name == "" {
		name = "antfly_citation"
	}
	return rag.NewCitationEvaluator(name)
}

// NewCitationCoverageEvaluator creates a citation coverage evaluator for Antfly.
func NewCitationCoverageEvaluator(name string) *rag.CitationCoverageEvaluator {
	if name == "" {
		name = "antfly_citation_coverage"
	}
	return rag.NewCitationCoverageEvaluator(name)
}
