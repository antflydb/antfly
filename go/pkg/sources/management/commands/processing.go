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

package commands

import (
	"context"
	"errors"
)

type RepresentationView struct {
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	Declared bool   `json:"declared"`
	State    string `json:"state"`
	Reason   string `json:"reason,omitempty"`
}

type ProcessingView struct {
	SourceID        string               `json:"source_id"`
	SearchVersion   int                  `json:"search_version"`
	Reported        bool                 `json:"reported"`
	Consistency     string               `json:"consistency,omitempty"`
	ObservedAt      int                  `json:"observed_at,omitempty"`
	Indexes         []string             `json:"indexes"`
	Representations []RepresentationView `json:"representations"`
}

func ProcessingStatus(ctx context.Context, env Env, ref string) (ProcessingView, error) {
	return ProcessingView{}, errors.New("not implemented")
}

type ArtifactView struct {
	Name        string `json:"name"`
	Artifact    string `json:"artifact"`
	ContentType string `json:"content_type"`
	Purpose     string `json:"purpose,omitempty"`
	Generated   bool   `json:"generated"`
	State       string `json:"state"`
	Reason      string `json:"reason,omitempty"`
}

func ListArtifacts(ctx context.Context, env Env, ref string) ([]ArtifactView, error) {
	return nil, errors.New("not implemented")
}
