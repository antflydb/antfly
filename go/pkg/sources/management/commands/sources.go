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

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

func Capabilities(ctx context.Context, env Env) (management.Capabilities, error) {
	return management.Capabilities{}, errors.New("not implemented")
}

type ListOptions struct {
	Limit  int
	Cursor string
	All    bool
}

func ListSources(ctx context.Context, env Env, opts ListOptions) (management.SourcePage, error) {
	return management.SourcePage{}, errors.New("not implemented")
}

func ResolveSource(ctx context.Context, env Env, ref string) (management.Source, error) {
	return management.Source{}, errors.New("not implemented")
}

type ReadinessSummary struct {
	Reported    bool           `json:"reported"`
	Consistency string         `json:"consistency,omitempty"`
	ObservedAt  int            `json:"observed_at,omitempty"`
	Streams     map[string]int `json:"streams,omitempty"`
	Artifacts   map[string]int `json:"artifacts,omitempty"`
}

type StatusView struct {
	Source    management.Source `json:"source"`
	LatestRun *management.Run   `json:"latest_run,omitempty"`
	Readiness ReadinessSummary  `json:"readiness"`
}

func Status(ctx context.Context, env Env, ref string) (StatusView, error) {
	return StatusView{}, errors.New("not implemented")
}
