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
	"io"
	"time"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

type API interface {
	Capabilities(context.Context) (management.Capabilities, error)
	ListSources(ctx context.Context, limit int, cursor string) (management.SourcePage, error)
	GetSource(ctx context.Context, id string) (management.Source, error)
	PutSource(ctx context.Context, s management.Source) (management.Source, error)
	RemoveSource(ctx context.Context, id string, purge bool) (management.Mutation, error)
	ListRuns(ctx context.Context, id string, limit int, cursor string) (management.RunPage, error)
	GetRun(ctx context.Context, source, id string) (management.Run, error)
	StartRun(ctx context.Context, id, key string, in management.RunInput) (management.Run, error)
	CancelRun(ctx context.Context, source, id string) (management.Run, error)
}

var _ API = (*management.Client)(nil)

type Env struct {
	API   API
	Out   io.Writer
	JSON  bool
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
}
