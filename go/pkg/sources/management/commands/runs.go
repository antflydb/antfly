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
	"time"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

type RunOptions struct {
	Restart        bool
	IdempotencyKey string
}

type RunReceipt struct {
	Run            management.Run `json:"run"`
	IdempotencyKey string         `json:"idempotency_key"`
}

type WaitOptions struct {
	Timeout     time.Duration
	Interval    time.Duration
	MaxInterval time.Duration
}

func StartRun(ctx context.Context, env Env, ref string, opts RunOptions) (RunReceipt, error) {
	return RunReceipt{}, errors.New("not implemented")
}

func Wait(ctx context.Context, env Env, sourceID, runID string, opts WaitOptions) (management.Run, error) {
	return management.Run{}, errors.New("not implemented")
}

func ListRuns(ctx context.Context, env Env, ref string, opts ListOptions) (management.RunPage, error) {
	return management.RunPage{}, errors.New("not implemented")
}

func InspectRun(ctx context.Context, env Env, ref, runID string) (management.Run, error) {
	return management.Run{}, errors.New("not implemented")
}

func CancelRun(ctx context.Context, env Env, ref, runID string) (management.Run, error) {
	return management.Run{}, errors.New("not implemented")
}
