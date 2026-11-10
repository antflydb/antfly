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

// Archive retires a source while retaining its indexed content.
func Archive(ctx context.Context, env Env, ref string) (management.Mutation, error) {
	return management.Mutation{}, errors.New("not implemented")
}

// PurgeOptions requires explicit confirmation and controls duplicate prevention.
type PurgeOptions struct {
	Confirm        bool
	IdempotencyKey string // Empty generates a fresh key.
}

// PurgeReceipt records the retirement intent and the submitted purge run.
type PurgeReceipt struct {
	Mutation       management.Mutation `json:"mutation"`
	Run            management.Run      `json:"run"`
	IdempotencyKey string              `json:"idempotency_key"`
}

// Purge records a purge intent and submits deletion of source-owned records.
func Purge(ctx context.Context, env Env, ref string, opts PurgeOptions) (PurgeReceipt, error) {
	return PurgeReceipt{}, errors.New("not implemented")
}
