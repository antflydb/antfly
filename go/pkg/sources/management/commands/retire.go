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
	"fmt"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// Archive retires a source while retaining its indexed content.
func Archive(ctx context.Context, env Env, ref string) (management.Mutation, error) {
	source, err := ResolveSource(ctx, env, ref)
	if err != nil {
		return management.Mutation{}, err
	}
	mutation, err := env.API.RemoveSource(ctx, source.Id, false)
	if err != nil {
		return mutation, Classify(err)
	}
	if env.JSON {
		return mutation, Classify(env.writeJSON(mutation))
	}
	return mutation, Classify(env.writeHuman(fmt.Sprintf(
		"Archived source %s (%s); state %s.\nNew runs and media reads are blocked. Indexed content is retained; purge it explicitly with purge.\n",
		source.Name, source.Id, mutation.State)))
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
// If submission fails, the recorded intent remains available for retry.
func Purge(ctx context.Context, env Env, ref string, opts PurgeOptions) (PurgeReceipt, error) {
	if !opts.Confirm {
		return PurgeReceipt{}, &Error{Code: CodeInvalid, Message: "purge permanently deletes this source's indexed records; confirm explicitly to continue"}
	}
	if opts.IdempotencyKey != "" && !sourceID(opts.IdempotencyKey) {
		return PurgeReceipt{}, &Error{Code: CodeInvalid, Message: "idempotency key must match [A-Za-z0-9_-]{1,128}"}
	}
	if err := requireCapability(ctx, env, "purge_sources", "per_source_runs"); err != nil {
		return PurgeReceipt{}, err
	}
	source, err := ResolveSource(ctx, env, ref)
	if err != nil {
		return PurgeReceipt{}, err
	}
	key := opts.IdempotencyKey
	if key == "" {
		if key, err = newIdempotencyKey(); err != nil {
			return PurgeReceipt{}, Classify(err)
		}
	}
	mutation, err := env.API.RemoveSource(ctx, source.Id, true)
	receipt := PurgeReceipt{Mutation: mutation, IdempotencyKey: key}
	if err != nil {
		return receipt, Classify(err)
	}
	receipt.Run, err = env.API.StartRun(ctx, source.Id, key, management.RunInput{Operation: management.RunInputOperationPurge})
	if err != nil {
		err = Classify(err)
		failure := Error{Code: CodeFailed}
		var classified *Error
		if errors.As(err, &classified) {
			failure = *classified
		}
		failure.Message = fmt.Sprintf("purge intent is recorded for %s; purge can be retried with the same command: %v", source.Id, err)
		failure.err = err
		return receipt, &failure
	}
	if env.JSON {
		return receipt, Classify(env.writeJSON(receipt))
	}
	return receipt, Classify(env.writeHuman(fmt.Sprintf(
		"Purge intent recorded for %s (%s); purge run %s %s.\nIdempotency key: %s\n",
		source.Name, source.Id, receipt.Run.Id, receipt.Run.State, key)))
}
