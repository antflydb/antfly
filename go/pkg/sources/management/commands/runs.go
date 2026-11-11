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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// RunOptions controls acquisition submission and duplicate prevention.
type RunOptions struct {
	Restart        bool
	IdempotencyKey string // Empty generates a fresh key.
}

// RunReceipt includes the key needed to safely repeat the same submission.
type RunReceipt struct {
	Run            management.Run `json:"run"`
	IdempotencyKey string         `json:"idempotency_key"`
}

// WaitOptions bounds watching without affecting server-side execution.
type WaitOptions struct {
	Timeout     time.Duration // Zero defaults to 30 minutes.
	Interval    time.Duration // Zero defaults to 2 seconds.
	MaxInterval time.Duration // Zero defaults to 15 seconds.
}

// StartRun submits once; the receipt and any submission error carry the retry key.
func StartRun(ctx context.Context, env Env, ref string, opts RunOptions) (RunReceipt, error) {
	if opts.IdempotencyKey != "" && !sourceID(opts.IdempotencyKey) {
		return RunReceipt{}, &Error{Code: CodeInvalid, Message: "idempotency key must match [A-Za-z0-9_-]{1,128}"}
	}
	if err := requireCapability(ctx, env, "per_source_runs"); err != nil {
		return RunReceipt{}, err
	}
	source, err := ResolveSource(ctx, env, ref)
	if err != nil {
		return RunReceipt{}, err
	}
	key := opts.IdempotencyKey
	if key == "" {
		if key, err = newIdempotencyKey(); err != nil {
			return RunReceipt{}, Classify(err)
		}
	}
	run, err := env.API.StartRun(ctx, source.Id, key, management.RunInput{Restart: opts.Restart})
	receipt := RunReceipt{Run: run, IdempotencyKey: key}
	if err != nil {
		return receipt, submissionError(err, key)
	}
	if env.JSON {
		return receipt, Classify(env.writeJSON(receipt))
	}
	return receipt, Classify(env.writeHuman(fmt.Sprintf(
		"Run %s %s for source %s (%s).\nIdempotency key: %s (reuse it to retry this submission without starting a duplicate run)\n",
		run.Id, run.State, source.Name, source.Id, key)))
}

// Wait polls acquisition progress; stopping the watch never cancels the run.
func Wait(ctx context.Context, env Env, sourceID, runID string, opts WaitOptions) (last management.Run, err error) {
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Minute
	}
	if opts.Interval == 0 {
		opts.Interval = 2 * time.Second
	}
	if opts.MaxInterval == 0 {
		opts.MaxInterval = 15 * time.Second
	}
	now := env.Now
	if now == nil {
		now = time.Now
	}
	sleep := env.Sleep
	if sleep == nil {
		sleep = sleepContext
	}
	deadline := now().Add(opts.Timeout)
	pollCtx, stop := context.WithTimeout(ctx, opts.Timeout)
	defer stop()
	observed := false
	defer func() {
		if env.JSON && observed {
			if writeErr := env.writeJSON(last); writeErr != nil && err == nil {
				err = Classify(writeErr)
			}
		}
	}()
	interrupted := func() error {
		return &Error{Code: CodeInterrupted, Message: fmt.Sprintf("stopped watching; run %s continues on the server", runID)}
	}
	interval := min(opts.Interval, opts.MaxInterval)
	for {
		if ctx.Err() != nil {
			return last, interrupted()
		}
		if !now().Before(deadline) {
			state := ""
			if observed {
				state = fmt.Sprintf(" is still %s and", last.State)
			}
			return last, &Error{Code: CodeTimeout, Message: fmt.Sprintf(
				"stopped waiting after %s; run %s%s continues on the server", opts.Timeout, runID, state)}
		}
		run, pollErr := env.API.GetRun(pollCtx, sourceID, runID)
		if ctx.Err() != nil {
			return last, interrupted()
		}
		if pollErr != nil {
			if pollErr = Classify(pollErr); !transientPollError(pollErr) {
				return last, pollErr
			}
		} else {
			changed := !observed || run.State != last.State
			last, observed = run, true
			if !env.JSON && changed {
				if err := env.writeHuman(fmt.Sprintf("Run %s: %s\n", runID, run.State)); err != nil {
					return last, Classify(err)
				}
			}
			switch run.State {
			case "completed":
				if !env.JSON {
					summary := fmt.Sprintf("Acquisition completed: outcome %s", run.Outcome)
					if !run.Authoritative {
						summary += "; not an authoritative publication"
					}
					if err := env.writeHuman(summary + "\nSearch readiness is reported separately; see source status.\n"); err != nil {
						return last, Classify(err)
					}
				}
				return last, nil
			case "failed", "cancelled", "interrupted":
				message := fmt.Sprintf("run %s ended %s", runID, run.State)
				if run.Code != "" {
					message += " (" + run.Code + ")"
				}
				return last, &Error{Code: CodeRunFailed, Message: message}
			}
		}
		remaining := deadline.Sub(now())
		if remaining <= 0 {
			continue
		}
		if err := sleep(ctx, min(interval, remaining)); err != nil {
			return last, interrupted()
		}
		interval = min(interval*2, opts.MaxInterval)
	}
}

// transientPollError reports whether a classified poll failure is worth
// retrying: transport errors, timeouts, and server-side (5xx) failures.
func transientPollError(err error) bool {
	var failure *Error
	if !errors.As(err, &failure) {
		return false
	}
	switch failure.Code {
	case CodeTransport, CodeTimeout:
		return true
	case CodeFailed:
		return failure.HTTPStatus >= 500
	default:
		return false
	}
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// ListRuns prints one page or a bounded traversal of a source's acquisitions.
func ListRuns(ctx context.Context, env Env, ref string, opts ListOptions) (management.RunPage, error) {
	source, err := ResolveSource(ctx, env, ref)
	if err != nil {
		return management.RunPage{}, err
	}
	data, cursor, err := listPages(ctx, opts, "run", func(ctx context.Context, limit int, cursor string) ([]management.Run, string, error) {
		page, err := env.API.ListRuns(ctx, source.Id, limit, cursor)
		return page.Data, page.NextCursor, err
	})
	page := management.RunPage{Data: data, NextCursor: cursor}
	if err != nil {
		return page, Classify(err)
	}
	if env.JSON {
		return page, Classify(env.writeJSON(page))
	}
	var out strings.Builder
	if len(page.Data) == 0 {
		out.WriteString("No runs.\n")
	} else {
		out.WriteString("ID\tSTATE\tOUTCOME\tOPERATION\tAUTHORITATIVE\tCREATED\tFINISHED\n")
		for _, run := range page.Data {
			fmt.Fprintf(&out, "%s\t%s\t%s\t%s\t%t\t%s\t%s\n", run.Id, run.State, run.Outcome, run.Operation,
				run.Authoritative, runTime(run.CreatedAt), runTime(run.FinishedAt))
		}
	}
	if page.NextCursor != "" {
		fmt.Fprintf(&out, "Next cursor: %s\n", page.NextCursor)
	}
	return page, Classify(env.writeHuman(out.String()))
}

// InspectRun prints acquisition details separately from source readiness.
func InspectRun(ctx context.Context, env Env, ref, runID string) (management.Run, error) {
	source, err := ResolveSource(ctx, env, ref)
	if err != nil {
		return management.Run{}, err
	}
	run, err := env.API.GetRun(ctx, source.Id, runID)
	if err != nil {
		return run, Classify(err)
	}
	if env.JSON {
		return run, Classify(env.writeJSON(run))
	}
	return run, Classify(env.writeHuman(fmt.Sprintf(
		"Run:\t%s\nSource:\t%s\nOperation:\t%s\nState:\t%s\nOutcome:\t%s\nAuthoritative:\t%t\nCode:\t%s\nCreated:\t%s\nFinished:\t%s\n",
		run.Id, run.SourceId, run.Operation, run.State, run.Outcome, run.Authoritative, run.Code,
		runTime(run.CreatedAt), runTime(run.FinishedAt))))
}

// CancelRun requests cancellation only when the runtime advertises support.
func CancelRun(ctx context.Context, env Env, ref, runID string) (management.Run, error) {
	if err := requireCapability(ctx, env, "cancel_runs"); err != nil {
		return management.Run{}, err
	}
	source, err := ResolveSource(ctx, env, ref)
	if err != nil {
		return management.Run{}, err
	}
	run, err := env.API.CancelRun(ctx, source.Id, runID)
	if err != nil {
		return run, Classify(err)
	}
	if env.JSON {
		return run, Classify(env.writeJSON(run))
	}
	return run, Classify(env.writeHuman(fmt.Sprintf("Cancellation requested for run %s (state %s).\n", run.Id, run.State)))
}

// requireCapability fetches the runtime's capabilities once and checks each
// feature in order, returning the first unsupported one.
func requireCapability(ctx context.Context, env Env, features ...string) error {
	c, err := env.API.Capabilities(ctx)
	if err != nil {
		return Classify(err)
	}
	for _, feature := range features {
		if err := Require(c, feature); err != nil {
			return err
		}
	}
	return nil
}

// newIdempotencyKey returns a random key that satisfies the idempotency key format.
func newIdempotencyKey() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return "cli-" + hex.EncodeToString(random[:]), nil
}

func runTime(seconds int64) string {
	if seconds == 0 {
		return "-"
	}
	return time.Unix(seconds, 0).UTC().Format(time.RFC3339)
}
