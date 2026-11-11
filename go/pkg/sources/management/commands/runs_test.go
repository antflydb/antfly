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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// Embed the existing fake without changing pre-existing test files.
type runFakeAPI struct {
	*fakeAPI
	getRun    func(string, string) (management.Run, error)
	startRun  func(string, string, management.RunInput) (management.Run, error)
	cancelRun func(string, string) (management.Run, error)
}

var _ API = (*runFakeAPI)(nil)

func (f *runFakeAPI) GetRun(_ context.Context, source, id string) (management.Run, error) {
	f.calls = append(f.calls, apiCall{method: "GetRun", id: id})
	if f.getRun == nil {
		f.t.Fatal("unexpected GetRun call")
		return management.Run{}, nil
	}
	return f.getRun(source, id)
}

func (f *runFakeAPI) StartRun(_ context.Context, source, key string, in management.RunInput) (management.Run, error) {
	f.calls = append(f.calls, apiCall{method: "StartRun", id: source})
	if f.startRun == nil {
		f.t.Fatal("unexpected StartRun call")
		return management.Run{}, nil
	}
	return f.startRun(source, key, in)
}

func (f *runFakeAPI) CancelRun(_ context.Context, source, id string) (management.Run, error) {
	f.calls = append(f.calls, apiCall{method: "CancelRun", id: id})
	if f.cancelRun == nil {
		f.t.Fatal("unexpected CancelRun call")
		return management.Run{}, nil
	}
	return f.cancelRun(source, id)
}

type runClock struct {
	t      *testing.T
	now    time.Time
	sleeps []time.Duration
}

func newRunClock(t *testing.T) *runClock {
	return &runClock{t: t, now: time.Unix(1_700_000_000, 0)}
}

func (c *runClock) env(api API, out io.Writer, machine bool) Env {
	return Env{API: api, Out: out, JSON: machine, Now: func() time.Time { return c.now }, Sleep: c.sleep}
}

func (c *runClock) sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 || len(c.sleeps) >= 2000 {
		c.t.Fatalf("invalid or unbounded polling: sleep %s after %d sleeps", d, len(c.sleeps))
	}
	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)
	return nil
}

func TestStartRunReceipt(t *testing.T) {
	for _, tc := range []struct {
		name, key string
		restart   bool
		machine   bool
	}{
		{name: "generated human"},
		{name: "generated JSON", machine: true},
		{name: "supplied restart", key: "Retry_KEY-7", restart: true},
		{name: "supplied JSON", key: "a", machine: true},
		{name: "maximum key", key: strings.Repeat("x", 128)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := management.Run{Id: "run-7", SourceId: "src-1", State: "pending"}
			var key string
			api := &runFakeAPI{fakeAPI: &fakeAPI{t: t,
				capabilities: func() (management.Capabilities, error) {
					return management.Capabilities{PerSourceRuns: true}, nil
				},
				getSource: func(id string) (management.Source, error) {
					if id != "src-1" {
						t.Fatalf("resolved ID = %q", id)
					}
					return management.Source{Id: id, Name: "Docs"}, nil
				},
			}}
			api.startRun = func(id, gotKey string, in management.RunInput) (management.Run, error) {
				if id != "src-1" || in != (management.RunInput{Restart: tc.restart}) {
					t.Fatalf("submission = %q, %#v", id, in)
				}
				key = gotKey
				return want, nil
			}
			var out bytes.Buffer
			got, err := StartRun(context.Background(), Env{API: api, Out: &out, JSON: tc.machine}, "src-1", RunOptions{Restart: tc.restart, IdempotencyKey: tc.key})
			if err != nil {
				t.Fatal(err)
			}
			if tc.key == "" {
				if !regexp.MustCompile(`^cli-[0-9a-f]{32}$`).MatchString(key) {
					t.Fatalf("generated key = %q", key)
				}
			} else if key != tc.key {
				t.Fatalf("key = %q, want verbatim %q", key, tc.key)
			}
			if wantReceipt := (RunReceipt{Run: want, IdempotencyKey: key}); got != wantReceipt {
				t.Fatalf("receipt = %#v, want %#v", got, wantReceipt)
			}
			wantCalls := []apiCall{{method: "Capabilities"}, {method: "GetSource", id: "src-1"}, {method: "StartRun", id: "src-1"}}
			if !reflect.DeepEqual(api.calls, wantCalls) {
				t.Fatalf("calls = %#v, want %#v", api.calls, wantCalls)
			}
			if tc.machine {
				var decoded RunReceipt
				if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
					t.Fatal(err)
				}
				if decoded != got {
					t.Fatalf("JSON receipt = %#v, want %#v", decoded, got)
				}
			} else {
				wantText := fmt.Sprintf("Run run-7 pending for source Docs (src-1).\nIdempotency key: %s (reuse it to retry this submission without starting a duplicate run)\n", key)
				if out.String() != wantText {
					t.Fatalf("output = %q, want %q", out.String(), wantText)
				}
			}
		})
	}
}

func TestStartRunInvalidKeyMakesNoCalls(t *testing.T) {
	for _, key := range []string{"a b", strings.Repeat("a", 129), "a/b", "é"} {
		t.Run(key, func(t *testing.T) {
			api := &runFakeAPI{fakeAPI: &fakeAPI{t: t}}
			_, err := StartRun(context.Background(), Env{API: api, Out: io.Discard}, "src-1", RunOptions{IdempotencyKey: key})
			failure := assertCode(t, err, CodeInvalid)
			if failure.IdempotencyKey != "" {
				t.Errorf("validation error carries submission key %q", failure.IdempotencyKey)
			}
			if len(api.calls) != 0 {
				t.Fatalf("invalid key made calls: %#v", api.calls)
			}
		})
	}
}

func TestStartRunCapabilityGate(t *testing.T) {
	api := &runFakeAPI{fakeAPI: &fakeAPI{t: t, capabilities: func() (management.Capabilities, error) {
		return management.Capabilities{Runtime: "colony"}, nil
	}}}
	_, err := StartRun(context.Background(), Env{API: api, Out: io.Discard}, "src-1", RunOptions{})
	failure := assertCode(t, err, CodeUnsupported)
	if failure.ExitCode() != 6 {
		t.Fatalf("exit = %d, want 6", failure.ExitCode())
	}
	if !reflect.DeepEqual(api.calls, []apiCall{{method: "Capabilities"}}) {
		t.Fatalf("unsupported start made calls: %#v", api.calls)
	}
}

func TestStartRunSubmissionFailureReturnsIdempotencyKey(t *testing.T) {
	for _, suppliedKey := range []string{"", "retry-me"} {
		t.Run("key="+suppliedKey, func(t *testing.T) {
			api := &runFakeAPI{fakeAPI: &fakeAPI{t: t,
				capabilities: func() (management.Capabilities, error) {
					return management.Capabilities{PerSourceRuns: true}, nil
				},
				getSource: func(id string) (management.Source, error) { return management.Source{Id: id}, nil },
			}}
			original := &management.TransportError{}
			attempts := 0
			var submittedKey string
			api.startRun = func(id, key string, in management.RunInput) (management.Run, error) {
				attempts++
				submittedKey = key
				return management.Run{}, original
			}
			receipt, err := StartRun(context.Background(), Env{API: api, Out: io.Discard}, "src-1", RunOptions{IdempotencyKey: suppliedKey})
			failure := assertCode(t, err, CodeTransport)
			if _, ok := err.(*Error); !ok {
				t.Errorf("submission error = %T, want *Error", err)
			}
			if attempts != 1 {
				t.Errorf("POST attempts = %d, want 1", attempts)
			}
			if suppliedKey == "" {
				if !regexp.MustCompile(`^cli-[0-9a-f]{32}$`).MatchString(submittedKey) {
					t.Errorf("generated key = %q", submittedKey)
				}
			} else if submittedKey != suppliedKey {
				t.Errorf("submitted key = %q, want %q", submittedKey, suppliedKey)
			}
			if receipt.IdempotencyKey != submittedKey || failure.IdempotencyKey != receipt.IdempotencyKey {
				t.Errorf("submitted key=%q receipt=%#v error key=%q", submittedKey, receipt, failure.IdempotencyKey)
			}
			suffix := " (idempotency key " + submittedKey + "; retry with this key to avoid a duplicate submission)"
			if !strings.HasSuffix(failure.Message, suffix) || !strings.Contains(err.Error(), submittedKey) {
				t.Errorf("submission error = %q, want suffix %q", err.Error(), suffix)
			}
			var transport *management.TransportError
			if !errors.As(err, &transport) || transport != original {
				t.Errorf("submission error lost original transport error: %v", err)
			}
		})
	}
}

func TestWaitStateChangesAndBackoff(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opts   WaitOptions
		states []management.RunState
		sleeps []time.Duration
	}{
		{"defaults", WaitOptions{}, []management.RunState{"pending", "running", "completed"}, []time.Duration{2 * time.Second, 4 * time.Second}},
		{"capped and repeated", WaitOptions{Interval: time.Second, MaxInterval: 3 * time.Second}, []management.RunState{"pending", "pending", "running", "running", "completed"}, []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second}},
		{"default cap", WaitOptions{}, []management.RunState{"pending", "pending", "running", "running", "running", "completed"}, []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second, 15 * time.Second}},
	} {
		for _, machine := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%t", tc.name, machine), func(t *testing.T) {
				clock := newRunClock(t)
				var out bytes.Buffer
				polls := 0
				want := management.Run{Id: "run-1", SourceId: "src-1", State: "completed", Outcome: "partial", Authoritative: false}
				api := &runFakeAPI{fakeAPI: &fakeAPI{t: t}}
				api.getRun = func(source, id string) (management.Run, error) {
					if source != "src-1" || id != "run-1" {
						t.Fatalf("GetRun(%q, %q)", source, id)
					}
					if machine && out.Len() != 0 {
						t.Fatalf("JSON emitted before termination: %q", out.String())
					}
					if polls >= len(tc.states) {
						t.Fatal("polled beyond terminal state")
					}
					if polls == 0 && len(clock.sleeps) != 0 {
						t.Fatal("first poll was delayed")
					}
					run := want
					run.State = tc.states[polls]
					polls++
					return run, nil
				}
				got, err := Wait(context.Background(), clock.env(api, &out, machine), "src-1", "run-1", tc.opts)
				if err != nil {
					t.Fatal(err)
				}
				if got != want || polls != len(tc.states) {
					t.Fatalf("result = %#v after %d polls, want %#v after %d", got, polls, want, len(tc.states))
				}
				if !reflect.DeepEqual(clock.sleeps, tc.sleeps) {
					t.Fatalf("sleeps = %v, want %v", clock.sleeps, tc.sleeps)
				}
				if machine {
					var decoded management.Run
					if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
						t.Fatal(err)
					}
					if decoded != want {
						t.Fatalf("JSON = %#v, want %#v", decoded, want)
					}
				} else {
					wantText := "Run run-1: pending\nRun run-1: running\nRun run-1: completed\nAcquisition completed: outcome partial; not an authoritative publication\nSearch readiness is reported separately; see source status.\n"
					if out.String() != wantText {
						t.Fatalf("output = %q, want %q", out.String(), wantText)
					}
				}
			})
		}
	}
}

func TestWaitTerminalFailure(t *testing.T) {
	for _, state := range []management.RunState{"failed", "cancelled", "interrupted"} {
		t.Run(string(state), func(t *testing.T) {
			want := management.Run{Id: "run-1", State: state, Code: "upstream_unavailable"}
			api := &runFakeAPI{fakeAPI: &fakeAPI{t: t}, getRun: func(string, string) (management.Run, error) { return want, nil }}
			clock := newRunClock(t)
			var out bytes.Buffer
			got, err := Wait(context.Background(), clock.env(api, &out, true), "src-1", "run-1", WaitOptions{})
			failure := assertCode(t, err, CodeRunFailed)
			if failure.ExitCode() != 8 || failure.Message != fmt.Sprintf("run run-1 ended %s (upstream_unavailable)", state) {
				t.Fatalf("failure = %#v, exit %d", failure, failure.ExitCode())
			}
			if got != want || len(clock.sleeps) != 0 {
				t.Fatalf("terminal result = %#v, sleeps %v", got, clock.sleeps)
			}
			var decoded management.Run
			if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded != want {
				t.Fatalf("JSON = %#v, want %#v", decoded, want)
			}
		})
	}
}

func TestWaitDeadlineLeavesServerRunning(t *testing.T) {
	for _, successfulPoll := range []bool{true, false} {
		t.Run(fmt.Sprintf("successfulPoll=%t", successfulPoll), func(t *testing.T) {
			clock := newRunClock(t)
			start := clock.now
			want := management.Run{Id: "run-1", SourceId: "src-1", State: "running"}
			api := &runFakeAPI{fakeAPI: &fakeAPI{t: t}, getRun: func(string, string) (management.Run, error) {
				if !successfulPoll {
					return management.Run{}, &management.TransportError{}
				}
				return want, nil
			}}
			var out bytes.Buffer
			got, err := Wait(context.Background(), clock.env(api, &out, true), "src-1", "run-1", WaitOptions{Timeout: 5 * time.Second})
			failure := assertCode(t, err, CodeTimeout)
			wantMessage := "stopped waiting after 5s; run run-1 continues on the server"
			if successfulPoll {
				wantMessage = "stopped waiting after 5s; run run-1 is still running and continues on the server"
			}
			if failure.ExitCode() != 7 || failure.Message != wantMessage {
				t.Fatalf("failure = %#v, exit %d", failure, failure.ExitCode())
			}
			if elapsed := clock.now.Sub(start); elapsed != 5*time.Second {
				t.Fatalf("total fake sleep = %s, want exactly 5s", elapsed)
			}
			if !reflect.DeepEqual(clock.sleeps, []time.Duration{2 * time.Second, 3 * time.Second}) {
				t.Fatalf("deadline sleeps = %v, want [2s 3s]", clock.sleeps)
			}
			if successfulPoll {
				var decoded management.Run
				if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
					t.Fatal(err)
				}
				if got != want || decoded != want {
					t.Fatalf("returned/JSON run = %#v / %#v, want %#v", got, decoded, want)
				}
			} else if got != (management.Run{}) || out.Len() != 0 {
				t.Fatalf("no successful poll returned/printed a run: %#v / %q", got, out.String())
			}
			for _, call := range api.calls {
				if call.method != "GetRun" {
					t.Fatalf("waiting called %s; must not cancel", call.method)
				}
			}
		})
	}
}

func TestWaitCallerInterruption(t *testing.T) {
	for _, mode := range []string{"during sleep", "already cancelled", "sleep error", "cancelled sleep returns nil"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "already cancelled" {
				cancel()
			}
			clock := newRunClock(t)
			polls := 0
			api := &runFakeAPI{fakeAPI: &fakeAPI{t: t}, getRun: func(string, string) (management.Run, error) {
				polls++
				if polls > 1 {
					t.Fatal("polled after caller interruption")
				}
				return management.Run{Id: "run-1", State: "running"}, nil
			}}
			env := clock.env(api, io.Discard, false)
			env.Sleep = func(context.Context, time.Duration) error {
				if mode == "sleep error" {
					return errors.New("watch stopped")
				}
				cancel()
				if mode == "cancelled sleep returns nil" {
					return nil
				}
				return ctx.Err()
			}
			_, err := Wait(ctx, env, "src-1", "run-1", WaitOptions{})
			failure := assertCode(t, err, CodeInterrupted)
			if failure.ExitCode() != 130 || failure.Message != "stopped watching; run run-1 continues on the server" {
				t.Fatalf("failure = %#v, exit %d", failure, failure.ExitCode())
			}
			for _, call := range api.calls {
				if call.method != "GetRun" {
					t.Fatalf("watch interruption called %s; must not cancel", call.method)
				}
			}
		})
	}
}

func TestRunInterruptedExitCode(t *testing.T) {
	if got := (&Error{Code: CodeInterrupted}).ExitCode(); got != 130 {
		t.Fatalf("interrupted exit = %d, want 130", got)
	}
}

func TestWaitTransientErrors(t *testing.T) {
	clock := newRunClock(t)
	failures := []error{&management.TransportError{}, &management.HTTPError{Status: 503}, &management.TransportError{Timeout: true}}
	polls := 0
	want := management.Run{Id: "run-1", State: "completed", Outcome: "success", Authoritative: true}
	api := &runFakeAPI{fakeAPI: &fakeAPI{t: t}, getRun: func(string, string) (management.Run, error) {
		polls++
		if polls <= len(failures) {
			return management.Run{}, failures[polls-1]
		}
		return want, nil
	}}
	var out bytes.Buffer
	got, err := Wait(context.Background(), clock.env(api, &out, false), "src-1", "run-1", WaitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got != want || polls != 4 || !reflect.DeepEqual(clock.sleeps, []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second}) {
		t.Fatalf("transient polling = %#v, %d polls, sleeps %v", got, polls, clock.sleeps)
	}
	wantText := "Run run-1: completed\nAcquisition completed: outcome success\nSearch readiness is reported separately; see source status.\n"
	if out.String() != wantText {
		t.Fatalf("authoritative completion output = %q, want %q", out.String(), wantText)
	}
}

func TestWaitPermanentErrorsAbortImmediately(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
	}{{403, CodeForbidden}, {401, CodeUnauthorized}, {404, CodeNotFound}, {400, CodeInvalid}} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			clock := newRunClock(t)
			api := &runFakeAPI{fakeAPI: &fakeAPI{t: t}, getRun: func(string, string) (management.Run, error) {
				return management.Run{}, &management.HTTPError{Status: tc.status}
			}}
			_, err := Wait(context.Background(), clock.env(api, io.Discard, false), "src-1", "run-1", WaitOptions{})
			assertCode(t, err, tc.code)
			if len(api.calls) != 1 || len(clock.sleeps) != 0 {
				t.Fatalf("permanent failure was retried: calls %#v, sleeps %v", api.calls, clock.sleeps)
			}
		})
	}
}

func TestListRunsAllFollowsEmptyPages(t *testing.T) {
	pages := map[string]management.RunPage{
		"start": {Data: []management.Run{{Id: "one"}}, NextCursor: "a"},
		"a":     {Data: []management.Run{}, NextCursor: "b"},
		"b":     {Data: []management.Run{{Id: "two"}}},
	}
	api := &runFakeAPI{fakeAPI: &fakeAPI{t: t,
		getSource: func(id string) (management.Source, error) { return management.Source{Id: id}, nil },
		listRuns: func(id string, limit int, cursor string) (management.RunPage, error) {
			if id != "src-1" || limit != 2 {
				t.Fatalf("ListRuns ID/limit = %q/%d", id, limit)
			}
			page, ok := pages[cursor]
			if !ok {
				t.Fatalf("unexpected cursor %q", cursor)
			}
			return page, nil
		},
	}}
	var out bytes.Buffer
	got, err := ListRuns(context.Background(), Env{API: api, Out: &out, JSON: true}, "src-1", ListOptions{All: true, Limit: 2, Cursor: "start"})
	if err != nil {
		t.Fatal(err)
	}
	want := management.RunPage{Data: []management.Run{{Id: "one"}, {Id: "two"}}}
	var decoded management.RunPage
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(decoded, want) {
		t.Fatalf("returned/JSON page = %#v / %#v, want %#v", got, decoded, want)
	}
	wantCalls := []apiCall{{method: "GetSource", id: "src-1"}, {method: "ListRuns", id: "src-1", limit: 2, cursor: "start"}, {method: "ListRuns", id: "src-1", limit: 2, cursor: "a"}, {method: "ListRuns", id: "src-1", limit: 2, cursor: "b"}}
	if !reflect.DeepEqual(api.calls, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", api.calls, wantCalls)
	}
}

func TestListRunsLimitsAndSinglePage(t *testing.T) {
	for _, limit := range []int{-1, 0, 1, 100, 101} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			api := &runFakeAPI{fakeAPI: &fakeAPI{t: t,
				getSource: func(id string) (management.Source, error) { return management.Source{Id: id}, nil },
			}}
			api.listRuns = func(id string, gotLimit int, cursor string) (management.RunPage, error) {
				if limit < 0 || limit > 100 {
					t.Fatal("invalid limit reached ListRuns")
				}
				wantLimit := limit
				if wantLimit == 0 {
					wantLimit = 20
				}
				if gotLimit != wantLimit || cursor != "start" || id != "src-1" {
					t.Fatalf("ListRuns(%q, %d, %q)", id, gotLimit, cursor)
				}
				return management.RunPage{NextCursor: "next"}, nil
			}
			var out bytes.Buffer
			got, err := ListRuns(context.Background(), Env{API: api, Out: &out, JSON: true}, "src-1", ListOptions{Limit: limit, Cursor: "start"})
			if limit < 0 || limit > 100 {
				assertCode(t, err, CodeInvalid)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var decoded management.RunPage
			if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.Data == nil || len(decoded.Data) != 0 || decoded.NextCursor != "next" || !reflect.DeepEqual(decoded, got) {
				t.Fatalf("empty JSON page = %#v, returned %#v; want data:[] and next cursor", decoded, got)
			}
			if len(api.calls) != 2 {
				t.Fatalf("single page followed cursor: %#v", api.calls)
			}
		})
	}
}

func TestListRunsRejectsRepeatedCursor(t *testing.T) {
	api := &runFakeAPI{fakeAPI: &fakeAPI{t: t,
		getSource: func(id string) (management.Source, error) { return management.Source{Id: id}, nil },
		listRuns: func(string, int, string) (management.RunPage, error) {
			return management.RunPage{NextCursor: "start"}, nil
		},
	}}
	_, err := ListRuns(context.Background(), Env{API: api, Out: io.Discard}, "src-1", ListOptions{All: true, Cursor: "start"})
	assertCode(t, err, CodeFailed)
	if len(api.calls) != 2 {
		t.Fatalf("repeated cursor was followed: %#v", api.calls)
	}
}

func TestListRunsHuman(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty=%t", empty), func(t *testing.T) {
			api := &runFakeAPI{fakeAPI: &fakeAPI{t: t,
				getSource: func(id string) (management.Source, error) { return management.Source{Id: id}, nil },
				listRuns: func(string, int, string) (management.RunPage, error) {
					page := management.RunPage{NextCursor: "next"}
					if !empty {
						page.Data = []management.Run{{Id: "run-1", State: "completed", Outcome: "partial", Operation: "sync", Authoritative: false, CreatedAt: 1_700_000_000, FinishedAt: 1_700_000_060}, {Id: "run-2", State: "pending", Outcome: "unknown", Operation: "purge", Authoritative: true}}
					}
					return page, nil
				},
			}}
			var out bytes.Buffer
			_, err := ListRuns(context.Background(), Env{API: api, Out: &out}, "src-1", ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			want := "ID STATE OUTCOME OPERATION AUTHORITATIVE CREATED FINISHED run-1 completed partial sync false 2023-11-14T22:13:20Z 2023-11-14T22:14:20Z run-2 pending unknown purge true - - Next cursor: next"
			if empty {
				want = "No runs. Next cursor: next"
			}
			if got := strings.Join(strings.Fields(out.String()), " "); got != want {
				t.Fatalf("human table = %q, want %q", got, want)
			}
		})
	}
}

func TestInspectRunResolvesNameAndOutputs(t *testing.T) {
	for _, machine := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", machine), func(t *testing.T) {
			want := management.Run{Id: "run-1", SourceId: "src-1", Operation: "sync", State: "failed", Outcome: "failed", Authoritative: false, Code: "broken", CreatedAt: 1_700_000_000}
			api := &runFakeAPI{fakeAPI: &fakeAPI{t: t,
				listSources: func(int, string) (management.SourcePage, error) {
					return management.SourcePage{Data: []management.Source{{Id: "src-1", Name: "My Docs"}}}, nil
				},
				getSource: func(id string) (management.Source, error) {
					return management.Source{Id: id, Name: "My Docs"}, nil
				},
			}, getRun: func(source, id string) (management.Run, error) {
				if source != "src-1" || id != "run-1" {
					t.Fatalf("GetRun(%q, %q)", source, id)
				}
				return want, nil
			}}
			var out bytes.Buffer
			got, err := InspectRun(context.Background(), Env{API: api, Out: &out, JSON: machine}, "My Docs", "run-1")
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("run = %#v, want %#v", got, want)
			}
			wantCalls := []apiCall{{method: "ListSources", limit: 100}, {method: "GetSource", id: "src-1"}, {method: "GetRun", id: "run-1"}}
			if !reflect.DeepEqual(api.calls, wantCalls) {
				t.Fatalf("calls = %#v, want %#v", api.calls, wantCalls)
			}
			if machine {
				var decoded management.Run
				if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
					t.Fatal(err)
				}
				if decoded != want {
					t.Fatalf("JSON = %#v, want %#v", decoded, want)
				}
			} else {
				lines := strings.Split(strings.TrimSpace(out.String()), "\n")
				keys := []string{"Run", "Source", "Operation", "State", "Outcome", "Authoritative", "Code", "Created", "Finished"}
				values := []string{"run-1", "src-1", "sync", "failed", "failed", "false", "broken", "2023-11-14T22:13:20Z", "-"}
				if len(lines) != len(keys) {
					t.Fatalf("inspect lines = %q", out.String())
				}
				for i, key := range keys {
					parts := strings.SplitN(lines[i], ":", 2)
					if len(parts) != 2 || strings.TrimSpace(parts[0]) != key || strings.TrimSpace(parts[1]) != values[i] {
						t.Errorf("line %d = %q, want %s: %s", i, lines[i], key, values[i])
					}
				}
			}
		})
	}
}

func TestCancelRunCapabilityGate(t *testing.T) {
	api := &runFakeAPI{fakeAPI: &fakeAPI{t: t, capabilities: func() (management.Capabilities, error) {
		return management.Capabilities{Runtime: "colony"}, nil
	}}}
	_, err := CancelRun(context.Background(), Env{API: api, Out: io.Discard}, "My Docs", "run-1")
	failure := assertCode(t, err, CodeUnsupported)
	if failure.ExitCode() != 6 {
		t.Fatalf("exit = %d, want 6", failure.ExitCode())
	}
	if !reflect.DeepEqual(api.calls, []apiCall{{method: "Capabilities"}}) {
		t.Fatalf("unsupported cancellation resolved or mutated: %#v", api.calls)
	}
}

func TestCancelRunSuccess(t *testing.T) {
	for _, machine := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", machine), func(t *testing.T) {
			want := management.Run{Id: "run-1", SourceId: "src-1", State: "running"}
			api := &runFakeAPI{fakeAPI: &fakeAPI{t: t,
				capabilities: func() (management.Capabilities, error) { return management.Capabilities{CancelRuns: true}, nil },
				getSource:    func(id string) (management.Source, error) { return management.Source{Id: id}, nil },
			}, cancelRun: func(source, id string) (management.Run, error) {
				if source != "src-1" || id != "run-1" {
					t.Fatalf("CancelRun(%q, %q)", source, id)
				}
				return want, nil
			}}
			var out bytes.Buffer
			got, err := CancelRun(context.Background(), Env{API: api, Out: &out, JSON: machine}, "src-1", "run-1")
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("run = %#v, want %#v", got, want)
			}
			wantCalls := []apiCall{{method: "Capabilities"}, {method: "GetSource", id: "src-1"}, {method: "CancelRun", id: "run-1"}}
			if !reflect.DeepEqual(api.calls, wantCalls) {
				t.Fatalf("calls = %#v, want %#v", api.calls, wantCalls)
			}
			if machine {
				var decoded management.Run
				if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
					t.Fatal(err)
				}
				if decoded != want {
					t.Fatalf("JSON = %#v, want %#v", decoded, want)
				}
			} else if out.String() != "Cancellation requested for run run-1 (state running).\n" {
				t.Fatalf("output = %q", out.String())
			}
		})
	}
}
