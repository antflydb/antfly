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
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

func assertCode(t *testing.T, err error, code string) *Error {
	t.Helper()
	var failure *Error
	if !errors.As(err, &failure) {
		t.Fatalf("error = %T %v, want *commands.Error (%s)", err, err, code)
	}
	if failure.Code != code {
		t.Fatalf("code = %q, want %q", failure.Code, code)
	}
	return failure
}

func TestClassifyHTTP(t *testing.T) {
	for _, tc := range []struct {
		status       int
		server, code string
		exit         int
	}{
		{400, "bad_input", CodeInvalid, 2},
		{401, "bad_token", CodeUnauthorized, 3},
		{403, "access_denied", CodeForbidden, 3},
		{404, "source_not_found", CodeNotFound, 4},
		{409, "source_busy", CodeConflict, 5},
		{412, "revision_mismatch", CodeConflict, 5},
		{422, "bad_manifest", CodeInvalid, 2},
		{500, "internal", CodeFailed, 1},
		{501, "unimplemented", CodeUnsupported, 6},
		{500, "x_unsupported", CodeUnsupported, 6},
	} {
		t.Run(fmt.Sprintf("%d/%s", tc.status, tc.server), func(t *testing.T) {
			original := &management.HTTPError{Status: tc.status, Code: tc.server}
			got := assertCode(t, Classify(original), tc.code)
			if got.HTTPStatus != tc.status || got.ServerCode != tc.server {
				t.Errorf("HTTP fields = %d/%q, want %d/%q", got.HTTPStatus, got.ServerCode, tc.status, tc.server)
			}
			want := fmt.Sprintf("source-management request failed: HTTP %d %s", tc.status, tc.server)
			if got.Message != want || got.Error() != want {
				t.Errorf("message = %q / Error = %q, want %q", got.Message, got.Error(), want)
			}
			if got.ExitCode() != tc.exit {
				t.Errorf("exit = %d, want %d", got.ExitCode(), tc.exit)
			}
		})
	}
}

func TestClassifyTransportAndDeadline(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code string
	}{
		{"transport timeout", &management.TransportError{Timeout: true}, CodeTimeout},
		{"transport failure", &management.TransportError{}, CodeTransport},
		{"deadline", context.DeadlineExceeded, CodeTimeout},
		{"wrapped deadline", fmt.Errorf("operation: %w", context.DeadlineExceeded), CodeTimeout},
		{"wrapped transport", fmt.Errorf("operation: %w", &management.TransportError{Timeout: true}), CodeTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := assertCode(t, Classify(tc.err), tc.code)
			if got.HTTPStatus != 0 || got.ServerCode != "" {
				t.Fatalf("non-HTTP error has HTTP fields: %#v", got)
			}
		})
	}
}

func TestClassifyPreservesOtherErrors(t *testing.T) {
	original := errors.New("arbitrary error")
	classified := &Error{Code: CodeInvalid, Message: "invalid reference"}
	for _, tc := range []struct {
		name string
		err  error
	}{{"nil", nil}, {"arbitrary", original}, {"classified", classified}} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.err); got != tc.err {
				t.Fatalf("Classify = %v, want original %v", got, tc.err)
			}
		})
	}
}

func TestErrorExitCodes(t *testing.T) {
	for _, tc := range []struct {
		code string
		exit int
	}{
		{CodeUnauthorized, 3}, {CodeForbidden, 3}, {CodeNotFound, 4}, {CodeConflict, 5},
		{CodeInvalid, 2}, {CodeUnsupported, 6}, {CodeTimeout, 7}, {CodeRunFailed, 8},
		{CodeTransport, 1}, {CodeFailed, 1},
	} {
		t.Run(tc.code, func(t *testing.T) {
			if got := (&Error{Code: tc.code}).ExitCode(); got != tc.exit {
				t.Fatalf("ExitCode = %d, want %d", got, tc.exit)
			}
		})
	}
}

func TestErrorMessageAndUnwrap(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure Error
		want    string
	}{
		{"message wins", Error{Code: CodeNotFound, ServerCode: "source_not_found", Message: "source missing"}, "source missing"},
		{"code only", Error{Code: CodeTransport}, "transport"},
		{"server code", Error{Code: CodeNotFound, ServerCode: "source_not_found"}, "not_found (source_not_found)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.failure.Error(); got != tc.want {
				t.Fatalf("Error = %q, want %q", got, tc.want)
			}
		})
	}
	cause := errors.New("underlying failure")
	if got := (&Error{Code: CodeFailed, err: cause}).Unwrap(); got != cause {
		t.Errorf("Unwrap = %v, want original cause", got)
	}
}

func TestUnsupported(t *testing.T) {
	for _, runtime := range []string{"colony", ""} {
		t.Run(runtime, func(t *testing.T) {
			got := Unsupported(runtime, "per_source_runs")
			if got == nil {
				t.Fatal("Unsupported returned nil")
			}
			assertCode(t, got, CodeUnsupported)
			name := runtime
			if name == "" {
				name = "this"
			}
			if want := name + " source management does not support per_source_runs"; got.Message != want {
				t.Fatalf("message = %q, want %q", got.Message, want)
			}
			if got.ExitCode() != 6 {
				t.Fatalf("exit = %d, want 6", got.ExitCode())
			}
		})
	}
}

func TestRequireBooleanCapabilities(t *testing.T) {
	for _, tc := range []struct {
		feature      string
		capabilities management.Capabilities
	}{
		{"per_source_runs", management.Capabilities{PerSourceRuns: true}},
		{"cancel_runs", management.Capabilities{CancelRuns: true}},
		{"configure_sources", management.Capabilities{ConfigureSources: true}},
		{"purge_sources", management.Capabilities{PurgeSources: true}},
		{"processing_jobs", management.Capabilities{ProcessingJobs: true}},
		{"processing_presets", management.Capabilities{ProcessingPresets: true}},
		{"artifact_discovery", management.Capabilities{ArtifactDiscovery: true}},
	} {
		t.Run(tc.feature, func(t *testing.T) {
			t.Run("satisfied", func(t *testing.T) {
				if err := Require(tc.capabilities, tc.feature); err != nil {
					t.Fatal(err)
				}
			})
			t.Run("unsatisfied", func(t *testing.T) {
				got := assertCode(t, Require(management.Capabilities{Runtime: "standalone"}, tc.feature), CodeUnsupported)
				if !strings.Contains(got.Message, "standalone") || !strings.Contains(got.Message, tc.feature) || got.ExitCode() != 6 {
					t.Fatalf("unsupported failure: %#v", got)
				}
			})
		})
	}
}

func TestRequireSearchVersionAndUnknownFeature(t *testing.T) {
	t.Run("present", func(t *testing.T) {
		if err := Require(management.Capabilities{SearchVersions: []int{1, 2}}, "search_version:2"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("absent", func(t *testing.T) {
		assertCode(t, Require(management.Capabilities{Runtime: "colony", SearchVersions: []int{1}}, "search_version:2"), CodeUnsupported)
	})
	for _, feature := range []string{"unknown", "search_version:two", ""} {
		t.Run(feature, func(t *testing.T) { assertCode(t, Require(management.Capabilities{}, feature), CodeInvalid) })
	}
}
