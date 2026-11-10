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
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

const (
	CodeUnauthorized = "unauthorized"
	CodeForbidden    = "forbidden"
	CodeNotFound     = "not_found"
	CodeConflict     = "conflict"
	CodeInvalid      = "invalid"
	CodeUnsupported  = "unsupported"
	CodeTimeout      = "timeout"
	CodeRunFailed    = "run_failed"
	CodeInterrupted  = "interrupted"
	CodeTransport    = "transport"
	CodeFailed       = "request_failed"
)

// Error provides a stable machine code, exit status, and safe human message.
type Error struct {
	Code       string
	HTTPStatus int
	ServerCode string
	Message    string
	err        error
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.ServerCode != "" {
		return e.Code + " (" + e.ServerCode + ")"
	}
	return e.Code
}

func (e *Error) Unwrap() error { return e.err }

// ExitCode maps invalid input to 2, auth failures to 3, missing resources to 4,
// conflicts to 5, unsupported features to 6, timeouts to 7, failed runs to 8,
// and watch interruption to 130. Other failures exit with 1.
func (e *Error) ExitCode() int {
	switch e.Code {
	case CodeInvalid:
		return 2
	case CodeUnauthorized, CodeForbidden:
		return 3
	case CodeNotFound:
		return 4
	case CodeConflict:
		return 5
	case CodeUnsupported:
		return 6
	case CodeTimeout:
		return 7
	case CodeRunFailed:
		return 8
	case CodeInterrupted:
		return 130
	default:
		return 1
	}
}

// Classify translates management failures without exposing transport details.
// Unrecognized errors, including caller-defined errors, are returned unchanged.
func Classify(err error) error {
	if err == nil {
		return nil
	}
	var classified *Error
	if errors.As(err, &classified) {
		return err
	}
	var failure *management.HTTPError
	if errors.As(err, &failure) {
		code := CodeFailed
		switch failure.Status {
		case http.StatusBadRequest, http.StatusUnprocessableEntity:
			code = CodeInvalid
		case http.StatusUnauthorized:
			code = CodeUnauthorized
		case http.StatusForbidden:
			code = CodeForbidden
		case http.StatusNotFound:
			code = CodeNotFound
		case http.StatusConflict, http.StatusPreconditionFailed:
			code = CodeConflict
		}
		if failure.Status == http.StatusNotImplemented || strings.Contains(failure.Code, "unsupported") {
			code = CodeUnsupported
		}
		message := fmt.Sprintf("source-management request failed: HTTP %d", failure.Status)
		if failure.Code != "" {
			message += " " + failure.Code
		}
		return &Error{Code: code, HTTPStatus: failure.Status, ServerCode: failure.Code, Message: message, err: err}
	}
	var transport *management.TransportError
	if errors.As(err, &transport) {
		code := CodeTransport
		if transport.Timeout || errors.Is(err, context.DeadlineExceeded) {
			code = CodeTimeout
		}
		return &Error{Code: code, Message: transport.Error(), err: err}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Code: CodeTimeout, Message: "source-management request timed out", err: err}
	}
	return err
}

// Unsupported describes a capability unavailable in the hosting runtime.
func Unsupported(runtime, feature string) *Error {
	if runtime == "" {
		runtime = "this"
	}
	return &Error{Code: CodeUnsupported, Message: runtime + " source management does not support " + feature}
}

// Require checks a boolean capability or an advertised search_version:N.
func Require(c management.Capabilities, feature string) error {
	var supported bool
	switch feature {
	case "per_source_runs":
		supported = c.PerSourceRuns
	case "cancel_runs":
		supported = c.CancelRuns
	case "configure_sources":
		supported = c.ConfigureSources
	case "purge_sources":
		supported = c.PurgeSources
	case "processing_jobs":
		supported = c.ProcessingJobs
	case "processing_presets":
		supported = c.ProcessingPresets
	case "artifact_discovery":
		supported = c.ArtifactDiscovery
	default:
		raw, ok := strings.CutPrefix(feature, "search_version:")
		version, err := strconv.Atoi(raw)
		if !ok || err != nil {
			return &Error{Code: CodeInvalid, Message: fmt.Sprintf("unknown source-management feature %q", feature)}
		}
		supported = slices.Contains(c.SearchVersions, version)
	}
	if !supported {
		return Unsupported(c.Runtime, feature)
	}
	return nil
}
