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
	"errors"

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
	CodeTransport    = "transport"
	CodeFailed       = "request_failed"
)

type Error struct {
	Code       string
	HTTPStatus int
	ServerCode string
	Message    string
	err        error
}

func (e *Error) Error() string                                { return "" }
func (e *Error) Unwrap() error                                { return nil }
func (e *Error) ExitCode() int                                { return 0 }
func Classify(err error) error                                { return errors.New("not implemented") }
func Unsupported(runtime, feature string) *Error              { return nil }
func Require(c management.Capabilities, feature string) error { return errors.New("not implemented") }
