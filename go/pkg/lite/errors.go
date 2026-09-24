// Copyright 2026 Antfly, Inc.
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

package lite

import (
	"encoding/json"
	"fmt"
)

// ErrorCode is a stable Antfly C ABI error code.
type ErrorCode uint32

const (
	OK              ErrorCode = 0
	InvalidArgument ErrorCode = 1
	NotFound        ErrorCode = 2
	VersionConflict ErrorCode = 3
	IntentConflict  ErrorCode = 4
	TxnNotFound     ErrorCode = 5
	Busy            ErrorCode = 6
	// OutcomeUnknown means publication succeeded in the running process, but
	// crash durability could not be confirmed. Inspect the destination and do
	// not retry the operation automatically.
	OutcomeUnknown ErrorCode = 7
	// Unsupported means the operation requires a platform or filesystem
	// capability that is unavailable. Retrying unchanged will not succeed.
	Unsupported ErrorCode = 8
	// Stalled reports that a bounded drain such as RunUntilIdle detected a
	// managed index making no forward progress and gave up.
	Stalled ErrorCode = 9
	// Cancelled means the caller stopped the operation by returning false
	// from its progress or streaming callback (see Inference.Pull and
	// Inference.GenerateStream).
	Cancelled ErrorCode = 10
	Internal  ErrorCode = 255
)

var errorCodeNames = map[ErrorCode]string{
	OK:              "ANTFLY_OK",
	InvalidArgument: "ANTFLY_INVALID_ARGUMENT",
	NotFound:        "ANTFLY_NOT_FOUND",
	VersionConflict: "ANTFLY_VERSION_CONFLICT",
	IntentConflict:  "ANTFLY_INTENT_CONFLICT",
	TxnNotFound:     "ANTFLY_TXN_NOT_FOUND",
	Busy:            "ANTFLY_BUSY",
	OutcomeUnknown:  "ANTFLY_OUTCOME_UNKNOWN",
	Unsupported:     "ANTFLY_UNSUPPORTED",
	Stalled:         "ANTFLY_STALLED",
	Cancelled:       "ANTFLY_CANCELLED",
	Internal:        "ANTFLY_INTERNAL",
}

var errorCodeDescriptions = map[ErrorCode]string{
	OK:              "operation completed successfully",
	InvalidArgument: "an argument, request, path, or open mode is invalid",
	NotFound:        "the requested database object was not found",
	VersionConflict: "a version predicate did not match the current document version",
	IntentConflict:  "a transaction intent conflicts with the requested operation",
	TxnNotFound:     "the requested transaction was not found",
	Busy:            "the requested resource is temporarily busy or changed during streaming; stabilize it and retry",
	OutcomeUnknown:  "the operation was published, but crash durability could not be confirmed; inspect the destination and do not retry automatically",
	Unsupported:     "the operation requires a capability that is not supported by this platform or filesystem",
	Stalled:         "a bounded drain made no forward progress for its configured stall window and gave up",
	Cancelled:       "the caller cancelled the operation",
	Internal:        "an internal error occurred",
}

func (code ErrorCode) Error() string {
	return code.Name() + ": " + code.Description()
}

// Name returns the stable symbolic C ABI name for code.
func (code ErrorCode) Name() string {
	if name, ok := errorCodeNames[code]; ok {
		return name
	}
	return "ANTFLY_UNKNOWN_ERROR"
}

// Description returns a short stable description for code.
func (code ErrorCode) Description() string {
	if description, ok := errorCodeDescriptions[code]; ok {
		return description
	}
	return "unknown Antfly error code"
}

// InferenceAPIError is the JSON error body an Antfly inference call returns
// on failure: {"error": "...", "message": "..."}. Code is a stable string
// such as "MODEL_NOT_FOUND"; either field may be empty when the body was not
// present or not in this shape.
type InferenceAPIError struct {
	Code    string `json:"error"`
	Message string `json:"message"`
}

// InferenceError is returned by failed Inference JSON calls. It carries the
// stable C ABI error code (see ErrorCode) plus the inference runtime's JSON
// error body, when one was returned. errors.Is(err, code) and
// errors.As(err, &inferenceErr) both work against it.
type InferenceError struct {
	// ErrorCode is the C ABI error code mapped from the call's HTTP-shaped
	// failure (see antfly.h's "Embedded inference without a database").
	ErrorCode ErrorCode
	// API is the parsed JSON error body, when the body was valid JSON in the
	// {"error":..., "message":...} shape.
	API InferenceAPIError
	// Body is the raw JSON error body returned by the call, if any.
	Body []byte
}

func (e *InferenceError) Error() string {
	if e.API.Code != "" || e.API.Message != "" {
		return fmt.Sprintf("lite: inference %s: %s (%s)", e.API.Code, e.API.Message, e.ErrorCode.Name())
	}
	if len(e.Body) > 0 {
		return fmt.Sprintf("lite: inference: %s: %s", e.ErrorCode.Error(), e.Body)
	}
	return "lite: inference: " + e.ErrorCode.Error()
}

// Unwrap exposes the underlying stable ErrorCode for errors.Is/errors.As.
func (e *InferenceError) Unwrap() error {
	return e.ErrorCode
}

// newInferenceError builds an InferenceError from a failed call's error code
// and response body (which, per the C ABI contract, is filled with a JSON
// error document on failure even though the call also returned a non-OK
// code). code must not be OK.
func newInferenceError(code ErrorCode, body []byte) error {
	err := &InferenceError{ErrorCode: code, Body: body}
	_ = json.Unmarshal(body, &err.API)
	return err
}
