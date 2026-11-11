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

package sources

import (
	"context"
	"errors"
	"fmt"
	"github.com/minio/minio-go/v7"
	"net"
	"net/http"
)

// SyncFailure is safe to persist and expose: no raw provider errors, URLs,
// tokens, object paths, or response bodies enter this contract.
type SyncFailure struct {
	SourceID        string `json:"source_id,omitempty"`
	Phase           string `json:"phase"`
	Code            string `json:"code"`
	Retryable       bool   `json:"retryable"`
	RestartRequired bool   `json:"restart_required,omitempty"`
}
type sourceSyncError struct {
	sourceID, phase string
	cause           error
}

func (e *sourceSyncError) Error() string {
	return fmt.Sprintf("source %s: %s: %v", e.sourceID, e.phase, e.cause)
}
func (e *sourceSyncError) Unwrap() error { return e.cause }
func describeSyncFailure(err error) *SyncFailure {
	f := &SyncFailure{Phase: "catalog", Code: "operation_failed", Retryable: true}
	cause := err
	var source *sourceSyncError
	if errors.As(err, &source) {
		f.SourceID, f.Phase = source.sourceID, source.phase
		cause = source.cause
	}
	var changed *RevisionChangedError
	var restart *restartRequiredError
	var network net.Error
	status := 0
	var publication *PublishHTTPError
	if errors.As(err, &publication) {
		status = publication.Status
	}
	var google *GoogleHTTPError
	if errors.As(err, &google) {
		status = google.Status
	}
	if response := minio.ToErrorResponse(cause); response.StatusCode != 0 {
		status = response.StatusCode
	}
	switch {
	case errors.Is(err, context.Canceled):
		f.Code, f.Retryable = "cancelled", false
	case errors.Is(err, context.DeadlineExceeded):
		f.Code = "timeout"
	case errors.As(err, &changed):
		f.Code = "source_version_changed"
		f.RestartRequired = errors.As(err, &restart)
		f.Retryable = !f.RestartRequired
	case errors.Is(err, ErrInvalidInput):
		f.Code, f.Retryable = "invalid_configuration", false
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		f.Code, f.Retryable = "provider_access_denied", false
	case status == http.StatusNotFound:
		f.Code = "source_not_found"
	case status == http.StatusTooManyRequests:
		f.Code = "provider_rate_limited"
	case errors.As(err, &network):
		f.Code = "provider_unavailable"
	}
	return f
}
