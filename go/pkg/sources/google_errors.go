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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// GoogleHTTPError retains the legacy Google error type while exposing the same
// cooldown and rate-limit contracts as collaboration API errors. Response bodies
// and provider messages are never retained in errors or persisted diagnostics.
type GoogleHTTPError struct {
	Status  int
	Reason  string
	RetryAt time.Time
}

func (e *GoogleHTTPError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("Google source returned HTTP %d (%s)", e.Status, e.Reason)
	}
	return fmt.Sprintf("Google source returned HTTP %d", e.Status)
}
func (e *GoogleHTTPError) AntflyRetryAfter() time.Time { return e.RetryAt }
func (e *GoogleHTTPError) RateLimited() bool {
	return e.Status == 429 || (e.Status == 403 && googleRateLimitReason(e.Reason))
}
func (e *GoogleHTTPError) Retryable() bool { return e.RateLimited() || e.Status >= 500 }

// IsRateLimited recognizes wrapped legacy Google and collaboration errors alike.
// Runtimes use it for bounded audit waits and durable acquisition-progress budgets.
func IsRateLimited(err error) bool {
	var limit interface{ RateLimited() bool }
	return errors.As(err, &limit) && limit.RateLimited()
}

func googleRateLimitReason(reason string) bool {
	// Gmail, Calendar and Drive document these reasons as exponential-backoff
	// failures, whether the HTTP status is 403 or 429. Usage caps such as
	// dailyLimitExceeded/quotaExceeded are distinct from transient rate limits.
	return reason == "rateLimitExceeded" || reason == "userRateLimitExceeded"
}

const maxGoogleErrorBytes = 64 << 10

func googleResponseError(resp *http.Response) *GoogleHTTPError {
	api := &GoogleHTTPError{Status: resp.StatusCode, RetryAt: providerRetryAt(resp.Header.Get("Retry-After"), time.Now())}
	if resp.Body == nil {
		return api
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxGoogleErrorBytes+1))
	if err != nil || len(raw) > maxGoogleErrorBytes {
		return api
	}
	var envelope struct {
		Error struct {
			Errors []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &envelope) != nil {
		return api
	}
	for _, entry := range envelope.Error.Errors {
		reason := entry.Reason
		if !validGoogleErrorReason(reason) {
			api.Reason = ""
			return api
		}
		// A permanent/unknown reason wins over a rate-limit reason in mixed errors.
		// Only a complete, recognized quota response may reclassify an HTTP 403.
		if !googleRateLimitReason(reason) {
			api.Reason = reason
			return api
		}
		if api.Reason == "" {
			api.Reason = reason
		}
	}
	return api
}

func validGoogleErrorReason(reason string) bool {
	if len(reason) == 0 || len(reason) > 128 {
		return false
	}
	for _, r := range reason {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return false
		}
	}
	return true
}
