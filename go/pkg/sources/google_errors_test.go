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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func googleQuotaBody(reasons ...string) string {
	entries := make([]map[string]string, 0, len(reasons))
	for _, reason := range reasons {
		entries = append(entries, map[string]string{"reason": reason, "message": "provider-secret must not enter diagnostics"})
	}
	raw, _ := json.Marshal(map[string]any{"error": map[string]any{"errors": entries, "message": "provider-secret must not enter diagnostics"}})
	return string(raw)
}

func TestGoogleErrorClassificationAndCooldown(t *testing.T) {
	for _, tc := range []struct {
		name           string
		status         int
		body           string
		retry, limited bool
	}{
		{"user quota", 403, googleQuotaBody("userRateLimitExceeded"), true, true},
		{"project quota", 403, googleQuotaBody("rateLimitExceeded"), true, true},
		{"multiple quotas", 403, googleQuotaBody("rateLimitExceeded", "userRateLimitExceeded"), true, true},
		{"permission", 403, googleQuotaBody("insufficientPermissions"), false, false},
		{"domain policy", 403, googleQuotaBody("domainPolicy"), false, false},
		{"usage cap", 403, googleQuotaBody("quotaExceeded"), false, false},
		{"daily cap", 403, googleQuotaBody("dailyLimitExceeded"), false, false},
		{"unknown", 403, googleQuotaBody("futureReason"), false, false},
		{"permission overrides quota", 403, googleQuotaBody("rateLimitExceeded", "insufficientPermissions"), false, false},
		{"quota cannot override permission", 403, googleQuotaBody("insufficientPermissions", "rateLimitExceeded"), false, false},
		{"invalid reason", 403, googleQuotaBody("rateLimitExceeded", "reason\nprovider-secret"), false, false},
		{"missing reason", 403, `{"error":{"errors":[{}]}}`, false, false},
		{"malformed", 403, `{broken`, false, false},
		{"no body", 403, "", false, false},
		{"401 cannot be reclassified", 401, googleQuotaBody("rateLimitExceeded"), false, false},
		{"404 cannot be reclassified", 404, googleQuotaBody("rateLimitExceeded"), false, false},
		{"plain throttle", 429, "", true, true},
		{"unavailable", 503, "", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			response := &http.Response{StatusCode: tc.status, Header: http.Header{"Retry-After": []string{"600"}}, Body: io.NopCloser(strings.NewReader(tc.body))}
			api := googleResponseError(response)
			require.Equal(t, tc.status, api.Status)
			require.Equal(t, tc.retry, api.Retryable())
			require.Equal(t, tc.retry, (&GoogleConnector{}).IsRetryable(api))
			require.Equal(t, tc.limited, IsRateLimited(fmt.Errorf("wrapped: %w", api)))
			require.WithinDuration(t, now.Add(10*time.Minute), api.RetryAt, time.Second)
			require.NotContains(t, api.Error(), "provider-secret")
			// New Workspace and legacy Google adapters share identical classification.
			shared := &ProviderHTTPError{Provider: ProviderGoogleWorkspace, Status: api.Status, Code: api.Reason, RetryAt: api.RetryAt}
			require.Equal(t, tc.retry, shared.Retryable())
			require.Equal(t, tc.limited, IsRateLimited(shared))
			require.Equal(t, api.RetryAt, shared.AntflyRetryAfter())
		})
	}
	require.False(t, IsRateLimited(nil))
	require.False(t, (&ProviderHTTPError{Provider: ProviderSlack, Status: 403, Code: "userRateLimitExceeded"}).Retryable())
}

type googleCountingReader struct {
	io.Reader
	read int
}

func (r *googleCountingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.read += n
	return n, err
}
func TestGoogleErrorBodyIsBounded(t *testing.T) {
	body := &googleCountingReader{Reader: strings.NewReader(strings.Repeat("x", 1<<20))}
	api := googleResponseError(&http.Response{StatusCode: 403, Header: http.Header{"Retry-After": []string{"600"}}, Body: io.NopCloser(body)})
	require.LessOrEqual(t, body.read, maxGoogleErrorBytes+1)
	require.Empty(t, api.Reason)
	require.False(t, api.Retryable())
	require.False(t, api.RetryAt.IsZero())
}

func TestWorkspaceDriveDownloadPreservesGoogleCooldown(t *testing.T) {
	for _, native := range []bool{false, true} {
		for _, status := range []int{403, 429, 503} {
			t.Run(fmt.Sprintf("native=%v/status=%d", native, status), func(t *testing.T) {
				deadline := time.Now().UTC().Add(10 * time.Minute).Truncate(time.Second)
				downloads := 0
				c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"calendar", "drive"}, DriveBase: "https://fixture.test/drive"}
				c.HTTPClient = &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
					if r.URL.Query().Get("alt") == "media" || strings.HasSuffix(r.URL.Path, "/export") {
						downloads++
						require.Equal(t, native, strings.HasSuffix(r.URL.Path, "/export"))
						return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{deadline.Format(http.TimeFormat)}}, Request: r, Body: io.NopCloser(strings.NewReader(googleQuotaBody("userRateLimitExceeded")))}, nil
					}
					mime := "text/plain"
					if native {
						mime = "application/vnd.google-apps.document"
					}
					raw, _ := json.Marshal(map[string]any{"id": "file", "version": "1", "name": "notes.txt", "size": "5", "mimeType": mime})
					return &http.Response{StatusCode: 200, Header: http.Header{}, Request: r, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
				})}
				_, err := c.workspaceDriveFile(t.Context(), "file")
				var api *GoogleHTTPError
				require.ErrorAs(t, err, &api)
				require.True(t, api.Retryable())
				require.Equal(t, status != 503, IsRateLimited(err))
				require.Equal(t, deadline, api.AntflyRetryAfter())
				require.Equal(t, 1, downloads)
			})
		}
	}
}

func TestWorkspaceQuotaAuditsResumeSuccessfulScopeChecks(t *testing.T) {
	for _, resource := range []string{"gmail", "calendar", "drive"} {
		for _, status := range []int{403, 429} {
			t.Run(fmt.Sprintf("%s/%d", resource, status), func(t *testing.T) {
				limited := true
				checks, waits := map[string]int{}, 0
				c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{resource}, APIBase: "https://fixture.test", DriveBase: "https://fixture.test"}
				c.HTTPClient = &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
					checks[r.URL.Path]++
					fail := r.URL.Path == "/labels/INBOX" || r.URL.Path == "/users/me/calendarList/two" || r.URL.Path == "/files/file"
					if limited && fail {
						return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{"10"}}, Request: r, Body: io.NopCloser(strings.NewReader(googleQuotaBody("rateLimitExceeded")))}, nil
					}
					var value any
					switch r.URL.Path {
					case "/profile":
						value = map[string]any{"emailAddress": "alice@example.com"}
					case "/labels/INBOX":
						value = map[string]any{"id": "INBOX"}
					case "/users/me/calendarList/one":
						value = map[string]any{"id": "one", "accessRole": "reader"}
					case "/users/me/calendarList/two":
						value = map[string]any{"id": "two", "accessRole": "reader"}
					case "/files/folder":
						value = map[string]any{"id": "folder", "capabilities": map[string]any{"canListChildren": true}}
					case "/files/file":
						value = map[string]any{"id": "file", "version": "1", "capabilities": map[string]any{"canDownload": true}}
					default:
						t.Fatalf("unexpected audit request %s", r.URL)
					}
					raw, _ := json.Marshal(value)
					return &http.Response{StatusCode: 200, Header: http.Header{}, Request: r, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
				})}
				c.accessWait = func(context.Context, time.Duration) error { waits++; limited = false; return nil }
				config := SourceConfig(`{"resource":"gmail","label_ids":["INBOX"],"permission_ttl_seconds":60}`)
				inventory := Inventory{}
				switch resource {
				case "calendar":
					config = SourceConfig(`{"resource":"calendar","calendar_ids":["one","two"],"permission_ttl_seconds":60}`)
				case "drive":
					config = SourceConfig(`{"resource":"drive","folder_id":"folder","permission_ttl_seconds":60}`)
					inventory.Objects = []Item{{Metadata: map[string]any{"file_id": "file"}}}
				}
				require.NoError(t, c.AuthorizeInventory(t.Context(), Request{Mode: ImportModeSource, Config: config}, inventory))
				require.Equal(t, 1, waits)
				switch resource {
				case "gmail":
					require.Equal(t, 1, checks["/profile"])
					require.Equal(t, 2, checks["/labels/INBOX"])
				case "calendar":
					require.Equal(t, 1, checks["/users/me/calendarList/one"])
					require.Equal(t, 2, checks["/users/me/calendarList/two"])
				case "drive":
					require.Equal(t, 1, checks["/files/folder"])
					require.Equal(t, 2, checks["/files/file"])
				}
			})
		}
	}
}
