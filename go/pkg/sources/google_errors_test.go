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
	"golang.org/x/oauth2"
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

func TestGoogleTokenRefreshFailuresAreSafeAndRetryable(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
		retry  bool
	}{
		{503, "temporarily_unavailable", true}, {429, "temporarily_unavailable", true},
		{400, "temporarily_unavailable", true}, {400, "server_error", true},
		{400, "invalid_grant", false}, {401, "invalid_client", false}, {400, "provider-secret", false},
	} {
		for _, resource := range []string{"gmail", "calendar", "drive", "gcs"} {
			t.Run(fmt.Sprintf("%s/%d/%s", resource, tc.status, tc.code), func(t *testing.T) {
				deadline := time.Now().UTC().Add(10 * time.Minute).Truncate(time.Second)
				calls := 0
				transport := collaborationTransport(func(r *http.Request) (*http.Response, error) {
					calls++
					require.Equal(t, "oauth2.googleapis.com", r.URL.Host)
					body, _ := json.Marshal(map[string]string{"error": tc.code, "error_description": "provider-secret"})
					return &http.Response{StatusCode: tc.status, Header: http.Header{"Content-Type": []string{"application/json"}, "Retry-After": []string{deadline.Format(http.TimeFormat)}}, Request: r, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
				})
				ctx := context.WithValue(t.Context(), oauth2.HTTPClient, &http.Client{Transport: transport})
				config := oauth2.Config{ClientID: "client", ClientSecret: "provider-secret", Endpoint: oauth2.Endpoint{TokenURL: "https://oauth2.googleapis.com/token", AuthStyle: oauth2.AuthStyleInParams}}
				client := oauth2.NewClient(ctx, config.TokenSource(ctx, &oauth2.Token{RefreshToken: "provider-secret"}))
				c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, HTTPClient: client}
				var err error
				switch resource {
				case "gmail", "calendar":
					var out any
					err = c.get(ctx, resource, "/profile", &out)
				case "drive":
					var out any
					err = c.drive().get(ctx, client, "/files/file", &out)
				case "gcs":
					_, err = (&GoogleConnector{ProviderName: ProviderGCS}).read(ctx, client, Request{Config: SourceConfig(`{"bucket":"bucket"}`)}, Item{Path: "file", Revision: "1", Metadata: map[string]any{"bucket": "bucket"}})
				}
				var api *GoogleHTTPError
				require.ErrorAs(t, err, &api)
				require.Equal(t, tc.status, api.Status)
				require.Equal(t, tc.retry, api.Retryable())
				require.Equal(t, tc.retry, (&GoogleConnector{}).IsRetryable(err))
				require.Equal(t, tc.status == 429, IsRateLimited(err))
				require.Equal(t, deadline, api.AntflyRetryAfter())
				require.NotContains(t, err.Error(), "provider-secret")
				var raw *oauth2.RetrieveError
				require.NotErrorAs(t, err, &raw, "raw refresh body must not survive normalization")
				require.Equal(t, 1, calls)
			})
		}
	}
}

func TestGoogleTokenNormalizationPreservesNonOAuthErrors(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, fmt.Errorf("transport failed")} {
		require.Equal(t, err, googleRequestError(err))
	}
	// API error reasons cannot accidentally acquire OAuth retry semantics.
	require.False(t, (&GoogleHTTPError{Status: 400, Reason: "temporarily_unavailable"}).Retryable())
}

type googleTestTokenSource func() (*oauth2.Token, error)

func (f googleTestTokenSource) Token() (*oauth2.Token, error) { return f() }

func TestDriveRefreshFailureBeforeDownloadRetainsCooldown(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprint(native), func(t *testing.T) {
			deadline := time.Now().UTC().Add(10 * time.Minute).Truncate(time.Second)
			tokens, metadata := 0, 0
			source := googleTestTokenSource(func() (*oauth2.Token, error) {
				tokens++
				if tokens <= 2 {
					return &oauth2.Token{AccessToken: "fixture"}, nil
				}
				return nil, &oauth2.RetrieveError{Response: &http.Response{StatusCode: 503, Header: http.Header{"Retry-After": []string{deadline.Format(http.TimeFormat)}}}, ErrorCode: "temporarily_unavailable", Body: []byte("provider-secret"), ErrorDescription: "provider-secret"}
			})
			base := collaborationTransport(func(r *http.Request) (*http.Response, error) {
				metadata++
				require.Empty(t, r.URL.Query().Get("alt"))
				require.False(t, strings.HasSuffix(r.URL.Path, "/export"))
				mime := "text/plain"
				if native {
					mime = "application/vnd.google-apps.document"
				}
				body, _ := json.Marshal(map[string]any{"id": "file", "name": "notes", "version": "1", "size": "5", "mimeType": mime})
				return &http.Response{StatusCode: 200, Header: http.Header{}, Request: r, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
			})
			c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, HTTPClient: &http.Client{Transport: &oauth2.Transport{Source: source, Base: base}}}
			_, err := c.workspaceDriveFile(t.Context(), "file")
			var api *GoogleHTTPError
			require.ErrorAs(t, err, &api)
			require.Equal(t, 503, api.Status)
			require.True(t, api.Retryable())
			require.Equal(t, deadline, api.AntflyRetryAfter())
			require.NotContains(t, err.Error(), "provider-secret")
			require.Equal(t, 2, metadata)
			require.Equal(t, 3, tokens)
		})
	}
}

func TestGoogleJWTRefreshResponseUsesBoundedCodeOnly(t *testing.T) {
	for _, tc := range []struct {
		body  string
		retry bool
	}{
		{`{"error":"temporarily_unavailable","error_description":"provider-secret"}`, true},
		{`{"error":"invalid_grant","error_description":"provider-secret"}`, false},
		{`{"error":"provider-secret"}`, false},
		{`{"error":"temporarily_unavailable","error_description":"` + strings.Repeat("x", maxGoogleErrorBytes) + `"}`, false},
	} {
		raw := &oauth2.RetrieveError{Response: &http.Response{StatusCode: 400}, Body: []byte(tc.body)}
		err := googleRequestError(raw)
		var api *GoogleHTTPError
		require.ErrorAs(t, err, &api)
		require.Equal(t, tc.retry, api.Retryable())
		require.NotContains(t, err.Error(), "provider-secret")
	}
}
