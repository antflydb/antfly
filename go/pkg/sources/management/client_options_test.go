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

package management

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientOptionsTokenPerRequest(t *testing.T) {
	headers := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Get("Authorization")
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "request-context")
	calls := 0
	client, err := NewClientWithOptions(server.URL, Options{Token: func(got context.Context) (string, error) {
		if got.Value(key{}) != "request-context" {
			t.Error("token callback lost request context")
		}
		calls++
		return fmt.Sprintf("token-%d", calls), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("token called at construction: %d", calls)
	}
	for i := 1; i <= 2; i++ {
		if _, err := client.Capabilities(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-headers:
			if want := fmt.Sprintf("Bearer token-%d", i); got != want {
				t.Fatalf("Authorization = %q, want %q", got, want)
			}
		default:
			t.Fatal("request not sent")
		}
	}
	if calls != 2 {
		t.Fatalf("token calls = %d, want 2", calls)
	}
}

func TestClientOptionsRejectsTokenBeforeSending(t *testing.T) {
	tokenErr := errors.New("credential provider unavailable")
	for _, tc := range []struct {
		name, token string
		err         error
		want        string
	}{
		{"callback error", "", tokenErr, "source-management token: credential provider unavailable"},
		{"CRLF", "a\r\nb", nil, "invalid source-management token"},
		{"empty", "", nil, "invalid source-management token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); fmt.Fprint(w, `{}`) }))
			defer server.Close()
			client, err := NewClientWithOptions(server.URL, Options{Token: func(context.Context) (string, error) { return tc.token, tc.err }})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Capabilities(context.Background())
			if err == nil || err.Error() != tc.want {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("does not wrap token error: %v", err)
			}
			if got := requests.Load(); got != 0 {
				t.Fatalf("sent %d requests with invalid token", got)
			}
		})
	}
}

func TestClientOptionsHonorsHTTPTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()
	const token = "private-timeout-token"
	httpClient := &http.Client{Timeout: 50 * time.Millisecond}
	client, err := NewClientWithOptions(server.URL, Options{HTTPClient: httpClient, Token: func(context.Context) (string, error) { return token, nil }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Capabilities(context.Background())
	var transport *TransportError
	if !errors.As(err, &transport) || !transport.Timeout {
		t.Fatalf("error = %T %v, want timeout TransportError", err, err)
	}
	if err.Error() != "source-management transport timed out" || strings.Contains(err.Error(), server.URL) || strings.Contains(err.Error(), token) {
		t.Fatalf("unsafe or incorrect transport error: %q", err.Error())
	}
	if errors.Unwrap(transport) != nil {
		t.Fatalf("client timeout exposed non-context cause: %v", errors.Unwrap(transport))
	}
	if httpClient.Timeout != 50*time.Millisecond {
		t.Fatal("caller timeout mutated")
	}
}

type optionsRoundTripper func(*http.Request) (*http.Response, error)

func (f optionsRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestClientOptionsCopiesClientAndRejectsRedirects(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/capabilities" {
			w.Header().Set("Location", "/redirected")
			w.WriteHeader(http.StatusFound)
			return
		}
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()
	redirectErr := errors.New("caller redirect policy")
	redirects, transports := 0, 0
	httpClient := &http.Client{Timeout: time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { redirects++; return redirectErr },
		Transport:     optionsRoundTripper(func(r *http.Request) (*http.Response, error) { transports++; return http.DefaultTransport.RoundTrip(r) }),
	}
	client, err := NewClientWithOptions(server.URL, Options{HTTPClient: httpClient, Token: func(context.Context) (string, error) { return "token", nil }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Capabilities(context.Background())
	var failure *HTTPError
	if !errors.As(err, &failure) || failure.Status != http.StatusFound {
		t.Fatalf("error = %v, want HTTP 302", err)
	}
	if requests.Load() != 1 || transports != 1 || redirects != 0 {
		t.Fatalf("requests=%d transports=%d redirects=%d", requests.Load(), transports, redirects)
	}
	if httpClient.CheckRedirect == nil {
		t.Fatal("caller redirect policy cleared")
	}
	if got := httpClient.CheckRedirect(nil, nil); got != redirectErr || redirects != 1 {
		t.Fatalf("caller redirect policy mutated: %v", got)
	}
}

func TestClientOptionsContextDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	client, err := NewClientWithOptions(server.URL, Options{Token: func(context.Context) (string, error) { return "secret-deadline-token", nil }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = client.Capabilities(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error does not wrap context deadline: %v", err)
	}
	var transport *TransportError
	if !errors.As(err, &transport) || !transport.Timeout {
		t.Fatalf("error = %T %v, want timeout TransportError", err, err)
	}
	if strings.Contains(err.Error(), server.URL) || strings.Contains(err.Error(), "secret-deadline-token") {
		t.Fatalf("unsafe error: %v", err)
	}
}

func TestClientOptionsValidation(t *testing.T) {
	for _, endpoint := range []string{"http://remote.example/sources/v1", "https://user:secret@example.com/sources/v1", "https://example.com?token=secret", "https://example.com/#fragment", "://invalid"} {
		t.Run(endpoint, func(t *testing.T) {
			_, got := NewClientWithOptions(endpoint, Options{Token: func(context.Context) (string, error) { return "token", nil }})
			_, want := NewClient(endpoint, "token")
			if got == nil || want == nil || got.Error() != want.Error() {
				t.Fatalf("validation = %v, want %v", got, want)
			}
		})
	}
	t.Run("nil token", func(t *testing.T) {
		if _, err := NewClientWithOptions("https://example.com", Options{}); err == nil || err.Error() != "invalid source-management token" {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestTransportErrorContract(t *testing.T) {
	for _, tc := range []struct {
		timeout bool
		cause   error
		message string
	}{
		{false, nil, "source-management transport failed"},
		{true, nil, "source-management transport timed out"},
		{true, context.DeadlineExceeded, "source-management transport timed out"},
		{false, context.Canceled, "source-management transport failed"},
	} {
		t.Run(fmt.Sprintf("timeout=%t/cause=%v", tc.timeout, tc.cause), func(t *testing.T) {
			err := &TransportError{Timeout: tc.timeout, cause: tc.cause}
			if got := err.Error(); got != tc.message {
				t.Errorf("Error = %q, want %q", got, tc.message)
			}
			if got := err.Unwrap(); got != tc.cause {
				t.Errorf("Unwrap = %v, want %v", got, tc.cause)
			}
		})
	}
}
