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
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Options supplies request credentials and an optional HTTP client.
type Options struct {
	// HTTPClient is shallow-copied; redirects are always disabled.
	// Nil uses a client with a 30-second timeout.
	HTTPClient *http.Client

	// Token is required and is called once per request.
	Token func(context.Context) (string, error)
}

// NewClientWithOptions targets an explicit HTTPS or loopback HTTP management mount.
func NewClientWithOptions(base string, opts Options) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid source-management endpoint")
	}
	local := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if u.Scheme != "https" && !(u.Scheme == "http" && local) {
		return nil, fmt.Errorf("source-management endpoint requires HTTPS or loopback HTTP")
	}
	if opts.Token == nil {
		return nil, fmt.Errorf("invalid source-management token")
	}
	client := http.Client{Timeout: 30 * time.Second}
	if opts.HTTPClient != nil {
		client = *opts.HTTPClient
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{base: strings.TrimRight(base, "/"), token: opts.Token, http: &client}, nil
}

// TransportError reports a failure without exposing URLs or credentials.
type TransportError struct {
	Timeout bool
	cause   error
}

func (e *TransportError) Error() string {
	if e.Timeout {
		return "source-management transport timed out"
	}
	return "source-management transport failed"
}

// Unwrap exposes only the request context's terminal error, never transport details.
func (e *TransportError) Unwrap() error { return e.cause }
