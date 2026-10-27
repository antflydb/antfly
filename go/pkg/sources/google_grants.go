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
	"net/http"
	"strings"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// GoogleHTTPClient preserves verified service-account scope provenance, which
// token endpoints may omit from their response. Authorized-user grants always
// come from the actual refreshed token, never requested or persisted consent.
func GoogleHTTPClient(ctx context.Context, credentials *google.Credentials) *http.Client {
	client := oauth2.NewClient(ctx, credentials.TokenSource)
	if source, ok := credentials.TokenSource.(googleCredentialTokens); ok && len(source.serviceAccountScopes) > 0 {
		client.Transport = &googleCredentialTransport{Transport: client.Transport.(*oauth2.Transport), serviceAccountScopes: strings.Join(source.serviceAccountScopes, " ")}
	}
	return client
}

type googleCredentialTransport struct {
	*oauth2.Transport
	serviceAccountScopes string
}

type driveContentGrant struct{ allFiles bool }

// Bind authorization metadata to exactly the token whose scopes were proven.
// Unknown/malformed scope proof is transient and cannot renew a lease. No token,
// raw scope text, provider response, or token-info URL enters an error or cache.
func driveContentClient(client *http.Client) (*http.Client, driveContentGrant, error) {
	unknown := func() (*http.Client, driveContentGrant, error) {
		return nil, driveContentGrant{}, &GoogleHTTPError{Status: 502, Reason: "missing_scope_proof"}
	}
	if client == nil {
		return unknown()
	}
	var transport *oauth2.Transport
	serviceScopes := ""
	switch t := client.Transport.(type) {
	case *oauth2.Transport:
		transport = t
	case *googleCredentialTransport:
		transport = t.Transport
		serviceScopes = t.serviceAccountScopes
	default:
		return unknown()
	}
	if transport == nil || transport.Source == nil {
		return unknown()
	}
	token, err := transport.Source.Token()
	if err != nil {
		return nil, driveContentGrant{}, googleRequestError(err)
	}
	if token == nil || !token.Valid() {
		return unknown()
	}
	raw := token.Extra("scope")
	scope, ok := raw.(string)
	if raw == nil && serviceScopes != "" {
		scope, ok = serviceScopes, true
	}
	if !ok || len(scope) == 0 || len(scope) > 16<<10 {
		return unknown()
	}
	parts := strings.Split(scope, " ")
	if len(parts) > 256 {
		return unknown()
	}
	grant := driveContentGrant{}
	perFile := false
	for _, part := range parts {
		if len(part) == 0 || len(part) > 1024 {
			return unknown()
		}
		for _, r := range part {
			if r < 0x21 || r > 0x7e || r == '"' || r == '\\' {
				return unknown()
			}
		}
		switch part {
		case "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.readonly":
			grant.allFiles = true
		case "https://www.googleapis.com/auth/drive.file":
			perFile = true
		}
	}
	if !grant.allFiles && !perFile {
		return nil, grant, &GoogleHTTPError{Status: 403, Reason: "content_scope_required"}
	}
	pinned := *client
	pinned.Transport = &oauth2.Transport{Base: transport.Base, Source: oauth2.StaticTokenSource(token)}
	pinned.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &pinned, grant, nil
}
