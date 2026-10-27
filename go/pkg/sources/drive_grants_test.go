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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func withDriveScopes(client *http.Client, scope any) *http.Client {
	copy := *client
	token := (&oauth2.Token{AccessToken: "fixture-drive-token", TokenType: "Bearer"}).WithExtra(map[string]any{"scope": scope})
	copy.Transport = &oauth2.Transport{Base: client.Transport, Source: oauth2.StaticTokenSource(token)}
	return &copy
}

func TestDriveGrantsAndPerFileAuthorization(t *testing.T) {
	const prefix = "https://www.googleapis.com/auth/"
	for _, tc := range []struct {
		name          string
		scope         any
		app, download bool
		status        int
	}{
		{"readonly", prefix + "drive.readonly", false, true, 0},
		{"full drive", prefix + "drive", false, true, 0},
		{"per file authorized", prefix + "drive.file", true, true, 0},
		{"per file not authorized", prefix + "drive.file", false, true, 403},
		{"mixed metadata and per file", prefix + "drive.file " + prefix + "drive.metadata.readonly", false, true, 403},
		{"metadata only", prefix + "drive.metadata.readonly", false, true, 403},
		{"file download revoked", prefix + "drive.readonly", true, false, 400},
		{"missing proof", nil, true, true, 502},
		{"malformed proof", 42, true, true, 502},
		{"empty proof", "", true, true, 502},
		{"oversized proof", strings.Repeat("x", (16<<10)+1), true, true, 502},
		{"control character", prefix + "drive.readonly\n", true, true, 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads := 0
			client := withDriveScopes(fixtureClient(t, func(r *http.Request) (int, any) {
				reads++
				require.Equal(t, "Bearer fixture-drive-token", r.Header.Get("Authorization"))
				require.Equal(t, "/files/file", r.URL.Path)
				require.Empty(t, r.URL.Query().Get("alt"), "authorization must not download content")
				require.Contains(t, r.URL.Query().Get("fields"), "isAppAuthorized")
				return 200, map[string]any{"id": "file", "version": "v1", "isAppAuthorized": tc.app, "capabilities": map[string]any{"canDownload": tc.download}}
			}), tc.scope)
			c := &GoogleConnector{ProviderName: ProviderGoogleDrive, HTTPClient: client, APIBase: "https://fixture.test"}
			err := c.ValidateStagedSource(t.Context(), Request{}, Item{Metadata: map[string]any{"file_id": "file"}})
			if tc.status == 0 {
				require.NoError(t, err)
				require.Equal(t, 1, reads)
			} else if tc.status == 400 {
				require.ErrorIs(t, err, ErrInvalidInput)
			} else {
				var api *GoogleHTTPError
				require.ErrorAs(t, err, &api)
				require.Equal(t, tc.status, api.Status)
				require.Equal(t, tc.status >= 500, api.Retryable())
			}
		})
	}
}

type rotatingDriveTokens struct{ calls int }

func (s *rotatingDriveTokens) Token() (*oauth2.Token, error) {
	s.calls++
	scope := "https://www.googleapis.com/auth/drive.readonly"
	if s.calls > 1 {
		scope = "https://www.googleapis.com/auth/drive.metadata.readonly"
	}
	return (&oauth2.Token{AccessToken: scope, TokenType: "Bearer"}).WithExtra(map[string]any{"scope": scope}), nil
}
func TestDriveMetadataUsesExactlyTheProvenToken(t *testing.T) {
	source := &rotatingDriveTokens{}
	base := fixtureClient(t, func(r *http.Request) (int, any) {
		require.Equal(t, "Bearer https://www.googleapis.com/auth/drive.readonly", r.Header.Get("Authorization"))
		return 200, map[string]any{"id": "file", "capabilities": map[string]any{"canDownload": true}}
	})
	client := *base
	client.Transport = &oauth2.Transport{Base: base.Transport, Source: source}
	c := &GoogleConnector{ProviderName: ProviderGoogleDrive, HTTPClient: &client, APIBase: "https://fixture.test"}
	require.NoError(t, c.ValidateStagedSource(t.Context(), Request{}, Item{Metadata: map[string]any{"file_id": "file"}}))
	require.Equal(t, 1, source.calls, "the permission metadata request must not acquire a different token")
	err := c.ValidateStagedSource(t.Context(), Request{}, Item{Metadata: map[string]any{"file_id": "file"}})
	var api *GoogleHTTPError
	require.ErrorAs(t, err, &api)
	require.Equal(t, 403, api.Status)
}

func TestServiceAccountGrantProvenanceAndExplicitScopeResponse(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	encodedKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	secret, err := json.Marshal(map[string]string{"type": "service_account", "client_email": "fixture@example.iam.gserviceaccount.com", "private_key": string(encodedKey), "token_uri": "https://oauth2.googleapis.com/token"})
	require.NoError(t, err)
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "omitted scope", true: "explicit downscope"}[explicit], func(t *testing.T) {
			client := fixtureClient(t, func(r *http.Request) (int, any) {
				if r.URL.Path == "/token" {
					require.NoError(t, r.ParseForm())
					assertion := strings.Split(r.Form.Get("assertion"), ".")
					require.Len(t, assertion, 3)
					claim, err := base64.RawURLEncoding.DecodeString(assertion[1])
					require.NoError(t, err)
					var payload map[string]any
					require.NoError(t, json.Unmarshal(claim, &payload))
					require.Equal(t, "https://www.googleapis.com/auth/drive.readonly", payload["scope"])
					body := map[string]any{"access_token": "service-account-token", "token_type": "Bearer", "expires_in": 3600}
					if explicit {
						body["scope"] = "https://www.googleapis.com/auth/drive.metadata.readonly"
					}
					return 200, body
				}
				require.Equal(t, "Bearer service-account-token", r.Header.Get("Authorization"))
				return 200, map[string]any{"id": "file", "capabilities": map[string]any{"canDownload": true}}
			})
			ctx := context.WithValue(t.Context(), oauth2.HTTPClient, client)
			credentials, err := GoogleCredentialsForServices(ctx, ProviderGoogleWorkspace, string(secret), []string{"drive"})
			require.NoError(t, err)
			c := &GoogleConnector{ProviderName: ProviderGoogleDrive, HTTPClient: GoogleHTTPClient(ctx, credentials), APIBase: "https://fixture.test"}
			err = c.ValidateStagedSource(ctx, Request{}, Item{Metadata: map[string]any{"file_id": "file"}})
			if explicit {
				var api *GoogleHTTPError
				require.ErrorAs(t, err, &api)
				require.Equal(t, 403, api.Status)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
