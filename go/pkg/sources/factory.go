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
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"net/http"
	"os"
	"time"
)

// Connection contains secret references, never secret values. Applications can
// provide a different Factory backed by their own credential/identity service.
type Connection struct {
	// SecretRefs map credential fields (token, credentials, access_key_id,
	// secret_access_key, session_token) to runtime secret-store keys.
	SecretRefs      map[string]string `json:"secret_refs,omitempty"`
	Services        []string          `json:"services,omitempty"`
	Provider        string            `json:"provider"`
	TokenEnv        string            `json:"token_env,omitempty"`
	CredentialsEnv  string            `json:"credentials_env,omitempty"`
	AccessKeyEnv    string            `json:"access_key_env,omitempty"`
	SecretKeyEnv    string            `json:"secret_key_env,omitempty"`
	SessionTokenEnv string            `json:"session_token_env,omitempty"`
	S3Endpoint      string            `json:"s3_endpoint,omitempty"`
	S3Insecure      bool              `json:"s3_insecure,omitempty"`
	LocalRoot       string            `json:"local_root,omitempty"`
}
type Factory func(context.Context, Connection) (Connector, error)

func EnvironmentFactory(ctx context.Context, connection Connection) (Connector, error) {
	return CredentialFactory(nil)(ctx, connection)
}

// CredentialFactory uses the caller's secret manager without mutating process
// environment variables. Explicit env references remain supported for operators.
func CredentialFactory(resolve func(context.Context, string) (string, error)) Factory {
	return func(ctx context.Context, connection Connection) (Connector, error) {
		allowed := map[string]string{"token": connection.TokenEnv, "credentials": connection.CredentialsEnv,
			"access_key_id": connection.AccessKeyEnv, "secret_access_key": connection.SecretKeyEnv,
			"session_token": connection.SessionTokenEnv}
		for field, ref := range connection.SecretRefs {
			env, ok := allowed[field]
			if !ok || ref == "" || env != "" {
				return nil, fmt.Errorf("invalid or ambiguous credential reference")
			}
		}
		secret := func(field, name string) (string, error) {
			if ref := connection.SecretRefs[field]; ref != "" {
				if resolve == nil {
					return "", fmt.Errorf("runtime secret resolver required")
				}
				value, err := resolve(ctx, ref)
				if err != nil || value == "" {
					return "", fmt.Errorf("credential secret %s is unavailable", ref)
				}
				return value, nil
			}
			if name == "" {
				return "", fmt.Errorf("credential environment reference required")
			}
			value, ok := os.LookupEnv(name)
			if !ok || value == "" {
				return "", fmt.Errorf("credential environment variable %s is not set", name)
			}
			return value, nil
		}
		switch connection.Provider {
		case ProviderLocal:
			if connection.TokenEnv != "" || connection.CredentialsEnv != "" || connection.AccessKeyEnv != "" || connection.SecretKeyEnv != "" || connection.SessionTokenEnv != "" || len(connection.SecretRefs) != 0 {
				return nil, fmt.Errorf("%w: local connections take no credentials", ErrInvalidInput)
			}
			root, err := localRootAllowed(connection.LocalRoot, os.Getenv(LocalRootsEnv))
			if err != nil {
				return nil, err
			}
			return &LocalConnector{Root: root}, nil
		case ProviderGoogleWorkspace, ProviderGCS:
			raw, err := secret("credentials", connection.CredentialsEnv)
			if err != nil {
				return nil, err
			}
			auth, err := GoogleCredentialsForServices(ctx, connection.Provider, raw, connection.Services)
			if err != nil {
				return nil, err
			}
			client := GoogleHTTPClient(ctx, auth)
			client.Timeout = 60 * time.Second
			client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
			if connection.Provider == ProviderGoogleWorkspace {
				return &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, HTTPClient: client, Services: connection.Services}, nil
			}
			return &GoogleConnector{ProviderName: connection.Provider, HTTPClient: client}, nil
		case ProviderSlack, ProviderDiscord:
			token, err := secret("token", connection.TokenEnv)
			if err != nil {
				return nil, err
			}
			return &CollaborationConnector{ProviderName: connection.Provider, HTTPClient: &http.Client{Timeout: 60 * time.Second}, AccessToken: func(context.Context) (string, error) { return token, nil }}, nil
		case ProviderGitHub:
			token, err := secret("token", connection.TokenEnv)
			if err != nil {
				return nil, err
			}
			return &GitHubConnector{Client: &GitHubClient{HTTPClient: &http.Client{Timeout: 60 * time.Second}}, Access: func(context.Context) (GitHubAccess, error) { return GitHubAccess{Token: token}, nil }}, nil
		case ProviderS3:
			access, err := secret("access_key_id", connection.AccessKeyEnv)
			if err != nil {
				return nil, err
			}
			key, err := secret("secret_access_key", connection.SecretKeyEnv)
			if err != nil {
				return nil, err
			}
			session := ""
			if connection.SessionTokenEnv != "" || connection.SecretRefs["session_token"] != "" {
				session, err = secret("session_token", connection.SessionTokenEnv)
				if err != nil {
					return nil, err
				}
			}
			endpoint := connection.S3Endpoint
			if endpoint == "" {
				endpoint = "s3.amazonaws.com"
			}
			transport, err := minio.DefaultTransport(!connection.S3Insecure)
			if err != nil {
				return nil, err
			}
			transport.DisableCompression = true
			client, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(access, key, session), Secure: !connection.S3Insecure, Transport: transport})
			if err != nil {
				return nil, err
			}
			connector := &S3Connector{Client: client}
			connector.transport = transport
			return connector, nil
		default:
			return nil, fmt.Errorf("%w: unsupported provider %q", ErrInvalidInput, connection.Provider)
		}
	}

}
