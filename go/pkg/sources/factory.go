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
	"golang.org/x/oauth2"
	"net/http"
	"os"
	"time"
)

// Connection contains secret references, never secret values. Applications can
// provide a different Factory backed by their own credential/identity service.
type Connection struct {
	Provider        string `json:"provider"`
	TokenEnv        string `json:"token_env,omitempty"`
	CredentialsEnv  string `json:"credentials_env,omitempty"`
	AccessKeyEnv    string `json:"access_key_env,omitempty"`
	SecretKeyEnv    string `json:"secret_key_env,omitempty"`
	SessionTokenEnv string `json:"session_token_env,omitempty"`
	S3Endpoint      string `json:"s3_endpoint,omitempty"`
	S3Insecure      bool   `json:"s3_insecure,omitempty"`
}
type Factory func(context.Context, Connection) (Connector, error)

func EnvironmentFactory(ctx context.Context, connection Connection) (Connector, error) {
	secret := func(name string) (string, error) {
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
	case ProviderGoogleDrive, ProviderGCS:
		raw, err := secret(connection.CredentialsEnv)
		if err != nil {
			return nil, err
		}
		auth, err := GoogleCredentials(ctx, connection.Provider, raw)
		if err != nil {
			return nil, err
		}
		client := oauth2.NewClient(ctx, auth.TokenSource)
		client.Timeout = 60 * time.Second
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		return &GoogleConnector{ProviderName: connection.Provider, HTTPClient: client}, nil
	case ProviderGitHub:
		token, err := secret(connection.TokenEnv)
		if err != nil {
			return nil, err
		}
		return &GitHubConnector{Client: &GitHubClient{HTTPClient: &http.Client{Timeout: 60 * time.Second}}, Access: func(context.Context) (GitHubAccess, error) { return GitHubAccess{Token: token}, nil }}, nil
	case ProviderS3:
		access, err := secret(connection.AccessKeyEnv)
		if err != nil {
			return nil, err
		}
		key, err := secret(connection.SecretKeyEnv)
		if err != nil {
			return nil, err
		}
		session := ""
		if connection.SessionTokenEnv != "" {
			session, err = secret(connection.SessionTokenEnv)
			if err != nil {
				return nil, err
			}
		}
		endpoint := connection.S3Endpoint
		if endpoint == "" {
			endpoint = "s3.amazonaws.com"
		}
		client, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(access, key, session), Secure: !connection.S3Insecure})
		if err != nil {
			return nil, err
		}
		return &S3Connector{Client: client}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported provider %q", ErrInvalidInput, connection.Provider)
	}
}
