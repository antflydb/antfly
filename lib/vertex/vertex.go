// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package vertex

import (
	"fmt"
	"net/http"
	"os"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"google.golang.org/api/option"
)

// CloudPlatformScope is the standard Google Cloud Platform OAuth2 scope.
const CloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

// LoadCredentials resolves Google Cloud credentials using the standard priority:
//  1. credentialsPath (explicit file path, if non-nil and non-empty)
//  2. GOOGLE_APPLICATION_CREDENTIALS env var
//  3. Application Default Credentials (ADC)
func LoadCredentials(credentialsPath *string, scopes []string) (*auth.Credentials, error) {
	detectOpts := &credentials.DetectOptions{
		Scopes: scopes,
	}

	if credentialsPath != nil && *credentialsPath != "" {
		creds, err := credentials.NewCredentialsFromFile(credentials.ServiceAccount, *credentialsPath, detectOpts)
		if err != nil {
			return nil, fmt.Errorf("loading credentials from %s: %w", *credentialsPath, err)
		}
		return creds, nil
	}

	if envPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); envPath != "" {
		creds, err := credentials.NewCredentialsFromFile(credentials.ServiceAccount, envPath, detectOpts)
		if err != nil {
			return nil, fmt.Errorf("loading credentials from GOOGLE_APPLICATION_CREDENTIALS: %w", err)
		}
		return creds, nil
	}

	creds, err := credentials.DetectDefault(detectOpts)
	if err != nil {
		return nil, fmt.Errorf("detecting default credentials: %w", err)
	}
	return creds, nil
}

// AuthClientOption returns an option.ClientOption that provides the given credentials
// to a Google Cloud client library.
func AuthClientOption(creds *auth.Credentials) option.ClientOption {
	return option.WithAuthCredentials(creds)
}

// AuthHTTPClient creates an HTTP client that automatically adds auth tokens to requests.
func AuthHTTPClient(creds *auth.Credentials) *http.Client {
	return &http.Client{
		Transport: &authTransport{
			base:  http.DefaultTransport,
			creds: creds,
		},
	}
}

// authTransport is an http.RoundTripper that adds auth tokens to requests.
type authTransport struct {
	base  http.RoundTripper
	creds *auth.Credentials
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := t.creds.Token(req.Context())
	if err != nil {
		return nil, fmt.Errorf("getting token: %w", err)
	}
	reqClone := req.Clone(req.Context())
	reqClone.Header.Set("Authorization", "Bearer "+token.Value)
	return t.base.RoundTrip(reqClone)
}
