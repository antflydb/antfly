/*
Copyright 2025 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//go:generate go tool oapi-codegen --config=cfg.yaml ../../openapi.yaml

package client

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"

	"github.com/antflydb/antfly/pkg/client/oapi"
	"github.com/antflydb/antfly/pkg/libaf/json"
)

// AntflyClient is a client for interacting with the Antfly API
type AntflyClient struct {
	client *oapi.Client
}

// NewAntflyClient creates a new Antfly client with an HTTP client.
func NewAntflyClient(baseURL string, httpClient *http.Client) (*AntflyClient, error) {
	client, err := oapi.NewClient(baseURL, oapi.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}
	return &AntflyClient{
		client: client,
	}, nil
}

// NewAntflyClientWithOptions creates a new Antfly client with variadic options.
// Use with WithBasicAuth, WithApiKey, or WithBearerToken for authentication.
func NewAntflyClientWithOptions(baseURL string, opts ...oapi.ClientOption) (*AntflyClient, error) {
	client, err := oapi.NewClient(baseURL, opts...)
	if err != nil {
		return nil, err
	}
	return &AntflyClient{
		client: client,
	}, nil
}

// WithBasicAuth returns a RequestEditorFn that adds HTTP Basic Authentication.
func WithBasicAuth(username, password string) oapi.RequestEditorFn {
	encoded := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	return func(_ context.Context, req *http.Request) error {
		req.Header.Set("Authorization", "Basic "+encoded)
		return nil
	}
}

// WithApiKey returns a RequestEditorFn that adds API Key authentication.
// The credential is sent as: Authorization: ApiKey base64(keyID:keySecret)
func WithApiKey(keyID, keySecret string) oapi.RequestEditorFn {
	encoded := base64.StdEncoding.EncodeToString([]byte(keyID + ":" + keySecret))
	return func(_ context.Context, req *http.Request) error {
		req.Header.Set("Authorization", "ApiKey "+encoded)
		return nil
	}
}

// WithBearerToken returns a RequestEditorFn that adds Bearer token authentication.
// The token should be base64(keyID:keySecret) for Antfly API keys, or an opaque token
// from a proxy.
func WithBearerToken(token string) oapi.RequestEditorFn {
	return func(_ context.Context, req *http.Request) error {
		req.Header.Set("Authorization", "Bearer "+token)
		return nil
	}
}

// APIError represents a structured error response from the Antfly API.
// Callers can use errors.As to extract it:
//
//	var apiErr *client.APIError
//	if errors.As(err, &apiErr) {
//	    fmt.Println(apiErr.StatusCode, apiErr.Message)
//	}
type APIError struct {
	// StatusCode is the HTTP status code returned by the server.
	StatusCode int
	// Message is the error message from the server.
	Message string
}

func (e *APIError) Error() string {
	return e.Message
}

func readErrorResponse(resp *http.Response) error {
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading http response: %w", err)
	}

	// Try to parse as JSON error response
	var errResp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(respBody, &errResp); err == nil && errResp.Error != "" {
		return &APIError{
			StatusCode: resp.StatusCode,
			Message:    errResp.Error,
		}
	}

	// Fallback for non-JSON responses
	return &APIError{
		StatusCode: resp.StatusCode,
		Message:    string(respBody),
	}
}
