/*
Copyright 2026 The Antfly Contributors

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

//go:generate go tool oapi-codegen --config=cfg.yaml ../../../openapi.yaml

package sdk

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/antflydb/antfly/go/pkg/libaf/json"
	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

// Config configures the consolidated Antfly SDK.
type Config struct {
	// BaseURL is the Antfly server URL without a product prefix, for example
	// http://localhost:8080.
	BaseURL string
	// InferenceBaseURL optionally points inference operations at a different server.
	// When empty, inference uses BaseURL.
	InferenceBaseURL string
	// HTTPClient is shared by Antfly and inference clients when provided.
	HTTPClient *http.Client
}

// Client is the consolidated SDK entrypoint. Antfly operations are exposed via
// Antfly and ML operations via Inference.
type Client struct {
	antfly    *AntflyClient
	inference *InferenceClient
}

// NewClient creates a consolidated SDK client. The generated client uses the
// public contract rooted at /db/v1, /auth/v1, and /ai/v1.
func NewClient(config Config) (*Client, error) {
	baseURL := strings.TrimRight(config.BaseURL, "/")
	antfly, err := NewAntflyClient(baseURL, config.HTTPClient)
	if err != nil {
		return nil, fmt.Errorf("creating antfly client: %w", err)
	}

	inferenceBaseURL := strings.TrimRight(config.InferenceBaseURL, "/")
	if inferenceBaseURL == "" {
		inferenceBaseURL = baseURL
	}
	inference, err := NewInferenceClient(inferenceBaseURL, config.HTTPClient)
	if err != nil {
		return nil, fmt.Errorf("creating inference client: %w", err)
	}

	return &Client{antfly: antfly, inference: inference}, nil
}

// Antfly returns the Antfly product API surface.
func (c *Client) Antfly() *AntflyClient {
	return c.antfly
}

// Inference returns the Antfly inference API surface.
func (c *Client) Inference() *InferenceClient {
	return c.inference
}

// AntflyClient is a client for interacting with the Antfly API
type AntflyClient struct {
	client *oapi.Client
}

// NewAntflyClient creates a new Antfly client with an HTTP client.
func NewAntflyClient(baseURL string, httpClient *http.Client) (*AntflyClient, error) {
	client, err := oapi.NewClient(NormalizeBaseURL(baseURL), oapi.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}
	return &AntflyClient{
		client: client,
	}, nil
}

// NewAntflyClientWithOptions creates a new Antfly client with variadic options.
// Use with WithBasicAuth, WithApiKey, or WithToken for authentication.
func NewAntflyClientWithOptions(baseURL string, opts ...oapi.ClientOption) (*AntflyClient, error) {
	client, err := oapi.NewClient(NormalizeBaseURL(baseURL), opts...)
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

// WithToken returns a RequestEditorFn that adds token authentication.
func WithToken(token string) oapi.RequestEditorFn {
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
