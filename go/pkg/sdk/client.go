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
	"strconv"
	"strings"

	"github.com/antflydb/antfly/go/pkg/libaf/json"
	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

const maxErrorResponseBytes int64 = 1 << 20

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
	// RequestEditors are applied to both Antfly and inference requests. Use
	// WithBasicAuth, WithApiKey, or WithToken for authentication.
	RequestEditors []oapi.RequestEditorFn
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
	antflyOptions := make([]oapi.ClientOption, 0, len(config.RequestEditors)+1)
	inferenceOptions := make([]oapi.ClientOption, 0, len(config.RequestEditors)+1)
	if config.HTTPClient != nil {
		antflyOptions = append(antflyOptions, oapi.WithHTTPClient(config.HTTPClient))
		inferenceOptions = append(inferenceOptions, oapi.WithHTTPClient(config.HTTPClient))
	}
	for _, editor := range config.RequestEditors {
		if editor == nil {
			continue
		}
		antflyOptions = append(antflyOptions, oapi.WithRequestEditorFn(editor))
		inferenceOptions = append(inferenceOptions, oapi.WithRequestEditorFn(editor))
	}

	antfly, err := NewAntflyClientWithOptions(baseURL, antflyOptions...)
	if err != nil {
		return nil, fmt.Errorf("creating antfly client: %w", err)
	}

	inferenceBaseURL := strings.TrimRight(config.InferenceBaseURL, "/")
	if inferenceBaseURL == "" {
		inferenceBaseURL = baseURL
	}
	inference, err := NewInferenceClientWithOptions(inferenceBaseURL, inferenceOptions...)
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

// NewAntflyClientWithToken creates a new Antfly client that sends a bearer token
// on every request. This is convenient for Antfly Cloud API tokens.
func NewAntflyClientWithToken(baseURL string, httpClient *http.Client, token string) (*AntflyClient, error) {
	opts := []oapi.ClientOption{oapi.WithHTTPClient(httpClient)}
	if strings.TrimSpace(token) != "" {
		opts = append(opts, oapi.WithRequestEditorFn(WithToken(strings.TrimSpace(token))))
	}
	return NewAntflyClientWithOptions(baseURL, opts...)
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

// HierarchyCursorStaleError reports that a hierarchy traversal cursor belongs
// to an older source-artifact revision. Retrying the same cursor cannot
// succeed; callers should restart the traversal without SearchAfter.
type HierarchyCursorStaleError struct {
	StatusCode     int
	Code           string
	Message        string
	Action         string
	RestartWithout string
	Retryable      bool
}

func (e *HierarchyCursorStaleError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Code
}

// QueryTemporarilyUnavailableError reports a retryable query dependency or
// read-availability failure. RetryAfterSeconds is zero when the server did not
// provide a valid positive Retry-After value.
type QueryTemporarilyUnavailableError struct {
	StatusCode        int
	Code              string
	Message           string
	Retryable         bool
	RetryAfterSeconds int
}

func (e *QueryTemporarilyUnavailableError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Code
}

// StorageResourceExhaustedError reports a retryable storage admission failure.
// RetryAfterMS is the server's precise delay; RetryAfterSeconds is retained
// from the HTTP header for schedulers that operate in whole seconds.
type StorageResourceExhaustedError struct {
	StatusCode        int
	Code              string
	Message           string
	Retryable         bool
	RetryAfterMS      int
	RetryAfterSeconds int
}

func (e *StorageResourceExhaustedError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Code
}

type structuredAPIErrorResponse struct {
	Status         int    `json:"status"`
	Error          string `json:"error"`
	Code           string `json:"code"`
	Message        string `json:"message"`
	Action         string `json:"action"`
	RestartWithout string `json:"restart_without"`
	Retryable      *bool  `json:"retryable"`
	RetryAfterMS   int    `json:"retry_after_ms"`
}

func queryRetryAfterSeconds(header http.Header) int {
	seconds, err := strconv.Atoi(header.Get("Retry-After"))
	if err != nil || seconds <= 0 {
		return 0
	}
	return seconds
}

func isQueryTemporarilyUnavailableCode(code string) bool {
	switch code {
	case "doc_identity_unavailable",
		"read_requires_primary",
		"standby_read_unavailable",
		"storage_read_temporarily_unavailable",
		"index_rebuilding",
		"query_embedding_temporarily_unavailable":
		return true
	default:
		return false
	}
}

func readLimitedBody(r io.Reader, maxBytes int64) ([]byte, bool, error) {
	if maxBytes <= 0 {
		body, err := io.ReadAll(r)
		return body, false, err
	}

	body, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(body)) <= maxBytes {
		return body, false, nil
	}
	return body[:maxBytes], true, nil
}

func readErrorResponse(resp *http.Response) error {
	respBody, truncated, err := readLimitedBody(resp.Body, maxErrorResponseBytes)
	if err != nil {
		return fmt.Errorf("reading http response: %w", err)
	}

	// Preserve actionable query failures as typed errors so convenience-client
	// callers do not need to parse wire JSON or switch to the generated client.
	var errResp structuredAPIErrorResponse
	parsedStructuredError := json.Unmarshal(respBody, &errResp) == nil
	if parsedStructuredError {
		if resp.StatusCode == http.StatusTooManyRequests &&
			errResp.Code == "storage_resource_exhausted" &&
			errResp.Retryable != nil && *errResp.Retryable {
			retryAfterSeconds := queryRetryAfterSeconds(resp.Header)
			retryAfterMS := errResp.RetryAfterMS
			if retryAfterMS <= 0 && retryAfterSeconds > 0 {
				retryAfterMS = retryAfterSeconds * 1000
			}
			return &StorageResourceExhaustedError{
				StatusCode:        resp.StatusCode,
				Code:              errResp.Code,
				Message:           errResp.Message,
				Retryable:         true,
				RetryAfterMS:      retryAfterMS,
				RetryAfterSeconds: retryAfterSeconds,
			}
		}
		if resp.StatusCode == http.StatusConflict &&
			errResp.Status == http.StatusConflict &&
			errResp.Error == "hierarchy_cursor_stale" &&
			errResp.Action == "restart_hierarchy_traversal" &&
			errResp.RestartWithout == "search_after" &&
			errResp.Retryable != nil && !*errResp.Retryable {
			return &HierarchyCursorStaleError{
				StatusCode:     resp.StatusCode,
				Code:           errResp.Error,
				Message:        errResp.Message,
				Action:         errResp.Action,
				RestartWithout: errResp.RestartWithout,
				Retryable:      false,
			}
		}
		if resp.StatusCode == http.StatusServiceUnavailable &&
			errResp.Retryable != nil && *errResp.Retryable &&
			isQueryTemporarilyUnavailableCode(errResp.Code) {
			return &QueryTemporarilyUnavailableError{
				StatusCode:        resp.StatusCode,
				Code:              errResp.Code,
				Message:           errResp.Message,
				Retryable:         true,
				RetryAfterSeconds: queryRetryAfterSeconds(resp.Header),
			}
		}
	}
	if parsedStructuredError && errResp.Error != "" {
		return &APIError{
			StatusCode: resp.StatusCode,
			Message:    errResp.Error,
		}
	}

	// Fallback for non-JSON responses
	message := string(respBody)
	if truncated {
		message = fmt.Sprintf("%s (response body exceeded %d bytes)", message, maxErrorResponseBytes)
	}
	return &APIError{
		StatusCode: resp.StatusCode,
		Message:    message,
	}
}
