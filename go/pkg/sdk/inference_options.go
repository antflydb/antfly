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

package sdk

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

const (
	maxInferenceJSONResponseBytes   int64 = 16 << 20
	maxInferenceBinaryResponseBytes int64 = 64 << 20
)

type inferenceResponseLimitDoer struct {
	next oapi.HttpRequestDoer
}

func (d inferenceResponseLimitDoer) Do(req *http.Request) (*http.Response, error) {
	resp, err := d.next.Do(req)
	if err != nil || resp == nil || resp.Body == nil || !isInferenceAPIPath(req.URL.Path) {
		return resp, err
	}
	if maxBytes := inferenceResponseLimit(req, resp); maxBytes > 0 {
		resp.Body = &inferenceLimitedBody{
			ReadCloser: http.MaxBytesReader(nil, resp.Body, maxBytes),
			maxBytes:   maxBytes,
		}
	}
	return resp, nil
}

func isInferenceAPIPath(path string) bool {
	for _, component := range []string{"/ai/v1", "/ml/v1"} {
		if strings.HasSuffix(path, component) || strings.Contains(path, component+"/") {
			return true
		}
	}
	return false
}

func inferenceResponseLimit(req *http.Request, resp *http.Response) int64 {
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return maxErrorResponseBytes
	}
	contentType := inferenceMediaType(resp.Header.Get("Content-Type"))
	if contentType == "" {
		return maxInferenceJSONResponseBytes
	}
	switch contentType {
	case "text/event-stream":
		if requestAcceptsEventStream(req) {
			return 0
		}
		return maxInferenceJSONResponseBytes
	case "application/octet-stream", "application/x-sparse-vectors":
		return maxInferenceBinaryResponseBytes
	default:
		return maxInferenceJSONResponseBytes
	}
}

func requestAcceptsEventStream(req *http.Request) bool {
	if req == nil {
		return false
	}
	for _, value := range strings.Split(req.Header.Get("Accept"), ",") {
		if inferenceMediaType(strings.TrimSpace(value)) == "text/event-stream" {
			return true
		}
	}
	return false
}

func inferenceMediaType(contentType string) string {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return ""
	}
	return strings.ToLower(mediaType)
}

type inferenceLimitedBody struct {
	io.ReadCloser
	maxBytes int64
}

func (b *inferenceLimitedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return n, fmt.Errorf("inference response exceeded %d bytes", b.maxBytes)
	}
	return n, err
}

func withInferenceResponseLimits() oapi.ClientOption {
	return func(client *oapi.Client) error {
		if client.Client == nil {
			client.Client = http.DefaultClient
		}
		client.Client = inferenceResponseLimitDoer{next: client.Client}
		return nil
	}
}

// NewInferenceClientWithOptions creates an inference client with generated
// client options such as WithHTTPClient and WithRequestEditorFn.
func NewInferenceClientWithOptions(baseURL string, opts ...oapi.ClientOption) (*InferenceClient, error) {
	apiURL := normalizeInferenceBaseURL(baseURL)
	opts = append(append([]oapi.ClientOption(nil), opts...), withInferenceResponseLimits())
	client, err := oapi.NewClientWithResponses(apiURL, opts...)
	if err != nil {
		return nil, err
	}
	return &InferenceClient{client: client, baseURL: apiURL}, nil
}
