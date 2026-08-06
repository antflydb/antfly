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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

const maxGenerateSSEEventBytes = 16 << 20

// InferenceAPIError is a structured error returned by the inference API.
type InferenceAPIError struct {
	StatusCode int
	Code       string
	Message    string
	// Retryable is nil when the server did not classify the error.
	Retryable *bool
	prefix    string
}

func (e *InferenceAPIError) Error() string {
	if e.prefix != "" {
		return fmt.Sprintf("%s: %s", e.prefix, e.Message)
	}
	return fmt.Sprintf("inference request failed (%d): %s", e.StatusCode, e.Message)
}

func newInferenceAPIError(statusCode int, prefix string, payload *oapi.InferenceError) *InferenceAPIError {
	return &InferenceAPIError{
		StatusCode: statusCode,
		Code:       payload.Error,
		Message:    inferenceErrorDetail(payload),
		Retryable:  payload.Retryable,
		prefix:     prefix,
	}
}

func inferenceErrorPrefix(statusCode int) string {
	switch statusCode {
	case http.StatusBadRequest:
		return "bad request"
	case http.StatusUnauthorized:
		return "unauthorized"
	case http.StatusForbidden:
		return "content not allowed"
	case http.StatusNotFound:
		return "model not found"
	case http.StatusRequestEntityTooLarge:
		return "content too large"
	case http.StatusInternalServerError:
		return "server error"
	case http.StatusBadGateway:
		return "content fetch failed"
	case http.StatusServiceUnavailable:
		return "service unavailable"
	case http.StatusInsufficientStorage:
		return "memory budget exceeded"
	default:
		return ""
	}
}

func inferenceResponseError(statusCode int, body []byte) error {
	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		return nil
	}

	truncated := int64(len(body)) > maxErrorResponseBytes
	if truncated {
		body = body[:maxErrorResponseBytes]
	}
	var payload oapi.InferenceError
	if json.Unmarshal(body, &payload) == nil && (payload.Error != "" || payload.Message != "") {
		return newInferenceAPIError(statusCode, inferenceErrorPrefix(statusCode), &payload)
	}

	message := string(body)
	if truncated {
		message += fmt.Sprintf(" (response body exceeded %d bytes)", maxErrorResponseBytes)
	}
	if message == "" {
		message = http.StatusText(statusCode)
	}
	return &InferenceAPIError{
		StatusCode: statusCode,
		Message:    message,
		prefix:     inferenceErrorPrefix(statusCode),
	}
}

// GenerateStream generates text and invokes onChunk for each SSE completion chunk.
// It returns only after the server sends [DONE], the context is cancelled, or an
// HTTP, stream, decode, or callback error occurs.
func (c *InferenceClient) GenerateStream(
	ctx context.Context,
	req oapi.InferenceGenerateRequest,
	onChunk func(oapi.InferenceGenerateChunk) error,
) error {
	if onChunk == nil {
		return errors.New("generation stream callback is required")
	}

	req.Stream = true
	resp, err := c.client.GenerateContent(ctx, req, func(_ context.Context, request *http.Request) error {
		request.Header.Set("Accept", "text/event-stream")
		return nil
	})
	if err != nil {
		return fmt.Errorf("sending generation stream request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return readInferenceAPIError(resp)
	}

	contentType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || contentType != "text/event-stream" {
		body, truncated, readErr := readLimitedBody(resp.Body, maxErrorResponseBytes)
		if readErr != nil {
			return fmt.Errorf("reading unexpected generation response: %w", readErr)
		}
		detail := string(body)
		if truncated {
			detail += " (truncated)"
		}
		return fmt.Errorf("generation stream returned content type %q: %s", resp.Header.Get("Content-Type"), detail)
	}

	done := false
	err = scanSSE(resp.Body, func(event, data string) (bool, error) {
		if event == "error" {
			return false, fmt.Errorf("generation stream failed: %s", data)
		}
		if strings.TrimSpace(data) == "[DONE]" {
			done = true
			return false, nil
		}

		var chunk oapi.InferenceGenerateChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return false, fmt.Errorf("decoding generation stream event: %w", err)
		}
		if err := onChunk(chunk); err != nil {
			return false, fmt.Errorf("generation stream callback: %w", err)
		}
		return true, nil
	})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	if !done {
		return fmt.Errorf("generation stream ended before [DONE]: %w", io.ErrUnexpectedEOF)
	}
	return nil
}

func readInferenceAPIError(resp *http.Response) error {
	body, truncated, err := readLimitedBody(resp.Body, maxErrorResponseBytes)
	if err != nil {
		return fmt.Errorf("reading inference error response: %w", err)
	}

	var capacity oapi.TransientCapacity
	if json.Unmarshal(body, &capacity) == nil && validInferenceCapacityError(&capacity) {
		return inferenceCapacityError(&capacity)
	}

	var payload oapi.InferenceError
	if json.Unmarshal(body, &payload) == nil && (payload.Error != "" || payload.Message != "") {
		return newInferenceAPIError(resp.StatusCode, "", &payload)
	}

	message := string(body)
	if truncated {
		message += fmt.Sprintf(" (response body exceeded %d bytes)", maxErrorResponseBytes)
	}
	if message == "" {
		message = http.StatusText(resp.StatusCode)
	}
	return &InferenceAPIError{StatusCode: resp.StatusCode, Message: message}
}

// scanSSE parses complete SSE frames. It joins multiple data fields with a
// newline, ignores comments/heartbeats, accepts CRLF, and dispatches a pending
// frame at EOF as required by the event-stream format.
func scanSSE(r io.Reader, yield func(event, data string) (bool, error)) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), maxGenerateSSEEventBytes)

	var event string
	var data strings.Builder
	hasData := false
	dispatch := func() (bool, error) {
		if !hasData {
			event = ""
			return true, nil
		}
		value := data.String()
		eventType := event
		event = ""
		data.Reset()
		hasData = false
		return yield(eventType, value)
	}

	for scanner.Scan() {
		if !utf8.Valid(scanner.Bytes()) {
			return errors.New("generation stream contained invalid UTF-8")
		}
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			ok, err := dispatch()
			if err != nil || !ok {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}

		field, value, found := strings.Cut(line, ":")
		if !found {
			value = ""
		} else if strings.HasPrefix(value, " ") {
			value = value[1:]
		}
		switch field {
		case "event":
			event = value
		case "data":
			if hasData {
				if data.Len()+1+len(value) > maxGenerateSSEEventBytes {
					return fmt.Errorf("generation SSE event exceeded %d bytes", maxGenerateSSEEventBytes)
				}
				data.WriteByte('\n')
			} else if len(value) > maxGenerateSSEEventBytes {
				return fmt.Errorf("generation SSE event exceeded %d bytes", maxGenerateSSEEventBytes)
			}
			data.WriteString(value)
			hasData = true
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading generation stream: %w", err)
	}
	_, err := dispatch()
	return err
}
