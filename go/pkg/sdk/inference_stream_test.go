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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInferenceClientGenerateStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ai/v1/generate", r.URL.Path)
		assert.Equal(t, "text/event-stream", r.Header.Get("Accept"))

		var request oapi.InferenceGenerateRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		assert.True(t, request.Stream)

		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		_, _ = io.WriteString(w, ": heartbeat\r\n\r\nevent: message\r\ndata: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\r\ndata: \"created\":1,\"model\":\"test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\r\n\r\ndata: [DONE]\r\n\r\n")
	}))
	defer server.Close()

	client, err := NewInferenceClient(server.URL, server.Client())
	require.NoError(t, err)

	var chunks []oapi.InferenceGenerateChunk
	err = client.GenerateStream(context.Background(), oapi.InferenceGenerateRequest{
		Model:    "test",
		Messages: []oapi.InferenceChatMessage{NewUserMessage("hello")},
	}, func(chunk oapi.InferenceGenerateChunk) error {
		chunks = append(chunks, chunk)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, chunks, 1)
	require.Len(t, chunks[0].Choices, 1)
	assert.Equal(t, "hi", chunks[0].Choices[0].Delta.Content)
}

func TestInferenceClientGenerateStreamReturnsTypedHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInsufficientStorage)
		_, _ = io.WriteString(w, `{"error":"MEMORY_BUDGET_EXCEEDED","message":"model needs 8 GiB","retryable":true}`)
	}))
	defer server.Close()

	client, err := NewInferenceClient(server.URL, server.Client())
	require.NoError(t, err)
	err = client.GenerateStream(context.Background(), oapi.InferenceGenerateRequest{Model: "test"}, func(oapi.InferenceGenerateChunk) error { return nil })

	var apiErr *InferenceAPIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusInsufficientStorage, apiErr.StatusCode)
	assert.Equal(t, "MEMORY_BUDGET_EXCEEDED", apiErr.Code)
	assert.Equal(t, "model needs 8 GiB (MEMORY_BUDGET_EXCEEDED)", apiErr.Message)
	require.NotNil(t, apiErr.Retryable)
	assert.True(t, *apiErr.Retryable)
}

func TestScanSSEFramesAndErrors(t *testing.T) {
	input := ": keepalive\r\n\r\nevent: token\r\ndata: {\"a\":\r\ndata: 1}\r\n\r\ndata: tail"
	reader := &chunkedReader{data: input, chunkSize: 1}

	var frames []string
	err := scanSSE(reader, func(event, data string) (bool, error) {
		frames = append(frames, fmt.Sprintf("%s:%s", event, data))
		return true, nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"token:{\"a\":\n1}", ":tail"}, frames)

	want := errors.New("stop")
	err = scanSSE(strings.NewReader("data: one\n\ndata: two\n\n"), func(_, _ string) (bool, error) {
		return false, want
	})
	assert.ErrorIs(t, err, want)
}

func TestScanSSERejectsMalformedUTF8(t *testing.T) {
	input := append([]byte(`data: {"content":"`), 0xff)
	input = append(input, []byte(`"}`+"\n\n")...)
	err := scanSSE(bytes.NewReader(input), func(_, _ string) (bool, error) {
		return true, nil
	})
	require.EqualError(t, err, "generation stream contained invalid UTF-8")
}

func TestInferenceClientGenerateStreamRequiresDone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"test","choices":[]}`+"\n\n")
	}))
	defer server.Close()

	client, err := NewInferenceClient(server.URL, server.Client())
	require.NoError(t, err)
	err = client.GenerateStream(context.Background(), oapi.InferenceGenerateRequest{Model: "test"}, func(oapi.InferenceGenerateChunk) error { return nil })
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestInferenceClientGenerateStreamHonorsContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"test","choices":[]}`+"\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	client, err := NewInferenceClient(server.URL, server.Client())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	err = client.GenerateStream(ctx, oapi.InferenceGenerateRequest{Model: "test"}, func(oapi.InferenceGenerateChunk) error {
		cancel()
		return nil
	})
	assert.ErrorIs(t, err, context.Canceled)
}
