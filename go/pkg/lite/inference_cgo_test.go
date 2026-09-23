// Copyright 2026 Antfly, Inc.
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

//go:build cgo && libantfly

package lite

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestInferenceOpenDefaultsAndClose(t *testing.T) {
	inf, err := OpenInference(nil)
	if err != nil {
		t.Fatalf("OpenInference(nil): %v", err)
	}
	if err := inf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Double close is fine.
	if err := inf.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestInferenceOpenWithOptionsTempModelsDir(t *testing.T) {
	modelsDir := t.TempDir()
	inf, err := OpenInference(&InferenceOptions{ModelsDir: modelsDir})
	if err != nil {
		t.Fatalf("OpenInference(options): %v", err)
	}
	defer inf.Close()

	body, err := inf.ListModels()
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	var result struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode ListModels response: %v; raw=%s", err, body)
	}
	if len(result.Data) != 0 {
		t.Fatalf("ListModels on empty temp models dir: data = %v, want empty", result.Data)
	}
}

func TestInferenceChunkNoModelRequired(t *testing.T) {
	inf, err := OpenInference(&InferenceOptions{ModelsDir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenInference: %v", err)
	}
	defer inf.Close()

	request := []byte(`{"input":"Ants live in colonies. Workers gather food."}`)
	body, err := inf.Chunk(request)
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	var result struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode Chunk response: %v; raw=%s", err, body)
	}
	if len(result.Data) == 0 {
		t.Fatalf("Chunk response has no data: %s", body)
	}
}

func TestInferenceEmbedMissingModelIsNotFound(t *testing.T) {
	inf, err := OpenInference(&InferenceOptions{ModelsDir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenInference: %v", err)
	}
	defer inf.Close()

	request := []byte(`{"model":"no/such-model","input":["hello"]}`)
	_, err = inf.Embed(request)
	if err == nil {
		t.Fatalf("Embed with missing model succeeded, want error")
	}
	if !errors.Is(err, NotFound) {
		t.Fatalf("Embed with missing model error = %v, want NotFound", err)
	}
	var infErr *InferenceError
	if !errors.As(err, &infErr) {
		t.Fatalf("Embed error = %v (%T), want *InferenceError", err, err)
	}
	if infErr.API.Code != "MODEL_NOT_FOUND" {
		t.Fatalf("Embed error API code = %q, want MODEL_NOT_FOUND; body=%s", infErr.API.Code, infErr.Body)
	}
	if len(infErr.Body) == 0 {
		t.Fatalf("Embed error body is empty, want a JSON error document")
	}
}

func TestInferencePullEmptyRequestIsInvalidArgument(t *testing.T) {
	inf, err := OpenInference(&InferenceOptions{ModelsDir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenInference: %v", err)
	}
	defer inf.Close()

	_, err = inf.Pull([]byte(`{}`), nil)
	if err == nil {
		t.Fatalf("Pull({}) succeeded, want error")
	}
	if !errors.Is(err, InvalidArgument) {
		t.Fatalf("Pull({}) error = %v, want InvalidArgument", err)
	}
	var infErr *InferenceError
	if !errors.As(err, &infErr) {
		t.Fatalf("Pull error = %v (%T), want *InferenceError", err, err)
	}
	if len(infErr.Body) == 0 {
		t.Fatalf("Pull error body is empty, want a JSON error document")
	}
}

func TestInferenceGenerateStreamingIsInvalidArgument(t *testing.T) {
	inf, err := OpenInference(&InferenceOptions{ModelsDir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenInference: %v", err)
	}
	defer inf.Close()

	request := []byte(`{"model":"no/such-model","messages":[{"role":"user","content":"hi"}],"stream":true}`)
	_, err = inf.Generate(request)
	if err == nil {
		t.Fatalf("Generate with stream:true succeeded, want error")
	}
	if !errors.Is(err, InvalidArgument) {
		t.Fatalf("Generate with stream:true error = %v, want InvalidArgument", err)
	}
}

func TestInferenceCallsAfterCloseFail(t *testing.T) {
	inf, err := OpenInference(&InferenceOptions{ModelsDir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenInference: %v", err)
	}
	if err := inf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := inf.ListModels(); !errors.Is(err, InvalidArgument) {
		t.Fatalf("ListModels after Close error = %v, want InvalidArgument", err)
	}
	if _, err := inf.Chunk([]byte(`{"input":"hi"}`)); !errors.Is(err, InvalidArgument) {
		t.Fatalf("Chunk after Close error = %v, want InvalidArgument", err)
	}
	if _, err := inf.Pull([]byte(`{"model":"a/b"}`), nil); !errors.Is(err, InvalidArgument) {
		t.Fatalf("Pull after Close error = %v, want InvalidArgument", err)
	}

	// Double close remains safe.
	if err := inf.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestInferenceEmbedLocalModel embeds with a real local model when present
// under ~/.antfly/inference/models, mirroring
// liteLocalEmbeddingModelAvailable's use elsewhere in this package.
func TestInferenceEmbedLocalModel(t *testing.T) {
	if !liteLocalEmbeddingModelAvailable() {
		t.Skip("Qwen3-Embedding-0.6B-GGUF model is not present under ~/.antfly/inference/models/Qwen")
	}

	inf, err := OpenInference(nil)
	if err != nil {
		t.Fatalf("OpenInference: %v", err)
	}
	defer inf.Close()

	request := []byte(`{"model":"Qwen/Qwen3-Embedding-0.6B-GGUF","input":["a","b"]}`)
	body, err := inf.Embed(request)
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	var result struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode Embed response: %v; raw=%s", err, body)
	}
	if len(result.Data) != 2 {
		t.Fatalf("Embed response data length = %d, want 2; raw=%s", len(result.Data), body)
	}
}

// TestInferencePullNetworkGated pulls a real (tiny) model from the network
// into a temp models directory when ANTFLY_INFERENCE_PULL_TEST_MODEL is set,
// asserting the progress callback fires and the pulled model shows up in
// ListModels.
func TestInferencePullNetworkGated(t *testing.T) {
	model := os.Getenv("ANTFLY_INFERENCE_PULL_TEST_MODEL")
	if model == "" {
		t.Skip("ANTFLY_INFERENCE_PULL_TEST_MODEL is not set")
	}

	modelsDir := filepath.Join(t.TempDir(), "models")
	inf, err := OpenInference(&InferenceOptions{ModelsDir: modelsDir})
	if err != nil {
		t.Fatalf("OpenInference: %v", err)
	}
	defer inf.Close()

	var progressCalls int
	var lastModel string
	body, err := inf.Pull([]byte(`{"model":"`+model+`"}`), func(p PullProgress) {
		progressCalls++
		lastModel = p.Model
	})
	if err != nil {
		t.Fatalf("Pull(%s): %v", model, err)
	}
	if progressCalls == 0 {
		t.Fatalf("Pull(%s) progress callback was never called", model)
	}
	if lastModel == "" {
		t.Fatalf("Pull(%s) progress callback never reported a model reference", model)
	}
	if !bytes.Contains(body, []byte(modelsDir)) {
		t.Fatalf("Pull(%s) result = %s, want it to mention models_dir %s", model, body, modelsDir)
	}

	listBody, err := inf.ListModels()
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if !bytes.Contains(listBody, []byte(model)) {
		t.Fatalf("ListModels() = %s, want it to contain pulled model %s", listBody, model)
	}
}
