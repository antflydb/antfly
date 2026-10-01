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
	"context"
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

	_, err = inf.Pull(context.Background(), []byte(`{}`), nil)
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

func TestInferenceGenerateStreamMissingModelIsNotFound(t *testing.T) {
	inf, err := OpenInference(&InferenceOptions{ModelsDir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenInference: %v", err)
	}
	defer inf.Close()

	request := []byte(`{"model":"no/such-model","messages":[{"role":"user","content":"hi"}]}`)
	err = inf.GenerateStream(context.Background(), request, func(chunk []byte) bool {
		t.Fatalf("onChunk called for a missing-model request: %s", chunk)
		return true
	})
	if err == nil {
		t.Fatalf("GenerateStream with missing model succeeded, want error")
	}
	if !errors.Is(err, NotFound) {
		t.Fatalf("GenerateStream with missing model error = %v, want NotFound", err)
	}
	var infErr *InferenceError
	if !errors.As(err, &infErr) {
		t.Fatalf("GenerateStream error = %v (%T), want *InferenceError", err, err)
	}
	if infErr.API.Code != "MODEL_NOT_FOUND" {
		t.Fatalf("GenerateStream error API code = %q, want MODEL_NOT_FOUND; body=%s", infErr.API.Code, infErr.Body)
	}
}

func TestInferenceGenerateStreamNilCallbackIsInvalidArgument(t *testing.T) {
	inf, err := OpenInference(&InferenceOptions{ModelsDir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenInference: %v", err)
	}
	defer inf.Close()

	request := []byte(`{"model":"no/such-model","messages":[{"role":"user","content":"hi"}]}`)
	err = inf.GenerateStream(context.Background(), request, nil)
	if !errors.Is(err, InvalidArgument) {
		t.Fatalf("GenerateStream with nil onChunk error = %v, want InvalidArgument", err)
	}
}

func TestInferenceGenerateStreamInvalidJSONIsInvalidArgument(t *testing.T) {
	inf, err := OpenInference(&InferenceOptions{ModelsDir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenInference: %v", err)
	}
	defer inf.Close()

	err = inf.GenerateStream(context.Background(), []byte(`not json`), func(chunk []byte) bool {
		t.Fatalf("onChunk called for a malformed request: %s", chunk)
		return true
	})
	if !errors.Is(err, InvalidArgument) {
		t.Fatalf("GenerateStream with invalid JSON error = %v, want InvalidArgument", err)
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
	if _, err := inf.Pull(context.Background(), []byte(`{"model":"a/b"}`), nil); !errors.Is(err, InvalidArgument) {
		t.Fatalf("Pull after Close error = %v, want InvalidArgument", err)
	}
	if err := inf.GenerateStream(context.Background(), []byte(`{"model":"a/b","messages":[]}`), func([]byte) bool { return true }); !errors.Is(err, InvalidArgument) {
		t.Fatalf("GenerateStream after Close error = %v, want InvalidArgument", err)
	}

	// Double close remains safe.
	if err := inf.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestInferencePullNetworkGated pulls a real (tiny) model from the network
// into a temp models directory when ANTFLY_INFERENCE_PULL_TEST_MODEL is set,
// asserting the progress callback fires and the pulled model shows up in
// ListModels. It first cancels on the very first progress report and checks
// the pull is reported Cancelled after exactly one callback and the model
// does not appear in ListModels, then pulls the same model to completion.
//
// Pull's progress callback is a rendezvous: the download waits for each
// report to return before continuing, and a false return is always honored
// as ANTFLY_CANCELLED (even on the final report, before the model is
// installed), so cancelling on the first report is deterministic rather than
// a race against the download's own completion.
func TestInferencePullNetworkGated(t *testing.T) {
	model := os.Getenv("ANTFLY_INFERENCE_PULL_TEST_MODEL")
	if model == "" {
		t.Skip("ANTFLY_INFERENCE_PULL_TEST_MODEL is not set")
	}

	modelsDir := filepath.Join(t.TempDir(), "models")
	request := []byte(`{"model":"` + model + `"}`)

	inf, err := OpenInference(&InferenceOptions{ModelsDir: modelsDir})
	if err != nil {
		t.Fatalf("OpenInference: %v", err)
	}
	defer inf.Close()

	var cancelCalls int
	_, cancelErr := inf.Pull(context.Background(), request, func(p PullProgress) bool {
		cancelCalls++
		return false
	})
	if !errors.Is(cancelErr, Cancelled) {
		t.Fatalf("Pull(%s) cancelled on first report error = %v, want Cancelled", model, cancelErr)
	}
	if cancelCalls != 1 {
		t.Fatalf("Pull(%s) cancelled on first report delivered %d callbacks, want exactly 1", model, cancelCalls)
	}

	listAfterCancel, err := inf.ListModels()
	if err != nil {
		t.Fatalf("ListModels after cancelled pull: %v", err)
	}
	if bytes.Contains(listAfterCancel, []byte(model)) {
		t.Fatalf("ListModels() after cancelled pull = %s, want it to not contain %s", listAfterCancel, model)
	}

	var progressCalls int
	var lastModel string
	body, err := inf.Pull(context.Background(), request, func(p PullProgress) bool {
		progressCalls++
		lastModel = p.Model
		return true
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

// liteGemmaGenerateModelAvailable reports whether a local generative model is
// present for TestInferenceGenerateStreamLocalModel, mirroring
// liteLocalEmbeddingModelAvailable's pattern for the embedding model.
func liteGemmaGenerateModelAvailable() bool {
	home, err := os.UserHomeDir()
	if err != nil {
		return false
	}
	matches, err := filepath.Glob(filepath.Join(home, ".antfly", "inference", "models", "ggml-org", "gemma-4-e2b-it-gguf*"))
	if err != nil {
		return false
	}
	return len(matches) > 0
}

// TestInferenceGenerateStreamLocalModel streams a real local generation when
// present under ~/.antfly/inference/models, and separately confirms
// cancelling after a couple of chunks stops generation and reports exactly
// as many callbacks as were allowed through.
func TestInferenceGenerateStreamLocalModel(t *testing.T) {
	if !liteGemmaGenerateModelAvailable() {
		t.Skip("gemma-4-e2b-it-gguf model is not present under ~/.antfly/inference/models/ggml-org")
	}

	const model = "ggml-org/gemma-4-e2b-it-gguf:gguf:Q4_0"
	request := []byte(`{"model":"` + model + `","messages":[{"role":"user","content":"Count from one to twenty in words."}],"max_tokens":48}`)

	t.Run("completes", func(t *testing.T) {
		inf, err := OpenInference(nil)
		if err != nil {
			t.Fatalf("OpenInference: %v", err)
		}
		defer inf.Close()

		var chunks int
		err = inf.GenerateStream(context.Background(), request, func(chunk []byte) bool {
			chunks++
			if !bytes.Contains(chunk, []byte("chat.completion.chunk")) {
				t.Fatalf("chunk %d = %s, want it to contain chat.completion.chunk", chunks, chunk)
			}
			return true
		})
		if err != nil {
			t.Fatalf("GenerateStream: %v", err)
		}
		if chunks <= 2 {
			t.Fatalf("GenerateStream delivered %d chunks, want more than 2", chunks)
		}
	})

	t.Run("cancels", func(t *testing.T) {
		inf, err := OpenInference(nil)
		if err != nil {
			t.Fatalf("OpenInference: %v", err)
		}
		defer inf.Close()

		var chunks int
		err = inf.GenerateStream(context.Background(), request, func(chunk []byte) bool {
			chunks++
			return chunks < 2
		})
		if !errors.Is(err, Cancelled) {
			t.Fatalf("GenerateStream stopped after 2 chunks error = %v, want Cancelled", err)
		}
		if chunks != 2 {
			t.Fatalf("GenerateStream stopped after 2 chunks delivered %d callbacks, want exactly 2", chunks)
		}
	})
}
