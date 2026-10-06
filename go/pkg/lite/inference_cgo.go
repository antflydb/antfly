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

//go:build cgo

package lite

/*
#include "antfly.h"
#include <stdlib.h>

// antflyLiteInferencePullProgress and antflyLiteInferenceStreamChunk are
// defined (and exported) in Go below. These extern declarations match the
// signatures cgo generates for them, so the trampolines can call them
// without pulling in the generated _cgo_export.h. Each callback handle
// travels as a uintptr_t rather than void* end to end (including across the
// cgo.Handle boundary on the Go side) so no Go code has to convert a bare
// uintptr to unsafe.Pointer -- go vet's unsafeptr check (rightly) flags that
// pattern, and every C signature here can legitimately use uintptr_t
// instead of void* for an opaque handle. Both callbacks return uint8_t
// (0 or 1) rather than the Go bool cgo would otherwise map, again so the
// exported function's real C signature is unambiguous and matches the
// extern declaration exactly.
extern uint8_t antflyLiteInferencePullProgress(uintptr_t context, antfly_inference_pull_progress *progress);
extern uint8_t antflyLiteInferenceStreamChunk(uintptr_t context, antfly_slice chunk_json);

// antflyLiteInferencePullProgressTrampoline has the exact C ABI signature
// antfly_inference_pull_progress_fn requires (including the const the
// callback struct pointer carries), and forwards to the exported Go
// function. Passing a Go function value directly to C isn't possible with
// cgo, so this static C shim is the function pointer actually registered
// with antfly_inference_pull_json.
static bool antflyLiteInferencePullProgressTrampoline(void *context, const antfly_inference_pull_progress *progress) {
    return antflyLiteInferencePullProgress((uintptr_t)context, (antfly_inference_pull_progress *)progress) != 0;
}

// antflyLiteInferenceStreamTrampoline is the antfly_inference_stream_fn shim,
// same idea as antflyLiteInferencePullProgressTrampoline above.
static bool antflyLiteInferenceStreamTrampoline(void *context, antfly_slice chunk_json) {
    return antflyLiteInferenceStreamChunk((uintptr_t)context, chunk_json) != 0;
}

// antflyLiteInferencePull adapts antfly_inference_pull_json's void*
// progress_context to a uintptr_t handle, doing the int-to-pointer cast in C
// instead of Go. A progress callback is always registered (Pull always has
// one internally, to honor context cancellation even with a nil caller
// callback), so there is no has_progress flag here unlike the stream call
// below, whose callback is optional at the Go API level.
static antfly_error_code antflyLiteInferencePull(
    antfly_inference *inference,
    antfly_slice request_json,
    uintptr_t progress_handle,
    antfly_buffer *out
) {
    return antfly_inference_pull_json(inference, request_json, antflyLiteInferencePullProgressTrampoline, (void *)progress_handle, out);
}

// antflyLiteInferenceGenerateStream adapts
// antfly_inference_generate_stream_json's void* chunk_context to a uintptr_t
// handle the same way.
static antfly_error_code antflyLiteInferenceGenerateStream(
    antfly_inference *inference,
    antfly_slice request_json,
    uintptr_t chunk_handle,
    antfly_buffer *out
) {
    return antfly_inference_generate_stream_json(inference, request_json, antflyLiteInferenceStreamTrampoline, (void *)chunk_handle, out);
}
*/
import "C"

import (
	"context"
	"runtime"
	"runtime/cgo"
	"sync"
	"time"
	"unsafe"
)

// InferenceOptions configures OpenInference. The zero value uses the
// runtime's own defaults for every field (see antfly_inference_options_init
// in antfly.h); a nil *InferenceOptions passed to OpenInference does the
// same.
type InferenceOptions struct {
	// ModelsDir overrides the models directory. Empty uses
	// $ANTFLY_INFERENCE_MODELS_DIR, else ~/.antfly/inference/models.
	ModelsDir string
	// HostBudgetMB, BackendBudgetMB, ProcessMemoryBudgetMB, CombinedBudgetMB,
	// KVBudgetMB, and ScratchBudgetMB are explicit resource-budget overrides
	// in MiB; 0 means automatic. These mirror OpenOptions' inference budget
	// fields and the antfly CLI's --inference-*-budget-mb flags.
	HostBudgetMB          uint32
	BackendBudgetMB       uint32
	ProcessMemoryBudgetMB uint32
	CombinedBudgetMB      uint32
	KVBudgetMB            uint32
	ScratchBudgetMB       uint32
	// CallTimeout bounds each call; the C ABI takes whole milliseconds and
	// CallTimeout is rounded up to the nearest millisecond. Zero means no
	// deadline. Because models run in the calling process, a call that has
	// already reached a GPU driver cannot be interrupted: the timeout takes
	// effect only when the call returns.
	CallTimeout time.Duration
}

// Inference is an embedded Antfly inference runtime handle with no database
// attached: models load on first use and stay cached until Close.
//
// Like DB, an *Inference is safe for concurrent use by multiple goroutines.
// Close waits for in-flight calls to finish; calls made after Close return
// InvalidArgument. Models run in the calling process on every backend,
// including Metal, CUDA, and ONNX: once a call reaches the device or driver
// it cannot be interrupted, so CallTimeout and Close only take effect when
// the call returns on its own, and a driver fault terminates the process.
type Inference struct {
	// mu is held shared for the duration of every C call and exclusively by
	// Close, so the handle is never freed under an in-flight call.
	mu     sync.RWMutex
	handle unsafe.Pointer
}

// InferenceOptionsSize returns the loaded C ABI size of
// antfly_inference_options.
func InferenceOptionsSize() uint32 {
	return uint32(C.antfly_inference_options_size())
}

func compiledInferenceOptionsSize() uint32 {
	return uint32(C.sizeof_antfly_inference_options)
}

// OpenInference opens an embedded inference runtime handle with no database
// attached. opts may be nil to use the runtime's defaults. OpenInference
// returns Unsupported when this build does not link the inference runtime or
// the runtime cannot start.
func OpenInference(opts *InferenceOptions) (*Inference, error) {
	if err := ValidateABI(); err != nil {
		return nil, err
	}
	if got, want := InferenceOptionsSize(), compiledInferenceOptionsSize(); got != want {
		return nil, InvalidArgument
	}

	var cOptsPtr *C.antfly_inference_options
	if opts != nil {
		var cOpts C.antfly_inference_options
		if err := check(C.antfly_inference_options_init(&cOpts)); err != nil {
			return nil, err
		}
		modelsDir, cleanupModelsDir := makeCStringSlice([]byte(opts.ModelsDir))
		defer cleanupModelsDir()
		cOpts.models_dir = modelsDir
		cOpts.host_budget_mb = C.uint32_t(opts.HostBudgetMB)
		cOpts.backend_budget_mb = C.uint32_t(opts.BackendBudgetMB)
		cOpts.process_memory_budget_mb = C.uint32_t(opts.ProcessMemoryBudgetMB)
		cOpts.combined_budget_mb = C.uint32_t(opts.CombinedBudgetMB)
		cOpts.kv_budget_mb = C.uint32_t(opts.KVBudgetMB)
		cOpts.scratch_budget_mb = C.uint32_t(opts.ScratchBudgetMB)
		if opts.CallTimeout > 0 {
			cOpts.call_timeout_ms = C.uint64_t((opts.CallTimeout + time.Millisecond - 1) / time.Millisecond)
		}
		cOptsPtr = &cOpts
	}

	var handle *C.antfly_inference
	if err := check(C.antfly_inference_open(cOptsPtr, &handle)); err != nil {
		return nil, err
	}
	return newInference(unsafe.Pointer(handle)), nil
}

func newInference(handle unsafe.Pointer) *Inference {
	inf := &Inference{handle: handle}
	runtime.SetFinalizer(inf, (*Inference).closeFinalizer)
	return inf
}

func (inf *Inference) closeFinalizer() {
	_ = inf.Close()
}

// Close releases the inference handle, waiting for in-flight calls on other
// goroutines to finish first. It is safe to call more than once and
// concurrently.
func (inf *Inference) Close() error {
	if inf == nil {
		return nil
	}
	inf.mu.Lock()
	handle := inf.handle
	inf.handle = nil
	inf.mu.Unlock()
	if handle == nil {
		return nil
	}
	runtime.SetFinalizer(inf, nil)
	C.antfly_inference_close((*C.antfly_inference)(handle))
	return nil
}

// acquire returns the live handle and holds it open until release is called.
// Callers must not call another acquiring method before releasing: a pending
// Close would block the nested acquire and deadlock.
func (inf *Inference) acquire() (handle unsafe.Pointer, release func(), err error) {
	if inf == nil {
		return nil, nil, InvalidArgument
	}
	inf.mu.RLock()
	if inf.handle == nil {
		inf.mu.RUnlock()
		return nil, nil, InvalidArgument
	}
	return inf.handle, inf.mu.RUnlock, nil
}

// call invokes fn with the live handle and request, and turns a failure into
// an *InferenceError carrying the JSON error body fn wrote to *out (the C
// ABI fills *out with a JSON error document on failure too).
func (inf *Inference) call(request []byte, fn func(unsafe.Pointer, C.antfly_slice, *C.antfly_buffer) C.antfly_error_code) ([]byte, error) {
	handle, release, err := inf.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	defer runtime.KeepAlive(inf)
	cInput, cleanup := makeCStringSlice(request)
	defer cleanup()

	var out C.antfly_buffer
	code := fn(handle, cInput, &out)
	body := takeBuffer(out)
	if code == C.ANTFLY_OK {
		return body, nil
	}
	return nil, newInferenceError(ErrorCode(code), body)
}

// Embed returns embeddings for request, the JSON body of the inference API's
// POST /ai/v1/embed.
func (inf *Inference) Embed(request []byte) ([]byte, error) {
	return inf.call(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_inference_embed_json((*C.antfly_inference)(handle), input, out)
	})
}

// Rerank scores request against the inference API's POST /ai/v1/rerank.
func (inf *Inference) Rerank(request []byte) ([]byte, error) {
	return inf.call(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_inference_rerank_json((*C.antfly_inference)(handle), input, out)
	})
}

// Chunk splits request's input per the inference API's POST /ai/v1/chunk.
func (inf *Inference) Chunk(request []byte) ([]byte, error) {
	return inf.call(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_inference_chunk_json((*C.antfly_inference)(handle), input, out)
	})
}

// Generate runs the inference API's POST /ai/v1/generate. A request with
// "stream": true fails with InvalidArgument: responses are always complete.
// Use GenerateStream to stream chunks instead.
func (inf *Inference) Generate(request []byte) ([]byte, error) {
	return inf.call(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_inference_generate_json((*C.antfly_inference)(handle), input, out)
	})
}

// GenerateStream streams a generate request: request is the same JSON body
// Generate takes, minus "stream" (antfly_inference_generate_stream_json sets
// it for you). onChunk is called synchronously on the calling goroutine for
// each "chat.completion.chunk" JSON chunk as the model produces tokens; the
// chunk is only valid during the callback, so GenerateStream copies it
// before calling onChunk. onChunk must be non-nil.
//
// ctx is checked before each callback; if it is done, or onChunk returns
// false, generation stops and GenerateStream returns Cancelled. A nil ctx is
// treated as context.Background() (no cancellation via ctx).
//
// A request rejected before generation starts (such as a missing model)
// fails like Generate, with the JSON error available via
// errors.As(err, &InferenceError{}); a failure mid-stream returns Internal
// with API code "STREAM_FAILED".
func (inf *Inference) GenerateStream(ctx context.Context, request []byte, onChunk func(chunk []byte) bool) error {
	if onChunk == nil {
		return InvalidArgument
	}
	if ctx == nil {
		ctx = context.Background()
	}
	handle, release, err := inf.acquire()
	if err != nil {
		return err
	}
	defer release()
	defer runtime.KeepAlive(inf)
	cInput, cleanup := makeCStringSlice(request)
	defer cleanup()

	callback := func(chunk []byte) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		return onChunk(chunk)
	}
	h := cgo.NewHandle(callback)
	defer h.Delete()

	var out C.antfly_buffer
	code := C.antflyLiteInferenceGenerateStream((*C.antfly_inference)(handle), cInput, C.uintptr_t(h), &out)
	body := takeBuffer(out)
	if code == C.ANTFLY_OK {
		return nil
	}
	return newInferenceError(ErrorCode(code), body)
}

//export antflyLiteInferenceStreamChunk
func antflyLiteInferenceStreamChunk(handleCtx C.uintptr_t, chunk C.antfly_slice) C.uint8_t {
	fn, ok := cgo.Handle(uintptr(handleCtx)).Value().(func([]byte) bool)
	if !ok || fn == nil {
		return 1
	}
	if fn(goBytesFromSlice(chunk)) {
		return 1
	}
	return 0
}

func goBytesFromSlice(s C.antfly_slice) []byte {
	if s.ptr == nil || s.len == 0 {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(s.ptr), C.int(s.len))
}

// GenerateBatch runs up to 128 non-streaming generate requests in one call
// (POST /ai/v1/generate/batch); per-item failures are reported in the
// response rather than as a Go error.
func (inf *Inference) GenerateBatch(request []byte) ([]byte, error) {
	return inf.call(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_inference_generate_batch_json((*C.antfly_inference)(handle), input, out)
	})
}

// Rewrite runs the inference API's POST /ai/v1/rewrite.
func (inf *Inference) Rewrite(request []byte) ([]byte, error) {
	return inf.call(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_inference_rewrite_json((*C.antfly_inference)(handle), input, out)
	})
}

// Extract runs the inference API's POST /ai/v1/extract.
func (inf *Inference) Extract(request []byte) ([]byte, error) {
	return inf.call(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_inference_extract_json((*C.antfly_inference)(handle), input, out)
	})
}

// Read runs OCR via the inference API's POST /ai/v1/read.
func (inf *Inference) Read(request []byte) ([]byte, error) {
	return inf.call(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_inference_read_json((*C.antfly_inference)(handle), input, out)
	})
}

// Transcribe runs the inference API's POST /ai/v1/transcribe.
func (inf *Inference) Transcribe(request []byte) ([]byte, error) {
	return inf.call(request, func(handle unsafe.Pointer, input C.antfly_slice, out *C.antfly_buffer) C.antfly_error_code {
		return C.antfly_inference_transcribe_json((*C.antfly_inference)(handle), input, out)
	})
}

// ListModels returns the installed models, as returned by the inference
// API's GET /ai/v1/models.
func (inf *Inference) ListModels() ([]byte, error) {
	handle, release, err := inf.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	defer runtime.KeepAlive(inf)

	var out C.antfly_buffer
	code := C.antfly_inference_list_models_json((*C.antfly_inference)(handle), &out)
	body := takeBuffer(out)
	if code == C.ANTFLY_OK {
		return body, nil
	}
	return nil, newInferenceError(ErrorCode(code), body)
}

// PullProgress reports one file's download progress for a Pull call.
type PullProgress struct {
	// Model is the model reference being pulled (one per requested variant).
	Model           string
	File            string
	BytesDownloaded uint64
	// TotalBytes is 0 when unknown.
	TotalBytes uint64
	FilesDone  uint64
	FilesTotal uint64
	// Cached reports that File was already present and verified; nothing was
	// downloaded for it.
	Cached bool
}

// Pull downloads a model from the Hugging Face Hub into the handle's models
// directory, like `antfly inference pull`. request is the JSON body
// documented on antfly_inference_pull_json in antfly.h, e.g.
// {"model": "owner/name[:variant]"}.
//
// progress, if non-nil, is called synchronously on the calling goroutine as
// each file starts, every 16 MiB, and as each file completes. ctx is checked
// at the same points; if it is done, or progress returns false, the download
// stops and Pull returns Cancelled. Completed files stay staged, so a later
// Pull for the same model resumes rather than restarts. A nil ctx is treated
// as context.Background() (no cancellation via ctx).
func (inf *Inference) Pull(ctx context.Context, request []byte, progress func(PullProgress) bool) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	handle, release, err := inf.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	defer runtime.KeepAlive(inf)
	cInput, cleanup := makeCStringSlice(request)
	defer cleanup()

	callback := func(p PullProgress) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		if progress == nil {
			return true
		}
		return progress(p)
	}
	h := cgo.NewHandle(callback)
	defer h.Delete()

	var out C.antfly_buffer
	code := C.antflyLiteInferencePull((*C.antfly_inference)(handle), cInput, C.uintptr_t(h), &out)
	body := takeBuffer(out)
	if code == C.ANTFLY_OK {
		return body, nil
	}
	return nil, newInferenceError(ErrorCode(code), body)
}

//export antflyLiteInferencePullProgress
func antflyLiteInferencePullProgress(handleCtx C.uintptr_t, progress *C.antfly_inference_pull_progress) C.uint8_t {
	if progress == nil {
		return 1
	}
	fn, ok := cgo.Handle(uintptr(handleCtx)).Value().(func(PullProgress) bool)
	if !ok || fn == nil {
		return 1
	}
	report := PullProgress{
		Model:           goStringFromSlice(progress.model),
		File:            goStringFromSlice(progress.file),
		BytesDownloaded: uint64(progress.bytes_downloaded),
		TotalBytes:      uint64(progress.total_bytes),
		FilesDone:       uint64(progress.files_done),
		FilesTotal:      uint64(progress.files_total),
		Cached:          bool(progress.cached),
	}
	if fn(report) {
		return 1
	}
	return 0
}

func goStringFromSlice(s C.antfly_slice) string {
	if s.ptr == nil || s.len == 0 {
		return ""
	}
	return C.GoStringN((*C.char)(unsafe.Pointer(s.ptr)), C.int(s.len))
}
