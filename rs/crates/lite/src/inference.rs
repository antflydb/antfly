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

//! Embedded inference without a database: `antfly_inference_open`/`close`
//! plus the `/ai/v1`-mirroring JSON call surface (embed, rerank, chunk,
//! generate, generate/batch, rewrite, extract, read, transcribe, models,
//! pull). See `zig/CAPI.md`'s "Inference" and "Inference In Process"
//! sections and the `Embedded inference without a database` block in
//! `zig/pkg/antfly/include/antfly.h`.
//!
//! [`Inference`] mirrors [`crate::Database`]'s handle-safety story (it is
//! `Send + Sync`, close waits for in-flight calls and is idempotent/
//! concurrency-safe, and calls after close fail), but is a separate C ABI
//! handle kind and registry -- it does not require or share a
//! [`crate::Database`].

use std::ffi::c_void;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::time::Duration;

use antfly_lite_sys::{
    self as sys, antfly_buffer, antfly_error_code, antfly_inference, antfly_inference_options,
    antfly_inference_pull_progress, antfly_slice,
};

use crate::db::{SUPPORTED_ABI_VERSION, abi_version};
use crate::error::Error;
use crate::ffi::{HandleGate, borrow_slice, check, path_to_bytes, take_buffer};

/// Returns the loaded C ABI size of `antfly_inference_options`.
pub fn inference_options_size() -> u32 {
    unsafe { sys::antfly_inference_options_size() }
}

fn compiled_inference_options_size() -> u32 {
    std::mem::size_of::<antfly_inference_options>() as u32
}

/// Verifies that the loaded C library matches the header this binding was
/// compiled against, for the inference handle kind specifically. Every
/// [`Inference::open`]/[`Inference::open_default`] call checks this first,
/// like [`crate::validate_abi`] does for [`crate::Database`]. A mismatch is
/// reported as [`Error::Internal`].
fn validate_inference_abi() -> crate::Result<()> {
    if abi_version() != SUPPORTED_ABI_VERSION {
        return Err(Error::Internal);
    }
    if inference_options_size() != compiled_inference_options_size() {
        return Err(Error::Internal);
    }
    Ok(())
}

// ---------------------------------------------------------------------
// InferenceError / InferenceResult
// ---------------------------------------------------------------------

/// An [`Error`] together with the JSON error body an `Inference` JSON call
/// left behind, when there was one.
///
/// Unlike most `*_json` methods elsewhere in this crate, `antfly_inference_*`
/// calls are documented to always reset and fill their output buffer, even
/// on failure, with the runtime's JSON error (`{"error":...,"message":...}`)
/// -- see the "Embedded inference without a database" block in
/// `zig/pkg/antfly/include/antfly.h`. This type carries that body forward
/// instead of discarding it; every buffer is freed (via `antfly_buffer_free`)
/// either way, whether the call succeeded or not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InferenceError {
    pub error: Error,
    /// The raw JSON error body, decoded as UTF-8 (lossy). Empty when the
    /// call never reached the runtime (for example, calling a closed
    /// handle) and so left no body to report.
    pub body: String,
}

impl InferenceError {
    fn new(error: Error, body: Vec<u8>) -> Self {
        InferenceError {
            error,
            body: String::from_utf8_lossy(&body).into_owned(),
        }
    }

    /// The `"message"` field of the JSON error body, if `body` is a JSON
    /// object with a string `"message"`. Requires the `serde` feature.
    #[cfg(feature = "serde")]
    pub fn message(&self) -> Option<String> {
        let value: serde_json::Value = serde_json::from_str(&self.body).ok()?;
        value.get("message")?.as_str().map(str::to_owned)
    }
}

impl fmt::Display for InferenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.body.is_empty() {
            fmt::Display::fmt(&self.error, f)
        } else {
            write!(f, "{}: {}", self.error, self.body)
        }
    }
}

impl std::error::Error for InferenceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// A `Result` whose error is [`InferenceError`], returned by `Inference`'s
/// JSON call methods (as opposed to [`crate::Result`], used by
/// [`Inference::open`]/[`Inference::close`], which do not produce a JSON
/// body to report).
pub type InferenceResult<T> = std::result::Result<T, InferenceError>;

// ---------------------------------------------------------------------
// InferenceOptions
// ---------------------------------------------------------------------

/// Configures [`Inference::open`].
///
/// All fields default to automatic: an empty `models_dir` uses
/// `$ANTFLY_INFERENCE_MODELS_DIR`, else `~/.antfly/inference/models`; the
/// `*_budget_mb` fields default to 0 (automatic/host-detected sizing, the
/// same knobs as [`crate::OpenOptions`]'s embedded-inference budgets); and
/// `call_timeout` defaults to `None` (no per-call deadline).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InferenceOptions {
    pub models_dir: Option<PathBuf>,
    pub host_budget_mb: u32,
    pub backend_budget_mb: u32,
    pub process_memory_budget_mb: u32,
    pub combined_budget_mb: u32,
    pub kv_budget_mb: u32,
    pub scratch_budget_mb: u32,
    pub call_timeout: Option<Duration>,
}

impl InferenceOptions {
    /// Starts a builder with all defaults (automatic models directory and
    /// resource budgets, no per-call timeout).
    pub fn new() -> Self {
        Self::default()
    }

    pub fn models_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.models_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    pub fn host_budget_mb(mut self, mb: u32) -> Self {
        self.host_budget_mb = mb;
        self
    }

    pub fn backend_budget_mb(mut self, mb: u32) -> Self {
        self.backend_budget_mb = mb;
        self
    }

    pub fn process_memory_budget_mb(mut self, mb: u32) -> Self {
        self.process_memory_budget_mb = mb;
        self
    }

    pub fn combined_budget_mb(mut self, mb: u32) -> Self {
        self.combined_budget_mb = mb;
        self
    }

    pub fn kv_budget_mb(mut self, mb: u32) -> Self {
        self.kv_budget_mb = mb;
        self
    }

    pub fn scratch_budget_mb(mut self, mb: u32) -> Self {
        self.scratch_budget_mb = mb;
        self
    }

    /// Deadline applied to each call. `None` (the default) or
    /// [`Duration::ZERO`] means no deadline. Sub-millisecond durations are
    /// rounded up, since the C ABI takes whole milliseconds.
    pub fn call_timeout(mut self, timeout: Duration) -> Self {
        self.call_timeout = Some(timeout);
        self
    }

    fn call_timeout_ms(&self) -> u64 {
        match self.call_timeout {
            Some(d) if !d.is_zero() => {
                let ms = d.as_nanos().div_ceil(1_000_000);
                u64::try_from(ms).unwrap_or(u64::MAX)
            }
            _ => 0,
        }
    }
}

// ---------------------------------------------------------------------
// PullProgress
// ---------------------------------------------------------------------

/// One report from an [`Inference::pull`] progress callback. Owned (copied
/// out of the C ABI's borrowed, callback-lifetime-only struct) so it can be
/// used freely -- stored, sent across threads, etc. -- without carrying a
/// lifetime tied to the callback invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullProgress {
    /// The model reference being pulled (one per requested variant).
    pub model: String,
    pub file: String,
    pub bytes_downloaded: u64,
    /// 0 when unknown.
    pub total_bytes: u64,
    pub files_done: u64,
    pub files_total: u64,
    /// The file was already present and verified; nothing was downloaded.
    pub cached: bool,
}

impl PullProgress {
    /// # Safety
    /// `raw`'s `model`/`file` slices must point at initialized memory valid
    /// for the duration of this call (upheld because this is only ever
    /// called from the trampoline libantfly invokes synchronously, on the
    /// slices it just populated for that one call).
    unsafe fn from_raw(raw: &antfly_inference_pull_progress) -> Self {
        PullProgress {
            model: unsafe { slice_to_string(raw.model) },
            file: unsafe { slice_to_string(raw.file) },
            bytes_downloaded: raw.bytes_downloaded,
            total_bytes: raw.total_bytes,
            files_done: raw.files_done,
            files_total: raw.files_total,
            cached: raw.cached,
        }
    }
}

/// # Safety
/// `slice` must point at `slice.len` initialized bytes, or have a null/
/// zero-length pointer.
unsafe fn slice_to_string(slice: antfly_slice) -> String {
    if slice.ptr.is_null() || slice.len == 0 {
        return String::new();
    }
    let bytes = unsafe { std::slice::from_raw_parts(slice.ptr, slice.len) };
    String::from_utf8_lossy(bytes).into_owned()
}

// ---------------------------------------------------------------------
// Inference
// ---------------------------------------------------------------------

/// An embedded inference runtime handle, with no database. Models load on
/// first use and stay cached until the handle closes.
///
/// `Inference` is `Send + Sync` and safe for concurrent use from any
/// thread, exactly like [`crate::Database`]: libantfly's inference handles
/// have the same "close waits for in-flight calls, a closed or foreign
/// handle is rejected" contract. See `zig/CAPI.md`'s "Inference In
/// Process" section for what running on a GPU/driver backend means for
/// `call_timeout`/close/process-fault behavior: once a call reaches the
/// device or driver it runs to completion, and a driver fault terminates
/// the process.
pub struct Inference {
    gate: HandleGate<antfly_inference>,
}

// SAFETY: see the identical justification on `Database` in `db.rs` --
// libantfly's `antfly_inference` handles share the same "serialized"
// threading contract as `antfly_db` handles (zig/CAPI.md "Thread Safety"),
// and `HandleGate` only prevents calling into a handle after
// `antfly_inference_close` has returned.
unsafe impl Send for Inference {}
unsafe impl Sync for Inference {}

impl Inference {
    fn from_handle(handle: *mut antfly_inference) -> Inference {
        Inference {
            gate: HandleGate::new(handle),
        }
    }

    fn with_handle<R>(
        &self,
        f: impl FnOnce(*mut antfly_inference) -> crate::Result<R>,
    ) -> crate::Result<R> {
        self.gate.with_handle(f)
    }

    /// Runs a no-input `*_json` call (currently just `list_models`),
    /// wrapping the outcome in [`InferenceResult`] per [`InferenceError`]'s
    /// doc comment.
    fn read_buffer(
        &self,
        f: impl FnOnce(*mut antfly_inference, *mut antfly_buffer) -> antfly_error_code,
    ) -> InferenceResult<Vec<u8>> {
        let raw = self.with_handle(|handle| {
            let mut out = antfly_buffer::default();
            let code = f(handle, &mut out);
            let body = unsafe { take_buffer(out) };
            Ok((code, body))
        });
        finish(raw)
    }

    /// Runs a `*_json` call that takes a request body, wrapping the outcome
    /// in [`InferenceResult`] per [`InferenceError`]'s doc comment.
    fn call_json(
        &self,
        request: &[u8],
        f: impl FnOnce(*mut antfly_inference, antfly_slice, *mut antfly_buffer) -> antfly_error_code,
    ) -> InferenceResult<Vec<u8>> {
        let raw = self.with_handle(|handle| {
            let mut out = antfly_buffer::default();
            let code = f(handle, borrow_slice(request), &mut out);
            let body = unsafe { take_buffer(out) };
            Ok((code, body))
        });
        finish(raw)
    }

    // -- Open / close -----------------------------------------------------

    /// Opens an embedded inference runtime using explicit options.
    pub fn open(options: &InferenceOptions) -> crate::Result<Inference> {
        validate_inference_abi()?;

        let mut c_opts: antfly_inference_options = unsafe { std::mem::zeroed() };
        check(unsafe { sys::antfly_inference_options_init(&mut c_opts) })?;

        // `models_dir_bytes` must outlive the `antfly_inference_open` call
        // below, since `c_opts.models_dir` only borrows it.
        let models_dir_bytes = options.models_dir.as_deref().map(path_to_bytes);
        if let Some(bytes) = &models_dir_bytes {
            c_opts.models_dir = borrow_slice(bytes);
        }
        c_opts.host_budget_mb = options.host_budget_mb;
        c_opts.backend_budget_mb = options.backend_budget_mb;
        c_opts.process_memory_budget_mb = options.process_memory_budget_mb;
        c_opts.combined_budget_mb = options.combined_budget_mb;
        c_opts.kv_budget_mb = options.kv_budget_mb;
        c_opts.scratch_budget_mb = options.scratch_budget_mb;
        c_opts.call_timeout_ms = options.call_timeout_ms();

        let mut handle: *mut antfly_inference = std::ptr::null_mut();
        check(unsafe { sys::antfly_inference_open(&c_opts, &mut handle) })?;
        Ok(Inference::from_handle(handle))
    }

    /// Opens an embedded inference runtime with every default: models
    /// directory from `$ANTFLY_INFERENCE_MODELS_DIR`/
    /// `~/.antfly/inference/models`, automatic resource budgets, no
    /// per-call timeout. Equivalent to `antfly_inference_open(NULL, ...)`.
    pub fn open_default() -> crate::Result<Inference> {
        validate_inference_abi()?;
        let mut handle: *mut antfly_inference = std::ptr::null_mut();
        check(unsafe { sys::antfly_inference_open(std::ptr::null(), &mut handle) })?;
        Ok(Inference::from_handle(handle))
    }

    /// Releases the embedded inference handle, waiting for in-flight calls
    /// on other threads to finish first -- including any in-progress
    /// [`Inference::pull`] or [`Inference::generate_stream`], neither of
    /// which `close` itself cancels (their own progress/chunk callbacks can,
    /// by returning `false`). Idempotent and safe to call concurrently with
    /// itself or from several threads at once; calls made after (or racing)
    /// `close` fail. Takes `&self`, not `self`, for the same reason as
    /// [`crate::Database::close`]. [`Drop`] also closes, for callers who
    /// never need to close early.
    pub fn close(&self) -> crate::Result<()> {
        self.gate
            .close(|handle| unsafe { sys::antfly_inference_close(handle) });
        Ok(())
    }

    // -- JSON calls ---------------------------------------------------------

    /// `POST /embed`.
    pub fn embed(&self, request: impl AsRef<[u8]>) -> InferenceResult<Vec<u8>> {
        self.call_json(request.as_ref(), |h, req, out| unsafe {
            sys::antfly_inference_embed_json(h, req, out)
        })
    }

    /// `POST /rerank`.
    pub fn rerank(&self, request: impl AsRef<[u8]>) -> InferenceResult<Vec<u8>> {
        self.call_json(request.as_ref(), |h, req, out| unsafe {
            sys::antfly_inference_rerank_json(h, req, out)
        })
    }

    /// `POST /chunk`.
    pub fn chunk(&self, request: impl AsRef<[u8]>) -> InferenceResult<Vec<u8>> {
        self.call_json(request.as_ref(), |h, req, out| unsafe {
            sys::antfly_inference_chunk_json(h, req, out)
        })
    }

    /// `POST /generate`. A request with `"stream": true` fails with
    /// [`Error::InvalidArgument`]: responses from this method are always
    /// complete. Use [`Inference::generate_stream`] to stream.
    pub fn generate(&self, request: impl AsRef<[u8]>) -> InferenceResult<Vec<u8>> {
        self.call_json(request.as_ref(), |h, req, out| unsafe {
            sys::antfly_inference_generate_json(h, req, out)
        })
    }

    /// Streams a generate request (the same body as [`Inference::generate`];
    /// `"stream"` is set for the caller). `on_chunk` is called on the
    /// calling thread for each chunk -- the JSON of a `chat.completion.chunk`,
    /// valid only for the duration of the call -- as the model produces
    /// tokens; generation waits for each call to return before producing
    /// the next chunk.
    ///
    /// Returning `false` from `on_chunk` stops generation and this method
    /// returns [`Error::Cancelled`] (wrapped in [`InferenceError`]);
    /// cancellation is always honored at the chunk where `on_chunk` returns
    /// `false`. A request rejected before generation starts (for example a
    /// missing model) fails like
    /// [`Inference::generate`], with the JSON error body attached; a
    /// failure mid-stream fails with [`Error::Internal`] and a JSON body
    /// naming `"STREAM_FAILED"`.
    ///
    /// A panic inside `on_chunk` is caught and does not unwind across the C
    /// ABI boundary (unwinding through `extern "C"` is undefined behavior):
    /// it cancels the stream (as if `on_chunk` had returned `false`) and is
    /// resumed once the underlying call returns.
    pub fn generate_stream(
        &self,
        request: impl AsRef<[u8]>,
        on_chunk: &mut dyn FnMut(&[u8]) -> bool,
    ) -> InferenceResult<()> {
        // Same shape as `pull`'s callback plumbing below: a type-erased
        // closure plus a slot to stash a panic payload so it can be resumed
        // after the FFI call returns instead of unwinding through
        // `extern "C"`.
        struct CallbackCtx<'a> {
            on_chunk: &'a mut dyn FnMut(&[u8]) -> bool,
            panic: Option<Box<dyn std::any::Any + Send>>,
        }

        unsafe extern "C" fn trampoline(ctx: *mut c_void, chunk_json: antfly_slice) -> bool {
            if ctx.is_null() {
                return true;
            }
            let ctx = unsafe { &mut *ctx.cast::<CallbackCtx>() };
            if ctx.panic.is_some() {
                // A previous invocation already panicked; stop the stream
                // and don't call into (possibly now-invalid) user code
                // again before we get a chance to resume that panic.
                return false;
            }
            let bytes: &[u8] = if chunk_json.ptr.is_null() || chunk_json.len == 0 {
                &[]
            } else {
                unsafe { std::slice::from_raw_parts(chunk_json.ptr, chunk_json.len) }
            };
            let result = std::panic::catch_unwind(AssertUnwindSafe(|| (ctx.on_chunk)(bytes)));
            match result {
                Ok(cont) => cont,
                Err(payload) => {
                    ctx.panic = Some(payload);
                    false
                }
            }
        }

        let raw = self.with_handle(|handle| {
            let mut out = antfly_buffer::default();
            let mut ctx = CallbackCtx {
                on_chunk,
                panic: None,
            };
            let ctx_ptr = std::ptr::addr_of_mut!(ctx).cast::<c_void>();

            let code = unsafe {
                sys::antfly_inference_generate_stream_json(
                    handle,
                    borrow_slice(request.as_ref()),
                    Some(trampoline),
                    ctx_ptr,
                    &mut out,
                )
            };
            let body = unsafe { take_buffer(out) };

            if let Some(payload) = ctx.panic.take() {
                std::panic::resume_unwind(payload);
            }

            Ok((code, body))
        });

        match raw {
            Ok((code, _body)) if code == sys::ANTFLY_OK => Ok(()),
            Ok((code, body)) => Err(InferenceError::new(Error::from_code(code), body)),
            Err(e) => Err(InferenceError::new(e, Vec::new())),
        }
    }

    /// `POST /generate/batch`: up to 128 non-streaming generate requests in
    /// one call; per-item failures are reported in the response body, not
    /// as an `Err`.
    pub fn generate_batch(&self, request: impl AsRef<[u8]>) -> InferenceResult<Vec<u8>> {
        self.call_json(request.as_ref(), |h, req, out| unsafe {
            sys::antfly_inference_generate_batch_json(h, req, out)
        })
    }

    /// `POST /rewrite`.
    pub fn rewrite(&self, request: impl AsRef<[u8]>) -> InferenceResult<Vec<u8>> {
        self.call_json(request.as_ref(), |h, req, out| unsafe {
            sys::antfly_inference_rewrite_json(h, req, out)
        })
    }

    /// `POST /extract`.
    pub fn extract(&self, request: impl AsRef<[u8]>) -> InferenceResult<Vec<u8>> {
        self.call_json(request.as_ref(), |h, req, out| unsafe {
            sys::antfly_inference_extract_json(h, req, out)
        })
    }

    /// `POST /read` (OCR).
    pub fn read(&self, request: impl AsRef<[u8]>) -> InferenceResult<Vec<u8>> {
        self.call_json(request.as_ref(), |h, req, out| unsafe {
            sys::antfly_inference_read_json(h, req, out)
        })
    }

    /// `POST /transcribe`.
    pub fn transcribe(&self, request: impl AsRef<[u8]>) -> InferenceResult<Vec<u8>> {
        self.call_json(request.as_ref(), |h, req, out| unsafe {
            sys::antfly_inference_transcribe_json(h, req, out)
        })
    }

    /// `GET /models`: the installed models.
    pub fn list_models(&self) -> InferenceResult<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_inference_list_models_json(h, out) })
    }

    /// Downloads a model from the Hugging Face Hub into the handle's models
    /// directory, like `antfly inference pull`. `request` is documented on
    /// `antfly_inference_pull_json` in `zig/pkg/antfly/include/antfly.h`
    /// (`{"model": "owner/name[:variant]", ...}`).
    ///
    /// `progress`, if given, is called synchronously on the calling thread
    /// as each file starts, every 16 MiB, and as it completes; the download
    /// waits for each call to return before continuing. Returning `false`
    /// cancels the pull: this method then fails with [`Error::Cancelled`]
    /// (wrapped in [`InferenceError`]) and the model is not installed.
    /// Cancellation takes effect at the report where `progress` returns
    /// `false` -- it is always honored, even at the very last report -- not
    /// before. Completed files stay staged, so pulling the same model again
    /// resumes rather than restarts. A panic inside `progress` is caught
    /// and does not unwind across the C ABI boundary (unwinding through
    /// `extern "C"` is undefined behavior): it cancels the pull (as if `progress` had
    /// returned `false`) and is resumed once the underlying
    /// `antfly_inference_pull_json` call returns.
    ///
    /// [`Inference::close`] waits for a pull in progress. On success the
    /// response is `{"models": [...], "models_dir": "..."}`; a model
    /// missing from the hub fails with [`Error::NotFound`], a bad request
    /// or a model over the configured size limits with
    /// [`Error::InvalidArgument`], and a network or hub failure with
    /// [`Error::Busy`].
    pub fn pull(
        &self,
        request: impl AsRef<[u8]>,
        progress: Option<&mut dyn FnMut(&PullProgress) -> bool>,
    ) -> InferenceResult<Vec<u8>> {
        // Carries the caller's closure (type-erased behind the trait
        // object) plus a slot to stash a panic payload if it panics, so the
        // panic can be resumed after the FFI call returns instead of
        // unwinding through `extern "C"` (undefined behavior).
        struct CallbackCtx<'a> {
            progress: &'a mut dyn FnMut(&PullProgress) -> bool,
            panic: Option<Box<dyn std::any::Any + Send>>,
        }

        unsafe extern "C" fn trampoline(
            ctx: *mut c_void,
            raw: *const antfly_inference_pull_progress,
        ) -> bool {
            if ctx.is_null() || raw.is_null() {
                return true;
            }
            let ctx = unsafe { &mut *ctx.cast::<CallbackCtx>() };
            if ctx.panic.is_some() {
                // A previous invocation already panicked; cancel and don't
                // call into (possibly now-invalid) user code again before we
                // get a chance to resume that panic.
                return false;
            }
            let progress = unsafe { PullProgress::from_raw(&*raw) };
            let result = std::panic::catch_unwind(AssertUnwindSafe(|| (ctx.progress)(&progress)));
            match result {
                Ok(cont) => cont,
                Err(payload) => {
                    ctx.panic = Some(payload);
                    false
                }
            }
        }

        let raw = self.with_handle(|handle| {
            let mut out = antfly_buffer::default();

            let (progress_fn, progress_ctx, mut callback_ctx): (
                sys::antfly_inference_pull_progress_fn,
                *mut c_void,
                Option<CallbackCtx>,
            ) = match progress {
                Some(f) => {
                    let mut ctx = CallbackCtx {
                        progress: f,
                        panic: None,
                    };
                    let ctx_ptr = std::ptr::addr_of_mut!(ctx).cast::<c_void>();
                    (Some(trampoline as _), ctx_ptr, Some(ctx))
                }
                None => (None, std::ptr::null_mut(), None),
            };

            let code = unsafe {
                sys::antfly_inference_pull_json(
                    handle,
                    borrow_slice(request.as_ref()),
                    progress_fn,
                    progress_ctx,
                    &mut out,
                )
            };
            let body = unsafe { take_buffer(out) };

            if let Some(ctx) = &mut callback_ctx
                && let Some(payload) = ctx.panic.take()
            {
                std::panic::resume_unwind(payload);
            }

            Ok((code, body))
        });
        finish(raw)
    }
}

/// Turns the outcome of a raw call (`Ok((code, body))` for anything that
/// actually reached the C ABI, `Err(e)` only when [`HandleGate::with_handle`]
/// itself refused the call, e.g. because the handle is closed) into
/// [`InferenceResult`].
fn finish(raw: crate::Result<(antfly_error_code, Vec<u8>)>) -> InferenceResult<Vec<u8>> {
    match raw {
        Ok((code, body)) if code == sys::ANTFLY_OK => Ok(body),
        Ok((code, body)) => Err(InferenceError::new(Error::from_code(code), body)),
        Err(e) => Err(InferenceError::new(e, Vec::new())),
    }
}

impl Drop for Inference {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl std::fmt::Debug for Inference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inference")
            .field("open", &self.gate.is_open())
            .finish()
    }
}
