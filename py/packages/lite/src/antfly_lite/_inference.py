# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Embedded Antfly inference, without a database.

Threading model (see zig/CAPI.md "Embedded inference without a database" and
"Thread Safety"): an Inference handle is safe for concurrent use by multiple
threads, with the same guard discipline as Database: close() waits for
in-flight calls on other threads to finish; calls made after close() raise
InvalidArgumentError.

Every call reaches into libantfly, and on every backend (including Metal,
CUDA, and ONNX) that call runs in this process: once it reaches the device or
driver it cannot be interrupted, so call_timeout_ms and close() only take
effect when the call returns, and a driver fault terminates the process. See
the README's "Embedded inference" section.

On failure, the C API still returns a JSON error body ({"error": ...,
"message": ...}); this module folds that into the raised exception's message
and always releases the underlying buffer.

pull()'s progress callback and generate_stream()'s on_chunk callback can
cancel the call by returning False (None/True continue, so existing
callbacks that return nothing keep working); a cancelled call raises
errors.CancelledError. Cancellation only takes effect at the next report
(pull: each file's start, every 16 MiB, and its end; generate_stream: each
chunk). If the callback itself raises, the exception is captured, the call
is cancelled the same way, and the original exception is re-raised after
the underlying C call returns -- it must never unwind across the C ABI
boundary. Callbacks always run on the calling thread.
"""

from __future__ import annotations

import ctypes
import json
import os
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from . import _ffi, errors
from ._json import JSONInput, decode_json_response, encode_json_input

__all__ = [
    "PullProgress",
    "Inference",
]


@dataclass(frozen=True)
class PullProgress:
    """One progress report from Inference.pull() (antfly_inference_pull_progress).

    Copied out of the callback's borrowed C slices, so it remains valid
    after the callback returns.
    """

    model: str
    file: str
    bytes_downloaded: int
    total_bytes: int
    """0 means unknown."""
    files_done: int
    files_total: int
    cached: bool
    """The file was already present and verified; nothing was downloaded."""


def _error_from_body(code: int, body: bytes) -> errors.AntflyError:
    """Build the AntflyError for `code`, folding in the runtime's JSON error
    body ({"error": ..., "message": ...}) when present."""
    error_name: str | None = None
    message: str | None = None
    if body:
        try:
            parsed = json.loads(body)
        except ValueError:
            parsed = None
        if isinstance(parsed, dict):
            raw_error = parsed.get("error")
            raw_message = parsed.get("message")
            error_name = str(raw_error) if raw_error is not None else None
            message = str(raw_message) if raw_message is not None else None
    if error_name and message:
        text = f"{error_name}: {message}"
    else:
        text = error_name or message
    cls = errors.error_class_for_code(code)
    return cls(code, text)


class Inference:
    """An embedded Antfly inference handle: embeddings, reranking, chunking,
    generation, rewriting, extraction, OCR, transcription, and model pulls,
    with no database attached.

    Do not construct directly; use Inference.open().
    """

    def __init__(self, handle: int) -> None:
        self._lib = _ffi.get_lib()
        self._handle: int | None = handle
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._active = 0
        self._closing = False
        self._closed = False

    # -- open / lifecycle ---------------------------------------------------

    @classmethod
    def open(
        cls,
        *,
        models_dir: str | os.PathLike[str] | None = None,
        host_budget_mb: int = 0,
        backend_budget_mb: int = 0,
        process_memory_budget_mb: int = 0,
        combined_budget_mb: int = 0,
        kv_budget_mb: int = 0,
        scratch_budget_mb: int = 0,
        call_timeout_ms: int = 0,
    ) -> Inference:
        """Start the embedded inference runtime with no database.

        models_dir defaults to $ANTFLY_INFERENCE_MODELS_DIR, else
        ~/.antfly/inference/models. The budget_mb arguments are the same
        resource knobs as antfly_lite.OpenOptions' inference budgets (0
        means automatic). call_timeout_ms is a per-call deadline (0 means
        none); it can only take effect once a call returns control to the
        library (see the README).

        Raises UnsupportedError if this build does not link the inference
        runtime or the runtime cannot start.
        """
        _ffi.validate_abi()
        lib = _ffi.get_lib()
        c_opts = _ffi.AntflyInferenceOptions()
        errors.raise_for_code(lib.antfly_inference_options_init(ctypes.byref(c_opts)))
        keep_alive: object = None
        if models_dir is not None:
            models_dir_slice, keep_alive = _ffi.make_slice(_ffi.path_to_bytes(models_dir))
            c_opts.models_dir = models_dir_slice
        c_opts.host_budget_mb = host_budget_mb
        c_opts.backend_budget_mb = backend_budget_mb
        c_opts.process_memory_budget_mb = process_memory_budget_mb
        c_opts.combined_budget_mb = combined_budget_mb
        c_opts.kv_budget_mb = kv_budget_mb
        c_opts.scratch_budget_mb = scratch_budget_mb
        c_opts.call_timeout_ms = call_timeout_ms

        handle = ctypes.c_void_p()
        code = lib.antfly_inference_open(ctypes.byref(c_opts), ctypes.byref(handle))
        _ = keep_alive  # keep the models_dir buffer alive through the call above
        errors.raise_for_code(code)
        if handle.value is None:
            raise errors.InternalError(message="antfly_inference_open returned ANTFLY_OK with a null handle")
        return cls(handle.value)

    def __enter__(self) -> Inference:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _acquire(self) -> int:
        with self._cond:
            # See Database._acquire: `_closing` (not just `_closed`) must be
            # checked here to avoid starving the closer's wait loop below.
            if self._closing or self._handle is None:
                raise errors.InvalidArgumentError()
            self._active += 1
            return self._handle

    def _release(self) -> None:
        with self._cond:
            self._active -= 1
            if self._active == 0:
                self._cond.notify_all()

    def close(self) -> None:
        """Release the embedded inference handle, waiting for in-flight
        calls on other threads to finish first. Safe to call more than once
        and concurrently, and safe even if open() never succeeded."""
        with self._cond:
            if self._closing or self._closed:
                while not self._closed:
                    self._cond.wait()
                return
            self._closing = True
            while self._active > 0:
                self._cond.wait()
            handle = self._handle
            self._handle = None
            self._closed = True
            self._cond.notify_all()
        if handle is not None:
            self._lib.antfly_inference_close(ctypes.c_void_p(handle))

    # -- low-level call helpers ----------------------------------------------

    def _json_read(self, fn, *, raw: bool) -> Any:
        handle = self._acquire()
        try:
            out = _ffi.AntflyBuffer()
            code = fn(ctypes.c_void_p(handle), ctypes.byref(out))
            # The runtime fills *out with the response body whether or not
            # the call succeeded (a failure carries the JSON error body);
            # take_buffer always copies-then-frees, matching that contract.
            body = _ffi.take_buffer(out)
            if code != errors.OK:
                raise _error_from_body(code, body)
            return decode_json_response(body, raw)
        finally:
            self._release()

    def _json_call(self, fn, request: JSONInput, *, raw: bool) -> Any:
        data = encode_json_input(request)
        handle = self._acquire()
        try:
            sl, _keep = _ffi.make_slice(data)
            out = _ffi.AntflyBuffer()
            code = fn(ctypes.c_void_p(handle), sl, ctypes.byref(out))
            body = _ffi.take_buffer(out)
            if code != errors.OK:
                raise _error_from_body(code, body)
            return decode_json_response(body, raw)
        finally:
            self._release()

    # -- /ai/v1 routes --------------------------------------------------------

    def embed(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_inference_embed_json, request, raw=raw)

    def rerank(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_inference_rerank_json, request, raw=raw)

    def chunk(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_inference_chunk_json, request, raw=raw)

    def generate(self, request: JSONInput, *, raw: bool = False) -> Any:
        """Responses are always complete: a request with "stream": true
        raises InvalidArgumentError (there is no streaming sink for this
        call). Use generate_stream() to stream."""
        return self._json_call(self._lib.antfly_inference_generate_json, request, raw=raw)

    def generate_stream(self, request: JSONInput, on_chunk: Callable[[Any], Any]) -> None:
        """Stream a generate request; "stream": true is set for you (do not
        set it in `request`).

        on_chunk is called synchronously on the calling thread for each
        streamed chunk -- the parsed JSON of a "chat.completion.chunk" -- as
        the model produces tokens. Returning False from on_chunk stops
        generation and raises CancelledError (None/True continue).

        A request rejected before generation starts (such as a missing
        model) raises like generate() does, with the runtime's JSON error
        folded into the exception. A failure mid-stream raises InternalError
        with a STREAM_FAILED body. On success (generation ran to
        completion), returns None -- there is no final response body to
        return, only the chunks already delivered to on_chunk.
        """
        data = encode_json_input(request)
        handle = self._acquire()
        try:
            sl, _keep = _ffi.make_slice(data)
            out = _ffi.AntflyBuffer()
            callback_exc: list[BaseException] = []

            def _on_chunk(_ctx: object, chunk_slice: Any) -> bool:
                try:
                    raw_chunk = _ffi.slice_to_bytes(chunk_slice)
                    chunk = json.loads(raw_chunk) if raw_chunk else None
                    result = on_chunk(chunk)
                    return result is not False
                except BaseException as exc:  # noqa: BLE001 - must not unwind through C
                    callback_exc.append(exc)
                    return False

            c_on_chunk = _ffi.AntflyInferenceStreamFn(_on_chunk)
            code = self._lib.antfly_inference_generate_stream_json(
                ctypes.c_void_p(handle), sl, c_on_chunk, None, ctypes.byref(out)
            )
            body = _ffi.take_buffer(out)
            if callback_exc:
                raise callback_exc[0]
            if code != errors.OK:
                # On cancellation raised directly (not via the callback
                # exception above), the C API leaves *out* empty rather than
                # a JSON error body; _error_from_body still produces a good
                # CancelledError from the stable per-code description.
                raise _error_from_body(code, body)
            return None
        finally:
            self._release()

    def generate_batch(self, request: JSONInput, *, raw: bool = False) -> Any:
        """Up to 128 non-streaming generate requests in one call; per-item
        failures are reported in the response, not raised."""
        return self._json_call(self._lib.antfly_inference_generate_batch_json, request, raw=raw)

    def rewrite(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_inference_rewrite_json, request, raw=raw)

    def extract(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_inference_extract_json, request, raw=raw)

    def read(self, request: JSONInput, *, raw: bool = False) -> Any:
        """OCR."""
        return self._json_call(self._lib.antfly_inference_read_json, request, raw=raw)

    def transcribe(self, request: JSONInput, *, raw: bool = False) -> Any:
        return self._json_call(self._lib.antfly_inference_transcribe_json, request, raw=raw)

    def list_models(self, *, raw: bool = False) -> Any:
        """The installed models, as returned by GET /ai/v1/models."""
        return self._json_read(self._lib.antfly_inference_list_models_json, raw=raw)

    # -- pull -----------------------------------------------------------------

    def pull(
        self,
        request: JSONInput,
        *,
        progress: Callable[[PullProgress], Any] | None = None,
        raw: bool = False,
    ) -> Any:
        """Download a model from the Hugging Face Hub into this handle's
        models directory, like `antfly inference pull`.

        request is {"model": "owner/name[:variant]", ...} (see antfly.h);
        "model" is required. progress, if given, is called synchronously on
        the calling thread as each file starts, every 16 MiB, and as it
        completes; close() waits for the pull to finish.

        Returning False from `progress` cancels the pull: the download stops
        at the next report and CancelledError is raised (None/True
        continue). Completed files stay staged, so pulling the same model
        again resumes rather than restarts. If `progress` raises, the
        exception is captured, the pull is cancelled the same way, and the
        original exception is re-raised after the underlying C call returns
        (it must not unwind across the C ABI boundary); it takes priority
        over any error the pull call itself reports.
        """
        data = encode_json_input(request)
        handle = self._acquire()
        try:
            sl, _keep = _ffi.make_slice(data)
            out = _ffi.AntflyBuffer()
            callback_exc: list[BaseException] = []

            if progress is None:
                # A NULL function pointer, not a Python None: ctypes callback
                # argtypes require an actual CFUNCTYPE instance.
                c_progress = _ffi.AntflyInferencePullProgressFn(0)
            else:

                def _on_progress(_ctx: object, progress_ptr: Any) -> bool:
                    try:
                        p = progress_ptr.contents
                        result = progress(
                            PullProgress(
                                model=_ffi.slice_to_bytes(p.model).decode("utf-8", "replace"),
                                file=_ffi.slice_to_bytes(p.file).decode("utf-8", "replace"),
                                bytes_downloaded=int(p.bytes_downloaded),
                                total_bytes=int(p.total_bytes),
                                files_done=int(p.files_done),
                                files_total=int(p.files_total),
                                cached=bool(p.cached),
                            )
                        )
                        return result is not False
                    except BaseException as exc:  # noqa: BLE001 - must not unwind through C
                        callback_exc.append(exc)
                        return False

                c_progress = _ffi.AntflyInferencePullProgressFn(_on_progress)

            code = self._lib.antfly_inference_pull_json(
                ctypes.c_void_p(handle), sl, c_progress, None, ctypes.byref(out)
            )
            body = _ffi.take_buffer(out)
            if callback_exc:
                raise callback_exc[0]
            if code != errors.OK:
                raise _error_from_body(code, body)
            return decode_json_response(body, raw)
        finally:
            self._release()
