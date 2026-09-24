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

// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import koffi from "koffi";
import { validateAbi, validateInferenceAbi } from "./abi.js";
import {
  type AntflyError,
  CancelledError,
  checkCode,
  ErrorCode,
  errorFromCode,
  InvalidArgumentError,
} from "./errors.js";
import {
  callAsync,
  type JsonInput,
  jsonSlice,
  newBufferOut,
  parseJson,
  stringSlice,
  takeBuffer,
  type Uint64Like,
} from "./marshal.js";
import {
  AntflyInferencePullProgress,
  loadNative,
  type NativeLibrary,
  PAntflyInferencePullProgressFn,
  PAntflyInferenceStreamFn,
} from "./native.js";
import type { InferenceOptions, PullProgress } from "./types.js";

/**
 * Return false from a pull() onProgress or generateStream() onChunk
 * callback to cancel; anything else (including void/undefined) continues.
 */
// biome-ignore lint/suspicious/noConfusingVoidType: void (not undefined) lets a plain, non-returning callback (`(p) => { ... }`) be passed without an explicit `return true`/`return undefined`, via TS's void-return-position rule.
type CallbackResult = boolean | void;

type NativeOptionsStruct = Record<string, unknown>;

function buildInferenceOptionsStruct(
  native: NativeLibrary,
  opts: InferenceOptions
): NativeOptionsStruct {
  const options: NativeOptionsStruct = {};
  checkCode(native.inferenceOptionsInit(options));
  options.models_dir = stringSlice(opts.modelsDir ?? "");
  options.host_budget_mb = opts.hostBudgetMb ?? 0;
  options.backend_budget_mb = opts.backendBudgetMb ?? 0;
  options.process_memory_budget_mb = opts.processMemoryBudgetMb ?? 0;
  options.combined_budget_mb = opts.combinedBudgetMb ?? 0;
  options.kv_budget_mb = opts.kvBudgetMb ?? 0;
  options.scratch_budget_mb = opts.scratchBudgetMb ?? 0;
  options.call_timeout_ms = (opts.callTimeoutMs ?? 0) as Uint64Like;
  return options;
}

/**
 * Builds the AntflyError for a failed Inference call. Unlike the rest of
 * this binding, every antfly_inference_*_json call fills its antfly_buffer
 * output with a JSON error body ({"error": ..., "message": ...}) even on
 * failure (see antfly.h's "Embedded inference without a database"); this
 * folds that body into the thrown error's message and `.body`.
 */
function errorFromResponseBody(code: number, body: Buffer): AntflyError {
  let parsed: unknown;
  if (body.length > 0) {
    try {
      parsed = JSON.parse(body.toString("utf8"));
    } catch {
      parsed = undefined;
    }
  }
  let message: string | undefined;
  if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
    const b = parsed as Record<string, unknown>;
    const errPart = typeof b.error === "string" ? b.error : undefined;
    const msgPart = typeof b.message === "string" ? b.message : undefined;
    message = [errPart, msgPart].filter((s): s is string => Boolean(s)).join(": ") || undefined;
  }
  const err = errorFromCode(code, message);
  err.body = parsed;
  return err;
}

/**
 * Decodes an antfly_slice {ptr, len} value (borrowed memory, valid only
 * during the callback that handed it over) to a copied Buffer. Struct
 * arguments passed by value to a koffi callback (like antfly_slice
 * chunk_json in antfly_inference_stream_fn) already arrive as a plain
 * {ptr, len} object -- confirmed against the real antfly_inference_generate_stream_json
 * call, unlike a pointer-to-struct argument (antfly_inference_pull_progress
 * *), which koffi hands over as a raw address requiring koffi.decode.
 */
function decodeSliceBytes(slice: { ptr: unknown; len: unknown } | null | undefined): Buffer {
  if (!slice || slice.ptr == null) {
    return Buffer.alloc(0);
  }
  const len = Number(slice.len ?? 0);
  if (len === 0) {
    return Buffer.alloc(0);
  }
  const view = koffi.decode(slice.ptr, "uint8_t", len) as Uint8Array;
  return Buffer.from(view);
}

/** Decodes an antfly_slice {ptr, len} value (borrowed memory) to a UTF-8 string. */
function decodeSliceUtf8(slice: { ptr: unknown; len: unknown } | null | undefined): string {
  return decodeSliceBytes(slice).toString("utf8");
}

interface DecodedPullProgress {
  model: { ptr: unknown; len: unknown };
  file: { ptr: unknown; len: unknown };
  bytes_downloaded: number | bigint;
  total_bytes: number | bigint;
  files_done: number | bigint;
  files_total: number | bigint;
  cached: boolean;
}

function toPullProgress(decoded: DecodedPullProgress): PullProgress {
  return {
    model: decodeSliceUtf8(decoded.model),
    file: decodeSliceUtf8(decoded.file),
    bytesDownloaded: BigInt(decoded.bytes_downloaded ?? 0),
    totalBytes: BigInt(decoded.total_bytes ?? 0),
    filesDone: BigInt(decoded.files_done ?? 0),
    filesTotal: BigInt(decoded.files_total ?? 0),
    cached: Boolean(decoded.cached),
  };
}

/**
 * An embedded Antfly inference runtime handle: no database, just model
 * inference (embed, rerank, chunk, generate, rewrite, extract, read/OCR,
 * transcribe) via the libantfly antfly_inference_* C ABI. Models load on
 * first use and stay cached until close().
 *
 * Every JSON call except pull() and generateStream() is async (runs on
 * koffi's worker thread pool via fn.async(...), like Database) and has the
 * same close semantics: close() waits for every call this binding has
 * dispatched before freeing the native handle, and rejects new calls
 * immediately once close() has started.
 *
 * pull() and generateStream() are the exceptions: both run synchronously
 * on the calling (JS main) thread and block the event loop for their
 * duration. This is required for their callbacks (pull's onProgress,
 * generateStream's onChunk) -- JS execution is single-threaded, so a
 * callback koffi delivers from a background thread (as happens for a call
 * made via fn.async(...), which runs on koffi's worker thread pool) has to
 * be queued back onto the JS main thread rather than invoked in a true
 * blocking round-trip; per koffi's docs that queuing only runs "as soon as
 * the event loop has a chance to run", which could reorder or delay a
 * report arbitrarily relative to the (already-freed) antfly_slice data it
 * points to, and could even deadlock if the main thread never yields.
 * Calling synchronously keeps the whole call, including every callback
 * invocation, on one OS thread throughout, matching antfly.h's "called on
 * the calling thread" guarantee exactly. See README.md's "Embedded
 * inference" section.
 */
export class Inference implements AsyncDisposable {
  #native: NativeLibrary;
  #handle: unknown;
  #state: "open" | "closing" | "closed" = "open";
  #pending = new Set<Promise<unknown>>();
  #closeTask: Promise<void> | undefined;
  #finalizerToken: object = {};

  /** @internal use Inference.open() instead of the constructor. */
  constructor(native: NativeLibrary, handle: unknown) {
    this.#native = native;
    this.#handle = handle;
    inferenceFinalizationRegistry.register(this, { native, handle }, this.#finalizerToken);
  }

  /** Starts the embedded inference runtime with no database. Returns UnsupportedError if this build does not link the inference runtime or it cannot start. */
  static async open(options: InferenceOptions = {}): Promise<Inference> {
    validateAbi();
    validateInferenceAbi();
    const native = loadNative();
    const optsStruct = buildInferenceOptionsStruct(native, options);
    const outHandle: unknown[] = [null];
    const code = await callAsync(native.inferenceOpen, optsStruct, outHandle);
    checkCode(code);
    return new Inference(native, outHandle[0]);
  }

  #run<T>(op: (handle: unknown) => Promise<T>): Promise<T> {
    if (this.#state !== "open") {
      return Promise.reject(new InvalidArgumentError("inference handle is closed"));
    }
    const handle = this.#handle;
    const promise = op(handle);
    this.#pending.add(promise);
    const cleanup = () => this.#pending.delete(promise);
    promise.then(cleanup, cleanup);
    return promise;
  }

  #track<T>(promise: Promise<T>): Promise<T> {
    this.#pending.add(promise);
    const cleanup = () => this.#pending.delete(promise);
    promise.then(cleanup, cleanup);
    return promise;
  }

  // biome-ignore lint/suspicious/noExplicitAny: koffi function signatures vary per call site
  #invokeJson(fn: any, request: JsonInput): Promise<Buffer> {
    return this.#run(async (handle) => {
      const out = newBufferOut();
      const code = await callAsync(fn, handle, jsonSlice(request), out);
      const body = takeBuffer(this.#native, out);
      if (code !== ErrorCode.OK) {
        throw errorFromResponseBody(code, body);
      }
      return body;
    });
  }

  /** Idempotent; waits for in-flight calls before freeing the native handle. Calls made after close() rejects with InvalidArgumentError. */
  async close(): Promise<void> {
    if (this.#state === "closed") return;
    if (this.#state === "closing") {
      await this.#closeTask;
      return;
    }
    this.#state = "closing";
    inferenceFinalizationRegistry.unregister(this.#finalizerToken);
    const handle = this.#handle;
    this.#closeTask = (async () => {
      while (this.#pending.size > 0) {
        await Promise.allSettled([...this.#pending]);
      }
      if (handle != null) {
        await callAsync(this.#native.inferenceClose, handle);
      }
      this.#handle = null;
      this.#state = "closed";
    })();
    await this.#closeTask;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  // --- JSON calls (POST /ai/v1/<route>) ---

  embedRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeJson(this.#native.inferenceEmbedJson, request);
  }
  async embed(request: JsonInput): Promise<unknown> {
    return parseJson(await this.embedRaw(request));
  }

  rerankRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeJson(this.#native.inferenceRerankJson, request);
  }
  async rerank(request: JsonInput): Promise<unknown> {
    return parseJson(await this.rerankRaw(request));
  }

  chunkRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeJson(this.#native.inferenceChunkJson, request);
  }
  async chunk(request: JsonInput): Promise<unknown> {
    return parseJson(await this.chunkRaw(request));
  }

  /** A generate request with "stream": true fails with InvalidArgumentError; responses are always complete. Use generateStream() to stream. */
  generateRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeJson(this.#native.inferenceGenerateJson, request);
  }
  async generate(request: JsonInput): Promise<unknown> {
    return parseJson(await this.generateRaw(request));
  }

  /**
   * Streams a generate request (antfly_inference_generate_stream_json): the
   * same request body as generate(), with "stream" set for you. onChunk is
   * called synchronously, on the calling thread, once per streamed chunk
   * (a parsed "chat.completion.chunk" JSON object), as the model produces
   * tokens.
   *
   * Returning `false` from onChunk stops generation early; the call then
   * rejects with CancelledError. A thrown error also stops generation and
   * is rethrown from this call (generation does not continue in the
   * background). generateStream() runs synchronously on the JS thread and
   * blocks the event loop for its duration -- see the class docstring for
   * why (the same koffi callback-threading constraint as pull()); this is
   * the reason this binding offers a callback, not an async iterator.
   *
   * A request rejected before generation starts (e.g. a missing model)
   * rejects the same way generate() does, with the JSON error body on
   * `.body`. A failure mid-stream rejects InternalError with
   * `.body.error === "STREAM_FAILED"`.
   */
  generateStreamRaw(
    request: JsonInput,
    onChunk: (chunkJson: Buffer) => CallbackResult
  ): Promise<Buffer> {
    if (this.#state !== "open") {
      return Promise.reject(new InvalidArgumentError("inference handle is closed"));
    }
    const handle = this.#handle;
    const native = this.#native;
    const run = (): Buffer => {
      let callbackError: unknown;
      let callbackThrew = false;
      const callbackPtr = koffi.register(
        (_context: unknown, chunk: { ptr: unknown; len: unknown }): boolean => {
          try {
            return onChunk(decodeSliceBytes(chunk)) !== false;
          } catch (err) {
            callbackThrew = true;
            callbackError = err;
            return false;
          }
        },
        PAntflyInferenceStreamFn
      );
      try {
        const out = newBufferOut();
        const code = native.inferenceGenerateStreamJson(
          handle,
          jsonSlice(request),
          callbackPtr,
          null,
          out
        );
        const body = takeBuffer(native, out);
        if (callbackThrew) {
          throw callbackError;
        }
        if (code !== ErrorCode.OK) {
          throw errorFromResponseBody(code, body);
        }
        return body;
      } finally {
        koffi.unregister(callbackPtr);
      }
    };
    return this.#track(Promise.resolve().then(run));
  }
  async generateStream(
    request: JsonInput,
    onChunk: (chunk: unknown) => CallbackResult
  ): Promise<unknown> {
    return parseJson(
      await this.generateStreamRaw(request, (chunkJson) => onChunk(parseJson(chunkJson)))
    );
  }

  /** Up to 128 non-streaming generate requests in one call; per-item failures are reported in the response. */
  generateBatchRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeJson(this.#native.inferenceGenerateBatchJson, request);
  }
  async generateBatch(request: JsonInput): Promise<unknown> {
    return parseJson(await this.generateBatchRaw(request));
  }

  rewriteRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeJson(this.#native.inferenceRewriteJson, request);
  }
  async rewrite(request: JsonInput): Promise<unknown> {
    return parseJson(await this.rewriteRaw(request));
  }

  extractRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeJson(this.#native.inferenceExtractJson, request);
  }
  async extract(request: JsonInput): Promise<unknown> {
    return parseJson(await this.extractRaw(request));
  }

  /** OCR. */
  readRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeJson(this.#native.inferenceReadJson, request);
  }
  async read(request: JsonInput): Promise<unknown> {
    return parseJson(await this.readRaw(request));
  }

  transcribeRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeJson(this.#native.inferenceTranscribeJson, request);
  }
  async transcribe(request: JsonInput): Promise<unknown> {
    return parseJson(await this.transcribeRaw(request));
  }

  /** The installed models, as returned by GET /ai/v1/models. */
  listModelsRaw(): Promise<Buffer> {
    return this.#run(async (handle) => {
      const out = newBufferOut();
      const code = await callAsync(this.#native.inferenceListModelsJson, handle, out);
      const body = takeBuffer(this.#native, out);
      if (code !== ErrorCode.OK) {
        throw errorFromResponseBody(code, body);
      }
      return body;
    });
  }
  async listModels(): Promise<unknown> {
    return parseJson(await this.listModelsRaw());
  }

  /**
   * Downloads a model from the Hugging Face Hub into the handle's models
   * directory, like `antfly inference pull`. request: {"model": "owner/name[:variant]",
   * required; "variants": [...]; "token": "..." (default $HF_TOKEN);
   * "tasks"/"capabilities" to override the model manifest; "projector":
   * "auto" | "none" | "match"; "max_artifact_bytes"/"max_model_bytes"}.
   *
   * onProgress (optional) is called synchronously, on the calling thread,
   * as each file starts, every 16 MiB, and as it completes; see the class
   * docstring for why pull() itself runs synchronously (blocking the event
   * loop) rather than on koffi's async worker pool. Returning `false` from
   * onProgress cancels the pull -- the call then rejects with
   * CancelledError, and completed files stay staged, so a later pull()
   * for the same model resumes rather than restarts. A thrown error also
   * cancels the pull and is rethrown from this call.
   *
   * `signal` (optional) is a best-effort, honestly-limited cancellation
   * knob: because pull() blocks the JS thread for its whole duration,
   * there is no way to interrupt it asynchronously from the outside the
   * way `fetch(url, { signal })` can. An already-aborted signal rejects
   * immediately without starting the pull; otherwise the signal is polled
   * only at the same report points as onProgress (each file start, every
   * 16 MiB, file end) -- an abort in between those points is not observed
   * until the next one. Prefer returning `false` from onProgress when you
   * need precise control.
   *
   * A model missing from the hub rejects with NotFoundError, a bad request
   * or a model over the size limits with InvalidArgumentError, and a
   * network or hub failure with BusyError. close() waits for a pull in
   * progress.
   */
  pullRaw(
    request: JsonInput,
    onProgress?: (progress: PullProgress) => CallbackResult,
    signal?: AbortSignal
  ): Promise<Buffer> {
    if (this.#state !== "open") {
      return Promise.reject(new InvalidArgumentError("inference handle is closed"));
    }
    if (signal?.aborted) {
      return Promise.reject(cancelledBySignal(signal));
    }
    const handle = this.#handle;
    const native = this.#native;
    const run = (): Buffer => {
      let callbackPtr: bigint | undefined;
      let callbackError: unknown;
      let callbackThrew = false;
      if (onProgress || signal) {
        callbackPtr = koffi.register((_context: unknown, progressPtr: unknown): boolean => {
          try {
            if (signal?.aborted) {
              return false;
            }
            const decoded = koffi.decode(
              progressPtr,
              AntflyInferencePullProgress
            ) as DecodedPullProgress;
            return onProgress?.(toPullProgress(decoded)) !== false;
          } catch (err) {
            callbackThrew = true;
            callbackError = err;
            return false;
          }
        }, PAntflyInferencePullProgressFn);
      }
      try {
        const out = newBufferOut();
        const code = native.inferencePullJson(
          handle,
          jsonSlice(request),
          callbackPtr ?? null,
          null,
          out
        );
        const body = takeBuffer(native, out);
        if (callbackThrew) {
          throw callbackError;
        }
        if (code !== ErrorCode.OK) {
          throw errorFromResponseBody(code, body);
        }
        return body;
      } finally {
        if (callbackPtr !== undefined) {
          koffi.unregister(callbackPtr);
        }
      }
    };
    return this.#track(Promise.resolve().then(run));
  }
  async pull(
    request: JsonInput,
    onProgress?: (progress: PullProgress) => CallbackResult,
    signal?: AbortSignal
  ): Promise<unknown> {
    return parseJson(await this.pullRaw(request, onProgress, signal));
  }
}

/** Builds the CancelledError thrown when pull()'s signal was already aborted. */
function cancelledBySignal(signal: AbortSignal): CancelledError {
  const reason = signal.reason;
  const detail =
    reason instanceof Error ? reason.message : reason !== undefined ? String(reason) : undefined;
  return new CancelledError(
    detail ? `pull cancelled before it started: ${detail}` : "pull cancelled before it started"
  );
}

const inferenceFinalizationRegistry = new FinalizationRegistry<{
  native: NativeLibrary;
  handle: unknown;
}>(({ native, handle }) => {
  if (handle != null) {
    try {
      native.inferenceClose(handle);
    } catch {
      // Best-effort only: the process may be tearing down.
    }
  }
});
