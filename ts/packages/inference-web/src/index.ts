// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import type {
  Backend,
  BundleFiles,
  ExtensionInferenceRequest,
  InferenceProgress,
  InferenceRequest,
  InferenceRequestV1,
  InferenceRequestV2,
  InferenceResponse,
  InferenceResponseV1,
  InferenceResponseV2,
  ModelInfo,
  RunOptions,
  RunResult,
  ValidationResult,
  WeightPrecision,
} from "./contracts.js";
import { InferenceError, type InferenceErrorCode, type InferenceState } from "./lifecycle.js";
import { RUNTIME_COMPATIBILITY } from "./runtime-compatibility.js";
export * from "./contracts.js";
export * from "./lifecycle.js";
export { RUNTIME_COMPATIBILITY } from "./runtime-compatibility.js";

type BundleInfo = {
  architecture: "span" | "boundary" | "decide" | "laya";
  precision: WeightPrecision;
  bytes: number;
  cpuReason?: string;
  config?: { laya?: { format?: string } };
};
type Runtime = {
  init: (url: string, options: object) => Promise<void>;
  loadExtractionBundle: (
    files: BundleFiles,
    precision?: WeightPrecision,
    progress?: (p: InferenceProgress) => void
  ) => Promise<BundleInfo>;
  runExtraction: (
    request: InferenceRequest | ExtensionInferenceRequest,
    validate: boolean
  ) => Promise<Omit<RunResult<unknown>, "backend">>;
  destroy: (reason?: Error) => void;
};
type Gpu = {
  init: () => Promise<boolean>;
  destroy: () => void;
  device?: {
    lost: Promise<{ message: string }>;
    addEventListener: (name: string, fn: (event: { error?: { message: string } }) => void) => void;
  };
  lastInitError?: string;
  maxBufferBytes?: number;
  onFatalError?: (error: Error) => void;
};
type AssetModule = {
  INFERENCE_RUNTIME_ID: string;
  inspectBundle: (files: BundleFiles, precision?: WeightPrecision) => Promise<BundleInfo>;
  InferenceWeb: new () => Runtime;
  WebGPUOps: new () => Gpu;
};
async function moduleAt(base: string, name: string): Promise<AssetModule> {
  const module = await import(
    /* @vite-ignore */ /* webpackIgnore: true */ /* turbopackIgnore: true */ new URL(name, base)
      .href
  );
  if (module.INFERENCE_RUNTIME_ID !== RUNTIME_COMPATIBILITY.runtimeId)
    throw new InferenceError(
      "RUNTIME_INCOMPATIBLE",
      `Incompatible runtime JavaScript: ${name}. Prepare matching assets.`
    );
  return module;
}
function modelIdentity(info: BundleInfo): Pick<ModelInfo, "family" | "architecture"> {
  switch (info.architecture) {
    case "span":
      return { family: "gliner2", architecture: "span" };
    case "boundary":
      return { family: "gliner25", architecture: "boundary" };
    case "decide":
      return { family: "decide", architecture: "span" };
    case "laya":
      return {
        family: info.config?.laya?.format === "opendecider" ? "opendecider" : "laya",
        architecture: "modernbert",
      };
  }
}
function failure(error: unknown, code: InferenceErrorCode): InferenceError {
  if (error instanceof InferenceError) return error;
  if ((error as { code?: unknown })?.code === "RUNTIME_INCOMPATIBLE") code = "RUNTIME_INCOMPATIBLE";
  const message = error instanceof Error ? error.message : String(error);
  return new InferenceError(code, message, { cause: error });
}
export async function detectCapabilities() {
  const gpu = (
    navigator as Navigator & {
      gpu?: {
        requestAdapter(): Promise<{
          limits: { maxBufferSize: number; maxStorageBufferBindingSize: number };
        } | null>;
      };
    }
  ).gpu;
  let adapter = null;
  try {
    adapter = await gpu?.requestAdapter();
  } catch {
    /* CPU remains usable. */
  }
  const isolated =
    globalThis.crossOriginIsolated === true && typeof SharedArrayBuffer !== "undefined";
  return {
    wasm: typeof WebAssembly !== "undefined",
    webgpu: Boolean(adapter && isolated),
    crossOriginIsolated: isolated,
    gpuLimits: adapter?.limits,
    opfs: Boolean(navigator.storage?.getDirectory),
    reason: !adapter
      ? "WebGPU adapter unavailable"
      : !isolated
        ? "WebGPU bridge requires COOP/COEP headers"
        : undefined,
  };
}
export class InferenceClient {
  private runtime: Runtime | null = null;
  private gpu: Gpu | null = null;
  private busy = false;
  private generation = 0;
  private disposed = false;
  private files: BundleFiles | null = null;
  private precision?: WeightPrecision;
  private requestedBackend: Backend = "auto";
  private interruption = new InferenceError("CANCELLED", "Inference cancelled");
  private compatibilityChecked = false;
  private listeners = new Set<(state: InferenceState) => void>();
  private snapshot: InferenceState = Object.freeze({
    status: "idle",
    model: null,
    operation: null,
    recovery: "none",
    error: null,
  });
  readonly assets: string;
  constructor(assets = "/inference/") {
    this.assets = new URL(assets, location.href).href;
  }
  get model(): ModelInfo | null {
    return this.snapshot.model;
  }
  get state(): InferenceState {
    return this.snapshot;
  }
  /** Immediately delivers the current snapshot; returns an unsubscribe function. */
  subscribe(listener: (state: InferenceState) => void): () => void {
    this.listeners.add(listener);
    this.notify(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }
  private notify(listener: (state: InferenceState) => void) {
    try {
      listener(this.snapshot);
    } catch (error) {
      globalThis.reportError?.(error);
    }
  }
  private setState(state: InferenceState) {
    this.snapshot = Object.freeze({
      ...state,
      error: state.error
        ? Object.freeze({ code: state.error.code, message: state.error.message })
        : null,
    });
    for (const listener of this.listeners) this.notify(listener);
  }
  private async verifyCompatibility(signal?: AbortSignal) {
    if (this.compatibilityChecked) return;
    try {
      const response = await fetch(new URL("runtime-manifest.json", this.assets), {
        signal,
        cache: "no-cache",
      });
      if (!response.ok) throw new Error(`Runtime manifest unavailable (${response.status})`);
      const manifest = await response.json();
      for (const [key, value] of Object.entries(RUNTIME_COMPATIBILITY))
        if (manifest[key] !== value) throw new Error(`Runtime manifest mismatch: ${key}`);
      this.compatibilityChecked = true;
    } catch (error) {
      if (signal?.aborted) throw error;
      throw failure(error, "RUNTIME_INCOMPATIBLE");
    }
  }
  async inspectBundle(files: BundleFiles, precision?: WeightPrecision) {
    await this.verifyCompatibility();
    const { inspectBundle } = await moduleAt(this.assets, "runtime/extraction-bundle.js");
    const result = await inspectBundle(files, precision);
    return {
      ...modelIdentity(result),
      precision: result.precision,
      bytes: result.bytes,
      qualified: false as const,
      cpuReason: result.cpuReason,
    };
  }
  private assertIdle() {
    if (this.disposed) throw new InferenceError("DISPOSED", "Inference client is disposed");
    if (this.busy) throw new InferenceError("BUSY", "Only one model operation may run at a time");
  }
  private stop(error = new InferenceError("CANCELLED", "Inference cancelled")) {
    this.interruption = error;
    this.generation++;
    const runtime = this.runtime,
      gpu = this.gpu;
    this.runtime = null;
    this.gpu = null;
    runtime?.destroy(error);
    gpu?.destroy();
  }
  private invalidate(error: InferenceError, status: "error" | "cancelled") {
    this.stop(error);
    this.setState({
      status,
      model: null,
      operation: null,
      recovery: this.files ? "reload-on-next-run" : "none",
      error,
    });
  }
  cancel() {
    if (!this.disposed)
      this.invalidate(new InferenceError("CANCELLED", "Inference cancelled"), "cancelled");
  }
  async loadModel(
    files: BundleFiles,
    options: RunOptions & { precision?: WeightPrecision; backend?: Backend } = {}
  ) {
    this.assertIdle();
    return this.load(files, options, false);
  }
  private async load(
    files: BundleFiles,
    options: RunOptions & { precision?: WeightPrecision; backend?: Backend },
    reload: boolean
  ): Promise<ModelInfo> {
    this.busy = true;
    // An explicit switch gives up the old model, including its reload recipe.
    // Recovery reloads retain their recipe so another attempt remains possible.
    if (!reload) {
      this.files = null;
      this.precision = undefined;
    }
    this.requestedBackend = options.backend ?? "auto";
    this.stop();
    const generation = this.generation;
    const check = () => {
      if (generation !== this.generation) throw this.interruption;
      if (options.signal?.aborted) throw failure(options.signal.reason, "CANCELLED");
    };
    const progress = (value: InferenceProgress) => {
      check();
      options.onProgress?.(value);
      check();
    };
    const abort = () => this.cancel();
    options.signal?.addEventListener("abort", abort, { once: true });
    let pendingGpu: Gpu | null = null;
    let model: ModelInfo;
    this.setState({
      status: "loading",
      model: null,
      operation: reload ? "reload" : "load",
      recovery: "none",
      error: null,
    });
    try {
      check();
      if (reload) progress({ stage: "reload", loaded: 0, total: 1 });
      progress({ stage: "compatibility", loaded: 0, total: 1 });
      await this.verifyCompatibility(options.signal);
      check();
      progress({ stage: "inspect", loaded: 0, total: 1 });
      const inspected = await this.inspectBundle(files, options.precision);
      check();
      const capabilities = await detectCapabilities();
      check();
      let fallbackReason: string | undefined;
      if (this.requestedBackend !== "wasm" && capabilities.webgpu && !inspected.cpuReason) {
        const { WebGPUOps } = await moduleAt(this.assets, "webgpu-ops.js");
        check();
        pendingGpu = new WebGPUOps();
        pendingGpu.maxBufferBytes = 1024 ** 3;
        progress({ stage: "gpu-init", loaded: 0, total: 1 });
        const initialized = await pendingGpu.init();
        check();
        if (!initialized) {
          fallbackReason = pendingGpu.lastInitError ?? "WebGPU initialization failed";
          pendingGpu.destroy();
          pendingGpu = null;
        } else {
          const gpu = pendingGpu;
          // Publish ownership only after init AND the cancellation check.
          this.gpu = gpu;
          pendingGpu = null;
          gpu.onFatalError = (error) => {
            if (generation === this.generation)
              this.invalidate(failure(error, "GPU_FAILED"), "error");
          };
          void gpu.device?.lost.then((info) => {
            if (generation === this.generation)
              this.invalidate(
                new InferenceError("DEVICE_LOST", `WebGPU device lost: ${info.message}`),
                "error"
              );
          });
          gpu.device?.addEventListener("uncapturederror", (event) => {
            if (generation === this.generation)
              this.invalidate(
                new InferenceError(
                  "GPU_FAILED",
                  `WebGPU failed: ${event.error?.message ?? "unknown device error"}`
                ),
                "error"
              );
          });
        }
      } else if (this.requestedBackend !== "wasm")
        fallbackReason = inspected.cpuReason ?? capabilities.reason;
      const { InferenceWeb } = await moduleAt(this.assets, "inference-web.js");
      check();
      const runtime = new InferenceWeb();
      this.runtime = runtime;
      progress({ stage: "runtime-init", loaded: 0, total: 1 });
      await runtime.init(
        new URL(
          this.gpu ? "antfly-extraction-webgpu.wasm" : "antfly-extraction-cpu.wasm",
          this.assets
        ).href,
        {
          worker: true,
          workerUrl: new URL("inference-worker.js", this.assets).href,
          wasmMemoryModel: "wasm32",
          gpu: this.gpu,
          expectedRuntimeId: RUNTIME_COMPATIBILITY.runtimeId,
        }
      );
      check();
      const loaded = await runtime.loadExtractionBundle(files, options.precision, progress);
      check();
      this.files = files;
      this.precision = options.precision;
      model = Object.freeze({
        ...loaded,
        family: inspected.family,
        architecture: inspected.architecture,
        qualified: false,
        backend: this.gpu ? "webgpu" : "wasm",
        fallbackReason,
      });
    } catch (error) {
      const rejected =
        generation !== this.generation ? this.interruption : failure(error, "MODEL_LOAD_FAILED");
      if (generation === this.generation)
        this.invalidate(rejected, rejected.code === "CANCELLED" ? "cancelled" : "error");
      throw rejected;
    } finally {
      // init can create a device after cancel/dispose; the local owner must
      // destroy it even when another generation has detached the client.
      pendingGpu?.destroy();
      options.signal?.removeEventListener("abort", abort);
      this.busy = false;
    }
    this.setState({ status: "ready", model, operation: null, recovery: "none", error: null });
    check();
    return model;
  }
  run(request: InferenceRequestV1, options?: RunOptions): Promise<RunResult<InferenceResponseV1>>;
  run(request: InferenceRequestV2, options?: RunOptions): Promise<RunResult<InferenceResponseV2>>;
  run(request: InferenceRequest, options?: RunOptions): Promise<RunResult<InferenceResponse>>;
  run(request: InferenceRequest, options: RunOptions = {}) {
    return this.execute<InferenceResponse>(request, options, false);
  }
  runExtension(
    request: ExtensionInferenceRequest,
    options: RunOptions = {}
  ): Promise<RunResult<Record<string, unknown>>> {
    return this.execute(request, options, false);
  }
  validateRequest(request: InferenceRequest, options: RunOptions = {}): Promise<ValidationResult> {
    return this.execute(request, options, true);
  }
  validateExtension(
    request: ExtensionInferenceRequest,
    options: RunOptions = {}
  ): Promise<ValidationResult> {
    return this.execute(request, options, true);
  }
  private async execute<T>(
    request: InferenceRequest | ExtensionInferenceRequest,
    options: RunOptions,
    validateOnly: boolean
  ): Promise<RunResult<T>> {
    this.assertIdle();
    if (options.signal?.aborted) throw failure(options.signal.reason, "CANCELLED");
    if (!this.runtime && this.files)
      await this.load(
        this.files,
        { precision: this.precision, backend: this.requestedBackend, ...options },
        true
      );
    this.assertIdle();
    if (!this.runtime || !this.model)
      throw new InferenceError("MODEL_NOT_LOADED", "Load a model first");
    this.busy = true;
    const generation = this.generation,
      backend = this.model.backend;
    const abort = () => this.cancel();
    options.signal?.addEventListener("abort", abort, { once: true });
    const check = () => {
      if (generation !== this.generation) throw this.interruption;
    };
    const model = this.model;
    let completed: RunResult<T> | undefined;
    let rejected: InferenceError | undefined;
    this.setState({
      status: validateOnly ? "validating" : "running",
      model,
      operation: validateOnly ? "validate" : "run",
      recovery: "none",
      error: null,
    });
    try {
      check();
      options.onProgress?.({
        stage: validateOnly ? "validation" : "inference",
        loaded: 0,
        total: 1,
      });
      check();
      const result = await this.runtime.runExtraction(request, validateOnly);
      check();
      options.onProgress?.({ stage: "complete", loaded: 1, total: 1 });
      check();
      completed = { ...result, value: result.value as T, backend };
    } catch (error) {
      if (generation !== this.generation) throw this.interruption;
      const raw = error as Error & { fatal?: boolean; code?: string };
      const fatal =
        raw?.fatal === true ||
        raw instanceof WebAssembly.RuntimeError ||
        raw?.cause instanceof WebAssembly.RuntimeError;
      rejected = failure(
        error,
        raw?.code === "RUNTIME_INCOMPATIBLE"
          ? "RUNTIME_INCOMPATIBLE"
          : fatal
            ? "RUNTIME_FAILED"
            : "INVALID_REQUEST"
      );
      if (fatal) {
        this.invalidate(rejected, "error");
        throw rejected;
      }
    } finally {
      options.signal?.removeEventListener("abort", abort);
      this.busy = false;
    }
    this.setState({
      status: "ready",
      model,
      operation: null,
      recovery: "none",
      error: rejected ?? null,
    });
    check();
    if (rejected) throw rejected;
    return completed!;
  }
  unloadModel() {
    this.files = null;
    this.precision = undefined;
    this.stop();
    this.setState({
      status: this.disposed ? "disposed" : "idle",
      model: null,
      operation: null,
      recovery: "none",
      error: null,
    });
  }
  dispose() {
    if (this.disposed) return;
    this.disposed = true;
    this.unloadModel();
    this.listeners.clear();
  }
}
export { clearModelCache, downloadCatalogModel } from "./model-cache.js";
