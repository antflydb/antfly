// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
export type Precision = "q8_0" | "q4_k" | "q4_0" | "fp32" | "fp16_encoder" | "fp16" | "bf16";
export type Backend = "auto" | "wasm" | "webgpu";
export type Progress = { stage: string; file?: string; loaded: number; total: number };
export type BundleFiles = Map<string, Blob> | File[];
export type ModelInfo = {
  architecture: "span" | "boundary" | "decide" | "laya";
  precision: Precision;
  bytes: number;
  backend: "wasm" | "webgpu";
  qualified: boolean;
  fallbackReason?: string;
};
export type RunResult = {
  value: Record<string, unknown>;
  elapsedMs: number;
  wasmBytes: number;
  backend: "wasm" | "webgpu";
};
export type Request = Record<string, unknown> & { schema_version: 1 | 2; model: string };
export type RunOptions = { signal?: AbortSignal; onProgress?: (progress: Progress) => void };
export type CatalogFile = { path: string; url: string; sha256: string; size_bytes: number };
export type CatalogModel = {
  id: string;
  name: string;
  architecture: "span" | "boundary" | "decide" | "laya";
  precision: Precision;
  files: CatalogFile[];
  qualification: "pending" | "passed";
  license: string;
};

// Runtime assets are copied byte-for-byte from zig/pkg/inference/web by the
// app's prepare script. No tokenizer or extraction logic lives in this package.
type Runtime = {
  init: (url: string, options: object) => Promise<void>;
  loadExtractionBundle: (
    files: BundleFiles,
    precision?: Precision,
    progress?: (p: Progress) => void
  ) => Promise<ModelInfo>;
  runExtraction: (request: Request, validate: boolean) => Promise<Omit<RunResult, "backend">>;
  unloadExtraction: () => Promise<void>;
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
async function moduleAt(base: string, name: string): Promise<any> {
  return import(
    /* @vite-ignore */ /* webpackIgnore: true */ /* turbopackIgnore: true */ new URL(name, base)
      .href
  );
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
  private precision?: Precision;
  private requestedBackend: Backend = "auto";
  model: ModelInfo | null = null;
  readonly assets: string;
  constructor(assets = "/inference/") {
    this.assets = new URL(assets, location.href).href;
  }
  async inspectBundle(files: BundleFiles, precision?: Precision) {
    const { inspectBundle } = await moduleAt(this.assets, "runtime/extraction-bundle.js");
    const result = await inspectBundle(files, precision);
    return {
      architecture: result.architecture,
      precision: result.precision,
      bytes: result.bytes,
      qualified: false,
      cpuReason: result.cpuReason as string | undefined,
    } as Omit<ModelInfo, "backend"> & { cpuReason?: string };
  }
  private assertIdle() {
    if (this.disposed) throw new Error("Inference client is disposed");
    if (this.busy) throw new Error("Only one model operation may run at a time");
  }
  private stop(reason?: Error) {
    this.generation++;
    this.runtime?.destroy(reason);
    this.runtime = null;
    this.gpu?.destroy();
    this.gpu = null;
    this.model = null;
  }
  cancel() {
    this.stop();
  }
  async loadModel(
    files: BundleFiles,
    options: RunOptions & { precision?: Precision; backend?: Backend } = {}
  ) {
    this.assertIdle();
    this.busy = true;
    this.stop();
    const generation = this.generation;
    const check = () => {
      options.signal?.throwIfAborted();
      if (generation !== this.generation) throw new DOMException("Loading cancelled", "AbortError");
    };
    const abort = () => this.cancel();
    options.signal?.addEventListener("abort", abort, { once: true });
    try {
      check();
      const inspected = await this.inspectBundle(files, options.precision);
      check();
      const capabilities = await detectCapabilities();
      check();
      this.requestedBackend = options.backend ?? "auto";
      let fallbackReason: string | undefined;
      if (this.requestedBackend !== "wasm" && capabilities.webgpu && !inspected.cpuReason) {
        const { WebGPUOps } = await moduleAt(this.assets, "webgpu-ops.js");
        check();
        const gpu: Gpu = new WebGPUOps();
        gpu.maxBufferBytes = 1024 ** 3;
        this.gpu = gpu;
        if (!(await gpu.init())) {
          fallbackReason = gpu.lastInitError ?? "WebGPU initialization failed";
          gpu.destroy();
          this.gpu = null;
        }
        check();
        if (this.gpu) {
          gpu.onFatalError = (error) => {
            if (generation === this.generation) this.stop(error);
          };
          void gpu.device?.lost.then((info) => {
            if (generation === this.generation)
              this.stop(new Error(`WebGPU device lost: ${info.message}. Reload the model.`));
          });
          gpu.device?.addEventListener("uncapturederror", (event) => {
            if (generation === this.generation)
              this.stop(
                new Error(
                  `WebGPU failed: ${event.error?.message ?? "unknown device error"}. Reload the model.`
                )
              );
          });
        }
      } else if (this.requestedBackend !== "wasm")
        fallbackReason = inspected.cpuReason ?? capabilities.reason;
      const { InferenceWeb } = await moduleAt(this.assets, "inference-web.js");
      check();
      const runtime: Runtime = new InferenceWeb();
      this.runtime = runtime;
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
        }
      );
      check();
      const loaded = await runtime.loadExtractionBundle(
        files,
        options.precision,
        options.onProgress
      );
      check();
      this.files = files;
      this.precision = options.precision;
      this.model = { ...loaded, backend: this.gpu ? "webgpu" : "wasm", fallbackReason };
      return this.model;
    } catch (error) {
      if (generation === this.generation) this.stop();
      throw error;
    } finally {
      options.signal?.removeEventListener("abort", abort);
      this.busy = false;
    }
  }
  async run(request: Request, options: RunOptions = {}, validateOnly = false): Promise<RunResult> {
    this.assertIdle();
    options.signal?.throwIfAborted();
    if (!this.runtime && this.files)
      await this.loadModel(this.files, {
        precision: this.precision,
        backend: this.requestedBackend,
        ...options,
      });
    if (!this.runtime || !this.model) throw new Error("Load a model first");
    this.busy = true;
    const generation = this.generation,
      backend = this.model.backend;
    const abort = () => this.cancel();
    options.signal?.addEventListener("abort", abort, { once: true });
    try {
      options.signal?.throwIfAborted();
      options.onProgress?.({ stage: "inference", loaded: 0, total: 1 });
      const result = await this.runtime.runExtraction(request, validateOnly);
      if (generation !== this.generation)
        throw new DOMException("Inference cancelled", "AbortError");
      options.onProgress?.({ stage: "complete", loaded: 1, total: 1 });
      return { ...result, backend };
    } catch (error) {
      // Request validation errors leave the loaded model usable. Traps and
      // worker failures can leave runtime state invalid and require a reload.
      const failure = error as Error & { fatal?: boolean };
      if (
        generation === this.generation &&
        (failure?.fatal === true ||
          failure instanceof WebAssembly.RuntimeError ||
          failure?.cause instanceof WebAssembly.RuntimeError)
      )
        this.stop();
      throw error;
    } finally {
      options.signal?.removeEventListener("abort", abort);
      this.busy = false;
    }
  }
  validateRequest(request: Request, options?: RunOptions) {
    return this.run(request, options, true);
  }
  unloadModel() {
    this.stop();
    this.files = null;
    this.precision = undefined;
  }
  dispose() {
    this.unloadModel();
    this.disposed = true;
  }
}

export { clearModelCache, downloadCatalogModel } from "./model-cache.js";
