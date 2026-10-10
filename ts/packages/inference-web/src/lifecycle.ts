// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import type { ModelInfo } from "./contracts.js";
export type InferenceErrorCode =
  | "DISPOSED"
  | "BUSY"
  | "MODEL_NOT_LOADED"
  | "CANCELLED"
  | "MODEL_LOAD_FAILED"
  | "INVALID_REQUEST"
  | "RUNTIME_FAILED"
  | "DEVICE_LOST"
  | "GPU_FAILED"
  | "RUNTIME_INCOMPATIBLE";
export class InferenceError extends Error {
  constructor(
    readonly code: InferenceErrorCode,
    message: string,
    options?: ErrorOptions
  ) {
    super(message, options);
    this.name = code === "CANCELLED" ? "AbortError" : "InferenceError";
  }
}
export type InferenceState = Readonly<{
  status:
    | "idle"
    | "loading"
    | "ready"
    | "running"
    | "validating"
    | "cancelled"
    | "error"
    | "disposed";
  model: ModelInfo | null;
  operation: "load" | "reload" | "run" | "validate" | null;
  recovery: "none" | "reload-on-next-run";
  error: Readonly<{ code: InferenceErrorCode; message: string }> | null;
}>;
