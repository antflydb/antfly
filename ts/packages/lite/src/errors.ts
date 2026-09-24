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

/**
 * Stable Antfly C ABI error codes (antfly_error_code in antfly.h). Values and
 * descriptions mirror go/pkg/lite/errors.go so behavior stays identical
 * across bindings; a conformance/errors test cross-checks these against the
 * loaded library's antfly_error_code_name/antfly_error_code_description.
 */
export enum ErrorCode {
  OK = 0,
  InvalidArgument = 1,
  NotFound = 2,
  VersionConflict = 3,
  IntentConflict = 4,
  TxnNotFound = 5,
  Busy = 6,
  /**
   * The operation crossed its publication point, but crash durability could
   * not be confirmed. Inspect the destination; do not retry automatically.
   */
  OutcomeUnknown = 7,
  /**
   * The operation requires a capability unavailable on this platform or
   * filesystem. Retrying unchanged will not succeed.
   */
  Unsupported = 8,
  /**
   * A bounded drain such as runUntilIdle detected a managed index making no
   * forward progress and gave up.
   */
  Stalled = 9,
  /**
   * The caller cancelled the call by returning false from its progress or
   * stream callback (Inference.pull()'s onProgress, Inference.generateStream()'s
   * onChunk).
   */
  Cancelled = 10,
  Internal = 255,
}

const ERROR_CODE_NAMES: Readonly<Record<number, string>> = {
  [ErrorCode.OK]: "ANTFLY_OK",
  [ErrorCode.InvalidArgument]: "ANTFLY_INVALID_ARGUMENT",
  [ErrorCode.NotFound]: "ANTFLY_NOT_FOUND",
  [ErrorCode.VersionConflict]: "ANTFLY_VERSION_CONFLICT",
  [ErrorCode.IntentConflict]: "ANTFLY_INTENT_CONFLICT",
  [ErrorCode.TxnNotFound]: "ANTFLY_TXN_NOT_FOUND",
  [ErrorCode.Busy]: "ANTFLY_BUSY",
  [ErrorCode.OutcomeUnknown]: "ANTFLY_OUTCOME_UNKNOWN",
  [ErrorCode.Unsupported]: "ANTFLY_UNSUPPORTED",
  [ErrorCode.Stalled]: "ANTFLY_STALLED",
  [ErrorCode.Cancelled]: "ANTFLY_CANCELLED",
  [ErrorCode.Internal]: "ANTFLY_INTERNAL",
};

const ERROR_CODE_DESCRIPTIONS: Readonly<Record<number, string>> = {
  [ErrorCode.OK]: "operation completed successfully",
  [ErrorCode.InvalidArgument]: "an argument, request, path, or open mode is invalid",
  [ErrorCode.NotFound]: "the requested database object was not found",
  [ErrorCode.VersionConflict]: "a version predicate did not match the current document version",
  [ErrorCode.IntentConflict]: "a transaction intent conflicts with the requested operation",
  [ErrorCode.TxnNotFound]: "the requested transaction was not found",
  [ErrorCode.Busy]:
    "the requested resource is temporarily busy or changed during streaming; stabilize it and retry",
  [ErrorCode.OutcomeUnknown]:
    "the operation was published, but crash durability could not be confirmed; inspect the destination and do not retry automatically",
  [ErrorCode.Unsupported]:
    "the operation requires a capability that is not supported by this platform or filesystem",
  [ErrorCode.Stalled]:
    "a bounded drain made no forward progress for its configured stall window and gave up",
  [ErrorCode.Cancelled]: "the caller cancelled the operation",
  [ErrorCode.Internal]: "an internal error occurred",
};

/** Stable symbolic C ABI name for code, e.g. "ANTFLY_BUSY". */
export function errorCodeName(code: number): string {
  return ERROR_CODE_NAMES[code] ?? "ANTFLY_UNKNOWN_ERROR";
}

/** Short stable description for code. */
export function errorCodeDescription(code: number): string {
  return ERROR_CODE_DESCRIPTIONS[code] ?? "unknown Antfly error code";
}

/**
 * AntflyError wraps a stable Antfly C ABI error code. `.code` is the numeric
 * antfly_error_code, `.codeName` is its stable symbolic name (e.g.
 * "ANTFLY_BUSY"), matching antfly_error_code_name.
 */
export class AntflyError extends Error {
  readonly code: number;
  readonly codeName: string;
  /**
   * Parsed JSON error body, when the failing call returned one. Currently
   * only Inference calls populate this (embed/rerank/.../pull): libantfly
   * always fills their antfly_buffer output with {"error": ..., "message":
   * ...} on failure, even though the call also returns a non-OK error code.
   */
  body?: unknown;

  constructor(code: number, message?: string) {
    const codeName = errorCodeName(code);
    super(message ?? `${codeName}: ${errorCodeDescription(code)}`);
    this.name = "AntflyError";
    this.code = code;
    this.codeName = codeName;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class InvalidArgumentError extends AntflyError {
  constructor(message?: string) {
    super(ErrorCode.InvalidArgument, message);
    this.name = "InvalidArgumentError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class NotFoundError extends AntflyError {
  constructor(message?: string) {
    super(ErrorCode.NotFound, message);
    this.name = "NotFoundError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class VersionConflictError extends AntflyError {
  constructor(message?: string) {
    super(ErrorCode.VersionConflict, message);
    this.name = "VersionConflictError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class IntentConflictError extends AntflyError {
  constructor(message?: string) {
    super(ErrorCode.IntentConflict, message);
    this.name = "IntentConflictError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class TxnNotFoundError extends AntflyError {
  constructor(message?: string) {
    super(ErrorCode.TxnNotFound, message);
    this.name = "TxnNotFoundError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class BusyError extends AntflyError {
  constructor(message?: string) {
    super(ErrorCode.Busy, message);
    this.name = "BusyError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class OutcomeUnknownError extends AntflyError {
  constructor(message?: string) {
    super(ErrorCode.OutcomeUnknown, message);
    this.name = "OutcomeUnknownError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class UnsupportedError extends AntflyError {
  constructor(message?: string) {
    super(ErrorCode.Unsupported, message);
    this.name = "UnsupportedError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class StalledError extends AntflyError {
  constructor(message?: string) {
    super(ErrorCode.Stalled, message);
    this.name = "StalledError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class CancelledError extends AntflyError {
  constructor(message?: string) {
    super(ErrorCode.Cancelled, message);
    this.name = "CancelledError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class InternalError extends AntflyError {
  constructor(message?: string) {
    super(ErrorCode.Internal, message);
    this.name = "InternalError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

const ERROR_CLASSES: Readonly<Record<number, new (message?: string) => AntflyError>> = {
  [ErrorCode.InvalidArgument]: InvalidArgumentError,
  [ErrorCode.NotFound]: NotFoundError,
  [ErrorCode.VersionConflict]: VersionConflictError,
  [ErrorCode.IntentConflict]: IntentConflictError,
  [ErrorCode.TxnNotFound]: TxnNotFoundError,
  [ErrorCode.Busy]: BusyError,
  [ErrorCode.OutcomeUnknown]: OutcomeUnknownError,
  [ErrorCode.Unsupported]: UnsupportedError,
  [ErrorCode.Stalled]: StalledError,
  [ErrorCode.Cancelled]: CancelledError,
  [ErrorCode.Internal]: InternalError,
};

/** Builds the typed AntflyError subclass for a raw antfly_error_code. */
export function errorFromCode(code: number, message?: string): AntflyError {
  const Ctor = ERROR_CLASSES[code];
  if (Ctor) {
    return new Ctor(message);
  }
  return new AntflyError(code, message);
}

/** Throws if code is not ANTFLY_OK (0); otherwise returns void. */
export function checkCode(code: number): void {
  if (code !== ErrorCode.OK) {
    throw errorFromCode(code);
  }
}
