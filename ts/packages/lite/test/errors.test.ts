// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import koffi from "koffi";
import { describe, expect, it } from "vitest";
import { abiVersion, threadingMode, validateAbi } from "../src/abi.js";
import {
  AntflyError,
  BusyError,
  ErrorCode,
  errorCodeDescription,
  errorCodeName,
  errorFromCode,
  IntentConflictError,
  InternalError,
  InvalidArgumentError,
  NotFoundError,
  OutcomeUnknownError,
  StalledError,
  TxnNotFoundError,
  UnsupportedError,
  VersionConflictError,
} from "../src/errors.js";
import { AntflyLiteOpenOptions, loadNative } from "../src/native.js";
import { SUPPORTED_ABI_VERSION, THREADING_SERIALIZED } from "../src/types.js";
import { describeWithLibrary } from "./helpers.js";

// Pure error-mapping tests: no native library required, and none loaded.
describe("errorCodeName / errorCodeDescription (pure)", () => {
  it("names every documented code", () => {
    expect(errorCodeName(ErrorCode.OK)).toBe("ANTFLY_OK");
    expect(errorCodeName(ErrorCode.InvalidArgument)).toBe("ANTFLY_INVALID_ARGUMENT");
    expect(errorCodeName(ErrorCode.NotFound)).toBe("ANTFLY_NOT_FOUND");
    expect(errorCodeName(ErrorCode.VersionConflict)).toBe("ANTFLY_VERSION_CONFLICT");
    expect(errorCodeName(ErrorCode.IntentConflict)).toBe("ANTFLY_INTENT_CONFLICT");
    expect(errorCodeName(ErrorCode.TxnNotFound)).toBe("ANTFLY_TXN_NOT_FOUND");
    expect(errorCodeName(ErrorCode.Busy)).toBe("ANTFLY_BUSY");
    expect(errorCodeName(ErrorCode.OutcomeUnknown)).toBe("ANTFLY_OUTCOME_UNKNOWN");
    expect(errorCodeName(ErrorCode.Unsupported)).toBe("ANTFLY_UNSUPPORTED");
    expect(errorCodeName(ErrorCode.Stalled)).toBe("ANTFLY_STALLED");
    expect(errorCodeName(ErrorCode.Internal)).toBe("ANTFLY_INTERNAL");
  });

  it("falls back for unknown codes", () => {
    expect(errorCodeName(999)).toBe("ANTFLY_UNKNOWN_ERROR");
    expect(errorCodeDescription(999)).toBe("unknown Antfly error code");
  });

  it("every code has a non-empty description", () => {
    for (const code of [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 255]) {
      expect(errorCodeDescription(code).length).toBeGreaterThan(0);
    }
  });
});

describe("errorFromCode (pure)", () => {
  const cases: Array<[number, new (message?: string) => AntflyError]> = [
    [ErrorCode.InvalidArgument, InvalidArgumentError],
    [ErrorCode.NotFound, NotFoundError],
    [ErrorCode.VersionConflict, VersionConflictError],
    [ErrorCode.IntentConflict, IntentConflictError],
    [ErrorCode.TxnNotFound, TxnNotFoundError],
    [ErrorCode.Busy, BusyError],
    [ErrorCode.OutcomeUnknown, OutcomeUnknownError],
    [ErrorCode.Unsupported, UnsupportedError],
    [ErrorCode.Stalled, StalledError],
    [ErrorCode.Internal, InternalError],
  ];

  it.each(cases)("maps code %i to its subclass", (code, Ctor) => {
    const err = errorFromCode(code);
    expect(err).toBeInstanceOf(Ctor);
    expect(err).toBeInstanceOf(AntflyError);
    expect(err.code).toBe(code);
    expect(err.codeName).toBe(errorCodeName(code));
  });

  it("falls back to AntflyError for unknown codes", () => {
    const err = errorFromCode(123);
    expect(err.constructor).toBe(AntflyError);
    expect(err.code).toBe(123);
  });
});

// Library-dependent: cross-checks our static error tables against the
// loaded libantfly's antfly_error_code_name/antfly_error_code_description,
// and validates ABI/struct-size agreement.
describeWithLibrary("native ABI cross-checks", () => {
  const codes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 255];

  it.each(codes)("name and description for code %i match the native library", (code) => {
    const native = loadNative();
    expect(errorCodeName(code)).toBe(native.errorCodeName(code));
    expect(errorCodeDescription(code)).toBe(native.errorCodeDescription(code));
  });

  it("abiVersion() matches SUPPORTED_ABI_VERSION", () => {
    expect(abiVersion()).toBe(SUPPORTED_ABI_VERSION);
  });

  it("threadingMode() reports THREADING_SERIALIZED", () => {
    expect(threadingMode()).toBe(THREADING_SERIALIZED);
  });

  it("validateAbi() does not throw", () => {
    expect(() => validateAbi()).not.toThrow();
  });

  it("antfly_lite_open_options_size() agrees with the compiled struct size", () => {
    const native = loadNative();
    expect(native.liteOpenOptionsSize()).toBe(koffi.sizeof(AntflyLiteOpenOptions));
  });
});
