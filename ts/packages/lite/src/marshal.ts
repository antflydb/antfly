// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { promisify } from "node:util";
import koffi from "koffi";
import { InvalidArgumentError } from "./errors.js";
import type { NativeLibrary } from "./native.js";

/** Accepted shapes for a uint64_t argument: plain number (safe up to 2^53) or bigint. */
export type Uint64Like = number | bigint;

/** Accepted shapes for a JSON request body: an object (JSON.stringify'd), a raw JSON string, or raw bytes. */
export type JsonInput = Record<string, unknown> | unknown[] | string | Uint8Array;

/** An antfly_slice-shaped input struct value: {ptr, len}, ptr null for empty. */
export interface SliceArg {
  ptr: Buffer | null;
  len: number;
}

/** Converts arbitrary bytes-like input to a Buffer (no copy for Buffer input). */
export function toRawBytes(input: Uint8Array | Buffer): Buffer {
  if (Buffer.isBuffer(input)) {
    return input;
  }
  if (input instanceof Uint8Array) {
    return Buffer.from(input.buffer, input.byteOffset, input.byteLength);
  }
  throw new InvalidArgumentError(`expected Buffer or Uint8Array, got ${typeof input}`);
}

/** Converts a JSON-ish request (object, string, or raw bytes) to UTF-8 bytes. */
export function toJsonBytes(input: JsonInput): Buffer {
  if (typeof input === "string") {
    return Buffer.from(input, "utf8");
  }
  if (Buffer.isBuffer(input) || input instanceof Uint8Array) {
    return toRawBytes(input);
  }
  return Buffer.from(JSON.stringify(input), "utf8");
}

/** Wraps bytes as an antfly_slice input struct value. */
export function sliceOf(bytes: Buffer): SliceArg {
  return bytes.length === 0 ? { ptr: null, len: 0 } : { ptr: bytes, len: bytes.length };
}

/** Wraps a JSON-ish request as an antfly_slice, keeping the backing Buffer reachable. */
export function jsonSlice(input: JsonInput): SliceArg {
  return sliceOf(toJsonBytes(input));
}

/** Wraps a string as a UTF-8 antfly_slice. */
export function stringSlice(input: string): SliceArg {
  return sliceOf(Buffer.from(input, "utf8"));
}

/** Parses a raw JSON Buffer; empty buffers decode to null. */
export function parseJson(buf: Buffer): unknown {
  if (buf.length === 0) {
    return null;
  }
  return JSON.parse(buf.toString("utf8"));
}

/** Decodes the {ptr, len} fields koffi wrote into an antfly_buffer out-param. */
export interface DecodedBuffer {
  ptr: bigint | number | null;
  len: bigint | number;
}

/** A fresh {ptr: null, len: 0} placeholder to pass as an antfly_buffer* out-param. */
export function newBufferOut(): DecodedBuffer {
  return { ptr: null, len: 0 };
}

/**
 * Copies the bytes referenced by a decoded antfly_buffer out-param into a
 * Buffer, then releases the native allocation with antfly_buffer_free. Must
 * be called exactly once per successful call that populates `out`.
 */
export function takeBuffer(native: NativeLibrary, out: DecodedBuffer): Buffer {
  try {
    const len = Number(out.len ?? 0);
    if (!out.ptr || len === 0) {
      return Buffer.alloc(0);
    }
    const view = koffi.decode(out.ptr, "uint8_t", len) as Uint8Array;
    return Buffer.from(view);
  } finally {
    native.bufferFree(out as unknown as object);
  }
}

/** Any koffi-declared function: callAsync only needs its `.async` member. */
export interface AnyNativeFn {
  // biome-ignore lint/suspicious/noExplicitAny: koffi function signatures vary per call site
  async: (...args: any[]) => void;
}

/**
 * Promisifies a koffi async function member. koffi's `fn.async(...args, cb)`
 * matches Node's (err, result) callback convention exactly, so
 * util.promisify works directly; the result is the function's C return
 * value (a raw antfly_error_code as a number). Any additional out-parameter
 * objects passed in `args` are mutated in place once the returned promise
 * settles (not before) -- callers read them only after awaiting.
 */
export function callAsync<T = number>(fn: AnyNativeFn, ...args: unknown[]): Promise<T> {
  // biome-ignore lint/suspicious/noExplicitAny: promisify's typings don't model variadic-args-plus-callback natively
  const bound = promisify(fn.async.bind(fn)) as (...a: any[]) => Promise<T>;
  return withNativeSlot(() => bound(...args));
}

/**
 * Caps in-flight native calls at the libuv threadpool size. koffi reserves a
 * full native stack (NATIVE_STACK_SIZE) for every async call it accepts,
 * including queued ones, so more calls than worker threads would only hold
 * memory. Excess calls wait here in JS instead.
 */
const nativeSlots = Math.max(1, Number.parseInt(process.env.UV_THREADPOOL_SIZE ?? "", 10) || 4);
let nativeActive = 0;
const nativeWaiters: Array<() => void> = [];

async function withNativeSlot<T>(run: () => Promise<T>): Promise<T> {
  if (nativeActive >= nativeSlots) {
    await new Promise<void>((resolve) => nativeWaiters.push(resolve));
  } else {
    nativeActive++;
  }
  try {
    return await run();
  } finally {
    const next = nativeWaiters.shift();
    if (next) {
      next();
    } else {
      nativeActive--;
    }
  }
}

/** 16-byte transaction id, accepted as a Uint8Array(16) or a 32-char hex string. */
export function txnIdBytes(id: Uint8Array | string): Buffer {
  if (typeof id === "string") {
    if (!/^[0-9a-fA-F]{32}$/.test(id)) {
      throw new InvalidArgumentError(`txnId must be 32 hex characters, got ${JSON.stringify(id)}`);
    }
    return Buffer.from(id, "hex");
  }
  const buf = toRawBytes(id);
  if (buf.length !== 16) {
    throw new InvalidArgumentError(`txnId must be 16 bytes, got ${buf.length}`);
  }
  return buf;
}

export function txnIdToHex(buf: Uint8Array): string {
  return Buffer.from(buf).toString("hex");
}
