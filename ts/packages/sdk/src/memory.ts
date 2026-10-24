// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import { isInteger, parse, stringify } from "lossless-json";
import createClient, { type Client } from "openapi-fetch";
import type { components, paths } from "./public-api.js";

/** Safe integers stay numbers; full-width native integers round-trip as bigint. */
export type MemoryNumbers<T> = T extends number
  ? T | bigint
  : T extends readonly (infer Item)[]
    ? MemoryNumbers<Item>[]
    : T extends object
      ? { [Key in keyof T]: MemoryNumbers<T[Key]> }
      : T;

export type MemoryPaths = MemoryNumbers<Pick<paths, Extract<keyof paths, `/memory/v1/${string}`>>>;
export type MemorySchemas = MemoryNumbers<components["schemas"]>;
export type OrdinaryPaths = Omit<paths, keyof MemoryPaths>;

export function parseMemoryJSON(text: string): unknown {
  return parse(text, null, {
    parseNumber: (token) => {
      const value = Number(token);
      return isInteger(token) && !Number.isSafeInteger(value) ? BigInt(token) : value;
    },
  });
}

export function stringifyMemoryJSON(value: unknown): string {
  const text = stringify(value, (_key, child) => {
    if (
      typeof child === "number" &&
      (!Number.isFinite(child) || (Number.isInteger(child) && !Number.isSafeInteger(child)))
    ) {
      throw new RangeError(
        "Memory integers must be safe numbers or bigint; unsafe numbers have already lost precision"
      );
    }
    return child;
  });
  if (text === undefined) throw new TypeError("Memory requests must be JSON values");
  return text;
}

/** Use the same auth/configuration as ordinary requests with an exact numeric codec. */
export function createMemoryClient(
  baseUrl: string,
  headers: Record<string, string>
): Client<MemoryPaths> {
  const transport = createClient<MemoryPaths>({
    baseUrl,
    headers,
    bodySerializer: stringifyMemoryJSON,
  });
  // openapi-fetch's default JSON parsing rounds numbers, including on its error
  // path. Decode success bodies as text and preserve raw error bodies before
  // that path consumes them. Other explicit parseAs modes remain unchanged.
  const errors = new WeakMap<Response, Promise<string>>();
  transport.use({
    onResponse({ response }) {
      if (!response.ok) errors.set(response, response.clone().text());
    },
  });
  type Options = { parseAs?: string; [key: string]: unknown };
  type Result = { data?: unknown; error?: unknown; response: Response };
  type Request = (method: string, path: string, options?: Options) => Promise<Result>;
  const request = transport.request as unknown as Request;
  const execute: Request = async (method, path, options) => {
    const json = options?.parseAs === undefined || options.parseAs === "json";
    const result = await request(method, path, {
      ...options,
      ...(json ? { parseAs: "text" } : {}),
    });
    if (!json) return result;
    if (result.response.ok) {
      return {
        ...result,
        ...(typeof result.data === "string"
          ? { data: result.data ? parseMemoryJSON(result.data) : undefined }
          : {}),
      };
    }
    const raw = await errors.get(result.response);
    if (raw) {
      try {
        return { ...result, error: parseMemoryJSON(raw) };
      } catch {
        /* Preserve the transport's plain-text error for non-JSON responses. */
      }
    }
    return result;
  };
  const client = { ...transport, request: execute };
  for (const method of [
    "GET",
    "POST",
    "PUT",
    "PATCH",
    "DELETE",
    "HEAD",
    "OPTIONS",
    "TRACE",
  ] as const) {
    // This boundary preserves openapi-fetch's path-dependent request/response
    // types; only the JSON numeric representation is widened by MemoryPaths.
    Object.assign(client, {
      [method]: (path: string, options?: Options) => execute(method, path, options),
    });
  }
  return client as unknown as Client<MemoryPaths>;
}
