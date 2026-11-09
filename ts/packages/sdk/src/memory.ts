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

/** Catalog scope for a memory call; omitted fields use the server defaults. */
export interface MemoryScopeOptions {
  /** `X-Antfly-Database` (server default `"default"`). */
  database?: string;
  /** `X-Antfly-Namespace` (server default `"public"`). */
  namespace?: string;
  /** `X-Antfly-Memory-Branch` (server default `""`, the main branch). */
  branch?: string;
}

/** Scope plus the idempotency key for mutating memory calls. */
export interface MemoryCallOptions extends MemoryScopeOptions {
  /** Stable per-principal key (≤256 chars); a random UUID when omitted. Reuse it to retry safely. */
  idempotencyKey?: string;
}

type MemoryGetQuery<P extends keyof MemoryPaths> = MemoryPaths[P] extends {
  get: { parameters: { query?: infer Query } };
}
  ? NonNullable<Query>
  : never;

export type MemoryListQuery = MemoryGetQuery<"/memory/v1/spaces/{space}/memories">;
export type MemoryHistoryQuery = MemoryGetQuery<"/memory/v1/spaces/{space}/memories/{id}/history">;
export type MemoryPageQuery = MemoryGetQuery<"/memory/v1/spaces/{space}/entities">;
export type MemoryEpisodeQuery = MemoryGetQuery<"/memory/v1/spaces/{space}/episodes">;

type MemoryAssertionInput = Omit<MemorySchemas["MemoryAssertion"], "evidence"> &
  Partial<Pick<MemorySchemas["MemoryAssertion"], "evidence">>;

/** Input for `client.memory.remember`: one text evidence item plus optional observation fields. */
export type MemoryRememberInput = Omit<
  MemorySchemas["MemoryObservationRequest"],
  "evidence" | "assertions"
> & {
  /** The text to remember. */
  text: string;
  /** Evidence id; a random UUID when omitted. */
  id?: string;
  /** Evidence `source_id`. */
  sourceId?: string;
  /** Extra evidence fields (author, event_time, thread_id, visibility, ...). */
  evidence?: Partial<Omit<MemorySchemas["MemoryEvidence"], "id" | "text" | "source_id">>;
  /** Assertions without `evidence` cite the remembered text. */
  assertions?: MemoryAssertionInput[];
};

export type MemoryScopeHeaders = {
  "X-Antfly-Database"?: string;
  "X-Antfly-Namespace"?: string;
  "X-Antfly-Memory-Branch"?: string;
};

/** Scope headers for the fields that were provided. */
export function memoryScopeHeaders(options?: MemoryScopeOptions): MemoryScopeHeaders {
  const headers: MemoryScopeHeaders = {};
  if (options?.database !== undefined) headers["X-Antfly-Database"] = options.database;
  if (options?.namespace !== undefined) headers["X-Antfly-Namespace"] = options.namespace;
  if (options?.branch !== undefined) headers["X-Antfly-Memory-Branch"] = options.branch;
  return headers;
}

/** Scope headers plus `Idempotency-Key`; a bare string is the key (legacy positional form). */
export function memoryMutationHeaders(
  options?: string | MemoryCallOptions
): MemoryScopeHeaders & { "Idempotency-Key": string } {
  const resolved = typeof options === "string" ? { idempotencyKey: options } : options;
  return {
    ...memoryScopeHeaders(resolved),
    "Idempotency-Key": resolved?.idempotencyKey ?? globalThis.crypto.randomUUID(),
  };
}
