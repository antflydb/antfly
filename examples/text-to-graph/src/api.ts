// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

export const MODEL: string = import.meta.env.VITE_ANTFLY_MODEL ?? "fastino/gliner2.5-base-v1";

export type Entity = {
  text: string;
  label: string;
  start: number;
  end: number;
  score?: number;
  /** Added from a relation endpoint rather than returned as an entity. */
  implicit?: boolean;
};

/** A relation endpoint. `entity_index` is absent when the span isn't in `entities`. */
export type Endpoint = {
  entity_index?: number;
  text?: string;
  label?: string;
  start?: number;
  end?: number;
};

export type Relation = {
  type: string;
  source: Endpoint;
  target: Endpoint;
  score?: number;
};

export type LineExtraction = {
  entities: Entity[];
  relations: Relation[];
  ms: number;
};

export type Schema = {
  entities: string[];
  relations: string[];
};

export class ExtractError extends Error {
  constructor(
    message: string,
    readonly kind: "unreachable" | "model" | "request",
    readonly retryAfterMs?: number,
  ) {
    super(message);
  }
}

/** Extract entities and relations from one line (one document). */
export async function extractLine(
  content: string,
  schema: Schema,
  signal?: AbortSignal,
): Promise<LineExtraction> {
  const started = performance.now();
  for (let attempt = 0; ; attempt++) {
    try {
      const ex = await extractOnce(content, schema, signal);
      return { ...ex, ms: performance.now() - started };
    } catch (err) {
      // The model admits one request at a time; back off when it says so.
      if (!(err instanceof ExtractError) || err.retryAfterMs === undefined || attempt >= 8) throw err;
      await new Promise((r) => setTimeout(r, Math.min(err.retryAfterMs!, 250 * (attempt + 1))));
      if (signal?.aborted) throw err;
    }
  }
}

async function extractOnce(content: string, schema: Schema, signal?: AbortSignal) {
  let res: Response;
  try {
    res = await fetch("/ai/v1/extract", {
      method: "POST",
      headers: { "content-type": "application/json" },
      signal,
      body: JSON.stringify({
        model: MODEL,
        inputs: [{ content }],
        schema: {
          entities: schema.entities,
          relations: schema.relations.map((type) => ({ type })),
        },
        options: { include_confidence: true, include_spans: true },
      }),
    });
  } catch (err) {
    if (signal?.aborted) throw err;
    throw new ExtractError("Can't reach Antfly", "unreachable");
  }
  // The Vite proxy answers 502/504 when nothing listens on the Antfly port.
  if (res.status === 502 || res.status === 504) {
    throw new ExtractError("Can't reach Antfly", "unreachable");
  }
  const body = await res.json().catch(() => null);
  if (!res.ok) {
    const message = body?.message ?? `HTTP ${res.status}`;
    const kind = res.status >= 500 || body?.error === "MODEL_NOT_FOUND" ? "model" : "request";
    throw new ExtractError(message, kind, body?.retryable ? (body.retry_after_ms ?? 200) : undefined);
  }
  const item = body?.data?.[0] ?? {};
  return resolve(content, item.entities ?? [], item.relations ?? []);
}

/** Map server offsets onto JS string indices (offsets may count UTF-8 bytes). */
function fixSpan(line: string, e: Entity): Entity {
  if (line.slice(e.start, e.end) === e.text) return e;
  const bytes = new TextEncoder().encode(line);
  const dec = new TextDecoder();
  const start = dec.decode(bytes.slice(0, e.start)).length;
  const end = dec.decode(bytes.slice(0, e.end)).length;
  if (line.slice(start, end) === e.text) return { ...e, start, end };
  const found = line.indexOf(e.text);
  return found >= 0 ? { ...e, start: found, end: found + e.text.length } : { ...e, start: 0, end: 0 };
}

/**
 * Normalize one line's result: entity spans in JS string indices, and every
 * relation endpoint pointing at an entry in `entities`. Endpoints without an
 * `entity_index` match an entity by text or are added as entities.
 */
function resolve(line: string, raw: Entity[], relations: Relation[]) {
  const entities = raw.map((e) => fixSpan(line, e));
  const indexOf = (p: Endpoint): number | null => {
    if (p.entity_index !== undefined && entities[p.entity_index]) return p.entity_index;
    if (!p.text) return null;
    const found = entities.findIndex((e) => e.text === p.text);
    if (found >= 0) return found;
    entities.push(
      fixSpan(line, { text: p.text, label: p.label ?? "other", start: p.start ?? 0, end: p.end ?? 0, implicit: true }),
    );
    return entities.length - 1;
  };
  const resolved: Relation[] = [];
  for (const r of relations) {
    const s = indexOf(r.source);
    const t = indexOf(r.target);
    if (s !== null && t !== null) {
      resolved.push({ ...r, source: { entity_index: s }, target: { entity_index: t } });
    }
  }
  return { entities, relations: resolved };
}

export type ModelStatus = "ready" | "missing" | "unreachable";

export async function checkModel(): Promise<ModelStatus> {
  try {
    const res = await fetch("/ai/v1/models");
    if (!res.ok) return "unreachable";
    const body = await res.json();
    // Pinned pulls are listed as "owner/name:variant@revision".
    const names = Object.keys(body?.extractors ?? {});
    return names.some((n) => n === MODEL || n.startsWith(`${MODEL}:`)) ? "ready" : "missing";
  } catch {
    return "unreachable";
  }
}
