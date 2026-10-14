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

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ExtractError, extractLine, type LineExtraction, type Schema } from "./api";

const DEBOUNCE_MS = 350;
const CONCURRENCY = 1;

type Entry = { ok: LineExtraction } | { err: ExtractError };

export type LineState = "empty" | "queued" | "running" | "done" | "error";

/**
 * Extracts each line as its own document. Results are cached per
 * (schema, line text), so editing one line only re-runs that line.
 */
export function useExtraction(lines: string[], schema: Schema) {
  const cache = useRef(new Map<string, Entry>());
  const queue = useRef<{ key: string; line: string; schema: Schema }[]>([]);
  const running = useRef(new Set<string>());
  const workers = useRef(0);
  const controller = useRef(new AbortController());
  const [, setVersion] = useState(0);
  const [nonce, setNonce] = useState(0);
  const [lastMs, setLastMs] = useState<number | null>(null);
  const schemaKey = useMemo(() => JSON.stringify(schema), [schema]);
  const keyFor = useCallback((line: string) => `${schemaKey}\u0000${line}`, [schemaKey]);
  const rerender = () => setVersion((v) => v + 1);

  // A schema change invalidates everything in flight.
  useEffect(() => {
    controller.current = new AbortController();
    const c = controller.current;
    return () => {
      c.abort();
      queue.current = [];
    };
  }, [schemaKey]);

  useEffect(() => {
    const timer = setTimeout(() => {
      const wanted = new Set(lines.filter((l) => l.trim()));
      // Drop queued lines that no longer exist in the text.
      queue.current = queue.current.filter((q) => wanted.has(q.line));
      const queuedKeys = new Set(queue.current.map((q) => q.key));
      for (const line of wanted) {
        const key = keyFor(line);
        if (!cache.current.has(key) && !running.current.has(key) && !queuedKeys.has(key)) {
          queue.current.push({ key, line, schema });
        }
      }
      const worker = async () => {
        workers.current++;
        try {
          for (let job = queue.current.shift(); job; job = queue.current.shift()) {
            const { key, line, schema } = job;
            const signal = controller.current.signal;
            running.current.add(key);
            rerender();
            try {
              const ex = await extractLine(line, schema, signal);
              cache.current.set(key, { ok: ex });
              setLastMs(ex.ms);
            } catch (err) {
              if (signal.aborted) continue;
              const e = err instanceof ExtractError ? err : new ExtractError(String(err), "request");
              cache.current.set(key, { err: e });
              // No point hammering a server that isn't there.
              if (e.kind === "unreachable") queue.current = [];
            } finally {
              running.current.delete(key);
              rerender();
            }
          }
        } finally {
          workers.current--;
        }
      };
      while (workers.current < CONCURRENCY && queue.current.length > 0) void worker();
      rerender();
    }, DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [lines, schema, keyFor, nonce]);

  const results = lines.map((l) => {
    const entry = cache.current.get(keyFor(l));
    return entry && "ok" in entry ? entry.ok : undefined;
  });

  const states: LineState[] = lines.map((l) => {
    if (!l.trim()) return "empty";
    const key = keyFor(l);
    if (running.current.has(key)) return "running";
    const entry = cache.current.get(key);
    if (entry) return "ok" in entry ? "done" : "error";
    return "queued";
  });

  const errors = lines
    .map((l) => cache.current.get(keyFor(l)))
    .flatMap((e) => (e && "err" in e ? [e.err] : []));
  const error =
    errors.find((e) => e.kind === "unreachable") ??
    errors.find((e) => e.kind === "model") ??
    errors[0] ??
    null;

  const retry = useCallback(() => {
    for (const [k, v] of cache.current) if ("err" in v) cache.current.delete(k);
    setNonce((n) => n + 1);
  }, []);

  return { results, states, error, lastMs, retry };
}
