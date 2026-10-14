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

import type { LineExtraction, Relation } from "./api";

/**
 * GLiNER2.5 scores every relation type for every entity pair, so a pair
 * often gets a strong correct type and a weaker wrong one ("acted_in" 0.86,
 * "directed" 0.69). Keep each pair's best type, plus near-ties, above a
 * floor. Tuned on the bundled examples with GLiNER2.5 base.
 */
const RELATION_FLOOR = 0.78;
const RELATION_TIE = 0.03;

export function keptRelations(ex: LineExtraction): Relation[] {
  const pair = (r: Relation) =>
    [r.source.entity_index, r.target.entity_index].sort((a, b) => a! - b!).join("|");
  const best = new Map<string, number>();
  for (const r of ex.relations) best.set(pair(r), Math.max(best.get(pair(r)) ?? 0, r.score ?? 1));
  return ex.relations.filter((r) => {
    const score = r.score ?? 1;
    return score >= RELATION_FLOOR && score >= best.get(pair(r))! - RELATION_TIE;
  });
}

export type Mention = { line: number; start: number; end: number };

export type GraphNode = {
  id: string;
  name: string;
  label: string;
  mentions: Mention[];
  lines: number[];
};

export type GraphLink = {
  id: string;
  source: string;
  target: string;
  type: string;
  lines: number[];
  score: number;
};

export type Graph = { nodes: GraphNode[]; links: GraphLink[] };

/** Canonical key for a surface form: "The Orchard Labs's" -> "orchard labs". */
export function normalize(text: string): string {
  return text
    .toLowerCase()
    .replace(/[’']s\b/g, "")
    .replace(/^(the|a|an)\s+/, "")
    .replace(/[^\p{L}\p{N}\s&-]/gu, "")
    .replace(/\s+/g, " ")
    .trim();
}

type Acc = {
  key: string;
  forms: Map<string, number>;
  labels: Map<string, number>;
  mentions: Mention[];
};

function bump(m: Map<string, number>, k: string) {
  m.set(k, (m.get(k) ?? 0) + 1);
}

function top(m: Map<string, number>, prefer: (a: string, b: string) => number = () => 0): string {
  return [...m.entries()].sort((a, b) => b[1] - a[1] || prefer(a[0], b[0]))[0][0];
}

/**
 * Merge per-line extractions into one graph. Mentions with the same
 * normalized text become one node; a bare first or last name ("Priya")
 * folds into the single longer name of the same type that ends or starts
 * with it ("Priya Raman").
 */
export function buildGraph(lines: (LineExtraction | undefined)[]): Graph {
  const accs = new Map<string, Acc>();
  const keyOf: (string | null)[][] = [];

  lines.forEach((ex, line) => {
    keyOf[line] = [];
    if (!ex) return;
    const used = new Set(keptRelations(ex).flatMap((r) => [r.source.entity_index, r.target.entity_index]));
    ex.entities.forEach((e, i) => {
      // Skip spans nested inside a longer one ("Oregon" in "Oregon State
      // University") and relation-only spans whose relations were dropped.
      const nested = ex.entities.some(
        (o) => o !== e && o.start <= e.start && o.end >= e.end && o.end - o.start > e.end - e.start,
      );
      const key = nested || (e.implicit && !used.has(i)) ? "" : normalize(e.text);
      if (!key) {
        keyOf[line][i] = null;
        return;
      }
      keyOf[line][i] = key;
      let acc = accs.get(key);
      if (!acc) accs.set(key, (acc = { key, forms: new Map(), labels: new Map(), mentions: [] }));
      bump(acc.forms, e.text.trim());
      bump(acc.labels, e.label);
      acc.mentions.push({ line, start: e.start, end: e.end });
    });
  });

  // Alias pass: a capitalized single name folds into the unique longer
  // capitalized name of the same type that starts or ends with it.
  const alias = new Map<string, string>();
  const label = (a: Acc) => top(a.labels);
  const proper = (a: Acc) => [...a.forms.keys()].every((f) => /^\p{Lu}/u.test(f));
  for (const acc of accs.values()) {
    if (acc.key.includes(" ") || !proper(acc)) continue;
    const candidates = [...accs.values()].filter((other) => {
      if (other === acc || !other.key.includes(" ") || label(other) !== label(acc) || !proper(other)) return false;
      const tokens = other.key.split(" ");
      return tokens[0] === acc.key || tokens[tokens.length - 1] === acc.key;
    });
    if (candidates.length === 1) alias.set(acc.key, candidates[0].key);
  }
  for (const [from, to] of alias) {
    const src = accs.get(from)!;
    const dst = accs.get(to)!;
    src.forms.forEach((n, f) => dst.forms.set(f, (dst.forms.get(f) ?? 0) + n));
    src.labels.forEach((n, l) => dst.labels.set(l, (dst.labels.get(l) ?? 0) + n));
    dst.mentions.push(...src.mentions);
    accs.delete(from);
  }
  const resolve = (key: string) => alias.get(key) ?? key;

  const nodes: GraphNode[] = [...accs.values()].map((acc) => ({
    id: acc.key,
    // Prefer the most frequent surface form; break ties with the longest.
    name: top(acc.forms, (a, b) => b.length - a.length),
    label: label(acc),
    mentions: acc.mentions,
    lines: [...new Set(acc.mentions.map((m) => m.line))],
  }));

  const links = new Map<string, GraphLink>();
  lines.forEach((ex, line) => {
    if (!ex) return;
    keptRelations(ex).forEach((r) => {
      const s = keyOf[line][r.source.entity_index!];
      const t = keyOf[line][r.target.entity_index!];
      if (!s || !t) return;
      const source = resolve(s);
      const target = resolve(t);
      if (source === target) return;
      const id = `${source}|${r.type}|${target}`;
      const score = r.score ?? 1;
      const link = links.get(id) ?? { id, source, target, type: r.type, lines: [], score };
      if (!link.lines.includes(line)) link.lines.push(line);
      link.score = Math.max(link.score, score);
      links.set(id, link);
    });
  });

  // Untyped relations can come back in both directions ("apples likes Johnny");
  // keep the more confident direction of each mirrored pair.
  for (const link of [...links.values()]) {
    const mirror = links.get(`${link.target}|${link.type}|${link.source}`);
    if (mirror && links.has(link.id) && mirror.score > link.score) links.delete(link.id);
    else if (mirror && links.has(link.id)) links.delete(mirror.id);
  }

  return { nodes, links: [...links.values()] };
}
