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

import { forwardRef, useImperativeHandle, useRef, type CSSProperties, type ReactNode } from "react";
import type { Graph } from "./graph";
import type { LineState } from "./useExtraction";
import type { Focus } from "./GraphView";

type Span = { start: number; end: number; node: string; label: string };

type Props = {
  text: string;
  onChange: (text: string) => void;
  lines: string[];
  states: LineState[];
  graph: Graph;
  color: (label: string) => string;
  focus: Focus;
  onHoverLine: (line: number | null) => void;
};

export type EditorHandle = { lineY: (line: number) => number | null };

const STATE_LABEL: Record<LineState, string> = {
  empty: "",
  queued: "waiting",
  running: "reading",
  done: "read",
  error: "failed",
};

/**
 * A plain textarea over a mirrored backdrop. The backdrop renders the same
 * text with entity highlights and line numbers; the textarea stays fully
 * native for editing, selection, and paste.
 */
export const Editor = forwardRef<EditorHandle, Props>(function Editor(
  { text, onChange, lines, states, graph, color, focus, onHoverLine },
  ref,
) {
  const lineEls = useRef<(HTMLDivElement | null)[]>([]);
  const scroller = useRef<HTMLDivElement>(null);

  useImperativeHandle(ref, () => ({
    lineY(line) {
      const el = lineEls.current[line];
      const box = scroller.current?.getBoundingClientRect();
      if (!el || !box) return null;
      const r = el.getBoundingClientRect();
      return Math.min(box.bottom, Math.max(box.top, r.top + r.height / 2));
    },
  }));

  // Spans per line, taken from the merged graph so colors match the nodes.
  const spans: Span[][] = lines.map(() => []);
  for (const n of graph.nodes) {
    for (const m of n.mentions) {
      if (m.end > m.start) spans[m.line]?.push({ start: m.start, end: m.end, node: n.id, label: n.label });
    }
  }

  const lineAt = (clientY: number) => {
    for (let i = 0; i < lineEls.current.length; i++) {
      const r = lineEls.current[i]?.getBoundingClientRect();
      if (r && clientY >= r.top && clientY < r.bottom) return lines[i]?.trim() ? i : null;
    }
    return null;
  };

  const focusLines =
    focus?.kind === "node" ? new Set(graph.nodes.find((n) => n.id === focus.id)?.lines ?? []) : null;

  return (
    <div className="editor-scroll" ref={scroller}>
      <div className="editor">
        <div className="backdrop" aria-hidden="true">
          {lines.map((line, i) => (
            <div
              key={i}
              ref={(el) => (lineEls.current[i] = el)}
              className={[
                "line",
                `is-${states[i]}`,
                focus?.kind === "line" && focus.line === i ? "is-hover" : "",
                focusLines?.has(i) ? "is-related" : "",
                focusLines && !focusLines.has(i) ? "is-dim" : "",
              ].join(" ")}
            >
              <span className="gutter" title={STATE_LABEL[states[i]]}>
                {line.trim() ? i + 1 : ""}
              </span>
              {highlight(line, spans[i], color, focus)}
            </div>
          ))}
        </div>
        <textarea
          value={text}
          onChange={(e) => onChange(e.target.value)}
          onMouseMove={(e) => onHoverLine(lineAt(e.clientY))}
          onMouseLeave={() => onHoverLine(null)}
          spellCheck={false}
          aria-label="Text, one document per line"
          placeholder="Type or paste sentences. Each line becomes a document."
        />
      </div>
    </div>
  );
});

function highlight(line: string, spans: Span[] = [], color: (l: string) => string, focus: Focus): ReactNode {
  if (!line) return "​";
  const sorted = [...spans].sort((a, b) => a.start - b.start || b.end - a.end);
  const out: ReactNode[] = [];
  let at = 0;
  for (const s of sorted) {
    if (s.start < at) continue; // overlapping span; keep the earlier, longer one
    if (s.start > at) out.push(line.slice(at, s.start));
    const on = focus?.kind === "node" && focus.id === s.node;
    out.push(
      <mark key={s.start} className={on ? "on" : undefined} style={{ "--c": color(s.label) } as CSSProperties}>
        {line.slice(s.start, s.end)}
      </mark>,
    );
    at = s.end;
  }
  if (at < line.length) out.push(line.slice(at));
  return out;
}
