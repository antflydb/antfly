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

import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { MODEL, checkModel, type LineExtraction, type ModelStatus, type Schema } from "./api";
import { Editor, type EditorHandle } from "./Editor";
import { EXAMPLES } from "./examples";
import { buildGraph } from "./graph";
import { GraphView, type Focus, type GraphViewHandle } from "./GraphView";
import { useExtraction } from "./useExtraction";

const TYPE_SLOTS = 8;

type Saved = { text: string; schema: Schema };

function readHash(): Saved | null {
  try {
    const raw = location.hash.slice(1);
    if (!raw) return null;
    const saved = JSON.parse(decodeURIComponent(escape(atob(raw))));
    if (typeof saved.text === "string" && Array.isArray(saved.schema?.entities)) return saved;
  } catch {
    /* fall through to the default example */
  }
  return null;
}

function writeHash(saved: Saved) {
  const encoded = btoa(unescape(encodeURIComponent(JSON.stringify(saved))));
  history.replaceState(null, "", `#${encoded}`);
}

// Stable ids for extraction objects, so the graph only rebuilds when a line's result changes.
const ids = new WeakMap<object, number>();
let nextId = 1;
const idOf = (o: LineExtraction | undefined) => {
  if (!o) return 0;
  if (!ids.has(o)) ids.set(o, nextId++);
  return ids.get(o)!;
};

export function App() {
  const initial = useMemo(() => readHash() ?? { text: EXAMPLES[0].text, schema: EXAMPLES[0].schema }, []);
  const [text, setText] = useState(initial.text);
  const [schema, setSchema] = useState<Schema>(initial.schema);
  const [focus, setFocus] = useState<Focus>(null);
  const [modelStatus, setModelStatus] = useState<ModelStatus | "checking">("checking");
  const [copied, setCopied] = useState(false);
  const editorRef = useRef<EditorHandle>(null);
  const graphRef = useRef<GraphViewHandle>(null);

  const lines = useMemo(() => text.split("\n"), [text]);
  const { results, states, error, lastMs, retry } = useExtraction(lines, schema);
  const signature = results.map(idOf).join(",");
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const graph = useMemo(() => buildGraph(results), [signature]);

  useEffect(() => {
    checkModel().then(setModelStatus);
  }, []);
  useEffect(() => {
    if (error?.kind === "unreachable") setModelStatus("unreachable");
  }, [error]);
  useEffect(() => {
    const t = setTimeout(() => writeHash({ text, schema }), 400);
    return () => clearTimeout(t);
  }, [text, schema]);

  const color = useCallback(
    (label: string) => {
      const i = schema.entities.indexOf(label);
      return i >= 0 ? `var(--t${i % TYPE_SLOTS})` : "var(--t-other)";
    },
    [schema.entities],
  );

  const onHoverNode = useCallback((id: string | null) => setFocus(id ? { kind: "node", id } : null), []);
  const onHoverLine = useCallback(
    (line: number | null) =>
      setFocus((f) => {
        if (line === null) return f?.kind === "line" ? null : f;
        return f?.kind === "line" && f.line === line ? f : { kind: "line", line };
      }),
    [],
  );
  const lineY = useCallback((line: number) => editorRef.current?.lineY(line) ?? null, []);

  const docs = lines.filter((l) => l.trim()).length;
  const busy = states.some((s) => s === "running" || s === "queued");

  const exportJson = () => {
    const blob = new Blob(
      [JSON.stringify({ model: MODEL, schema, documents: lines.filter((l) => l.trim()), ...graph }, null, 2)],
      { type: "application/json" },
    );
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "knowledge-graph.json";
    a.click();
    URL.revokeObjectURL(a.href);
  };

  const copyLink = async () => {
    writeHash({ text, schema });
    await navigator.clipboard.writeText(location.href);
    setCopied(true);
    setTimeout(() => setCopied(false), 1600);
  };

  return (
    <div className="app">
      <header className="top">
        <div className="brand">
          <span className="wordmark">antfly</span>
          <span className="slash">/</span>
          <h1>Text to graph</h1>
        </div>
        <div className={`status status-${modelStatus}`}>
          <span className="pulse" data-busy={busy || undefined} />
          <span className="model">{MODEL}</span>
          {lastMs !== null && modelStatus === "ready" && (
            <span className="latency">{Math.round(lastMs)} ms / line</span>
          )}
        </div>
      </header>

      <main className="split">
        <section className="left" aria-label="Input">
          <div className="schema">
            <ChipRow
              title="Entity types"
              items={schema.entities}
              swatch={(l) => color(l)}
              onChange={(entities) => setSchema((s) => ({ ...s, entities }))}
              placeholder="add type"
            />
            <ChipRow
              title="Relations"
              items={schema.relations}
              onChange={(relations) => setSchema((s) => ({ ...s, relations }))}
              placeholder="add relation"
            />
          </div>

          <Editor
            ref={editorRef}
            text={text}
            onChange={setText}
            lines={lines}
            states={states}
            graph={graph}
            color={color}
            focus={focus}
            onHoverLine={onHoverLine}
          />

          <footer className="left-foot">
            <span className="hint">One line per document. Use names, not pronouns: each line is read on its own.</span>
            <span className="examples">
              Examples:
              {EXAMPLES.map((ex) => (
                <button
                  key={ex.name}
                  className="text-btn"
                  onClick={() => {
                    setText(ex.text);
                    setSchema(ex.schema);
                  }}
                >
                  {ex.name}
                </button>
              ))}
            </span>
          </footer>
        </section>

        <section className="right" aria-label="Graph">
          <GraphView
            ref={graphRef}
            graph={graph}
            color={color}
            focus={focus}
            onHoverNode={onHoverNode}
            lineY={lineY}
          />

          {modelStatus === "unreachable" ? (
            <Notice title="Antfly isn't answering on port 8080">
              <p>Start it, then retry:</p>
              <pre>antfly standalone</pre>
              <button className="btn" onClick={() => { setModelStatus("checking"); checkModel().then(setModelStatus); retry(); }}>
                Retry
              </button>
            </Notice>
          ) : modelStatus === "missing" ? (
            <Notice title="GLiNER2.5 isn't installed">
              <p>Pull the model, restart Antfly, then retry:</p>
              <pre>antfly inference pull {MODEL}</pre>
              <button className="btn" onClick={() => { setModelStatus("checking"); checkModel().then(setModelStatus); retry(); }}>
                Retry
              </button>
            </Notice>
          ) : error ? (
            <Notice title="Some lines couldn't be read">
              <p>{error.message}</p>
              <button className="btn" onClick={retry}>Retry failed lines</button>
            </Notice>
          ) : graph.nodes.length === 0 && !busy ? (
            <div className="empty">
              {docs === 0
                ? "Write a sentence on the left. Its people, places, and things appear here."
                : "No entities found yet. Try adding an entity type that fits your text."}
            </div>
          ) : null}

          <footer className="graph-foot">
            <span className="counts">
              {graph.nodes.length} entities · {graph.links.length} relations · {docs} documents
            </span>
            <span className="actions">
              <button className="btn ghost" onClick={() => graphRef.current?.fit()}>Fit</button>
              <button className="btn ghost" onClick={copyLink}>{copied ? "Link copied" : "Copy link"}</button>
              <button className="btn ghost" onClick={exportJson} disabled={graph.nodes.length === 0}>
                Export JSON
              </button>
            </span>
          </footer>
        </section>
      </main>
    </div>
  );
}

function Notice({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="notice" role="status">
      <h2>{title}</h2>
      {children}
    </div>
  );
}

function ChipRow({
  title,
  items,
  onChange,
  placeholder,
  swatch,
}: {
  title: string;
  items: string[];
  onChange: (items: string[]) => void;
  placeholder: string;
  swatch?: (item: string) => string;
}) {
  const [draft, setDraft] = useState("");
  const add = () => {
    const v = draft.trim().toLowerCase().replace(/\s+/g, "_").replace(/::/g, "");
    if (v && !items.includes(v)) onChange([...items, v]);
    setDraft("");
  };
  const onKey = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter" || e.key === ",") {
      e.preventDefault();
      add();
    } else if (e.key === "Backspace" && !draft && items.length > 0) {
      onChange(items.slice(0, -1));
    }
  };
  return (
    <div className="chip-row">
      <span className="chip-title">{title}</span>
      <ul className="chips">
        {items.map((item) => (
          <li key={item} className="chip">
            {swatch && <span className="swatch" style={{ background: swatch(item) }} />}
            {item.replace(/_/g, " ")}
            <button
              className="chip-x"
              aria-label={`Remove ${item}`}
              onClick={() => onChange(items.filter((i) => i !== item))}
            >
              ×
            </button>
          </li>
        ))}
        <li>
          <input
            className="chip-input"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={onKey}
            onBlur={add}
            placeholder={placeholder}
            aria-label={`Add ${title.toLowerCase()}`}
            size={Math.max(placeholder.length, draft.length + 1)}
          />
        </li>
      </ul>
    </div>
  );
}
