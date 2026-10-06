import { Button } from "@antfly/design-system/primitives";
import React, { useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import "@antfly/design-system/styles.css";
import {
  type Backend,
  type BundleFiles,
  type CatalogModel,
  clearModelCache,
  detectCapabilities,
  downloadCatalogModel,
  InferenceClient,
  type ModelInfo,
  type Precision,
  type Request,
  type RunResult,
} from "@antfly/inference-web";
import advancedExamples from "./advanced-examples.json";
import catalogData from "./catalog.json";
import "./styles.css";
import "./results.css";
import { Results } from "./Results";

const catalog = catalogData as CatalogModel[];
const examples = {
  entities: "Ada Lovelace worked with Charles Babbage in London.",
  classification: "The setup was quick and the results were excellent.",
  structures: "Ada Lovelace, mathematician, London. Charles Babbage, inventor, London.",
  relations: "Ada Lovelace collaborated with Charles Babbage in London.",
};
type Task = keyof typeof examples;
const split = (value: string) =>
  value
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
function App() {
  const client = useRef<InferenceClient | null>(null);
  const abort = useRef<AbortController | null>(null);
  const [selected, setSelected] = useState("gliner25-small-q8");
  const [precision, setPrecision] = useState<Precision>("q8_0");
  const [backend, setBackend] = useState<Backend>("auto");
  const [files, setFiles] = useState<BundleFiles | null>(null);
  const [localName, setLocalName] = useState("");
  const [model, setModel] = useState<ModelInfo | null>(null);
  const [rememberedArchitecture, setRememberedArchitecture] = useState<
    ModelInfo["architecture"] | null
  >(null);
  const [task, setTask] = useState<Task>("entities");
  const [text, setText] = useState(examples.entities);
  const [labels, setLabels] = useState("person, organization, location");
  const [relationLabels, setRelationLabels] = useState("collaborated_with, located_in");
  const [threshold, setThreshold] = useState(0.5);
  const [decisionMode, setDecisionMode] = useState("single");
  const [instruction, setInstruction] = useState("Which tool is needed to handle this request?");
  const [decisionLabels, setDecisionLabels] = useState("search, fetch, none");
  const [windowed, setWindowed] = useState(false);
  const [advanced, setAdvanced] = useState(false);
  const [json, setJson] = useState("");
  const [result, setResult] = useState<RunResult | null>(null);
  const [resultText, setResultText] = useState<string | null>(null);
  const [reloadable, setReloadable] = useState(false);
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState("Choose a model or a local bundle to begin.");
  const [error, setError] = useState("");
  const [capability, setCapability] = useState("Checking browser capabilities…");
  const entry = catalog.find((item) => item.id === selected)!;
  const architecture = model?.architecture ?? rememberedArchitecture ?? entry.architecture;
  const decisionModel = architecture === "laya" || architecture === "decide";
  useEffect(() => {
    client.current = new InferenceClient();
    void detectCapabilities().then((c) =>
      setCapability(c.webgpu ? "WASM SIMD + WebGPU available" : `WASM CPU · ${c.reason}`)
    );
    return () => {
      abort.current?.abort();
      client.current?.dispose();
    };
  }, []);
  function request(): Request {
    if (advanced) return JSON.parse(json) as Request;
    const names = split(labels);
    if (decisionModel)
      return {
        schema_version: 2,
        model: files ? `local:${localName}` : selected,
        inputs: [{ content: text }],
        schema: {
          classifications: [
            {
              name: "decision",
              mode: architecture === "decide" && decisionMode === "boolean" ? "single" : decisionMode,
              instruction,
              labels: decisionMode === "boolean" ? ["false", "true"] : split(decisionLabels),
            },
          ],
        },
        options: { include_confidence: true, long_document: { mode: "reject" } },
      };
    if (architecture === "span")
      return {
        schema_version: 1,
        model: files ? `local:${localName}` : selected,
        task,
        text,
        labels: names,
        relation_labels: split(relationLabels),
        threshold,
        ...(task === "structures"
          ? { schema: { person: ["name::str", "occupation::str", "location::str"] } }
          : {}),
      };
    const schema =
      task === "entities"
        ? { entities: names }
        : task === "classification"
          ? { classifications: [{ name: "category", labels: names }] }
          : task === "relations"
            ? { entities: names, relations: split(relationLabels).map((type) => ({ type })) }
            : {
                structures: {
                  person: {
                    fields: {
                      name: { type: "str", cardinality: "required_one" },
                      occupation: { type: "str" },
                      location: { type: "str" },
                    },
                  },
                },
              };
    return {
      schema_version: 2,
      model: files ? `local:${localName}` : selected,
      inputs: [{ content: text }],
      schema,
      options: {
        threshold,
        include_confidence: true,
        include_spans: true,
        offset_unit: "utf16_codeunits",
        long_document: windowed
          ? { mode: "window", window_words: 256, overlap_words: 32, max_windows: 32 }
          : { mode: "reject" },
      },
    };
  }
  async function operation(fn: (signal: AbortSignal) => Promise<void>) {
    setBusy(true);
    setError("");
    abort.current = new AbortController();
    try {
      await fn(abort.current.signal);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setModel(client.current?.model ?? null);
    } finally {
      setBusy(false);
      abort.current = null;
    }
  }
  const progress = (p: { stage: string; file?: string; loaded: number; total: number }) =>
    setStatus(
      `${p.stage}${p.file ? ` · ${p.file}` : ""}${p.total ? ` · ${Math.round((p.loaded / p.total) * 100)}%` : ""}`
    );
  const load = () =>
    operation(async (signal) => {
      setResult(null);
      setModel(null);
      const bundle = files ?? (await downloadCatalogModel(entry, { signal, onProgress: progress }));
      const loaded = await client.current!.loadModel(bundle, {
        precision: files ? precision : entry.precision,
        backend,
        signal,
        onProgress: progress,
      });
      setRememberedArchitecture(loaded.architecture);
      setReloadable(true);
      setModel(loaded);
      setStatus(
        `Ready · ${loaded.backend.toUpperCase()} · ${loaded.precision} · ${(loaded.bytes / 1024 ** 2).toFixed(0)} MiB weights`
      );
    });
  const run = () =>
    operation(async (signal) => {
      setResult(null);
      const input = request();
      const content = Array.isArray(input.inputs)
        ? (input.inputs[0] as { content?: unknown })?.content
        : input.text;
      setResultText(typeof content === "string" ? content : null);
      const response = await client.current!.run(input, { signal, onProgress: progress });
      setModel(client.current!.model);
      setResult(response);
      setStatus("Complete. Input text stayed in this browser.");
    });
  function exportResult() {
    if (!result) return;
    const url = URL.createObjectURL(
      new Blob([JSON.stringify(result, null, 2)], { type: "application/json" })
    );
    const link = document.createElement("a");
    link.href = url;
    link.download = "extraction.json";
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 0);
  }
  return (
    <main>
      <header>
        <a className="brand" href="https://antfly.io" target="_blank" rel="noreferrer">
          antfly<span> / playground</span>
        </a>
        <span className="privacy">● On-device inference</span>
      </header>
      <section className="intro">
        <p className="eyebrow">SMALL MODELS. STRUCTURED KNOWLEDGE.</p>
        <h1>
          Find the meaning
          <br />
          <em>in your text.</em>
        </h1>
        <p>
          Explore GLiNER2, GLiNER2.5 and Laya with Antfly’s Zig inference engine. Your text never
          leaves this browser.
        </p>
      </section>
      <section className="model-panel" aria-labelledby="model-heading">
        <div className="section-heading">
          <h2 id="model-heading">01 / Load a model</h2>
          <span>{capability}</span>
        </div>
        <div className="controls">
          <label>
            Model
            <select
              aria-label="Model"
              value={selected}
              disabled={busy || Boolean(model)}
              onChange={(e) => {
                client.current!.unloadModel();
                setReloadable(false);
                setRememberedArchitecture(null);
                setSelected(e.target.value);
                setFiles(null);
                setLocalName("");
                setPrecision(catalog.find((c) => c.id === e.target.value)!.precision);
                setResult(null);
              }}
            >
              {catalog.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.name} · {item.precision}
                  {item.files.length ? "" : " · pending publication"}
                </option>
              ))}
            </select>
          </label>
          <label>
            Backend
            <select
              aria-label="Backend"
              value={backend}
              onChange={(e) => setBackend(e.target.value as Backend)}
              disabled={busy || Boolean(model)}
            >
              <option value="auto">Auto</option>
              <option value="wasm">WASM CPU</option>
              <option value="webgpu">WebGPU (CPU fallback)</option>
            </select>
          </label>
          <Button
            disabled={busy || Boolean(model) || (!files && !entry.files.length)}
            onClick={load}
          >
            ↓ {files ? "Load local bundle" : "Download & load"}
          </Button>
          <Button
            variant="outline"
            disabled={busy || !model}
            onClick={() => {
              client.current!.unloadModel();
              setReloadable(false);
              setRememberedArchitecture(null);
              setModel(null);
              setResult(null);
              setStatus("Model unloaded.");
            }}
          >
            Unload
          </Button>
        </div>
        <div className="local-controls">
          <label className="file-label">
            Or choose a local model folder
            <input
              type="file"
              multiple
              {...{ webkitdirectory: "" }}
              disabled={busy || Boolean(model)}
              onChange={(e) => {
                if (e.target.files?.length) {
                  client.current!.unloadModel();
                  setReloadable(false);
                  setRememberedArchitecture(null);
                  setResult(null);
                  setFiles(Array.from(e.target.files));
                  setLocalName(e.target.files[0].webkitRelativePath.split("/")[0]);
                }
              }}
            />
          </label>
          {localName && (
            <>
              <span>{localName}</span>
              <label>
                Local precision
                <select
                  value={precision}
                  onChange={(e) => setPrecision(e.target.value as Precision)}
                  disabled={busy || Boolean(model)}
                >
                  {["q8_0", "q4_k", "q4_0", "fp32", "fp16", "bf16", "fp16_encoder"].map((p) => (
                    <option key={p}>{p}</option>
                  ))}
                </select>
              </label>
            </>
          )}
          <button
            type="button"
            className="text-button"
            disabled={busy}
            onClick={() =>
              operation(async () => {
                await clearModelCache();
                setStatus("Downloaded model cache cleared.");
              })
            }
          >
            Clear downloaded models
          </button>
        </div>
        <p className="note">
          {files
            ? "Local files are never uploaded or added to the persistent cache."
            : entry.files.length
              ? `Explicit download · ${(entry.files.reduce((n, f) => n + f.size_bytes, 0) / 1024 ** 2).toFixed(0)} MiB · ${entry.license}`
              : "Q8 catalog artifacts are not published yet. Use a local converted bundle or select an FP32 reference model."}{" "}
          Browser qualification: {model?.qualified ? "passed" : "experimental / pending"}.
        </p>
        {model?.fallbackReason && <p className="notice">{model.fallbackReason}</p>}
        {model?.backend === "webgpu" && model.architecture === "boundary" && (
          <p className="note">WebGPU encoder · WASM task heads, constraints and decoding.</p>
        )}
        {model?.backend === "webgpu" && model.architecture === "laya" && (
          <p className="note">
            GPU-resident projection weights · WebGPU encoder and decision heads. Tokenization, token
            embedding lookup and final decision calibration use WASM CPU.
          </p>
        )}
      </section>
      <div className="workspace">
        <section className="editor" aria-labelledby="input-heading">
          <div className="section-heading">
            <h2 id="input-heading">02 / Define the task</h2>
            <label className="inline">
              <input
                type="checkbox"
                checked={advanced}
                disabled={busy}
                onChange={(e) => {
                  if (e.target.checked && !json) setJson(JSON.stringify(request(), null, 2));
                  setAdvanced(e.target.checked);
                }}
              />{" "}
              Advanced JSON
            </label>
          </div>
          {advanced ? (
            <>
              <p className="note">
                Canonical request; preserved verbatim. The builder is separate and does not modify
                this JSON.
                {architecture === "decide"
                  ? " Decide accepts named single, multi-label and ordinal classifications with instructions, label descriptions and examples; entity extraction and long-document windows are unsupported."
                  : architecture === "laya"
                    ? " Laya accepts named single, ordinal and boolean classifications with instructions; entity extraction and long-document windows are unsupported."
                    : " GLiNER2.5 supports mixed schemas, attributes, constraints and JointIE."}
              </p>
              {architecture === "boundary" && (
                <label>
                  Load advanced example (replaces JSON)
                  <select
                    defaultValue=""
                    disabled={busy}
                    onChange={(e) => {
                      const example =
                        advancedExamples[e.target.value as keyof typeof advancedExamples];
                      if (example) setJson(JSON.stringify(example, null, 2));
                      e.target.value = "";
                    }}
                  >
                    <option value="">Choose an example…</option>
                    {Object.keys(advancedExamples).map((name) => (
                      <option key={name} value={name}>
                        {name.replaceAll("_", " ")}
                      </option>
                    ))}
                  </select>
                </label>
              )}
              <label>
                Request JSON
                <textarea
                  className="code"
                  value={json}
                  onChange={(e) => setJson(e.target.value)}
                  spellCheck={false}
                  disabled={busy}
                  rows={22}
                />
              </label>
            </>
          ) : (
            <>
              {decisionModel ? (
                <>
                  <label>
                    Decision type
                    <select
                      aria-label="Decision type"
                      value={decisionMode}
                      disabled={busy}
                      onChange={(e) => {
                        const mode = e.target.value;
                        setDecisionMode(mode);
                        setResult(null);
                        setDecisionLabels(
                          mode === "ordinal" ? "low, medium, high" : "search, fetch, none"
                        );
                        setInstruction(
                          mode === "ordinal"
                            ? "How urgent is this request?"
                            : mode === "boolean"
                              ? "Does this request require searching for information?"
                              : "Which tool is needed to handle this request?"
                        );
                      }}
                    >
                      <option value="single">Single choice</option>
                      <option value="ordinal">Ordinal</option>
                      <option value="boolean">Boolean</option>
                    </select>
                  </label>
                  <label>
                    Decision instruction
                    <input
                      value={instruction}
                      onChange={(e) => setInstruction(e.target.value)}
                      disabled={busy}
                    />
                  </label>
                  {decisionMode !== "boolean" && (
                    <label>
                      Decision labels (ordered for ordinal)
                      <input
                        value={decisionLabels}
                        onChange={(e) => setDecisionLabels(e.target.value)}
                        disabled={busy}
                      />
                    </label>
                  )}
                  <p className="note">
                    {architecture === "decide"
                      ? "Decide scores classification labels with its [L] marker head. Use Advanced JSON for multiple tasks, label descriptions and examples. Load a local Q8 bundle with its matching encoder and head."
                      : "Laya returns typed decision distributions. Packed checkpoints share a state trunk across questions and support candidate branches and two-stage choices. Up to 16 questions per input in Advanced JSON."}
                  </p>
                </>
              ) : (
                <div className="task-tabs" role="group" aria-label="Extraction task">
                  {(["entities", "classification", "structures", "relations"] as Task[]).map(
                    (t) => (
                      <button
                        type="button"
                        key={t}
                        aria-pressed={task === t}
                        disabled={busy}
                        onClick={() => {
                          setTask(t);
                          setResult(null);
                          if (t === "classification") setLabels("positive, neutral, negative");
                          else setLabels("person, organization, location");
                        }}
                      >
                        {t}
                      </button>
                    )
                  )}
                </div>
              )}
              <label>
                Input text
                <textarea
                  value={text}
                  onChange={(e) => {
                    setText(e.target.value);
                    setResult(null);
                  }}
                  rows={9}
                  disabled={busy}
                  maxLength={256 * 1024}
                />
              </label>
              <div className="input-tools">
                <button
                  type="button"
                  className="text-button"
                  disabled={busy}
                  onClick={() => {
                    setText(
                      decisionModel
                        ? "Please search for the latest documentation about browser inference."
                        : examples[task]
                    );
                    setResult(null);
                  }}
                >
                  Use example
                </button>
                <label className="file-label">
                  Open .txt
                  <input
                    type="file"
                    accept=".txt,text/plain"
                    disabled={busy}
                    onChange={(e) => {
                      const file = e.target.files?.[0];
                      if (file)
                        void operation(async () => {
                          if (file.size > 256 * 1024) throw new Error("Text file exceeds 256 KiB");
                          setText(await file.text());
                          setResult(null);
                        });
                    }}
                  />
                </label>
                <span>{new TextEncoder().encode(text).length.toLocaleString()} bytes</span>
              </div>
              {!decisionModel &&
                (task !== "structures" ? (
                  <label>
                    {task === "classification" ? "Classification labels" : "Entity labels"}
                    <input
                      value={labels}
                      onChange={(e) => setLabels(e.target.value)}
                      disabled={busy}
                    />
                  </label>
                ) : (
                  <p className="note">
                    Extracts person records with name, occupation and location. Use Advanced JSON to
                    customize fields and cardinalities.
                  </p>
                ))}
              {!decisionModel && task === "relations" && (
                <label>
                  Relation types
                  <input
                    value={relationLabels}
                    onChange={(e) => setRelationLabels(e.target.value)}
                    disabled={busy}
                  />
                </label>
              )}
              <div className="controls">
                {!decisionModel && (
                  <label>
                    Threshold: {threshold.toFixed(2)}
                    <input
                      type="range"
                      min="0"
                      max="1"
                      step="0.05"
                      value={threshold}
                      onChange={(e) => setThreshold(Number(e.target.value))}
                      disabled={busy}
                    />
                  </label>
                )}
                {architecture === "boundary" && (
                  <label className="inline">
                    <input
                      type="checkbox"
                      checked={windowed}
                      onChange={(e) => setWindowed(e.target.checked)}
                      disabled={busy}
                    />{" "}
                    Long text: 256-word windows, 32-word overlap
                  </label>
                )}
              </div>
            </>
          )}
          <div className="run-actions">
            <Button disabled={busy || (!model && !reloadable)} onClick={run}>
              Run extraction ↗
            </Button>
            {busy && (
              <Button
                variant="outline"
                onClick={() => {
                  abort.current?.abort();
                  client.current?.cancel();
                  setModel(null);
                  setStatus("Cancelled. The model will reload on the next load/run.");
                }}
              >
                Cancel
              </Button>
            )}
          </div>
        </section>
        <section className="results" aria-labelledby="result-heading">
          <div className="section-heading">
            <h2 id="result-heading">03 / Inspect results</h2>
            <button type="button" className="text-button" disabled={!result} onClick={exportResult}>
              Export JSON ↓
            </button>
          </div>
          {result ? (
            <>
              <div className="metrics">
                <span>{(result.elapsedMs / 1000).toFixed(2)} s inference</span>
                <span>{result.backend.toUpperCase()}</span>
                <span>{(result.wasmBytes / 1024 ** 2).toFixed(0)} MiB WASM</span>
              </div>
              <Results value={result.value} text={resultText} />
              <details open>
                <summary>Raw response</summary>
                <pre>{JSON.stringify(result.value, null, 2)}</pre>
              </details>
            </>
          ) : (
            <div className="empty">
              <span>⟐</span>
              <h3>From text to structure.</h3>
              <p>
                Load a model, define what to look for,
                <br />
                and run your first extraction.
              </p>
            </div>
          )}
        </section>
      </div>
      <div className="status" role="status" aria-live="polite">
        {busy && <span className="spinner" />}
        {status}
      </div>
      {error && (
        <p role="alert" className="error">
          {error}
        </p>
      )}
      <footer>
        <span>Antfly / Zig + WebAssembly</span>
        <span>Experimental · One model at a time · No text telemetry</span>
      </footer>
    </main>
  );
}
createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
