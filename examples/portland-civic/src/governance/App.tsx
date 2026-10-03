import { useEffect, useRef, useState } from "react";
import {
  ArrowDown,
  ArrowRight,
  ArrowUpRight,
  BookOpen,
  Check,
  ChevronRight,
  FileText,
  GitBranch,
  Layers,
  Search,
  ShieldCheck,
  X,
} from "lucide-react";
import type {
  EvidenceBrief,
  EvidenceNode,
  EvidencePage,
  EvidenceSearch,
  GovernanceManifest,
  Passage,
  SourceDocument,
} from "./types.ts";
import "./style.css";

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`/api/governance/${path}`, init);
  const data = await response.json();
  if (!response.ok)
    throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}
type Tab =
  | "journey"
  | "search"
  | "versions"
  | "connections"
  | "gaps"
  | "sources";
const tabs: [Tab, string][] = [
  ["journey", "Decision timeline"],
  ["search", "Search the record"],
  ["versions", "Compare versions"],
  ["connections", "Connections"],
  ["gaps", "Open questions"],
  ["sources", "Source ledger"],
];
const date = (s: string) =>
  new Date(`${s}T12:00:00`).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
  });

function Citation({
  p,
  documents,
  open,
  compact = false,
}: {
  p: Passage;
  documents: SourceDocument[];
  open: (id: string) => void;
  compact?: boolean;
}) {
  const doc = documents.find((d) => d.id === p.document_id)!;
  return (
    <button
      className={`gov-citation ${compact ? "compact" : ""}`}
      onClick={() => open(p.id)}
    >
      <span className="gov-citation-head">
        <FileText size={15} />
        <strong>{doc.title}</strong>
        <span>
          {doc.extraction === "pdf-text" ? "PDF p." : "Event"} {p.page}
        </span>
        <ArrowUpRight size={15} />
      </span>
      {!compact && <span className="gov-excerpt">{p.text}</span>}
      <span className="gov-source-note">
        {doc.version} · Machine extracted · Awaiting review
      </span>
    </button>
  );
}
function SourceViewer({ id, close }: { id: string; close: () => void }) {
  const [data, setData] = useState<{
    passage: Passage;
    page: EvidencePage;
    document: SourceDocument;
  }>();
  const [error, setError] = useState("");
  const [copied, setCopied] = useState(false);
  const dialog = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const controller = new AbortController();
    setData(undefined);
    setError("");
    setCopied(false);
    api<typeof data>(`passage/${encodeURIComponent(id)}`, {
      signal: controller.signal,
    })
      .then(setData)
      .catch((e) => {
        if (e.name !== "AbortError") setError(e.message);
      });
    return () => controller.abort();
  }, [id]);
  useEffect(() => {
    dialog.current?.showModal();
  }, []);
  const doc = data?.document,
    p = data?.passage;
  async function copy() {
    const url = new URL("/governance", window.location.origin);
    url.searchParams.set("passage", id);
    try {
      await navigator.clipboard.writeText(
        `${doc!.title}, ${doc!.extraction === "pdf-text" ? "PDF page" : "event"} ${p!.page}. SHA-256 ${doc!.sha256}. ${url}`,
      );
      setCopied(true);
    } catch {
      setError(
        "Clipboard unavailable. The address bar contains this passage’s shareable link.",
      );
    }
  }
  return (
    <dialog
      className="gov-dialog"
      ref={dialog}
      onCancel={(e) => {
        e.preventDefault();
        close();
      }}
      aria-labelledby="source-title"
    >
      <div className="gov-dialog-top">
        <span className="gov-eyebrow">SOURCE INSPECTOR</span>
        <button aria-label="Close source inspector" onClick={close}>
          <X size={22} />
        </button>
      </div>
      {error && (
        <p role="alert" className="gov-error">
          {error}
        </p>
      )}
      {!data && !error && <p role="status">Loading preserved passage…</p>}
      {doc && p && data && (
        <>
          <h2 id="source-title">{doc.title}</h2>
          <p className="gov-muted">
            {doc.extraction === "pdf-text" ? "PDF page" : "Structured event"}{" "}
            {p.page} · {doc.published} · {doc.publisher}
          </p>
          <div className="gov-inspector-actions">
            {doc.extraction === "pdf-text" && (
              <a
                className="gov-primary"
                target="_blank"
                rel="noreferrer"
                href={`/api/governance/sources/${doc.sha256}.pdf#page=${p.page}`}
              >
                Preserved PDF <ArrowUpRight size={15} />
              </a>
            )}
            <a
              className="gov-secondary"
              target="_blank"
              rel="noreferrer"
              href={doc.source_url}
            >
              Original source <ArrowUpRight size={15} />
            </a>
            <button className="gov-secondary" onClick={copy}>
              {copied ? <Check size={15} /> : <BookOpen size={15} />}
              {copied ? "Citation copied" : "Copy citation"}
            </button>
          </div>
          <div className="gov-notice">
            {doc.extraction === "pdf-text"
              ? "Machine-extracted text, not proofread. PDF page numbers are file positions; printed page labels may differ."
              : "Procedural data imported from the Oregon Governance Atlas. Not independently checked against OLIS in this pilot."}
          </div>
          <pre className="gov-page-text">
            {data.page.text.slice(0, p.start)}
            <mark>{p.text}</mark>
            {data.page.text.slice(p.end)}
          </pre>
          {data.page.caption && (
            <p className="gov-notice">
              Machine caption (not source text): {data.page.caption}
            </p>
          )}
          <Autograph key={p.id} passage={p.id} />
          <dl className="gov-provenance">
            <dt>Text extraction</dt>
            <dd>
              {data.page.extraction || doc.extraction}
              {data.page.reader_model ? ` · ${data.page.reader_model}` : ""}
            </dd>
            <dt>Passage locator</dt>
            <dd>
              {p.id} · characters {p.start}–{p.end}
            </dd>
            <dt>Source SHA-256</dt>
            <dd>{doc.sha256}</dd>
            <dt>Acquired</dt>
            <dd>{doc.fetched_at}</dd>
            <dt>Review state</dt>
            <dd>Acquired → extracted → awaiting human review</dd>
            {doc.atlas_sha256 && (
              <>
                <dt>Atlas checksum</dt>
                <dd>
                  {doc.atlas_hash_matches
                    ? "Matches the Atlas’s preserved original"
                    : "Differs from the Atlas’s preserved original"}
                </dd>
              </>
            )}
          </dl>
        </>
      )}
    </dialog>
  );
}
function Compare({
  m,
  open,
}: {
  m: GovernanceManifest;
  open: (id: string) => void;
}) {
  const [left, setLeft] = useState("introduced"),
    [right, setRight] = useState("engrossed");
  const [lp, setLp] = useState(1),
    [rp, setRp] = useState(1);
  const [result, setResult] = useState<{
    left: EvidencePage;
    right: EvidencePage;
    changes: { value: string; added?: boolean; removed?: boolean }[];
    citations: string[];
  }>();
  const [error, setError] = useState("");
  const versions = m.documents.filter((d) =>
    ["introduced", "engrossed", "enrolled", "chapter"].includes(d.id),
  );
  useEffect(() => {
    const controller = new AbortController();
    setError("");
    setResult(undefined);
    api<typeof result>(
      `compare?left=${left}&right=${right}&left_page=${lp}&right_page=${rp}`,
      { signal: controller.signal },
    )
      .then(setResult)
      .catch((e) => {
        if (e.name !== "AbortError") setError(e.message);
      });
    return () => controller.abort();
  }, [left, right, lp, rp]);
  return (
    <>
      <div className="gov-section-heading">
        <div>
          <span className="gov-eyebrow">TEXT THROUGH TIME</span>
          <h2>From study bill to transportation package.</h2>
          <p>
            Compare preserved pages. A text difference shows what changed on the
            page; it does not explain why.
          </p>
        </div>
        <Layers size={28} />
      </div>
      <div className="gov-version-chain">
        {versions.map((d, i) => (
          <div key={d.id}>
            <span>{i + 1}</span>
            <strong>{d.version}</strong>
            <small>
              {d.published} · {d.page_count} pages
            </small>
            {i < versions.length - 1 && <ArrowRight size={17} />}
          </div>
        ))}
      </div>
      <div className="gov-compare-controls">
        {(["left", "right"] as const).map((side) => {
          const value = side === "left" ? left : right,
            n = side === "left" ? lp : rp;
          const set = side === "left" ? setLeft : setRight,
            setPage = side === "left" ? setLp : setRp;
          return (
            <div key={side}>
              <label>
                {side === "left" ? "Earlier version" : "Later version"}
                <select
                  aria-label={`${side} version`}
                  value={value}
                  onChange={(e) => {
                    set(e.target.value);
                    setPage(1);
                  }}
                >
                  {versions.map((d) => (
                    <option key={d.id} value={d.id}>
                      {d.version}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                PDF page
                <select
                  aria-label={`${side} page`}
                  value={n}
                  onChange={(e) => setPage(Number(e.target.value))}
                >
                  {Array.from(
                    {
                      length: versions.find((d) => d.id === value)!.page_count,
                    },
                    (_, i) => (
                      <option key={i}>{i + 1}</option>
                    ),
                  )}
                </select>
              </label>
            </div>
          );
        })}
      </div>
      <div className="gov-notice">
        Page comparison is literal and includes headers, line numbers and
        formatting. Page 1 is selected initially; this is not an aligned
        comparison of the entire acts. These are historical versions, not a
        statement of current law.
      </div>
      {error && (
        <p className="gov-error" role="alert">
          {error}
        </p>
      )}
      {result ? (
        <>
          <div className="gov-compare-pages">
            {[result.left, result.right].map((p, i) => (
              <article key={i}>
                <div>
                  <strong>
                    {m.documents.find((d) => d.id === p.document_id)!.version} ·
                    p. {p.page}
                  </strong>
                  <button onClick={() => open(result.citations[i])}>
                    Inspect source <ArrowUpRight size={14} />
                  </button>
                </div>
                <pre>{p.text}</pre>
              </article>
            ))}
          </div>
          <details className="gov-diff">
            <summary>
              Show line-by-line differences <span>− earlier / + later</span>
            </summary>
            <pre>
              {result.changes.map((c, i) => (
                <span
                  key={i}
                  className={c.added ? "added" : c.removed ? "removed" : ""}
                >
                  {c.value.split("\n").map((line, j) => (
                    <span key={j}>
                      {c.added ? "+ " : c.removed ? "− " : "  "}
                      {line}
                      {"\n"}
                    </span>
                  ))}
                </span>
              ))}
            </pre>
          </details>
        </>
      ) : (
        !error && <p role="status">Comparing preserved text…</p>
      )}
    </>
  );
}
function Connections({
  m,
  open,
}: {
  m: GovernanceManifest;
  open: (id: string) => void;
}) {
  const [root, setRoot] = useState("claim-effective");
  const [result, setResult] = useState<{
    root: EvidenceNode;
    snapshot_id: string;
    graph: {
      nodes?: {
        key: string;
        depth?: number;
        document?: EvidenceNode;
        path_edges?: { type?: string; edge_type?: string }[];
      }[];
      total: number;
    };
  }>();
  const [error, setError] = useState("");
  useEffect(() => {
    const controller = new AbortController();
    setError("");
    setResult(undefined);
    api<typeof result>(`graph?node=${encodeURIComponent(root)}`, {
      signal: controller.signal,
    })
      .then(setResult)
      .catch((e) => {
        if (e.name !== "AbortError") setError(e.message);
      });
    return () => controller.abort();
  }, [root]);
  return (
    <>
      <div className="gov-section-heading">
        <div>
          <span className="gov-eyebrow">FOLLOW THE EVIDENCE</span>
          <h2>Every connection has a reason.</h2>
          <p>
            Traverse explicit citation, involvement and version relationships in
            Antfly.
          </p>
        </div>
        <GitBranch size={28} />
      </div>
      <label className="gov-root-select">
        Start with
        <select value={root} onChange={(e) => setRoot(e.target.value)}>
          {m.nodes
            .filter((n) =>
              ["claim", "event", "document", "entity"].includes(n.kind),
            )
            .map((n) => (
              <option key={n.id} value={n.id}>
                {n.kind} · {n.title}
              </option>
            ))}
        </select>
      </label>
      <div className="gov-notice">
        “Involves” identifies an institution named in the linked evidence. These
        connections do not establish influence, motive or causality.
      </div>
      {error && (
        <p role="alert" className="gov-error">
          {error}
        </p>
      )}
      {result && (
        <div className="gov-graph">
          <div className="gov-graph-root">
            <GitBranch size={22} />
            <small>{result.root.kind}</small>
            <h3>{result.root.title}</h3>
            <span>
              2-hop outgoing traversal · {result.graph.total} returned nodes ·
              limit 30
            </span>
          </div>
          <ArrowDown className="gov-graph-arrow" />
          <div className="gov-graph-nodes">
            {result.graph.nodes
              ?.filter((n) => n.key !== `${result.snapshot_id}:${root}`)
              .map((n) => (
                <button
                  key={n.key}
                  onClick={() =>
                    n.document?.passage_id
                      ? open(n.document.passage_id)
                      : setRoot(
                          n.document?.id ||
                            n.key.replace(`${result.snapshot_id}:`, ""),
                        )
                  }
                >
                  <span className="gov-eyebrow">
                    {n.document?.kind || "node"} · hop {n.depth}
                  </span>
                  <strong>{n.document?.title || n.key}</strong>
                  <small>
                    {n.path_edges
                      ?.map((e) => e.type || e.edge_type)
                      .filter(Boolean)
                      .join(" → ") || "Explicit source relationship"}
                  </small>
                  <span>
                    {n.document?.passage_id
                      ? "Inspect passage"
                      : "Explore from here"}{" "}
                    <ChevronRight size={14} />
                  </span>
                </button>
              ))}
          </div>
          {!result.graph.nodes?.some(
            (n) => n.key !== `${result.snapshot_id}:${root}`,
          ) && <p>No outgoing connections recorded for this node.</p>}
        </div>
      )}
      {!result && !error && (
        <p role="status">Traversing evidence connections…</p>
      )}
    </>
  );
}

import { PipelineStatus } from "./PipelineStatus.tsx";
import { Autograph } from "./Autograph.tsx";

export default function GovernanceApp() {
  const initial = new URLSearchParams(window.location.search);
  const [tab, setTab] = useState<Tab>("journey");
  const [m, setM] = useState<GovernanceManifest>();
  const [error, setError] = useState("");
  const [selected, setSelected] = useState(initial.get("passage") || "");
  const [q, setQ] = useState("");
  const [intent, setIntent] = useState<"question" | "passages">("question");
  const [mode, setMode] = useState<"keyword" | "hybrid">("keyword");
  const [document, setDocument] = useState("");
  const [results, setResults] = useState<EvidenceSearch>();
  const [brief, setBrief] = useState<EvidenceBrief>();
  const [busy, setBusy] = useState(false);
  const [allEvents, setAllEvents] = useState(false);
  const request = useRef<AbortController | null>(null);
  useEffect(() => {
    const controller = new AbortController();
    api<GovernanceManifest>("manifest", { signal: controller.signal })
      .then((data) => {
        setM(data);
      })
      .catch((e) => {
        if (e.name !== "AbortError") setError(e.message);
      });
    return () => {
      controller.abort();
      request.current?.abort();
    };
  }, []);
  function open(id: string) {
    setSelected(id);
    const url = new URL(window.location.href);
    url.searchParams.set("passage", id);
    window.history.replaceState(null, "", url);
  }
  function close() {
    setSelected("");
    const url = new URL(window.location.href);
    url.searchParams.delete("passage");
    window.history.replaceState(null, "", url);
  }
  async function runSearch(question?: string) {
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    setBusy(true);
    setError("");
    setResults(undefined);
    setBrief(undefined);
    setTab("search");
    if (question) {
      setQ(question);
      setIntent("question");
    }
    try {
      if (question) {
        setDocument("");
        setBrief(
          await api<EvidenceBrief>("brief", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ question, mode }),
            signal: controller.signal,
          }),
        );
      } else
        setResults(
          await api<EvidenceSearch>(
            `search?${new URLSearchParams({ q, mode, document })}`,
            { signal: controller.signal },
          ),
        );
    } catch (e) {
      if ((e as Error).name !== "AbortError") setError((e as Error).message);
    } finally {
      if (!controller.signal.aborted) setBusy(false);
    }
  }
  const milestones = m?.events.filter(
    (e) =>
      allEvents ||
      /First reading|Do pass with amendments|Passed\.|Governor signed|Chapter 750/.test(
        e.text,
      ),
  );
  return (
    <div className="gov-app">
      <header className="gov-header">
        <a href="/governance" className="gov-brand">
          <span>
            <BookOpen size={23} />
          </span>
          <strong>
            Oregon <i>Decision Explorer</i>
          </strong>
        </a>
        <div>
          <a href="/">
            Permit explorer <ArrowUpRight size={14} />
          </a>
          <span className="gov-powered">
            Built with <b>antfly ✳</b>
          </span>
        </div>
      </header>
      <main className="gov-main">
        <div className="gov-breadcrumb">
          <a
            href="https://oregon.portlandciviclab.org/"
            target="_blank"
            rel="noreferrer"
          >
            Oregon Governance Atlas <ArrowUpRight size={12} />
          </a>
          <span>/</span> Decision pilot <span>/</span> 2017 regular session
        </div>
        <section className="gov-hero">
          <div>
            <span className="gov-eyebrow">
              FOLLOW A DECISION. INSPECT THE RECORD.
            </span>
            <h1>
              How a study bill became
              <br />a transportation package.
            </h1>
            <p>
              Trace HB 2017 from its first text to Oregon Laws chapter 750.
              Compare what changed, open the source behind a finding, and see
              where the evidence stops.
            </p>
            <div className="gov-hero-tags">
              <span>HB 2017</span>
              <span>2017 regular session</span>
              <span className="gov-enacted">
                <Check size={13} /> Enacted · Chapter 750
              </span>
            </div>
          </div>
          <aside className="gov-case-card">
            <div>
              <span className="gov-eyebrow">A BOUNDED CASE STUDY</span>
              <ShieldCheck size={22} />
            </div>
            <strong>Evidence you can inspect.</strong>
            <p>Preserved originals. Page-level citations. Explicit gaps.</p>
            <div className="gov-case-stats">
              <span>
                <b>
                  {m
                    ? m.documents.filter((d) => d.extraction === "pdf-text")
                        .length
                    : "—"}
                </b>{" "}
                primary PDFs
              </span>
              <span>
                <b>{m?.events.length || "—"}</b> recorded events
              </span>
              <span>
                <b>{m?.gaps.length || "—"}</b> open questions
              </span>
            </div>
            <small>
              Independent Antfly demo using public records and Atlas procedural
              data. No partnership implied.
            </small>
          </aside>
        </section>
        <PipelineStatus />
        <nav className="gov-tabs" aria-label="Decision views">
          {tabs.map(([id, label]) => (
            <button
              key={id}
              aria-current={tab === id ? "page" : undefined}
              className={tab === id ? "active" : ""}
              onClick={() => setTab(id)}
            >
              {label}
              {id === "gaps" && m && <span>{m.gaps.length}</span>}
            </button>
          ))}
        </nav>
        {error && (
          <p role="alert" className="gov-error">
            {error}
          </p>
        )}
        {!m && !error && <p role="status">Loading the evidence ledger…</p>}
        {m && (
          <div className="gov-content">
            {tab === "journey" && (
              <div className="gov-overview-grid">
                <section>
                  <div className="gov-section-heading">
                    <div>
                      <span className="gov-eyebrow">MARCH → OCTOBER 2017</span>
                      <h2>The path to enactment</h2>
                    </div>
                    <button
                      className="gov-text-button"
                      onClick={() => setAllEvents(!allEvents)}
                    >
                      {allEvents
                        ? "Show milestones"
                        : `All ${m.events.length} actions`}{" "}
                      <ChevronRight size={15} />
                    </button>
                  </div>
                  <p className="gov-muted">
                    Procedural history from the Atlas’s preserved OLIS data.
                    This pilot has not independently verified these events
                    against OLIS.
                  </p>
                  <ol className="gov-timeline">
                    {milestones?.map((e) => (
                      <li key={e.id}>
                        <time dateTime={e.date}>
                          {date(e.date)}
                          <span>2017</span>
                        </time>
                        <div className="gov-event-dot" />
                        <article>
                          <span className="gov-eyebrow">{e.chamber}</span>
                          <h3>{e.text}</h3>
                          {e.vote && <p className="gov-vote">{e.vote}</p>}
                          <div>
                            <button onClick={() => open(e.evidence[0])}>
                              Inspect event source <ArrowUpRight size={13} />
                            </button>
                            <a
                              href={e.source_url}
                              target="_blank"
                              rel="noreferrer"
                            >
                              Atlas record <ArrowUpRight size={13} />
                            </a>
                          </div>
                        </article>
                      </li>
                    ))}
                  </ol>
                  <div className="gov-timeline-end">
                    <span />
                    <div>
                      <strong>Oct 6 · General effective date</strong>
                      <p>
                        The chapter records this date. Specific provisions have
                        their own operative dates.
                      </p>
                      <button
                        onClick={() =>
                          runSearch(
                            m.claims.find((c) => c.id === "claim-effective")!
                              .question,
                          )
                        }
                      >
                        Inspect the conflicting date evidence{" "}
                        <ArrowRight size={14} />
                      </button>
                    </div>
                  </div>
                </section>
                <aside className="gov-sidebar">
                  <div className="gov-panel">
                    <span className="gov-eyebrow">START WITH A QUESTION</span>
                    <h3>What does the record establish?</h3>
                    {m.claims.map((c) => (
                      <button
                        className="gov-question"
                        key={c.id}
                        onClick={() => runSearch(c.question)}
                      >
                        {c.question}
                        <ArrowUpRight size={17} />
                      </button>
                    ))}
                  </div>
                  <div className="gov-conflict-card">
                    <span className="gov-eyebrow">
                      A DISAGREEMENT IN THE RECORD
                    </span>
                    <h3>Two sources. Two effective dates.</h3>
                    <p>
                      The session summary and enacted chapter disagree. The
                      source inspector makes the discrepancy visible.
                    </p>
                    <button
                      onClick={() =>
                        runSearch(
                          m.claims.find((c) => c.id === "claim-effective")!
                            .question,
                        )
                      }
                    >
                      Compare the evidence <ArrowRight size={15} />
                    </button>
                  </div>
                  <div className="gov-panel gov-scope">
                    <span className="gov-eyebrow">WHAT THIS PILOT COVERS</span>
                    <p>
                      Bill versions, enactment, fiscal and revenue estimates, a
                      staff summary, and an attributed procedural timeline.
                    </p>
                    <p>
                      It does not establish project delivery, actual spending,
                      current law, or legislators’ motives.
                    </p>
                    <button onClick={() => setTab("sources")}>
                      View acquisition and review states{" "}
                      <ArrowRight size={14} />
                    </button>
                  </div>
                </aside>
              </div>
            )}
            {tab === "search" && (
              <>
                <div className="gov-section-heading">
                  <div>
                    <span className="gov-eyebrow">
                      RETRIEVE BEFORE YOU CONCLUDE
                    </span>
                    <h2>Search the preserved record</h2>
                    <p>
                      {m.passage_count.toLocaleString()} passages with stable
                      source locators. Search rankings are not confidence
                      scores.
                    </p>
                  </div>
                  <Search size={28} />
                </div>
                <form
                  className="gov-search"
                  onSubmit={(e) => {
                    e.preventDefault();
                    if (intent === "question" && !q.trim()) return;
                    runSearch(intent === "question" ? q.trim() : undefined);
                  }}
                >
                  <label>
                    <Search size={20} />
                    <input
                      aria-label="Search legislative evidence"
                      value={q}
                      maxLength={500}
                      onChange={(e) => setQ(e.target.value)}
                      placeholder={
                        intent === "question"
                          ? "When did HB 2017 take effect?"
                          : "Try transit funding, accountability, or congestion pricing"
                      }
                    />
                  </label>
                  <button
                    className="gov-primary"
                    disabled={busy || (intent === "question" && !q.trim())}
                  >
                    {intent === "question"
                      ? "Build evidence brief"
                      : "Search record"}{" "}
                    <ArrowRight size={16} />
                  </button>
                </form>
                <div className="gov-search-options">
                  <label>
                    What would you like to do?
                    <select
                      aria-label="Search task"
                      value={intent}
                      onChange={(e) => {
                        request.current?.abort();
                        setBusy(false);
                        setIntent(e.target.value as typeof intent);
                        if (e.target.value === "question") setDocument("");
                        setBrief(undefined);
                        setResults(undefined);
                        setError("");
                      }}
                    >
                      <option value="question">Ask a question</option>
                      <option value="passages">Find passages</option>
                    </select>
                  </label>
                  <label>
                    Retrieval
                    <select
                      aria-label="Retrieval mode"
                      value={mode}
                      onChange={(e) => setMode(e.target.value as typeof mode)}
                    >
                      <option value="keyword">Keyword</option>
                      <option value="hybrid" disabled={!m.semantic_ready}>
                        Hybrid · keyword + semantic (experimental)
                        {!m.semantic_ready ? " (not ready)" : ""}
                      </option>
                    </select>
                  </label>
                  <label>
                    Source / historical version
                    <select
                      aria-label="Source filter"
                      value={document}
                      disabled={intent === "question"}
                      onChange={(e) => setDocument(e.target.value)}
                    >
                      <option value="">
                        {intent === "question"
                          ? "Briefs use all sources"
                          : "All preserved sources"}
                      </option>
                      {m.documents.map((d) => (
                        <option key={d.id} value={d.id}>
                          {d.title}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
                <p className="gov-muted gov-small">
                  Ask a question to get an evidence brief; choose Find passages
                  for raw search results. Antfly’s retrieval agent generates
                  briefs from retrieved source passages. Citations are checked
                  against those passages; conclusions still need human review.
                </p>
                {mode === "hybrid" && (
                  <p className="gov-notice">
                    Qwen3-Embedding combines semantic retrieval with keyword
                    search. A high search score does not establish a claim.
                  </p>
                )}
                {!m.semantic_ready && (
                  <p className="gov-notice">
                    {m.semantic
                      ? "Semantic enrichment is still running. Reload when ready; keyword search is available."
                      : "Keyword search is available. Configure an embedder and re-ingest to enable hybrid search."}
                  </p>
                )}
                {busy && (
                  <p className="gov-search-status" role="status">
                    Searching Antfly’s evidence index…
                  </p>
                )}
                {brief && (
                  <div className="gov-brief">
                    <div className={`gov-brief-heading ${brief.status}`}>
                      <span className="gov-eyebrow">
                        {brief.status === "not-established"
                          ? "NOT ESTABLISHED BY THIS PILOT"
                          : brief.status === "conflicting-sources"
                            ? "CONFLICTING SOURCES"
                            : "SOURCE-SUPPORTED FINDING · AWAITING REVIEW"}
                      </span>
                      {brief.claims?.length ? (
                        brief.claims.map((claim, i) => (
                          <div key={i}>
                            <p>{claim.text}</p>
                            <blockquote>{claim.quote}</blockquote>
                            {claim.evidence.map((id, n) => (
                              <button
                                className="gov-text-button"
                                key={id}
                                onClick={() => open(id)}
                              >
                                Source {n + 1}
                              </button>
                            ))}
                            {claim.contrary.map((id, n) => (
                              <button
                                className="gov-text-button"
                                key={id}
                                onClick={() => open(id)}
                              >
                                Contrary source {n + 1}
                              </button>
                            ))}
                          </div>
                        ))
                      ) : (
                        <h3>{brief.statement}</h3>
                      )}
                      <p>{brief.limitation}</p>
                      <small>
                        {brief.generator
                          ? `Generated by ${brief.generator} · citation check ${brief.citation_validation} · unreviewed`
                          : "Research candidates · awaiting review"}
                      </small>
                    </div>
                    {brief.evidence.length > 0 && (
                      <>
                        <h3>Supporting passages</h3>
                        {brief.evidence.map((p) => (
                          <Citation
                            key={p.id}
                            p={p}
                            documents={m.documents}
                            open={open}
                          />
                        ))}
                      </>
                    )}
                    {brief.contrary.length > 0 && (
                      <>
                        <h3 className="gov-rust">Contrary evidence</h3>
                        {brief.contrary.map((p) => (
                          <Citation
                            key={p.id}
                            p={p}
                            documents={m.documents}
                            open={open}
                          />
                        ))}
                      </>
                    )}
                    <details>
                      <summary>
                        Unreviewed research candidates (
                        {brief.candidates.length})
                      </summary>
                      <p className="gov-muted">
                        Similarity may help locate relevant material; these
                        passages do not by themselves answer the question.
                      </p>
                      {brief.candidates.map((p) => (
                        <Citation
                          key={p.id}
                          p={p}
                          documents={m.documents}
                          open={open}
                        />
                      ))}
                    </details>
                  </div>
                )}
                {results && (
                  <div aria-live="polite">
                    <div className="gov-results-meta">
                      <strong>{results.hits.length} ranked passages</strong>
                      <span>
                        {results.mode} · {results.took} ms · up to 12 results
                      </span>
                    </div>
                    {results.captions?.map((c) => {
                      const doc = m.documents.find(
                        (d) => d.id === c.document_id,
                      )!;
                      return (
                        <article
                          className="gov-notice"
                          key={`${c.document_id}:${c.page}`}
                        >
                          <strong>
                            Machine caption · {doc.title} · PDF page {c.page}
                          </strong>
                          <p>{c.caption}</p>
                          <p>
                            Visual description for discovery; not quoted source
                            text.
                          </p>
                          <a
                            href={`/api/governance/sources/${doc.sha256}.pdf#page=${c.page}`}
                            target="_blank"
                            rel="noreferrer"
                          >
                            Inspect preserved PDF
                          </a>
                        </article>
                      );
                    })}
                    {results.hits.length ? (
                      results.hits.map((p) => (
                        <Citation
                          key={p.id}
                          p={p}
                          documents={m.documents}
                          open={open}
                        />
                      ))
                    ) : (
                      <div className="gov-empty">
                        <h3>No matching passages</h3>
                        <p>
                          Try fewer words or another source. No match here does
                          not mean no evidence exists.
                        </p>
                      </div>
                    )}
                  </div>
                )}
                {!results && !brief && !busy && (
                  <div className="gov-search-starters">
                    <h3>Inspect a prepared finding</h3>
                    {m.claims.map((c) => (
                      <button
                        className="gov-question"
                        key={c.id}
                        onClick={() => runSearch(c.question)}
                      >
                        {c.question}
                        <ArrowUpRight size={17} />
                      </button>
                    ))}
                    <h3>Or test the limits</h3>
                    {m.gaps.slice(0, 2).map((g) => (
                      <button
                        className="gov-question"
                        key={g.id}
                        onClick={() => runSearch(g.question)}
                      >
                        {g.question}
                        <ArrowUpRight size={17} />
                      </button>
                    ))}
                  </div>
                )}
              </>
            )}
            {tab === "versions" && <Compare m={m} open={open} />}
            {tab === "connections" && <Connections m={m} open={open} />}
            {tab === "gaps" && (
              <>
                <div className="gov-section-heading">
                  <div>
                    <span className="gov-eyebrow">THE EDGE OF THE RECORD</span>
                    <h2>Questions we cannot yet answer</h2>
                    <p>
                      A research queue for this pilot. These are not claims that
                      the evidence does not exist elsewhere.
                    </p>
                  </div>
                  <BookOpen size={28} />
                </div>
                <div className="gov-gap-grid">
                  {m.gaps.map((g, i) => (
                    <article key={g.id}>
                      <span className="gov-gap-number">0{i + 1}</span>
                      <span className="gov-eyebrow">
                        OPEN · NEEDS ADDITIONAL SOURCES
                      </span>
                      <h3>{g.question}</h3>
                      <p>{g.reason}</p>
                      <div>
                        <strong>Evidence needed</strong>
                        <p>{g.needed}</p>
                      </div>
                      <button onClick={() => runSearch(g.question)}>
                        Find unreviewed candidates <Search size={15} />
                      </button>
                    </article>
                  ))}
                </div>
                <p className="gov-muted">
                  Candidate ranking searches the acquired pilot corpus. It is
                  not a ranking of the Atlas’s wider unprocessed collection.
                </p>
              </>
            )}
            {tab === "sources" && (
              <>
                <div className="gov-section-heading">
                  <div>
                    <span className="gov-eyebrow">ACQUIRED ≠ VERIFIED</span>
                    <h2>The source ledger</h2>
                    <p>
                      Original bytes, extraction coverage and review states
                      remain separate.
                    </p>
                  </div>
                  <ShieldCheck size={28} />
                </div>
                <div className="gov-ledger-summary">
                  <span>
                    <Check size={17} />{" "}
                    {
                      m.documents.filter((d) => d.extraction === "pdf-text")
                        .length
                    }{" "}
                    PDFs preserved with SHA-256
                  </span>
                  <span>
                    {m.passage_count.toLocaleString()} extracted passages
                  </span>
                  <span>0 human-reviewed documents</span>
                </div>
                <div className="gov-table-wrap">
                  <table className="gov-ledger">
                    <thead>
                      <tr>
                        <th>Document / version</th>
                        <th>Coverage</th>
                        <th>Review state</th>
                        <th>Original & provenance</th>
                      </tr>
                    </thead>
                    <tbody>
                      {m.documents.map((d) => (
                        <tr key={d.id}>
                          <td>
                            <strong>{d.title}</strong>
                            <small>
                              {d.published}
                              {d.supersedes &&
                                ` · follows ${m.documents.find((s) => s.id === d.supersedes)?.version}`}
                            </small>
                            {m.documents.some((s) => s.supersedes === d.id) && (
                              <span className="gov-historical">
                                Earlier historical version
                              </span>
                            )}
                          </td>
                          <td>
                            {d.indexed_pages.length} / {d.page_count}{" "}
                            {d.extraction === "pdf-text" ? "pages" : "events"}
                            <small>
                              {d.extraction === "pdf-text"
                                ? "PDF text extracted"
                                : "Atlas structured data"}
                            </small>
                          </td>
                          <td>
                            <span className="gov-review">
                              Awaiting human review
                            </span>
                            <small>
                              {d.extraction === "pdf-text"
                                ? "Bytes acquired; text not proofread"
                                : "Secondary procedural record"}
                            </small>
                          </td>
                          <td>
                            <a
                              href={d.source_url}
                              target="_blank"
                              rel="noreferrer"
                            >
                              Original <ArrowUpRight size={13} />
                            </a>
                            {d.extraction === "pdf-text" && (
                              <a
                                href={`/api/governance/sources/${d.sha256}.pdf`}
                                target="_blank"
                                rel="noreferrer"
                              >
                                Preserved PDF <ArrowUpRight size={13} />
                              </a>
                            )}
                            <details>
                              <summary>SHA-256 & acquisition</summary>
                              <code>{d.sha256}</code>
                              <small>{d.fetched_at}</small>
                              {d.atlas_sha256 && (
                                <small>
                                  {d.atlas_hash_matches
                                    ? "Matches Atlas checksum"
                                    : "Different from Atlas checksum"}
                                </small>
                              )}
                            </details>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="gov-notice">
                  The annual summary PDF is preserved in full; only pages
                  mentioning HB 2017 are indexed. Timeline dates and votes come
                  from the Atlas, not an independent OLIS ingestion. No hearing
                  audio, OCR, audits, litigation or later implementation records
                  have been acquired for this pilot.
                </div>
              </>
            )}
          </div>
        )}
        <footer className="gov-footer">
          <span>
            <BookOpen size={15} /> A source-bound legislative research pilot
          </span>
          <a
            href={m?.atlas_url || "https://oregon.portlandciviclab.org/"}
            target="_blank"
            rel="noreferrer"
          >
            Source methodology & case by Portland Civic Lab{" "}
            <ArrowUpRight size={13} />
          </a>
          <span>Retrieval by antfly</span>
        </footer>
      </main>
      {selected && <SourceViewer id={selected} close={close} />}
    </div>
  );
}
