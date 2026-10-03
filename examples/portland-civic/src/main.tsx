import React, { useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import GovernanceApp from "./governance/App.tsx";
import { useCitations } from "@antfly/components";
import {
  ArrowRight,
  ArrowUpRight,
  Building2,
  Check,
  ChevronLeft,
  ChevronRight,
  Compass,
  ExternalLink,
  FileText,
  Filter,
  Info,
  Leaf,
  LoaderCircle,
  MapPin,
  Search,
  Sparkles,
  X,
} from "lucide-react";
import { PermitMap } from "./PermitMap";
import type { Explanation, Filters, Permit, SearchResult } from "./types";
import "./style.css";

const initial: Filters = {
  q: "",
  neighborhood: "",
  status: "",
  permit_type: "",
  from: "",
  to: "",
  mode: "keyword",
  offset: 0,
};
const pretty = (s: string) =>
  s.toLowerCase().replace(/(^|[\s,-])\w/g, (c) => c.toUpperCase());
const number = (n: number) => n.toLocaleString();
const date = (d?: string) =>
  d
    ? new Date(d).toLocaleDateString("en-US", {
        year: "numeric",
        month: "short",
        day: "numeric",
        timeZone: "UTC",
      })
    : "Not recorded";
function readURL(): Filters {
  const params = new URLSearchParams(location.search);
  return {
    ...initial,
    ...Object.fromEntries(
      [...params].filter(
        ([k]) => k in initial && k !== "offset" && k !== "mode",
      ),
    ),
    mode: params.get("mode") === "hybrid" ? "hybrid" : "keyword",
    offset: Math.max(0, Number(params.get("offset")) || 0),
  };
}
function params(f: Filters) {
  return new URLSearchParams(
    Object.entries(f)
      .filter(([, v]) => v !== "" && v !== 0)
      .map(([k, v]) => [k, String(v)]),
  );
}
async function api<T>(url: string, init?: RequestInit): Promise<T> {
  const r = await fetch(url, init);
  const body = await r.json();
  if (!r.ok)
    throw new Error(body.error || "The request could not be completed");
  return body;
}
function CitationText({ answer }: { answer: Explanation }) {
  const { parseCitations } = useCitations();
  return (
    <>
      {answer.text.split("\n\n").map((paragraph, pi) => {
        const citations = parseCitations(paragraph);
        let cursor = 0;
        const parts: React.ReactNode[] = [];
        for (const citation of citations) {
          parts.push(paragraph.slice(cursor, citation.startIndex));
          for (const id of citation.ids) {
            const source = answer.sources.find((s) => s.id === id);
            if (source)
              parts.push(
                <a
                  key={`${citation.startIndex}-${id}`}
                  className="citation"
                  href={source.url}
                  target="_blank"
                  rel="noreferrer"
                  title={source.title}
                >
                  ↗ {answer.sources.indexOf(source) + 1}
                </a>,
              );
          }
          cursor = citation.endIndex;
        }
        parts.push(paragraph.slice(cursor));
        return <p key={pi}>{parts}</p>;
      })}
    </>
  );
}
function PermitDetail({
  permit: p,
  close,
}: {
  permit: Permit;
  close: () => void;
}) {
  const dialog = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    dialog.current?.showModal();
  }, []);
  return (
    <dialog
      ref={dialog}
      className="detail"
      onCancel={close}
      onClick={(e) => {
        if (e.target === e.currentTarget) close();
      }}
    >
      <div className="detail-top">
        <span className="eyebrow">THE PUBLIC RECORD</span>
        <button
          className="icon-button"
          onClick={close}
          aria-label="Close permit details"
        >
          <X size={20} />
        </button>
      </div>
      <span className="status">{p.status}</span>
      <h2>{pretty(p.address)}</h2>
      <p className="muted">
        <MapPin size={14} /> {pretty(p.neighborhood)} · {p.application}
      </p>
      <h3>What’s planned</h3>
      <p className="description">
        {p.description || "The source has no project description."}
      </p>
      <dl className="details-grid">
        <div>
          <dt>Permit type</dt>
          <dd>{p.permit_type}</dd>
        </div>
        <div>
          <dt>Submitted valuation</dt>
          <dd>
            {p.valuation === undefined
              ? "Not recorded"
              : `$${number(p.valuation)}`}
          </dd>
        </div>
        <div>
          <dt>Reported new units</dt>
          <dd>
            {p.new_units === undefined ? "Not recorded" : number(p.new_units)}
          </dd>
        </div>
        <div>
          <dt>Source ID</dt>
          <dd>{p.source_id}</dd>
        </div>
      </dl>
      <h3>Recorded timeline</h3>
      <ol className="timeline">
        {[
          ["Created", p.created_at],
          ["Issued", p.issued_at],
          ["Finaled", p.finaled_at],
        ].map(([label, d]) => (
          <li key={label}>
            <span className={d ? "timeline-dot complete" : "timeline-dot"} />
            <span>{label}</span>
            <strong>{date(d)}</strong>
          </li>
        ))}
      </ol>
      <div className="record-note">
        <Info size={17} />
        <span>
          A permit is an application record. Unit counts may overlap across
          permits and do not measure net new housing.
        </span>
      </div>
      <a
        className="primary source-button"
        href={p.source_url}
        target="_blank"
        rel="noreferrer"
      >
        Open original city record <ArrowUpRight size={17} />
      </a>
    </dialog>
  );
}
function App() {
  const [filters, setFilters] = useState<Filters>(readURL);
  const [draft, setDraft] = useState(filters.q);
  const [data, setData] = useState<SearchResult>();
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [retry, setRetry] = useState(0);
  const [selected, setSelected] = useState<Permit>();
  const [showFilters, setShowFilters] = useState(false);
  const [answer, setAnswer] = useState<Explanation>();
  const [answerError, setAnswerError] = useState("");
  const [explaining, setExplaining] = useState(false);
  const explanationRequest = useRef<AbortController | null>(null);
  useEffect(() => {
    const pop = () => {
      const f = readURL();
      setFilters(f);
      setDraft(f.q);
    };
    window.addEventListener("popstate", pop);
    return () => window.removeEventListener("popstate", pop);
  }, []);
  useEffect(() => {
    const controller = new AbortController();
    explanationRequest.current?.abort();
    setExplaining(false);
    setAnswer(undefined);
    setAnswerError("");
    setLoading(true);
    setError("");
    setSelected(undefined);
    api<SearchResult>(`/api/search?${params(filters)}`, {
      signal: controller.signal,
    })
      .then(setData)
      .catch((e) => {
        if (!controller.signal.aborted) {
          setError(e.message);
          setData(undefined);
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [filters, retry]);
  function change(next: Partial<Filters>) {
    const f = { ...filters, offset: 0, ...next };
    setFilters(f);
    history.pushState(null, "", `?${params(f)}`);
  }
  async function explain() {
    explanationRequest.current?.abort();
    const controller = new AbortController();
    explanationRequest.current = controller;
    setExplaining(true);
    setAnswerError("");
    try {
      setAnswer(
        await api<Explanation>("/api/explain", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ...filters, offset: "0" }),
          signal: controller.signal,
        }),
      );
    } catch (e) {
      if (!controller.signal.aborted)
        setAnswerError(
          e instanceof Error ? e.message : "Could not explain results",
        );
    } finally {
      if (!controller.signal.aborted) setExplaining(false);
    }
  }
  const m = data?.manifest;
  const activeCount = [
    filters.neighborhood,
    filters.status,
    filters.permit_type,
    filters.from,
    filters.to,
  ].filter(Boolean).length;
  return (
    <>
      <header className="site-header">
        <a href="/" className="brand">
          <span className="brand-mark">
            <Building2 size={22} />
          </span>
          <span>
            Portland<span className="brand-light"> in Progress</span>
          </span>
        </a>
        <div className="header-right">
          <a href="/governance">
            Decision explorer <ArrowUpRight size={14} />
          </a>
          <span className="city-tag">
            <span /> PORTLAND, OREGON
          </span>
          <a href="#about">
            About the data <ArrowUpRight size={14} />
          </a>
          <span className="powered">
            Built with <strong>antfly</strong>
            <span className="ant-mark">✳</span>
          </span>
        </div>
      </header>
      <main>
        <section className="hero">
          <div>
            <div className="eyebrow">
              <span className="tiny-line" /> A CITY, TAKING SHAPE
            </div>
            <h1>
              What’s changing
              <br />
              around <em>your corner?</em>
            </h1>
            <p>
              Explore the projects behind Portland’s building permits.
              <br className="desktop-break" /> Follow an idea from the
              application to the public record.
            </p>
          </div>
          <div className="hero-note">
            <Compass size={32} strokeWidth={1} />
            <span>
              Small additions.
              <br />
              New beginnings.
              <br />
              <strong>A closer look at our city.</strong>
            </span>
            <span className="coordinates">45.5152° N &nbsp; 122.6784° W</span>
          </div>
        </section>
        <section className="explorer" aria-label="Permit explorer">
          <div className="search-area">
            <form
              className="search-form"
              onSubmit={(e) => {
                e.preventDefault();
                change({ q: draft });
              }}
            >
              <Search size={23} />
              <input
                aria-label="Search permits"
                placeholder="An address, a neighborhood, a new possibility…"
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
              />
              <button type="submit" className="primary">
                Explore <ArrowRight size={17} />
              </button>
            </form>
            <div className="suggestions">
              <span>TRY A SEARCH</span>
              {["garage conversion", "apartments", "tenant improvement"].map(
                (q) => (
                  <button
                    key={q}
                    onClick={() => {
                      setDraft(q);
                      change({ q });
                    }}
                  >
                    {q} <ArrowUpRight size={12} />
                  </button>
                ),
              )}
            </div>
          </div>
          <div className={`filter-bar ${showFilters ? "filters-open" : ""}`}>
            <button
              className={`filter-toggle ${showFilters ? "active" : ""}`}
              onClick={() => setShowFilters(!showFilters)}
              aria-expanded={showFilters}
            >
              <Filter size={15} /> Filters{" "}
              {activeCount > 0 && <b>{activeCount}</b>}
            </button>
            <label className="select-label">
              <span className="sr-only">Neighborhood</span>
              <select
                aria-label="Neighborhood"
                value={filters.neighborhood}
                onChange={(e) => change({ neighborhood: e.target.value })}
              >
                <option value="">All neighborhoods</option>
                {m?.neighborhoods.map((n) => (
                  <option key={n} value={n}>
                    {pretty(n)}
                  </option>
                ))}
              </select>
            </label>
            <label className="select-label">
              <span className="sr-only">Permit status</span>
              <select
                aria-label="Permit status"
                value={filters.status}
                onChange={(e) => change({ status: e.target.value })}
              >
                <option value="">Any status</option>
                {m?.statuses.map((s) => (
                  <option key={s}>{s}</option>
                ))}
              </select>
            </label>
            <label className="select-label type-select">
              <span className="sr-only">Permit type</span>
              <select
                aria-label="Permit type"
                value={filters.permit_type}
                onChange={(e) => change({ permit_type: e.target.value })}
              >
                <option value="">All permit types</option>
                {m?.permit_types.map((t) => (
                  <option key={t}>{t}</option>
                ))}
              </select>
            </label>
            <span className="filter-spacer" />
            <div className="mode-switch" aria-label="Search mode">
              <button
                className={filters.mode === "keyword" ? "active" : ""}
                onClick={() => change({ mode: "keyword" })}
              >
                Keyword
              </button>
              <button
                className={filters.mode === "hybrid" ? "active" : ""}
                disabled={!m?.semantic}
                title={
                  m?.semantic
                    ? "Combine keyword and semantic search"
                    : "Configure an embedding model to enable hybrid search"
                }
                onClick={() => change({ mode: "hybrid" })}
              >
                <Sparkles size={12} /> Hybrid
              </button>
            </div>
          </div>
          {showFilters && (
            <div className="expanded-filters">
              <label>
                Created from
                <input
                  type="date"
                  aria-label="Created from"
                  value={filters.from}
                  onChange={(e) => change({ from: e.target.value })}
                />
              </label>
              <label>
                Created through
                <input
                  type="date"
                  aria-label="Created through"
                  value={filters.to}
                  onChange={(e) => change({ to: e.target.value })}
                />
              </label>
              <button
                className="text-button"
                onClick={() => {
                  setDraft("");
                  change(initial);
                }}
              >
                Clear search & filters <X size={13} />
              </button>
              <p>Dates refer to the permit’s creation date.</p>
            </div>
          )}
          <div className="explorer-body">
            <section
              className="results"
              aria-label="Search results"
              aria-busy={loading}
            >
              <div className="results-heading">
                <div>
                  <span className="eyebrow">THE PROJECTS</span>
                  <h2>
                    {loading
                      ? "Finding permits…"
                      : data
                        ? `${number(data.total)}${!data.exact ? "+" : ""} ${data.scope === "ranked_candidates" ? "ranked matches" : "permits"}`
                        : "Let’s get connected"}
                  </h2>
                </div>
                {data && !loading && (
                  <span className="query-time">
                    <span /> {data.took} ms
                  </span>
                )}
              </div>
              {error && (
                <div className="empty-state" role="alert">
                  <Info size={28} />
                  <h3>Couldn’t load the permits</h3>
                  <p>{error}</p>
                  <p>
                    Check that Antfly is running and a snapshot has been
                    ingested.
                  </p>
                  <button
                    className="primary"
                    onClick={() => setRetry(retry + 1)}
                  >
                    Try again
                  </button>
                </div>
              )}
              {loading && (
                <div className="loading">
                  <LoaderCircle className="spin" size={28} />
                  <p>Looking through the records</p>
                </div>
              )}
              {!loading && data && (
                <>
                  <p className="results-subtitle">
                    {data.scope === "ranked_candidates"
                      ? "Relevance-ranked candidates · not an exhaustive count"
                      : "Matching records in the imported snapshot"}
                  </p>
                  {!filters.neighborhood && data.neighborhoods.length > 0 && (
                    <div
                      className="neighborhood-summary"
                      aria-label="Leading neighborhoods across all matching records"
                    >
                      {data.neighborhoods.slice(0, 2).map((n) => (
                        <button
                          key={n.key}
                          onClick={() => change({ neighborhood: n.key })}
                        >
                          {pretty(n.key)} <b>{number(n.doc_count)}</b>
                        </button>
                      ))}
                    </div>
                  )}
                  {data.permits.length === 0 ? (
                    <div className="empty-state">
                      <Search size={28} />
                      <h3>No permits found</h3>
                      <p>Try a broader description or clear a filter.</p>
                      <button
                        className="text-button"
                        onClick={() => {
                          setDraft("");
                          change(initial);
                        }}
                      >
                        Reset search <ArrowRight size={15} />
                      </button>
                    </div>
                  ) : (
                    <div className="result-cards">
                      {data.permits.map((p, i) => (
                        <button
                          key={p.id}
                          className="permit-card"
                          onClick={() => setSelected(p)}
                        >
                          <div className="card-top">
                            <span className="permit-category">
                              <Building2 size={13} />
                              {p.permit_type}
                            </span>
                            <ArrowUpRight size={17} />
                          </div>
                          <h3>{pretty(p.address)}</h3>
                          <p className="card-description">
                            {p.description || "No description recorded"}
                          </p>
                          <div className="card-bottom">
                            <span
                              className={`status ${/final|issued/i.test(p.status) ? "issued" : ""}`}
                            >
                              {/final|issued/i.test(p.status) ? (
                                <Check size={10} />
                              ) : (
                                <span className="status-dot" />
                              )}
                              {p.status}
                            </span>
                            <span>{pretty(p.neighborhood)}</span>
                            <span className="map-index">
                              {filters.offset + i + 1}
                            </span>
                          </div>
                        </button>
                      ))}
                    </div>
                  )}
                  <div className="pagination">
                    <span>
                      {data.permits.length
                        ? `${filters.offset + 1}–${filters.offset + data.permits.length}`
                        : "0"}{" "}
                      displayed
                    </span>
                    <button
                      aria-label="Previous results"
                      disabled={!filters.offset}
                      onClick={() =>
                        change({ offset: Math.max(0, filters.offset - 40) })
                      }
                    >
                      <ChevronLeft size={17} />
                    </button>
                    <button
                      aria-label="Next results"
                      disabled={
                        data.permits.length < 40 ||
                        (data.exact && filters.offset + 40 >= data.total)
                      }
                      onClick={() => change({ offset: filters.offset + 40 })}
                    >
                      <ChevronRight size={17} />
                    </button>
                  </div>
                </>
              )}
            </section>
            <section className="map-panel" aria-label="Project map">
              <div className="map-label">
                <span className="map-label-dot" /> AROUND PORTLAND{" "}
                <span>
                  {data?.permits.filter((p) => p.location).length || 0} mapped
                  on this page
                </span>
              </div>
              <PermitMap
                permits={loading ? [] : data?.permits || []}
                selected={selected?.id}
                onSelect={setSelected}
              />
              <div className="map-bottom-note">
                <MapPin size={13} /> Select a pin or project to open its record
              </div>
            </section>
          </div>
          <div className="explainer">
            <div className="explainer-icon">
              <Sparkles size={20} />
            </div>
            <div>
              <h3>A little context goes a long way.</h3>
              <p>
                {data?.generation_enabled
                  ? "Get a summary grounded in the matching permit records."
                  : "Read an evidence brief with direct links to matching permit records."}
              </p>
            </div>
            <button
              className="outline-button"
              disabled={loading || !data?.permits.length || explaining}
              onClick={explain}
            >
              {explaining ? (
                <LoaderCircle className="spin" size={16} />
              ) : (
                <FileText size={16} />
              )}{" "}
              {explaining ? "Reading records…" : "Explain these results"}{" "}
              <ArrowRight size={15} />
            </button>
          </div>
          {(answer || answerError) && (
            <div className="answer" aria-live="polite">
              <div className="answer-heading">
                <span className="eyebrow">
                  {answer?.generated
                    ? "AI SUMMARY · CHECK THE SOURCES"
                    : "EVIDENCE BRIEF · DIRECT RECORD EXCERPTS"}
                </span>
                <button
                  className="icon-button"
                  aria-label="Close explanation"
                  onClick={() => {
                    setAnswer(undefined);
                    setAnswerError("");
                  }}
                >
                  <X size={17} />
                </button>
              </div>
              {answerError ? (
                <p role="alert">{answerError}</p>
              ) : (
                answer && (
                  <>
                    <CitationText answer={answer} />
                    <div className="answer-sources">
                      {answer.sources.map((s, i) => (
                        <a
                          key={s.id}
                          href={s.url}
                          target="_blank"
                          rel="noreferrer"
                        >
                          {i + 1}. {pretty(s.title)} <ExternalLink size={12} />
                        </a>
                      ))}
                    </div>
                  </>
                )
              )}
            </div>
          )}
        </section>
        <section className="context" aria-label="Citywide housing context">
          <div className="context-intro">
            <div className="eyebrow">THE BIGGER PICTURE</div>
            <h2>Beyond the block.</h2>
            <p>
              Housing trends from Portland Civic Lab.
              <br />
              Separate source coverage; unaffected by search filters.
            </p>
            <a
              href="https://www.portlandciviclab.org/dashboard/housing"
              target="_blank"
              rel="noreferrer"
            >
              Explore the housing dashboard <ArrowUpRight size={14} />
            </a>
          </div>
          <div className="metrics">
            {m?.housing?.series
              .filter((s) => s.points.length)
              .map((series) => {
                const points = series.points.slice(-12),
                  last = points.at(-1)!;
                const max = Math.max(...points.map((p) => p.value), 1);
                return (
                  <article className="metric" key={series.id}>
                    <span className="metric-title">{series.title}</span>
                    <div className="metric-value">
                      {series.unit === "USD" ? "$" : ""}
                      {number(last.value)}
                      <span>
                        {series.unit === "USD" ? "/ month" : series.unit}
                      </span>
                    </div>
                    <div
                      className="sparkline"
                      aria-label={`Last ${points.length} reported periods`}
                    >
                      {points.map((p) => (
                        <span
                          key={p.date}
                          style={{
                            height: `${Math.max(4, (p.value / max) * 100)}%`,
                          }}
                          title={`${p.date}: ${p.value}`}
                        />
                      ))}
                    </div>
                    <div className="metric-date">
                      As reported: {last.date}
                      <span>{m.housing?.data_status}</span>
                    </div>
                  </article>
                );
              }) || (
              <p className="context-unavailable">
                Housing context is unavailable until a snapshot is loaded.
              </p>
            )}
          </div>
        </section>
        <section className="about-data" id="about">
          <div>
            <Leaf size={20} />
            <h3>Public data. A clearer view.</h3>
            <p>
              An independent Antfly demo using City of Portland permit records
              and Portland Civic Lab’s curated housing data. Not affiliated with
              the city or the Lab.
            </p>
          </div>
          <div>
            <h4>Know what you’re looking at</h4>
            <p>
              {m
                ? `${number(m.imported_count)} imported records out of ${number(m.source_count)} source records matching the period. ${m.selection}`
                : "Dataset coverage will appear here once connected."}{" "}
              Permit counts are not housing-unit counts. Issued-permit
              processing times exclude applications still in review; incomplete
              periods should not be compared with full periods.
            </p>
          </div>
          <div>
            <h4>Follow the source</h4>
            <a
              href="https://www.portlandmaps.com/arcgis/rest/services/Public/BDS_Permit/FeatureServer/22"
              target="_blank"
              rel="noreferrer"
            >
              City of Portland / PortlandMaps <ArrowUpRight size={13} />
            </a>
            <a
              href="https://www.portlandciviclab.org/open-data"
              target="_blank"
              rel="noreferrer"
            >
              Portland Civic Lab · curated data CC BY <ArrowUpRight size={13} />
            </a>
            <a
              href="https://www.portlandciviclab.org/methodology"
              target="_blank"
              rel="noreferrer"
            >
              Sources & methodology <ArrowUpRight size={13} />
            </a>
            <p>
              Permit snapshot: {date(m?.fetched_at)}
              <br />
              Housing fetched: {date(m?.housing?.fetched_at)}
            </p>
          </div>
        </section>
      </main>
      <footer>
        <span>PORTLAND IN PROGRESS</span>
        <span>
          Made for the curious. Powered by{" "}
          <a href="https://antfly.io" target="_blank" rel="noreferrer">
            Antfly <ArrowUpRight size={12} />
          </a>
        </span>
      </footer>
      {selected && (
        <PermitDetail
          key={selected.id}
          permit={selected}
          close={() => setSelected(undefined)}
        />
      )}
    </>
  );
}
createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    {window.location.pathname.startsWith("/governance") ? (
      <GovernanceApp />
    ) : (
      <App />
    )}
  </React.StrictMode>,
);
