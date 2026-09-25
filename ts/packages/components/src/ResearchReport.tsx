import type { ResearchEvidence, ResearchReport as ResearchReportData } from "@antfly/sdk";
import { Fragment, type ReactNode, useMemo } from "react";
import { parseCitations } from "./citations";

/** Minimal shape ResearchReport needs from a ResearchAgentResult (streaming or final). */
export interface ResearchReportResult {
  report?: ResearchReportData;
  evidence?: ResearchEvidence[];
}

export interface ResearchReportProps {
  /** Research agent result (streaming or final) carrying the report and evidence registry. */
  result: ResearchReportResult;
  /**
   * Custom rendering for one resolved `[E#]` citation marker. Defaults to a
   * linked, tooltipped superscript pointing at the matching sources entry.
   * `evidence` is undefined when the marker did not resolve to any evidence.
   */
  renderCitation?: (evidence: ResearchEvidence | undefined, marker: string) => ReactNode;
  /** Render the executive summary above the sections. Default true. */
  showSummary?: boolean;
  /** Render a numbered sources list built from the evidence registry. Default true. */
  showSources?: boolean;
  /** Rendered when there is no report yet. */
  renderEmpty?: () => ReactNode;
  className?: string;
}

/** Best-effort human label for one evidence item. */
export function evidenceLabel(evidence: ResearchEvidence): string {
  if (evidence.title) return evidence.title;
  if (evidence.url) return evidence.url;
  if (evidence.table && evidence.doc_id) return `${evidence.table}/${evidence.doc_id}`;
  if (evidence.table) return evidence.table;
  return evidence.id;
}

function defaultRenderCitation(evidence: ResearchEvidence | undefined, marker: string): ReactNode {
  if (!evidence) {
    return (
      <sup className="react-af-research-citation react-af-research-citation-unresolved">
        {marker}
      </sup>
    );
  }
  const label = evidenceLabel(evidence);
  return (
    <sup className="react-af-research-citation">
      <a href={`#evidence-${evidence.id}`} title={label} aria-label={label}>
        {marker}
      </a>
    </sup>
  );
}

/** Split text on `[E#]`-style citation markers and resolve each against the evidence registry. */
function renderTextWithCitations(
  text: string,
  evidenceById: Map<string, ResearchEvidence>,
  renderCitation: NonNullable<ResearchReportProps["renderCitation"]>,
  keyPrefix: string
): ReactNode[] {
  const citations = parseCitations(text);
  if (citations.length === 0) return [text];

  const nodes: ReactNode[] = [];
  let cursor = 0;
  for (const citation of citations) {
    if (citation.startIndex > cursor) {
      nodes.push(text.slice(cursor, citation.startIndex));
    }
    for (const id of citation.ids) {
      nodes.push(
        <Fragment key={`${keyPrefix}-${citation.startIndex}-${id}`}>
          {renderCitation(evidenceById.get(id), `[${id}]`)}
        </Fragment>
      );
    }
    cursor = citation.endIndex;
  }
  if (cursor < text.length) nodes.push(text.slice(cursor));
  return nodes;
}

function Paragraphs({
  markdown,
  evidenceById,
  renderCitation,
  keyPrefix,
}: {
  markdown: string;
  evidenceById: Map<string, ResearchEvidence>;
  renderCitation: NonNullable<ResearchReportProps["renderCitation"]>;
  keyPrefix: string;
}) {
  const paragraphs = markdown.split(/\n{2,}/).filter((paragraph) => paragraph.trim().length > 0);
  return (
    <>
      {paragraphs.map((paragraph, index) => (
        // biome-ignore lint/suspicious/noArrayIndexKey: paragraphs are static once a section's markdown streams in
        <p key={`${keyPrefix}-${index}`} className="react-af-research-report-paragraph">
          {renderTextWithCitations(
            paragraph,
            evidenceById,
            renderCitation,
            `${keyPrefix}-${index}`
          )}
        </p>
      ))}
    </>
  );
}

/**
 * Renders a Research Agent report: title, executive summary, sections and a
 * sources list, with `[E#]` citation markers resolved against the evidence
 * registry and turned into linked, tooltipped references.
 *
 * Accepts a streaming or final research result, so it can render the report
 * as it is written (see useResearchStream's `reportMarkdown`/`sections`) or
 * the authoritative `ResearchAgentResult.report`.
 */
export default function ResearchReport({
  result,
  renderCitation = defaultRenderCitation,
  showSummary = true,
  showSources = true,
  renderEmpty,
  className,
}: ResearchReportProps) {
  const evidenceById = useMemo(() => {
    const map = new Map<string, ResearchEvidence>();
    for (const evidence of result.evidence ?? []) {
      map.set(evidence.id, evidence);
    }
    return map;
  }, [result.evidence]);

  const report = result.report;
  if (!report || (!report.sections?.length && !report.markdown)) {
    return renderEmpty ? renderEmpty() : null;
  }

  const hasSections = (report.sections?.length ?? 0) > 0;
  const rootClassName = ["react-af-research-report", className].filter(Boolean).join(" ");

  return (
    <article className={rootClassName}>
      {report.title && <h2 className="react-af-research-report-title">{report.title}</h2>}
      {showSummary && report.summary && (
        <p className="react-af-research-report-summary">
          {renderTextWithCitations(report.summary, evidenceById, renderCitation, "summary")}
        </p>
      )}
      {hasSections
        ? report.sections?.map((section, index) => (
            <section
              // biome-ignore lint/suspicious/noArrayIndexKey: sections are appended in order and never reordered
              key={`${section.heading}-${index}`}
              className="react-af-research-report-section"
            >
              <h3 className="react-af-research-report-heading">{section.heading}</h3>
              <Paragraphs
                markdown={section.markdown}
                evidenceById={evidenceById}
                renderCitation={renderCitation}
                keyPrefix={`section-${index}`}
              />
            </section>
          ))
        : report.markdown && (
            <div className="react-af-research-report-markdown">
              <Paragraphs
                markdown={report.markdown}
                evidenceById={evidenceById}
                renderCitation={renderCitation}
                keyPrefix="report"
              />
            </div>
          )}
      {showSources && evidenceById.size > 0 && (
        <div className="react-af-research-report-sources">
          <strong>Sources</strong>
          <ol>
            {Array.from(evidenceById.values()).map((evidence) => (
              <li key={evidence.id} id={`evidence-${evidence.id}`}>
                {evidence.url ? (
                  <a href={evidence.url} target="_blank" rel="noreferrer">
                    {evidenceLabel(evidence)}
                  </a>
                ) : (
                  evidenceLabel(evidence)
                )}
              </li>
            ))}
          </ol>
        </div>
      )}
    </article>
  );
}
