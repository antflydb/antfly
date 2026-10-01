import type { ResearchEvidence } from "@antfly/sdk";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import ResearchReport, { evidenceLabel, type ResearchReportResult } from "./ResearchReport";

const evidence: ResearchEvidence[] = [
  { id: "E1", source: "table", table: "docs", doc_id: "raft-101", title: "Raft consensus" },
  { id: "E2", source: "web", url: "https://example.com/bm25" },
];

describe("evidenceLabel", () => {
  it("prefers title, then url, then table/doc_id, then table, then id", () => {
    expect(evidenceLabel({ id: "E1", source: "table", title: "A title" })).toBe("A title");
    expect(evidenceLabel({ id: "E2", source: "web", url: "https://x.test" })).toBe(
      "https://x.test"
    );
    expect(evidenceLabel({ id: "E3", source: "table", table: "docs", doc_id: "d1" })).toBe(
      "docs/d1"
    );
    expect(evidenceLabel({ id: "E4", source: "table", table: "docs" })).toBe("docs");
    expect(evidenceLabel({ id: "E5", source: "fetch" })).toBe("E5");
  });
});

describe("ResearchReport", () => {
  it("renders nothing when there is no report", () => {
    const result: ResearchReportResult = {};
    const { container } = render(<ResearchReport result={result} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("calls renderEmpty when there is no report and it is provided", () => {
    const result: ResearchReportResult = {};
    render(<ResearchReport result={result} renderEmpty={() => <div>No report yet</div>} />);
    expect(screen.getByText("No report yet")).toBeInTheDocument();
  });

  it("renders the title, summary and sections", () => {
    const result: ResearchReportResult = {
      report: {
        title: "Hybrid search and reranking",
        summary: "An executive summary with no citations.",
        sections: [{ heading: "Overview", markdown: "First paragraph.\n\nSecond paragraph." }],
        markdown: "# Hybrid search and reranking\n\n...",
      },
      evidence: [],
    };

    const { container } = render(<ResearchReport result={result} />);

    expect(screen.getByText("Hybrid search and reranking")).toBeInTheDocument();
    expect(screen.getByText("Overview")).toBeInTheDocument();
    expect(screen.getByText("An executive summary with no citations.")).toBeInTheDocument();
    expect(screen.getByText("First paragraph.")).toBeInTheDocument();
    expect(screen.getByText("Second paragraph.")).toBeInTheDocument();
    // The full report.markdown fallback block is not rendered when structured sections exist.
    expect(container.querySelector(".react-af-research-report-markdown")).toBeNull();
  });

  it("resolves [E#] citation markers into linked, tooltipped references and a sources list", () => {
    const result: ResearchReportResult = {
      report: {
        sections: [
          {
            heading: "Overview",
            markdown: "BM25 ranks by lexical overlap [E1]. Vectors add semantics [E2].",
          },
        ],
        markdown: "BM25 ranks by lexical overlap [E1]. Vectors add semantics [E2].",
      },
      evidence,
    };

    const { container } = render(<ResearchReport result={result} />);

    const citationLinks = Array.from(
      container.querySelectorAll<HTMLAnchorElement>(".react-af-research-citation a")
    );
    expect(citationLinks).toHaveLength(2);
    expect(citationLinks[0]).toHaveAttribute("href", "#evidence-E1");
    expect(citationLinks[0]).toHaveAttribute("title", "Raft consensus");
    expect(citationLinks[0]).toHaveTextContent("[E1]");
    expect(citationLinks[1]).toHaveAttribute("href", "#evidence-E2");
    expect(citationLinks[1]).toHaveAttribute("title", "https://example.com/bm25");

    // Sources list: E1 has no url so it's plain text; E2 has a url so it's an external link.
    const sources = container.querySelector(".react-af-research-report-sources");
    expect(sources).not.toBeNull();
    expect(sources).toHaveTextContent("Raft consensus");
    const sourceLink = sources?.querySelector("a");
    expect(sourceLink).toHaveAttribute("href", "https://example.com/bm25");
    expect(sourceLink).toHaveAttribute("target", "_blank");
    expect(container.querySelector("#evidence-E1")?.tagName).toBe("LI");
  });

  it("renders an unresolved citation marker distinctly when the id has no matching evidence", () => {
    const result: ResearchReportResult = {
      report: { markdown: "This claim cites nothing real [E9]." },
      evidence: [],
    };
    render(<ResearchReport result={result} />);
    const marker = screen.getByText("[E9]");
    expect(marker.closest("a")).toBeNull();
    expect(marker).toHaveClass("react-af-research-citation-unresolved");
  });

  it("falls back to the full report markdown when there are no structured sections", () => {
    const result: ResearchReportResult = {
      report: { markdown: "Full report body with no sections." },
      evidence: [],
    };
    render(<ResearchReport result={result} />);
    expect(screen.getByText("Full report body with no sections.")).toBeInTheDocument();
  });

  it("hides the summary and sources when showSummary/showSources are false", () => {
    const result: ResearchReportResult = {
      report: {
        summary: "Hidden summary.",
        sections: [{ heading: "Section", markdown: "Body [E1]." }],
        markdown: "Body [E1].",
      },
      evidence,
    };
    const { container } = render(
      <ResearchReport result={result} showSummary={false} showSources={false} />
    );
    expect(screen.queryByText("Hidden summary.")).not.toBeInTheDocument();
    expect(container.querySelector(".react-af-research-report-sources")).toBeNull();
  });

  it("supports a custom renderCitation", () => {
    const result: ResearchReportResult = {
      report: { sections: [{ heading: "S", markdown: "See [E1]." }], markdown: "See [E1]." },
      evidence,
    };
    render(
      <ResearchReport
        result={result}
        renderCitation={(ev, marker) => <span data-testid="custom-cite">{ev?.id ?? marker}</span>}
      />
    );
    expect(screen.getByTestId("custom-cite")).toHaveTextContent("E1");
  });
});
