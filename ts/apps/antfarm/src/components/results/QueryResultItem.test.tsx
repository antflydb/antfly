/**
 * Unit tests for QueryResultItem component
 *
 * Tests verify that:
 * - Score is displayed without star ratings
 * - Score label "Score:" is shown instead of star icon
 * - Tooltip indicator (cursor-help) is present for score explanation
 */
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import QueryResultItem from "./QueryResultItem";

describe("QueryResultItem", () => {
  const mockHit = {
    _id: "test-doc-1",
    _source: {
      title: "Test Document",
      content: "This is test content",
    },
    _score: 0.0164,
  };

  it("should display score value without stars", () => {
    render(
      <QueryResultItem hit={mockHit} index={0} isExpanded={false} onToggle={() => undefined} />
    );

    // Score should be displayed
    const scoreElement = screen.getByText("0.0164");
    expect(scoreElement).toBeTruthy();

    // Stars should NOT be present (we removed them)
    // The old implementation had 5 stars with fill-yellow-400 class
    const stars = document.querySelectorAll('[class*="fill-yellow"]');
    expect(stars.length).toBe(0);
  });

  it("should display score label instead of star icon", () => {
    render(
      <QueryResultItem hit={mockHit} index={0} isExpanded={false} onToggle={() => undefined} />
    );

    // Should have "Score:" labels (in badge and expanded view)
    const scoreLabels = screen.getAllByText("Score:");
    expect(scoreLabels.length).toBeGreaterThan(0);
  });

  it("should have tooltip container with cursor-help for score", () => {
    render(
      <QueryResultItem hit={mockHit} index={0} isExpanded={false} onToggle={() => undefined} />
    );

    // The badge should have cursor-help class indicating tooltip presence
    const tooltipElements = document.querySelectorAll('[class*="cursor-help"]');
    expect(tooltipElements.length).toBeGreaterThan(0);
  });

  it("should display score section even when score is zero", () => {
    const hitWithZeroScore = {
      _id: "test-doc-2",
      _score: 0,
      _source: { title: "Zero Score" },
    };

    render(
      <QueryResultItem
        hit={hitWithZeroScore}
        index={0}
        isExpanded={false}
        onToggle={() => undefined}
      />
    );

    // Score 0 should still be displayed
    const scoreElement = screen.getByText("0.0000");
    expect(scoreElement).toBeTruthy();
  });

  it("should display hierarchy chunk preview and source label for document search hits", () => {
    const hierarchyHit = {
      _id: "doc-chunk-1",
      _score: 0.12,
      _source: {},
      hierarchy: {
        level: "chunk" as const,
        parent_doc_key: "source-doc-1",
        artifact: { text: "This standup chunk discusses blockers, progress, and follow-up work." },
        ancestors: {
          source: {
            document: { filename: "standup-june.docx", source_path: "standups/standup-june.docx" },
          },
        },
      },
    };

    render(
      <QueryResultItem hit={hierarchyHit} index={0} isExpanded={false} onToggle={() => undefined} />
    );

    expect(screen.getByText(/Source: standup-june\.docx/i)).toBeTruthy();
    expect(screen.getByText(/standup chunk discusses blockers/i)).toBeTruthy();
    expect(screen.queryByText(/No preview available/i)).toBeNull();
  });

  it("should flag symbol-heavy extracted text as low quality", () => {
    const lowQualityHit = {
      _id: "pdf-chunk-1",
      _score: 0.2,
      _source: {
        text: "- --  $  $   $ $ $ $ $ - - - % $ - - - / / / / / / / / -- -- %% $$ //// ---- ",
      },
    };

    render(
      <QueryResultItem
        hit={lowQualityHit}
        index={0}
        isExpanded={false}
        onToggle={() => undefined}
      />
    );

    expect(screen.getByText(/Low-quality extracted text/i)).toBeTruthy();
  });
});
