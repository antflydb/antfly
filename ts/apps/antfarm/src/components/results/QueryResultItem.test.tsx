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
});
