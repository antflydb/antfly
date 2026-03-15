import { renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { useCitations } from "./useCitations";

describe("useCitations", () => {
  it("should initialize without errors", () => {
    const { result } = renderHook(() => useCitations());

    expect(result.current.parseCitations).toBeInstanceOf(Function);
    expect(result.current.highlightCitations).toBeInstanceOf(Function);
    expect(result.current.extractCitationUrls).toBeInstanceOf(Function);
    expect(result.current.renderAsMarkdown).toBeInstanceOf(Function);
    expect(result.current.renderAsSequential).toBeInstanceOf(Function);
  });

  describe("parseCitations", () => {
    it("should parse citations with resource_id prefix", () => {
      const { result } = renderHook(() => useCitations());

      const text = "The system uses consensus [resource_id 1] and replication [resource_id 2, 3].";
      const citations = result.current.parseCitations(text);

      expect(citations).toHaveLength(2);
      expect(citations[0]).toEqual({
        originalText: "[resource_id 1]",
        ids: ["1"],
        startIndex: 26,
        endIndex: 41,
      });
      expect(citations[1]).toEqual({
        originalText: "[resource_id 2, 3]",
        ids: ["2", "3"],
        startIndex: 58,
        endIndex: 76,
      });
    });

    it("should parse citations without prefix (shorthand)", () => {
      const { result } = renderHook(() => useCitations());

      const text = "See docs [1, 2] and [3].";
      const citations = result.current.parseCitations(text);

      expect(citations).toHaveLength(2);
      expect(citations[0]).toEqual({
        originalText: "[1, 2]",
        ids: ["1", "2"],
        startIndex: 9,
        endIndex: 15,
      });
      expect(citations[1]).toEqual({
        originalText: "[3]",
        ids: ["3"],
        startIndex: 20,
        endIndex: 23,
      });
    });

    it("should return empty array for text without citations", () => {
      const { result } = renderHook(() => useCitations());

      const text = "This text has no citations.";
      const citations = result.current.parseCitations(text);

      expect(citations).toEqual([]);
    });

    it("should handle mixed citation formats", () => {
      const { result } = renderHook(() => useCitations());

      const text = "Text [resource_id 1] more text [2, 3] end.";
      const citations = result.current.parseCitations(text);

      expect(citations).toHaveLength(2);
      expect(citations[0].ids).toEqual(["1"]);
      expect(citations[1].ids).toEqual(["2", "3"]);
    });
  });

  describe("highlightCitations", () => {
    it("should replace citations with custom rendering", () => {
      const { result } = renderHook(() => useCitations());

      const text = "See docs [resource_id 1, 2] and [3].";
      const highlighted = result.current.highlightCitations(text, {
        renderCitation: (ids) => `<cite>${ids.join(",")}</cite>`,
      });

      expect(highlighted).toBe("See docs <cite>1,2</cite> and <cite>3</cite>.");
    });

    it("should provide all citation IDs to render function", () => {
      const { result } = renderHook(() => useCitations());

      const text = "Text [1] more [2] end [3].";
      const allIds: string[][] = [];

      result.current.highlightCitations(text, {
        renderCitation: (_ids, all) => {
          allIds.push(all);
          return "";
        },
      });

      // All render calls should receive the same complete list
      expect(allIds).toHaveLength(3);
      expect(allIds[0]).toEqual(["1", "2", "3"]);
      expect(allIds[1]).toEqual(["1", "2", "3"]);
      expect(allIds[2]).toEqual(["1", "2", "3"]);
    });

    it("should handle empty text", () => {
      const { result } = renderHook(() => useCitations());

      const highlighted = result.current.highlightCitations("", {
        renderCitation: () => "replaced",
      });

      expect(highlighted).toBe("");
    });
  });

  describe("extractCitationUrls", () => {
    it("should extract unique citation IDs in order", () => {
      const { result } = renderHook(() => useCitations());

      const text = "Text [resource_id 1, 2] more [3] and [2, 4].";
      const ids = result.current.extractCitationUrls(text);

      // Should be unique and in order of first appearance
      expect(ids).toEqual(["1", "2", "3", "4"]);
    });

    it("should return empty array for no citations", () => {
      const { result } = renderHook(() => useCitations());

      const text = "No citations here.";
      const ids = result.current.extractCitationUrls(text);

      expect(ids).toEqual([]);
    });

    it("should handle duplicate IDs", () => {
      const { result } = renderHook(() => useCitations());

      const text = "Text [1] more [2] and [1] again.";
      const ids = result.current.extractCitationUrls(text);

      // Should only include each ID once
      expect(ids).toEqual(["1", "2"]);
    });
  });

  describe("renderAsMarkdown", () => {
    it("should render IDs as markdown links", () => {
      const { result } = renderHook(() => useCitations());

      const rendered = result.current.renderAsMarkdown(["1", "2", "3"]);

      expect(rendered).toBe("[[1]](#hit-1), [[2]](#hit-2), [[3]](#hit-3)");
    });

    it("should handle single ID", () => {
      const { result } = renderHook(() => useCitations());

      const rendered = result.current.renderAsMarkdown(["doc1"]);

      expect(rendered).toBe("[[doc1]](#hit-doc1)");
    });

    it("should handle empty array", () => {
      const { result } = renderHook(() => useCitations());

      const rendered = result.current.renderAsMarkdown([]);

      expect(rendered).toBe("");
    });
  });

  describe("renderAsSequential", () => {
    it("should render with sequential numbering", () => {
      const { result } = renderHook(() => useCitations());

      const allIds = ["doc5", "doc2", "doc8"];
      const rendered = result.current.renderAsSequential(["doc5", "doc8"], allIds);

      // doc5 is 1st, doc8 is 3rd
      expect(rendered).toBe("[[1]](#hit-doc5), [[3]](#hit-doc8)");
    });

    it("should handle single ID with sequential numbering", () => {
      const { result } = renderHook(() => useCitations());

      const allIds = ["a", "b", "c"];
      const rendered = result.current.renderAsSequential(["b"], allIds);

      expect(rendered).toBe("[[2]](#hit-b)");
    });

    it("should handle empty IDs array", () => {
      const { result } = renderHook(() => useCitations());

      const rendered = result.current.renderAsSequential([], ["a", "b"]);

      expect(rendered).toBe("");
    });
  });

  describe("hook stability", () => {
    it("should return stable function references", () => {
      const { result, rerender } = renderHook(() => useCitations());

      const firstFunctions = {
        parseCitations: result.current.parseCitations,
        highlightCitations: result.current.highlightCitations,
        extractCitationUrls: result.current.extractCitationUrls,
        renderAsMarkdown: result.current.renderAsMarkdown,
        renderAsSequential: result.current.renderAsSequential,
      };

      rerender();

      // Functions should be the same references
      expect(result.current.parseCitations).toBe(firstFunctions.parseCitations);
      expect(result.current.highlightCitations).toBe(firstFunctions.highlightCitations);
      expect(result.current.extractCitationUrls).toBe(firstFunctions.extractCitationUrls);
      expect(result.current.renderAsMarkdown).toBe(firstFunctions.renderAsMarkdown);
      expect(result.current.renderAsSequential).toBe(firstFunctions.renderAsSequential);
    });
  });

  describe("integration with real citation text", () => {
    it("should handle complex RAG response", () => {
      const { result } = renderHook(() => useCitations());

      const ragText = `
        Raft is a consensus algorithm [resource_id raft-paper] designed for
        understandability. It provides the same guarantees as Paxos [paxos-paper].

        The algorithm has three states [resource_id raft-paper, raft-impl]:
        Leader, Follower, and Candidate.
      `;

      // Parse citations
      const citations = result.current.parseCitations(ragText);
      expect(citations).toHaveLength(3);

      // Extract IDs
      const ids = result.current.extractCitationUrls(ragText);
      expect(ids).toEqual(["raft-paper", "paxos-paper", "raft-impl"]);

      // Render with sequential numbers
      const rendered = result.current.highlightCitations(ragText, {
        renderCitation: (citationIds, allIds) =>
          result.current.renderAsSequential(citationIds, allIds),
      });

      expect(rendered).toContain("[[1]](#hit-raft-paper)");
      expect(rendered).toContain("[[2]](#hit-paxos-paper)");
      expect(rendered).toContain("[[1]](#hit-raft-paper), [[3]](#hit-raft-impl)");
    });

    it("should filter hits to only cited resources", () => {
      const { result } = renderHook(() => useCitations());

      const answer = "The system uses [1, 2] for replication.";
      const allHits = [
        { _id: "1", _source: { title: "Doc 1" } },
        { _id: "2", _source: { title: "Doc 2" } },
        { _id: "3", _source: { title: "Doc 3" } },
        { _id: "4", _source: { title: "Doc 4" } },
      ];

      const citedIds = result.current.extractCitationUrls(answer);
      const citedHits = allHits.filter((hit) => citedIds.includes(hit._id));

      expect(citedHits).toHaveLength(2);
      expect(citedHits[0]._id).toBe("1");
      expect(citedHits[1]._id).toBe("2");
    });
  });
});
