import { describe, expect, it } from "vitest";
import {
  parseCitations,
  renderAsMarkdownLinks,
  renderAsSequentialLinks,
  replaceCitations,
} from "./citations";

describe("parseCitations", () => {
  it("should parse single citation with resource_id prefix", () => {
    const text = "Some text [resource_id 1] more text";
    const citations = parseCitations(text);

    expect(citations).toHaveLength(1);
    expect(citations[0]).toEqual({
      originalText: "[resource_id 1]",
      ids: ["1"],
      startIndex: 10,
      endIndex: 25,
    });
  });

  it("should parse single citation with doc_id prefix (legacy)", () => {
    const text = "Some text [doc_id 1] more text";
    const citations = parseCitations(text);

    expect(citations).toHaveLength(1);
    expect(citations[0]).toEqual({
      originalText: "[doc_id 1]",
      ids: ["1"],
      startIndex: 10,
      endIndex: 20,
    });
  });

  it("should parse single citation without doc_id prefix", () => {
    const text = "Some text [1] more text";
    const citations = parseCitations(text);

    expect(citations).toHaveLength(1);
    expect(citations[0]).toEqual({
      originalText: "[1]",
      ids: ["1"],
      startIndex: 10,
      endIndex: 13,
    });
  });

  it("should parse multiple IDs in single citation with resource_id prefix", () => {
    const text = "Text [resource_id 1, 2, 3] more";
    const citations = parseCitations(text);

    expect(citations).toHaveLength(1);
    expect(citations[0]).toEqual({
      originalText: "[resource_id 1, 2, 3]",
      ids: ["1", "2", "3"],
      startIndex: 5,
      endIndex: 26,
    });
  });

  it("should parse multiple IDs in single citation with doc_id prefix (legacy)", () => {
    const text = "Text [doc_id 1, 2, 3] more";
    const citations = parseCitations(text);

    expect(citations).toHaveLength(1);
    expect(citations[0]).toEqual({
      originalText: "[doc_id 1, 2, 3]",
      ids: ["1", "2", "3"],
      startIndex: 5,
      endIndex: 21,
    });
  });

  it("should parse multiple IDs in single citation without doc_id prefix", () => {
    const text = "Text [1, 2, 3] more";
    const citations = parseCitations(text);

    expect(citations).toHaveLength(1);
    expect(citations[0]).toEqual({
      originalText: "[1, 2, 3]",
      ids: ["1", "2", "3"],
      startIndex: 5,
      endIndex: 14,
    });
  });

  it("should parse multiple separate citations", () => {
    const text = "First [resource_id 1] and second [2, 3] citation";
    const citations = parseCitations(text);

    expect(citations).toHaveLength(2);
    expect(citations[0]).toEqual({
      originalText: "[resource_id 1]",
      ids: ["1"],
      startIndex: 6,
      endIndex: 21,
    });
    expect(citations[1]).toEqual({
      originalText: "[2, 3]",
      ids: ["2", "3"],
      startIndex: 33,
      endIndex: 39,
    });
  });

  it("should handle mixed resource_id and doc_id formats", () => {
    const text = "First [resource_id 1] and second [doc_id 2]";
    const citations = parseCitations(text);

    expect(citations).toHaveLength(2);
    expect(citations[0].ids).toEqual(["1"]);
    expect(citations[1].ids).toEqual(["2"]);
  });

  it("should handle alphanumeric IDs", () => {
    const text = "Text [doc_id doc1, doc2] more";
    const citations = parseCitations(text);

    expect(citations).toHaveLength(1);
    expect(citations[0].ids).toEqual(["doc1", "doc2"]);
  });

  it("should handle UUID-like IDs", () => {
    const text = "Text [abc123-def456] more";
    const citations = parseCitations(text);

    expect(citations).toHaveLength(1);
    expect(citations[0].ids).toEqual(["abc123-def456"]);
  });

  it("should handle various whitespace patterns", () => {
    const text = "Text [doc_id   1,2,  3   ] more";
    const citations = parseCitations(text);

    expect(citations).toHaveLength(1);
    expect(citations[0].ids).toEqual(["1", "2", "3"]);
  });

  it("should handle mixed formats in same text", () => {
    const text = "[doc_id 1] and [2] and [doc_id 3, 4]";
    const citations = parseCitations(text);

    expect(citations).toHaveLength(3);
    expect(citations[0].ids).toEqual(["1"]);
    expect(citations[1].ids).toEqual(["2"]);
    expect(citations[2].ids).toEqual(["3", "4"]);
  });

  it("should return empty array for text without citations", () => {
    const text = "No citations here";
    const citations = parseCitations(text);

    expect(citations).toHaveLength(0);
  });

  it("should handle empty text", () => {
    const citations = parseCitations("");
    expect(citations).toHaveLength(0);
  });
});

describe("replaceCitations", () => {
  it("should replace citation using custom render function", () => {
    const text = "Text [doc_id 1] more";
    const result = replaceCitations(text, {
      renderCitation: (ids) => `{${ids.join(",")}}`,
    });

    expect(result).toBe("Text {1} more");
  });

  it("should replace multiple IDs in single citation", () => {
    const text = "Text [1, 2, 3] more";
    const result = replaceCitations(text, {
      renderCitation: (ids) => `{${ids.join(",")}}`,
    });

    expect(result).toBe("Text {1,2,3} more");
  });

  it("should replace multiple separate citations", () => {
    const text = "First [1] second [2]";
    const result = replaceCitations(text, {
      renderCitation: (ids) => `{${ids.join(",")}}`,
    });

    expect(result).toBe("First {1} second {2}");
  });

  it("should pass all citation IDs to render function", () => {
    const text = "First [1] second [2, 3]";
    let receivedAllIds: string[] = [];

    replaceCitations(text, {
      renderCitation: (_ids, allIds) => {
        receivedAllIds = allIds;
        return "";
      },
    });

    expect(receivedAllIds).toEqual(["1", "2", "3"]);
  });

  it("should maintain order of all citation IDs", () => {
    const text = "[5] then [3] then [1, 2]";
    let receivedAllIds: string[] = [];

    replaceCitations(text, {
      renderCitation: (_ids, allIds) => {
        receivedAllIds = allIds;
        return "";
      },
    });

    expect(receivedAllIds).toEqual(["5", "3", "1", "2"]);
  });

  it("should not include duplicate IDs in allCitationIds", () => {
    const text = "[1, 2] and [2, 3] and [1]";
    let receivedAllIds: string[] = [];

    replaceCitations(text, {
      renderCitation: (_ids, allIds) => {
        receivedAllIds = allIds;
        return "";
      },
    });

    expect(receivedAllIds).toEqual(["1", "2", "3"]);
  });

  it("should handle mixed citation formats", () => {
    const text = "[doc_id 1] and [2]";
    const result = replaceCitations(text, {
      renderCitation: (ids) => `{${ids.join(",")}}`,
    });

    expect(result).toBe("{1} and {2}");
  });
});

describe("renderAsMarkdownLinks", () => {
  it("should render single ID as markdown link", () => {
    const result = renderAsMarkdownLinks(["1"]);
    expect(result).toBe("[[1]](#hit-1)");
  });

  it("should render multiple IDs as comma-separated markdown links", () => {
    const result = renderAsMarkdownLinks(["1", "2", "3"]);
    expect(result).toBe("[[1]](#hit-1), [[2]](#hit-2), [[3]](#hit-3)");
  });

  it("should handle alphanumeric IDs", () => {
    const result = renderAsMarkdownLinks(["doc1", "doc2"]);
    expect(result).toBe("[[doc1]](#hit-doc1), [[doc2]](#hit-doc2)");
  });

  it("should handle UUID-like IDs", () => {
    const result = renderAsMarkdownLinks(["abc-123"]);
    expect(result).toBe("[[abc-123]](#hit-abc-123)");
  });
});

describe("renderAsSequentialLinks", () => {
  it("should render with sequential numbering", () => {
    const allIds = ["5", "7", "3"];
    const result = renderAsSequentialLinks(["5", "7"], allIds);
    expect(result).toBe("[[1]](#hit-5), [[2]](#hit-7)");
  });

  it("should handle single ID", () => {
    const allIds = ["5", "7", "3"];
    const result = renderAsSequentialLinks(["3"], allIds);
    expect(result).toBe("[[3]](#hit-3)");
  });

  it("should handle first citation", () => {
    const allIds = ["1", "2", "3"];
    const result = renderAsSequentialLinks(["1"], allIds);
    expect(result).toBe("[[1]](#hit-1)");
  });

  it("should handle multiple IDs with sequential numbers", () => {
    const allIds = ["a", "b", "c"];
    const result = renderAsSequentialLinks(["a", "c"], allIds);
    expect(result).toBe("[[1]](#hit-a), [[3]](#hit-c)");
  });
});

describe("integration: replaceCitations with helper functions", () => {
  it("should work with renderAsMarkdownLinks", () => {
    const text = "See [doc_id 1, 2] and [3]";
    const result = replaceCitations(text, {
      renderCitation: renderAsMarkdownLinks,
    });

    expect(result).toBe("See [[1]](#hit-1), [[2]](#hit-2) and [[3]](#hit-3)");
  });

  it("should work with renderAsSequentialLinks", () => {
    const text = "First [5] then [3, 7]";
    const result = replaceCitations(text, {
      renderCitation: renderAsSequentialLinks,
    });

    expect(result).toBe("First [[1]](#hit-5) then [[2]](#hit-3), [[3]](#hit-7)");
  });

  it("should support custom grouped rendering", () => {
    const text = "See references [1, 2, 3]";
    const result = replaceCitations(text, {
      renderCitation: (ids, allIds) => {
        const nums = ids.map((id) => allIds.indexOf(id) + 1);
        return `[${nums.join(",")}]`;
      },
    });

    expect(result).toBe("See references [1,2,3]");
  });

  it("should support superscript rendering", () => {
    const text = "Important finding [doc_id 1, 2]";
    const result = replaceCitations(text, {
      renderCitation: (ids, allIds) => {
        const nums = ids.map((id) => {
          const num = allIds.indexOf(id) + 1;
          const superscripts = ["⁰", "¹", "²", "³", "⁴", "⁵", "⁶", "⁷", "⁸", "⁹"];
          return num
            .toString()
            .split("")
            .map((d) => superscripts[parseInt(d, 10)])
            .join("");
        });
        return nums.join(",");
      },
    });

    expect(result).toBe("Important finding ¹,²");
  });
});
