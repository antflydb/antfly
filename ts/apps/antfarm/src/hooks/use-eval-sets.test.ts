/**
 * Unit tests for the useEvalSets hook
 */
import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useEvalSets } from "./use-eval-sets";

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {};
  return {
    getItem: vi.fn((key: string) => store[key] || null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value;
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key];
    }),
    clear: vi.fn(() => {
      store = {};
    }),
  };
})();

Object.defineProperty(window, "localStorage", { value: localStorageMock });

describe("useEvalSets", () => {
  beforeEach(() => {
    localStorageMock.clear();
    vi.clearAllMocks();
  });

  afterEach(() => {
    localStorageMock.clear();
  });

  describe("createEvalSet", () => {
    it("should create a new eval set", () => {
      const { result } = renderHook(() => useEvalSets());

      act(() => {
        result.current.createEvalSet("Test Set");
      });

      expect(result.current.evalSets["Test Set"]).toBeDefined();
      expect(result.current.evalSets["Test Set"].name).toBe("Test Set");
      expect(result.current.evalSets["Test Set"].items).toEqual([]);
    });

    it("should return null for duplicate set name", () => {
      const { result } = renderHook(() => useEvalSets());

      act(() => {
        result.current.createEvalSet("Test Set");
      });

      let duplicateResult!: ReturnType<typeof result.current.createEvalSet>;
      act(() => {
        duplicateResult = result.current.createEvalSet("Test Set");
      });

      expect(duplicateResult).toBeNull();
    });

    it("should return null for empty name", () => {
      const { result } = renderHook(() => useEvalSets());

      let emptyResult!: ReturnType<typeof result.current.createEvalSet>;
      act(() => {
        emptyResult = result.current.createEvalSet("   ");
      });

      expect(emptyResult).toBeNull();
    });
  });

  describe("addItem", () => {
    it("should add an item to an existing eval set", () => {
      const { result } = renderHook(() => useEvalSets());

      act(() => {
        result.current.createEvalSet("Test Set");
      });

      act(() => {
        result.current.addItem("Test Set", "What is 2+2?", "4");
      });

      const items = result.current.evalSets["Test Set"].items;
      expect(items).toHaveLength(1);
      expect(items[0].question).toBe("What is 2+2?");
      expect(items[0].referenceAnswer).toBe("4");
      expect(items[0].id).toBeDefined();
    });

    it("should return null when adding to non-existent set", () => {
      const { result } = renderHook(() => useEvalSets());

      let addResult!: ReturnType<typeof result.current.addItem>;
      act(() => {
        addResult = result.current.addItem("Non-existent", "Q", "A");
      });

      expect(addResult).toBeNull();
    });
  });

  describe("removeItem", () => {
    it("should remove an item from an eval set", () => {
      const { result } = renderHook(() => useEvalSets());

      act(() => {
        result.current.createEvalSet("Test Set");
      });

      act(() => {
        result.current.addItem("Test Set", "Q1", "A1");
      });

      const itemId = result.current.evalSets["Test Set"].items[0].id;

      act(() => {
        result.current.removeItem("Test Set", itemId);
      });

      expect(result.current.evalSets["Test Set"].items).toHaveLength(0);
    });

    it("should return false for non-existent item", () => {
      const { result } = renderHook(() => useEvalSets());

      act(() => {
        result.current.createEvalSet("Test Set");
      });

      let removeResult!: boolean;
      act(() => {
        removeResult = result.current.removeItem("Test Set", "fake-id");
      });

      expect(removeResult).toBe(false);
    });
  });

  describe("deleteEvalSet", () => {
    it("should delete an eval set", () => {
      const { result } = renderHook(() => useEvalSets());

      act(() => {
        result.current.createEvalSet("Test Set");
      });

      expect(result.current.evalSets["Test Set"]).toBeDefined();

      act(() => {
        result.current.deleteEvalSet("Test Set");
      });

      expect(result.current.evalSets["Test Set"]).toBeUndefined();
    });

    it("should return false for non-existent set", () => {
      const { result } = renderHook(() => useEvalSets());

      let deleteResult!: boolean;
      act(() => {
        deleteResult = result.current.deleteEvalSet("Non-existent");
      });

      expect(deleteResult).toBe(false);
    });
  });

  describe("getEvalSetNames", () => {
    it("should return sorted list of set names", () => {
      const { result } = renderHook(() => useEvalSets());

      act(() => {
        result.current.createEvalSet("Zebra");
        result.current.createEvalSet("Alpha");
        result.current.createEvalSet("Middle");
      });

      expect(result.current.getEvalSetNames()).toEqual(["Alpha", "Middle", "Zebra"]);
    });
  });

  describe("exportEvalSet", () => {
    it("should export eval set as JSON string", () => {
      const { result } = renderHook(() => useEvalSets());

      act(() => {
        result.current.createEvalSet("Test Set");
      });

      act(() => {
        result.current.addItem("Test Set", "Q1", "A1");
      });

      const exported = result.current.exportEvalSet("Test Set");
      expect(exported).toBeDefined();

      if (!exported) throw new Error("expected exported to be defined");
      const parsed = JSON.parse(exported);
      expect(parsed.name).toBe("Test Set");
      expect(parsed.items).toHaveLength(1);
      expect(parsed.items[0].question).toBe("Q1");
    });

    it("should return null for non-existent set", () => {
      const { result } = renderHook(() => useEvalSets());

      expect(result.current.exportEvalSet("Non-existent")).toBeNull();
    });
  });

  describe("importEvalSet", () => {
    it("should import a valid eval set", () => {
      const { result } = renderHook(() => useEvalSets());

      const importData = JSON.stringify({
        name: "Imported Set",
        createdAt: new Date().toISOString(),
        items: [
          { id: "1", question: "Q1", referenceAnswer: "A1", addedAt: new Date().toISOString() },
        ],
      });

      let importResult!: ReturnType<typeof result.current.importEvalSet>;
      act(() => {
        importResult = result.current.importEvalSet(importData);
      });

      expect(importResult?.success).toBe(true);
      expect(importResult?.name).toBe("Imported Set");
      expect(result.current.evalSets["Imported Set"]).toBeDefined();
    });

    it("should fail for duplicate name", () => {
      const { result } = renderHook(() => useEvalSets());

      act(() => {
        result.current.createEvalSet("Existing");
      });

      const importData = JSON.stringify({
        name: "Existing",
        items: [],
      });

      let importResult!: ReturnType<typeof result.current.importEvalSet>;
      act(() => {
        importResult = result.current.importEvalSet(importData);
      });

      expect(importResult?.success).toBe(false);
      expect(importResult?.error).toContain("already exists");
    });

    it("should fail for invalid JSON", () => {
      const { result } = renderHook(() => useEvalSets());

      let importResult!: ReturnType<typeof result.current.importEvalSet>;
      act(() => {
        importResult = result.current.importEvalSet("not valid json");
      });

      expect(importResult?.success).toBe(false);
      expect(importResult?.error).toContain("Failed to parse");
    });

    it("should fail for missing name", () => {
      const { result } = renderHook(() => useEvalSets());

      const importData = JSON.stringify({ items: [] });

      let importResult!: ReturnType<typeof result.current.importEvalSet>;
      act(() => {
        importResult = result.current.importEvalSet(importData);
      });

      expect(importResult?.success).toBe(false);
      expect(importResult?.error).toContain("missing name");
    });
  });

  describe("importPromptfooSet", () => {
    it("should import a valid promptfoo format dataset", () => {
      const { result } = renderHook(() => useEvalSets());

      const promptfooData = JSON.stringify([
        {
          vars: {
            question: "What is the capital of France?",
            reference_answer: "Paris is the capital of France.",
          },
          metadata: { category: "geography" },
        },
        {
          vars: {
            question: "When was the Eiffel Tower built?",
            reference_answer: "Between 1887 and 1889.",
          },
        },
      ]);

      let importResult!: ReturnType<typeof result.current.importPromptfooSet>;
      act(() => {
        importResult = result.current.importPromptfooSet(promptfooData, "My Promptfoo Set");
      });

      expect(importResult?.success).toBe(true);
      expect(importResult?.name).toBe("My Promptfoo Set");
      expect(importResult?.count).toBe(2);
      expect(result.current.evalSets["My Promptfoo Set"]).toBeDefined();
      expect(result.current.evalSets["My Promptfoo Set"].items).toHaveLength(2);
      expect(result.current.evalSets["My Promptfoo Set"].items[0].question).toBe(
        "What is the capital of France?"
      );
      expect(result.current.evalSets["My Promptfoo Set"].items[0].referenceAnswer).toBe(
        "Paris is the capital of France."
      );
    });

    it("should fail when name is empty", () => {
      const { result } = renderHook(() => useEvalSets());

      const promptfooData = JSON.stringify([{ vars: { question: "Q1", reference_answer: "A1" } }]);

      let importResult!: ReturnType<typeof result.current.importPromptfooSet>;
      act(() => {
        importResult = result.current.importPromptfooSet(promptfooData, "   ");
      });

      expect(importResult?.success).toBe(false);
      expect(importResult?.error).toContain("required");
    });

    it("should fail for duplicate name", () => {
      const { result } = renderHook(() => useEvalSets());

      act(() => {
        result.current.createEvalSet("Existing Set");
      });

      const promptfooData = JSON.stringify([{ vars: { question: "Q1", reference_answer: "A1" } }]);

      let importResult!: ReturnType<typeof result.current.importPromptfooSet>;
      act(() => {
        importResult = result.current.importPromptfooSet(promptfooData, "Existing Set");
      });

      expect(importResult?.success).toBe(false);
      expect(importResult?.error).toContain("already exists");
    });

    it("should fail for non-array JSON", () => {
      const { result } = renderHook(() => useEvalSets());

      let importResult!: ReturnType<typeof result.current.importPromptfooSet>;
      act(() => {
        importResult = result.current.importPromptfooSet('{"not": "an array"}', "Test Set");
      });

      expect(importResult?.success).toBe(false);
      expect(importResult?.error).toContain("expected an array");
    });

    it("should fail for invalid JSON", () => {
      const { result } = renderHook(() => useEvalSets());

      let importResult!: ReturnType<typeof result.current.importPromptfooSet>;
      act(() => {
        importResult = result.current.importPromptfooSet("not valid json", "Test Set");
      });

      expect(importResult?.success).toBe(false);
      expect(importResult?.error).toContain("Failed to parse");
    });

    it("should skip entries without vars or missing fields", () => {
      const { result } = renderHook(() => useEvalSets());

      const promptfooData = JSON.stringify([
        { vars: { question: "Valid Q", reference_answer: "Valid A" } },
        { vars: { question: "Missing answer" } }, // No reference_answer
        { vars: { reference_answer: "Missing question" } }, // No question
        { noVars: true }, // No vars object
      ]);

      let importResult!: ReturnType<typeof result.current.importPromptfooSet>;
      act(() => {
        importResult = result.current.importPromptfooSet(promptfooData, "Partial Set");
      });

      expect(importResult?.success).toBe(true);
      expect(importResult?.count).toBe(1);
      expect(result.current.evalSets["Partial Set"].items).toHaveLength(1);
    });

    it("should fail when no valid items are found", () => {
      const { result } = renderHook(() => useEvalSets());

      const promptfooData = JSON.stringify([
        { vars: { question: "Missing answer" } },
        { noVars: true },
      ]);

      let importResult!: ReturnType<typeof result.current.importPromptfooSet>;
      act(() => {
        importResult = result.current.importPromptfooSet(promptfooData, "Empty Set");
      });

      expect(importResult?.success).toBe(false);
      expect(importResult?.error).toContain("No valid items found");
    });
  });

  describe("localStorage persistence", () => {
    it("should save to localStorage when state changes", () => {
      const { result } = renderHook(() => useEvalSets());

      act(() => {
        result.current.createEvalSet("Persistent Set");
      });

      // Check localStorage was called with the correct key
      expect(localStorageMock.setItem).toHaveBeenCalledWith(
        "antfarm-eval-sets",
        expect.stringContaining("Persistent Set")
      );
    });

    it("should load from localStorage on init", () => {
      // Pre-populate localStorage
      const existingData = {
        "Preloaded Set": {
          name: "Preloaded Set",
          createdAt: new Date().toISOString(),
          items: [],
        },
      };
      localStorageMock.getItem.mockReturnValueOnce(JSON.stringify(existingData));

      const { result } = renderHook(() => useEvalSets());

      expect(result.current.evalSets["Preloaded Set"]).toBeDefined();
    });
  });
});
