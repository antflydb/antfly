import { useCallback, useEffect, useState } from "react";
import type { EvalItem, EvalSet, EvalSets } from "@/types/evals";

const STORAGE_KEY = "antfarm-eval-sets";

function generateId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

function loadFromStorage(): EvalSets {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    return stored ? JSON.parse(stored) : {};
  } catch {
    console.error("Failed to load eval sets from localStorage");
    return {};
  }
}

function saveToStorage(sets: EvalSets): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(sets));
  } catch (e) {
    console.error("Failed to save eval sets to localStorage:", e);
  }
}

export function useEvalSets() {
  const [evalSets, setEvalSets] = useState<EvalSets>(() => loadFromStorage());

  // Sync to localStorage whenever evalSets changes
  useEffect(() => {
    saveToStorage(evalSets);
  }, [evalSets]);

  const createEvalSet = useCallback(
    (name: string): EvalSet | null => {
      if (!name.trim()) return null;

      const trimmedName = name.trim();
      if (evalSets[trimmedName]) {
        console.warn(`Eval set "${trimmedName}" already exists`);
        return null;
      }

      const newSet: EvalSet = {
        name: trimmedName,
        createdAt: new Date().toISOString(),
        items: [],
      };

      setEvalSets((prev) => ({
        ...prev,
        [trimmedName]: newSet,
      }));

      return newSet;
    },
    [evalSets]
  );

  const deleteEvalSet = useCallback(
    (name: string): boolean => {
      if (!evalSets[name]) return false;

      setEvalSets((prev) => {
        const next = { ...prev };
        delete next[name];
        return next;
      });

      return true;
    },
    [evalSets]
  );

  const addItem = useCallback(
    (setName: string, question: string, referenceAnswer: string): EvalItem | null => {
      if (!evalSets[setName]) {
        console.warn(`Eval set "${setName}" does not exist`);
        return null;
      }

      const newItem: EvalItem = {
        id: generateId(),
        question: question.trim(),
        referenceAnswer: referenceAnswer.trim(),
        addedAt: new Date().toISOString(),
      };

      setEvalSets((prev) => ({
        ...prev,
        [setName]: {
          ...prev[setName],
          items: [...prev[setName].items, newItem],
        },
      }));

      return newItem;
    },
    [evalSets]
  );

  const removeItem = useCallback(
    (setName: string, itemId: string): boolean => {
      if (!evalSets[setName]) return false;

      const itemExists = evalSets[setName].items.some((item) => item.id === itemId);
      if (!itemExists) return false;

      setEvalSets((prev) => ({
        ...prev,
        [setName]: {
          ...prev[setName],
          items: prev[setName].items.filter((item) => item.id !== itemId),
        },
      }));

      return true;
    },
    [evalSets]
  );

  const getEvalSet = useCallback(
    (name: string): EvalSet | undefined => {
      return evalSets[name];
    },
    [evalSets]
  );

  const getEvalSetNames = useCallback((): string[] => {
    return Object.keys(evalSets).sort();
  }, [evalSets]);

  const exportEvalSet = useCallback(
    (name: string): string | null => {
      const set = evalSets[name];
      if (!set) return null;
      return JSON.stringify(set, null, 2);
    },
    [evalSets]
  );

  const importEvalSet = useCallback(
    (jsonString: string): { success: boolean; error?: string; name?: string } => {
      try {
        const parsed = JSON.parse(jsonString) as EvalSet;

        // Validate structure
        if (!parsed.name || typeof parsed.name !== "string") {
          return { success: false, error: "Invalid format: missing name" };
        }
        if (!Array.isArray(parsed.items)) {
          return { success: false, error: "Invalid format: missing items array" };
        }

        // Check for duplicate name
        if (evalSets[parsed.name]) {
          return { success: false, error: `Eval set "${parsed.name}" already exists` };
        }

        // Validate and normalize items
        const validItems: EvalItem[] = [];
        for (const item of parsed.items) {
          if (!item.question || !item.referenceAnswer) {
            continue; // Skip invalid items
          }
          validItems.push({
            id: item.id || generateId(),
            question: String(item.question).trim(),
            referenceAnswer: String(item.referenceAnswer).trim(),
            addedAt: item.addedAt || new Date().toISOString(),
          });
        }

        const importedSet: EvalSet = {
          name: parsed.name.trim(),
          createdAt: parsed.createdAt || new Date().toISOString(),
          items: validItems,
        };

        setEvalSets((prev) => ({
          ...prev,
          [importedSet.name]: importedSet,
        }));

        return { success: true, name: importedSet.name };
      } catch (e) {
        return { success: false, error: `Failed to parse JSON: ${e}` };
      }
    },
    [evalSets]
  );

  // Promptfoo format: array of { vars: { question, reference_answer }, metadata?: {...} }
  const importPromptfooSet = useCallback(
    (
      jsonString: string,
      name: string
    ): { success: boolean; error?: string; name?: string; count?: number } => {
      const trimmedName = name.trim();
      if (!trimmedName) {
        return { success: false, error: "Name is required" };
      }

      if (evalSets[trimmedName]) {
        return { success: false, error: `Eval set "${trimmedName}" already exists` };
      }

      try {
        const parsed = JSON.parse(jsonString);

        // Validate it's an array
        if (!Array.isArray(parsed)) {
          return { success: false, error: "Invalid promptfoo format: expected an array" };
        }

        // Extract items from promptfoo format
        const validItems: EvalItem[] = [];
        for (const entry of parsed) {
          const vars = entry.vars;
          if (!vars || typeof vars !== "object") {
            continue; // Skip entries without vars
          }

          const question = vars.question;
          const referenceAnswer = vars.reference_answer;

          if (!question || !referenceAnswer) {
            continue; // Skip entries without question or reference_answer
          }

          validItems.push({
            id: generateId(),
            question: String(question).trim(),
            referenceAnswer: String(referenceAnswer).trim(),
            addedAt: new Date().toISOString(),
          });
        }

        if (validItems.length === 0) {
          return {
            success: false,
            error: "No valid items found. Expected format: [{vars: {question, reference_answer}}]",
          };
        }

        const importedSet: EvalSet = {
          name: trimmedName,
          createdAt: new Date().toISOString(),
          items: validItems,
        };

        setEvalSets((prev) => ({
          ...prev,
          [importedSet.name]: importedSet,
        }));

        return { success: true, name: importedSet.name, count: validItems.length };
      } catch (e) {
        return { success: false, error: `Failed to parse JSON: ${e}` };
      }
    },
    [evalSets]
  );

  return {
    evalSets,
    createEvalSet,
    deleteEvalSet,
    addItem,
    removeItem,
    getEvalSet,
    getEvalSetNames,
    exportEvalSet,
    importEvalSet,
    importPromptfooSet,
  };
}
