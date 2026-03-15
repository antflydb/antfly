/**
 * Semantic search for the command palette.
 * Uses pre-computed embeddings and Termite's /api/embed for query embedding.
 */

import type { TermiteClient } from "@antfly/termite-sdk";
import commandIndex from "@/data/command-index.json";

export interface CommandItem {
  id: string;
  type: "navigation" | "action";
  label: string;
  description: string;
  href?: string;
  action?: string;
  icon: string;
}

export interface SemanticResult {
  item: CommandItem;
  score: number;
}

// Type for the command index JSON structure
interface CommandIndexEntry {
  id: string;
  type: string;
  label: string;
  description: string;
  href?: string;
  action?: string;
  icon: string;
  embedding: number[];
}

interface CommandIndexData {
  model: string;
  dimension: number;
  commands: CommandIndexEntry[];
}

// Cast the imported JSON to our type
const index = commandIndex as CommandIndexData;

/**
 * Compute cosine similarity between two vectors.
 * Returns a value between -1 and 1, where 1 means identical direction.
 */
function cosineSimilarity(a: number[], b: number[]): number {
  if (a.length !== b.length) {
    throw new Error(`Vector length mismatch: ${a.length} vs ${b.length}`);
  }

  let dot = 0;
  let normA = 0;
  let normB = 0;

  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }

  const denominator = Math.sqrt(normA) * Math.sqrt(normB);
  if (denominator === 0) return 0;

  return dot / denominator;
}

/**
 * Performs semantic search against the command palette items.
 * Uses Termite's /api/embed endpoint to embed the query, then
 * computes cosine similarity against pre-embedded command vectors.
 *
 * @param query - The user's search query
 * @param termiteClient - TermiteClient instance for embedding
 * @param limit - Maximum number of results to return (default: 3)
 * @returns Promise resolving to semantic search results sorted by score
 */
export async function semanticSearch(
  query: string,
  termiteClient: TermiteClient,
  limit = 3
): Promise<SemanticResult[]> {
  try {
    // Get query embedding from Termite
    // Use the same model as the pre-computed embeddings
    const response = await termiteClient.embed(index.model, query);
    if (!response.embeddings?.[0]) {
      return [];
    }
    const queryVec = response.embeddings[0];

    // Score all commands using cosine similarity
    const scored = index.commands.map((cmd) => ({
      item: {
        id: cmd.id,
        type: cmd.type as "navigation" | "action",
        label: cmd.label,
        description: cmd.description,
        href: cmd.href,
        action: cmd.action,
        icon: cmd.icon,
      },
      score: cosineSimilarity(queryVec, cmd.embedding),
    }));

    // Return top-k results sorted by score (highest first)
    return scored.sort((a, b) => b.score - a.score).slice(0, limit);
  } catch (e) {
    // Graceful degradation - Termite unavailable or model not loaded
    console.error("Semantic search failed:", e);
    return [];
  }
}

/**
 * Gets all command items (for reference/debugging).
 */
export function getAllCommandItems(): CommandItem[] {
  return index.commands.map((cmd) => ({
    id: cmd.id,
    type: cmd.type as "navigation" | "action",
    label: cmd.label,
    description: cmd.description,
    href: cmd.href,
    action: cmd.action,
    icon: cmd.icon,
  }));
}
