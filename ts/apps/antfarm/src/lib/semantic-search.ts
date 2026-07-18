/**
 * Semantic search for the command palette.
 * Uses pre-computed embeddings and Antfly inference's /ai/v1/embed for query embedding.
 */

import { type CommandMetadata, commandIndex } from "@/data/commands";

export interface SemanticResult {
  item: CommandMetadata;
  score: number;
}

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
 * Uses Antfly inference's /ai/v1/embed endpoint to embed the query, then
 * computes cosine similarity against pre-embedded command vectors.
 *
 * @param query - The user's search query
 * @param embedUrl - Connection-aware embedding endpoint
 * @param limit - Maximum number of results to return (default: 3)
 * @returns Promise resolving to semantic search results sorted by score
 */
export async function semanticSearch(
  query: string,
  embedUrl: string,
  limit = 3
): Promise<SemanticResult[]> {
  try {
    // Get query embedding from Antfly inference.
    // Use the same model as the pre-computed embeddings
    const request = await fetch(embedUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model: commandIndex.model, input: query }),
    });
    if (!request.ok) return [];
    const response = await request.json();
    const queryVec = response.data[0]?.embedding;
    if (!Array.isArray(queryVec)) {
      return [];
    }

    // Score all commands using cosine similarity
    const scored = commandIndex.commands.map((cmd) => ({
      item: {
        id: cmd.id,
        type: cmd.type,
        group: cmd.group,
        label: cmd.label,
        description: cmd.description,
        href: cmd.href,
        action: cmd.action,
        icon: cmd.icon,
        product: cmd.product,
        adminOnly: cmd.adminOnly,
      },
      score: cosineSimilarity(queryVec, cmd.embedding),
    }));

    // Return top-k results sorted by score (highest first)
    return scored.sort((a, b) => b.score - a.score).slice(0, limit);
  } catch (e) {
    // Graceful degradation - Antfly inference unavailable or model not loaded.
    console.error("Semantic search failed:", e);
    return [];
  }
}

/**
 * Gets all command items (for reference/debugging).
 */
export function getAllCommandItems(): CommandMetadata[] {
  return commandIndex.commands.map((cmd) => ({
    id: cmd.id,
    type: cmd.type,
    group: cmd.group,
    label: cmd.label,
    description: cmd.description,
    href: cmd.href,
    action: cmd.action,
    icon: cmd.icon,
    product: cmd.product,
    adminOnly: cmd.adminOnly,
  }));
}
