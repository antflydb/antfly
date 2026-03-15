/**
 * Citation parsing and rendering utilities for RAG responses.
 *
 * These utilities handle citations in the format:
 * - `[resource_id <ID>]` or `[resource_id <ID1>, <ID2>]` (backend-instructed format)
 * - `[<ID>]` or `[<ID1>, <ID2>]` (shorthand format)
 *
 * IDs can be any characters except closing brackets.
 *
 * @example
 * ```typescript
 * // Parse citations to get structured data
 * const citations = parseCitations("Some text [resource_id 1, 2] more text [3]");
 * // Returns: [
 * //   { originalText: "[resource_id 1, 2]", ids: ["1", "2"], startIndex: 10, endIndex: 26 },
 * //   { originalText: "[3]", ids: ["3"], startIndex: 37, endIndex: 40 }
 * // ]
 *
 * // Replace citations with custom rendering
 * const result = replaceCitations(text, {
 *   renderCitation: (ids) => renderAsMarkdownLinks(ids)
 * });
 *
 * // Sequential numbering
 * const sequential = replaceCitations(text, {
 *   renderCitation: (ids, allIds) => renderAsSequentialLinks(ids, allIds)
 * });
 * ```
 */

/**
 * Represents a parsed citation reference in text.
 */
export interface Citation {
  /** The original citation text as it appears in the source (e.g., "[resource_id 1, 2, 3]") */
  originalText: string;
  /** Array of resource IDs referenced in this citation (e.g., ["1", "2", "3"]) */
  ids: string[];
  /** Starting character position of the citation in the original text */
  startIndex: number;
  /** Ending character position of the citation in the original text */
  endIndex: number;
}

/**
 * Options for customizing how citations are rendered.
 */
export interface CitationRenderOptions {
  /**
   * Function that takes citation IDs and returns the rendered string.
   *
   * @param ids - The resource IDs for this specific citation
   * @param allCitationIds - All unique citation IDs in order of first appearance (for sequential numbering)
   * @returns The rendered string to replace the citation
   *
   * @example
   * ```typescript
   * // Render as markdown links with direct IDs
   * renderCitation: (ids) => ids.map(id => `[[${id}]](#hit-${id})`).join(', ')
   *
   * // Render with sequential numbering
   * renderCitation: (ids, allIds) => {
   *   return ids.map(id => {
   *     const num = allIds.indexOf(id) + 1;
   *     return `[[${num}]](#hit-${id})`;
   *   }).join(', ');
   * }
   *
   * // Grouped display like [1,2,3]
   * renderCitation: (ids, allIds) => {
   *   const nums = ids.map(id => allIds.indexOf(id) + 1);
   *   return `[${nums.join(',')}]`;
   * }
   * ```
   */
  renderCitation: (ids: string[], allCitationIds: string[]) => string;
}

/**
 * Regular expression for matching citation patterns.
 * Matches both `[resource_id X]` and `[X]` formats with comma-separated IDs.
 * Also supports legacy `[doc_id X]` format for backward compatibility.
 */
const CITATION_REGEX = /\[(?:(?:resource_id|doc_id)\s+)?([^\]]+)\]/g;

/**
 * Extract all citation IDs from text in order of first appearance.
 *
 * @param text - The text to extract citation IDs from
 * @returns Array of unique citation IDs in order of first appearance
 */
function extractAllCitationIds(text: string): string[] {
  const allIds: string[] = [];
  const citations = parseCitations(text);

  for (const citation of citations) {
    for (const id of citation.ids) {
      if (!allIds.includes(id)) {
        allIds.push(id);
      }
    }
  }

  return allIds;
}

/**
 * Parse all citations in text and return structured citation data.
 *
 * @param text - The text containing citations
 * @returns Array of parsed citation objects
 *
 * @example
 * ```typescript
 * const text = "The system uses consensus [resource_id 1] and replication [2, 3].";
 * const citations = parseCitations(text);
 * // Returns:
 * // [
 * //   { originalText: "[resource_id 1]", ids: ["1"], startIndex: 27, endIndex: 42 },
 * //   { originalText: "[2, 3]", ids: ["2", "3"], startIndex: 61, endIndex: 67 }
 * // ]
 * ```
 */
export function parseCitations(text: string): Citation[] {
  const citations: Citation[] = [];
  const regex = new RegExp(CITATION_REGEX);

  for (const match of text.matchAll(regex)) {
    const originalText = match[0];
    const idsString = match[1];

    // Split by comma and trim whitespace
    const ids = idsString
      .split(",")
      .map((id) => id.trim())
      .filter((id) => id.length > 0);

    citations.push({
      originalText,
      ids,
      startIndex: match.index ?? 0,
      endIndex: (match.index ?? 0) + originalText.length,
    });
  }

  return citations;
}

/**
 * Replace all citations in text using a custom render function.
 *
 * @param text - The text containing citations
 * @param options - Options controlling how citations are rendered
 * @returns The text with citations replaced by rendered output
 *
 * @example
 * ```typescript
 * const text = "See docs [resource_id 1, 2] and [3].";
 * const result = replaceCitations(text, {
 *   renderCitation: (ids) => ids.map(id => `[[${id}]](#hit-${id})`).join(', ')
 * });
 * // Result: "See docs [[1]](#hit-1), [[2]](#hit-2) and [[3]](#hit-3)."
 * ```
 */
export function replaceCitations(text: string, options: CitationRenderOptions): string {
  const allCitationIds = extractAllCitationIds(text);

  return text.replace(CITATION_REGEX, (_match, idsString) => {
    // Split by comma and trim whitespace
    const ids = idsString
      .split(",")
      .map((id: string) => id.trim())
      .filter((id: string) => id.length > 0);

    return options.renderCitation(ids, allCitationIds);
  });
}

/**
 * Helper function to render citations as markdown links using the actual resource IDs.
 *
 * @param ids - The resource IDs to render
 * @returns Comma-separated markdown links
 *
 * @example
 * ```typescript
 * renderAsMarkdownLinks(["1", "2", "3"])
 * // Returns: "[[1]](#hit-1), [[2]](#hit-2), [[3]](#hit-3)"
 * ```
 */
export function renderAsMarkdownLinks(ids: string[]): string {
  return ids.map((id) => `[[${id}]](#hit-${id})`).join(", ");
}

/**
 * Helper function to render citations as markdown links with sequential numbering.
 * The display number is based on the order of first appearance across all citations.
 *
 * @param ids - The resource IDs for this citation
 * @param allCitationIds - All unique citation IDs in order of first appearance
 * @returns Comma-separated markdown links with sequential numbers
 *
 * @example
 * ```typescript
 * // If allCitationIds is ["5", "7", "3"]
 * renderAsSequentialLinks(["5", "7"], ["5", "7", "3"])
 * // Returns: "[[1]](#hit-5), [[2]](#hit-7)"
 * ```
 */
export function renderAsSequentialLinks(ids: string[], allCitationIds: string[]): string {
  return ids
    .map((id) => {
      const sequentialNum = allCitationIds.indexOf(id) + 1;
      return `[[${sequentialNum}]](#hit-${id})`;
    })
    .join(", ");
}

/**
 * Extract all cited resource IDs from a summary text.
 * Useful for filtering search results to only show resources that were cited.
 *
 * @param summary - The RAG summary text containing citations
 * @returns Array of unique resource IDs in order of first appearance
 *
 * @example
 * ```typescript
 * const summary = "The system uses [resource_id 1] and [2, 3].";
 * const citedIds = getCitedResourceIds(summary);
 * // Returns: ["1", "2", "3"]
 *
 * // Filter hits to only cited resources
 * const citedHits = hits.filter(hit => citedIds.includes(hit._id));
 * ```
 */
export function getCitedResourceIds(summary: string): string[] {
  const citations = parseCitations(summary);
  const uniqueIds = new Set<string>();

  for (const citation of citations) {
    for (const id of citation.ids) {
      uniqueIds.add(id);
    }
  }

  return Array.from(uniqueIds);
}

/**
 * @deprecated Use getCitedResourceIds instead. This function name is deprecated.
 * Extract all cited resource IDs from a summary text.
 */
export function getCitedDocumentIds(summary: string): string[] {
  return getCitedResourceIds(summary);
}
