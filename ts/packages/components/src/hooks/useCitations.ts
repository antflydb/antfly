import { useCallback } from "react";
import {
  type Citation,
  type CitationRenderOptions,
  getCitedResourceIds,
  parseCitations,
  renderAsMarkdownLinks,
  renderAsSequentialLinks,
  replaceCitations,
} from "../citations";

/**
 * Hook for working with citations in RAG/Answer Agent responses.
 *
 * Provides utilities for parsing citations in text (e.g., `[resource_id 1, 2]` or `[1, 2]`),
 * replacing them with custom rendering, and extracting cited resource IDs.
 *
 * @returns Object with citation utility functions
 *
 * @example
 * ```typescript
 * const { parseCitations, highlightCitations, extractCitationUrls } = useCitations();
 *
 * // Parse citations from answer text
 * const citations = parseCitations("See docs [resource_id 1, 2]");
 * // Returns: [{ originalText: "[resource_id 1, 2]", ids: ["1", "2"], startIndex: 9, endIndex: 27 }]
 *
 * // Replace with markdown links
 * const rendered = highlightCitations(text, {
 *   renderCitation: (ids) => ids.map(id => `[[${id}]](#hit-${id})`).join(', ')
 * });
 *
 * // Get IDs of cited resources
 * const citedIds = extractCitationUrls(answer);
 * const citedHits = hits.filter(hit => citedIds.includes(hit._id));
 * ```
 */
export function useCitations() {
  /**
   * Parse all citations in text and return structured citation data.
   *
   * @param text - The text containing citations
   * @returns Array of parsed citation objects
   */
  const parse = useCallback((text: string): Citation[] => {
    return parseCitations(text);
  }, []);

  /**
   * Replace all citations in text using a custom render function.
   *
   * @param text - The text containing citations
   * @param options - Options controlling how citations are rendered
   * @returns The text with citations replaced by rendered output
   */
  const highlightCitations = useCallback((text: string, options: CitationRenderOptions): string => {
    return replaceCitations(text, options);
  }, []);

  /**
   * Extract all cited resource IDs from text in order of first appearance.
   *
   * @param text - The text containing citations
   * @returns Array of unique resource IDs
   */
  const extractCitationUrls = useCallback((text: string): string[] => {
    return getCitedResourceIds(text);
  }, []);

  /**
   * Render citations as markdown links using the actual resource IDs.
   *
   * @param ids - The resource IDs to render
   * @returns Comma-separated markdown links
   */
  const renderAsMarkdown = useCallback((ids: string[]): string => {
    return renderAsMarkdownLinks(ids);
  }, []);

  /**
   * Render citations as markdown links with sequential numbering.
   *
   * @param ids - The resource IDs for this citation
   * @param allCitationIds - All unique citation IDs in order of first appearance
   * @returns Comma-separated markdown links with sequential numbers
   */
  const renderAsSequential = useCallback((ids: string[], allCitationIds: string[]): string => {
    return renderAsSequentialLinks(ids, allCitationIds);
  }, []);

  return {
    parseCitations: parse,
    highlightCitations,
    extractCitationUrls,
    renderAsMarkdown,
    renderAsSequential,
  };
}
