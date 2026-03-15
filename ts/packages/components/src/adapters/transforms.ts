import type { GenerationConfidence, QueryHit } from "@antfly/sdk";

/** Props shape for a single source item. */
export interface SourceItemProps {
  href: string | undefined;
  title: string;
  key: string;
}

/** Map an Antfly QueryHit to the props shape AI Elements Source expects. */
export function hitToSourceProps(hit: QueryHit, index: number): SourceItemProps {
  const source = hit._source as Record<string, unknown> | undefined;
  return {
    key: hit._id ?? String(index),
    href:
      typeof source?.url === "string"
        ? source.url
        : typeof source?.link === "string"
          ? source.link
          : undefined,
    title:
      typeof source?.title === "string"
        ? source.title
        : typeof source?.name === "string"
          ? source.name
          : (hit._id ?? `Result ${index + 1}`),
  };
}

/** PromptInputSubmit status values. */
export type PromptInputStatus = "awaiting-message" | "submitted" | "streaming";

/**
 * Map streaming state to PromptInputSubmit status.
 *
 * @param isStreaming - Whether the turn is actively streaming
 * @param hasContent - Whether the assistant has produced any content yet.
 *   When true during streaming, returns "streaming". When false during
 *   streaming, returns "submitted" (request in-flight, no tokens yet).
 */
export function turnToStatus(isStreaming: boolean, hasContent = true): PromptInputStatus {
  if (!isStreaming) return "awaiting-message";
  return hasContent ? "streaming" : "submitted";
}

/** Format a confidence value as a human-readable label. */
export function confidenceLabel(confidence: GenerationConfidence): string {
  const pct = Math.round(confidence.generation_confidence * 100);
  return `${pct}% confident`;
}
