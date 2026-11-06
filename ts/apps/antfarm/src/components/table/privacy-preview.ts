export interface SourceFinding {
  start: number;
  end: number;
  category: string;
}
export interface Highlight {
  text: string;
  categories: string[];
}

/** Server offsets are half-open UTF-8 bytes, not JavaScript string indices. */
export function privacyHighlights(source: string, findings: SourceFinding[]): Highlight[] {
  const bytes = new TextEncoder().encode(source);
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const sorted = [...findings].sort((a, b) => a.start - b.start || b.end - a.end);
  const parts: Highlight[] = [];
  let cursor = 0;
  for (let i = 0; i < sorted.length; ) {
    const first = sorted[i];
    if (
      !Number.isInteger(first.start) ||
      !Number.isInteger(first.end) ||
      first.start < cursor ||
      first.end <= first.start ||
      first.end > bytes.length
    )
      throw new Error("Invalid privacy source span");
    const categories = new Set([first.category]);
    let end = first.end;
    i += 1;
    while (i < sorted.length && sorted[i].start < end) {
      const overlap = sorted[i++];
      if (
        !Number.isInteger(overlap.start) ||
        !Number.isInteger(overlap.end) ||
        overlap.end <= overlap.start ||
        overlap.end > bytes.length
      )
        throw new Error("Invalid privacy source span");
      end = Math.max(end, overlap.end);
      categories.add(overlap.category);
    }
    if (cursor < first.start)
      parts.push({ text: decoder.decode(bytes.subarray(cursor, first.start)), categories: [] });
    parts.push({
      text: decoder.decode(bytes.subarray(first.start, end)),
      categories: [...categories],
    });
    cursor = end;
  }
  if (cursor < bytes.length)
    parts.push({ text: decoder.decode(bytes.subarray(cursor)), categories: [] });
  return parts;
}
