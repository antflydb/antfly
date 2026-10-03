/** Normalize native extraction spans into the UTF-16 units used by citations. */
export function normalizeSpan<
  T extends { text: string; start: number; end: number },
>(source: string, span: T, unit: "utf8_bytes" | "utf16_codeunits"): T {
  const bytes = Buffer.from(source, "utf8");
  const length = unit === "utf8_bytes" ? bytes.length : source.length;
  if (
    !Number.isInteger(span.start) ||
    !Number.isInteger(span.end) ||
    span.start < 0 ||
    span.end <= span.start ||
    span.end > length
  )
    throw new Error("Invalid extraction span bounds");
  let { start, end } = span;
  if (unit === "utf8_bytes") {
    const decoder = new TextDecoder("utf-8", { fatal: true });
    if (decoder.decode(bytes.subarray(start, end)) !== span.text)
      throw new Error("Extraction span does not match source text");
    start = decoder.decode(bytes.subarray(0, start)).length;
    end = decoder.decode(bytes.subarray(0, end)).length;
  }
  if (source.slice(start, end) !== span.text)
    throw new Error("Extraction span does not match source text");
  return { ...span, start, end };
}
