import { test } from "node:test";
import assert from "node:assert/strict";
import { normalizeSpan } from "../server/governance-spans.ts";

test("native UTF-8 spans preserve exact citations after non-ASCII text", () => {
  const text = "📜 Café Oregon Department";
  const start = Buffer.byteLength("📜 Café ");
  const entity = normalizeSpan(
    text,
    { text: "Oregon Department", start, end: Buffer.byteLength(text) },
    "utf8_bytes",
  );
  assert.equal(entity.start, "📜 Café ".length);
  assert.equal(text.slice(entity.start, entity.end), entity.text);
  assert.deepEqual(normalizeSpan(text, entity, "utf16_codeunits"), entity);
});
test("native spans reject broken encoding, invented text and invalid bounds", () => {
  for (const span of [
    { text: "📜", start: 1, end: 4 },
    { text: "Wrong", start: 5, end: 11 },
    { text: "Oregon", start: -6, end: 11 },
    { text: "", start: 0, end: 0 },
  ])
    assert.throws(() => normalizeSpan("📜 Oregon", span, "utf8_bytes"));
});
