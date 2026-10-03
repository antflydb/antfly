import { test } from "node:test";
import assert from "node:assert/strict";
import { validateGeneratedBrief } from "../server/governance-agent.ts";
import { textQuality } from "../server/governance-extraction.ts";
const passage = {
  id: "source-p1-0",
  document_id: "source",
  page: 1,
  start: 0,
  end: 17,
  text: "Source statement.",
};
const sources = new Map([[passage.id, passage]]);
test("explicit contrary citations cannot be labeled settled support", () => {
  const other = { ...passage, id: "other", text: "Contrary statement." };
  const answer = validateGeneratedBrief(
    JSON.stringify({
      status: "source-supported",
      claims: [
        {
          text: "Sources disagree",
          quote: passage.text,
          citations: [passage.id],
          contrary: [other.id],
        },
      ],
      limitation: "Review both versions",
    }),
    new Map([...sources, [other.id, other]]),
    [],
  );
  assert.equal(answer.status, "conflicting-sources");
});
test("rejects invented quotes even when they cite a retrieved passage", () => {
  const answer = validateGeneratedBrief(
    JSON.stringify({
      status: "source-supported",
      claims: [
        {
          text: "A claim",
          quote: "An invented source quotation",
          citations: [passage.id],
        },
      ],
      limitation: "Unreviewed",
    }),
    sources,
    [],
  );
  assert.equal(answer.citation_validation, "rejected");
});
test("rejects fabricated citations and uncited supported answers", () => {
  for (const citations of [["invented"], []]) {
    const brief = validateGeneratedBrief(
      JSON.stringify({
        status: "source-supported",
        claims: [{ text: "A claim", quote: passage.text, citations }],
        limitation: "Unreviewed",
      }),
      sources,
      [],
    );
    assert.equal(brief.citation_validation, "rejected");
    assert.deepEqual(brief.evidence, []);
  }
});
test("requires explicit contrary citations for a conflict", () => {
  const brief = validateGeneratedBrief(
    JSON.stringify({
      status: "conflicting-sources",
      claims: [
        { text: "A claim", quote: passage.text, citations: [passage.id] },
      ],
      limitation: "Unreviewed",
    }),
    sources,
    [],
  );
  assert.equal(brief.citation_validation, "rejected");
});
test("accepts an explicit abstention and valid source-bound claims", () => {
  assert.equal(
    validateGeneratedBrief(
      JSON.stringify({
        status: "not-established",
        claims: [],
        limitation: "No spending ledger is in the retrieved record.",
      }),
      sources,
      [],
    ).citation_validation,
    "valid",
  );
  const brief = validateGeneratedBrief(
    JSON.stringify({
      status: "source-supported",
      claims: [
        { text: "A claim", quote: passage.text, citations: [passage.id] },
      ],
      limitation: "Unreviewed",
    }),
    sources,
    [],
  );
  assert.deepEqual(brief.evidence, [passage]);
});
test("OCR quality gate flags missing and corrupted text while retaining tables", () => {
  assert.ok(textQuality("").includes("insufficient-text"));
  assert.ok(textQuality("�".repeat(100)).includes("replacement-characters"));
  assert.deepEqual(
    textQuality(
      "MILEAGE TAX RATE TABLE\n26,001 to 28,000 65.4\n28,001 to 30,000 69.3",
    ),
    [],
  );
});
