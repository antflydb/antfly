import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { readSnapshot, validateSnapshot } from "../server/governance-data.ts";

const { data } = await readSnapshot();
test("every citation resolves to exact text in a checksummed preserved source", async () => {
  validateSnapshot(data);
  for (const doc of data.documents.filter((d) => d.extraction === "pdf-text")) {
    assert.equal(
      createHash("sha256")
        .update(await readFile(`data/governance/sources/${doc.sha256}.pdf`))
        .digest("hex"),
      doc.sha256,
    );
  }
  const tampered = structuredClone(data);
  tampered.passages[0].text = "Invented excerpt";
  assert.throws(() => validateSnapshot(tampered), /Invalid source locator/);
});
test("evaluation fixtures retain the actual contrary dates and secondary vote provenance", () => {
  const claim = data.claims.find((c) => c.id === "claim-effective")!;
  const passage = (id: string) => data.passages.find((p) => p.id === id)!;
  assert.match(
    passage(claim.evidence[0]).text,
    /Effective date October 6, 2017/,
  );
  assert.match(
    passage(claim.contrary[0]).text,
    /Effective Date: August 6, 2017/,
  );
  const votes = data.claims.find(
    (c) => c.question === "What were the floor vote totals?",
  )!;
  assert.ok(
    votes.evidence.every((id) => passage(id).document_id === "atlas-events"),
  );
  assert.match(votes.limitation, /not been independently checked/);
});
test("the checked-in PDF pages passed native-text quality checks and graph links resolve", () => {
  for (const page of data.pages.filter(
    (p) => p.document_id !== "atlas-events",
  )) {
    assert.equal(page.extraction, "native-text");
    assert.deepEqual(page.quality_flags, []);
  }
  const broken = structuredClone(data);
  broken.nodes[0].evidence = ["imaginary-source"];
  assert.throws(() => validateSnapshot(broken), /Dangling evidence/);
});
