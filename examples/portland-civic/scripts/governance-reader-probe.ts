import { createRequire } from "node:module";
import { writeFile } from "node:fs/promises";
import { readPage } from "../server/governance-extraction.ts";
const require = createRequire(import.meta.url);
const { createCanvas } = require(
  require.resolve("@napi-rs/canvas", {
    paths: [require.resolve("pdfjs-dist/package.json")],
  }),
);
const canvas = createCanvas(1100, 220);
const context = canvas.getContext("2d");
context.fillStyle = "white";
context.fillRect(0, 0, 1100, 220);
context.fillStyle = "black";
context.font = "40px sans-serif";
context.fillText("OREGON LAWS 2017 Chapter 750", 30, 75);
context.fillText("Effective date October 6, 2017", 30, 145);
const image = canvas.toBuffer("image/png");
// Explicit synthetic scan fixture, never ingested as a government source.
await writeFile("data/governance/cache/ocr-test-fixture.png", image);
const result = await readPage(image, "");
await writeFile(
  "data/governance/cache/ocr-test-result.json",
  JSON.stringify(result, null, 2),
);
if (
  result.extraction !== "florence-ocr" ||
  !/October\s+6,?\s+2017/i.test(result.text)
)
  throw new Error(`OCR fixture failed: ${JSON.stringify(result)}`);
console.log(
  "Florence native OCR recovered the date from the synthetic scan fixture.",
);
const visual = createCanvas(320, 320);
const drawing = visual.getContext("2d");
drawing.fillStyle = "white";
drawing.fillRect(0, 0, 320, 320);
drawing.fillStyle = "red";
drawing.beginPath();
drawing.arc(160, 160, 90, 0, Math.PI * 2);
drawing.fill();
const caption = await readPage(visual.toBuffer("image/png"), "");
await writeFile(
  "data/governance/cache/caption-test-result.json",
  JSON.stringify(caption, null, 2),
);
if (
  caption.text !== "" ||
  !caption.caption ||
  caption.extraction !== "unreadable"
)
  throw new Error(`Caption isolation failed: ${JSON.stringify(caption)}`);
console.log(
  "Florence native caption fallback preserved the distinction between image description and source text.",
);
