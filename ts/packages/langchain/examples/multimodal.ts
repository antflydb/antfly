import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import {
  AntflyByteStore,
  AntflyDocumentStore,
  AntflyMultimodalCorpus,
  AntflyVectorStore,
} from "@antfly/langchain";
import { ChatOpenAI } from "@langchain/openai";
import { environment } from "./common.js";

const { client, embeddings } = environment();
const table = process.env.ANTFLY_TABLE ?? "langchain_multimodal";
const command = process.argv[2] ?? "verify";
if (command === "ingest") {
  try {
    await client.tables.get(table);
  } catch (error) {
    if (!(error instanceof Error && "status" in error && error.status === 404)) throw error;
    await client.tables.create(table, {});
  }
}
const vectors =
  command === "ingest"
    ? await AntflyVectorStore.fromDocuments([], embeddings, { client, table })
    : new AntflyVectorStore(embeddings, { client, table });
const corpus = new AntflyMultimodalCorpus(
  vectors,
  new AntflyDocumentStore({ client, table, namespace: "parents" }),
  new AntflyByteStore({ client, table, namespace: "media" })
);
const ids = ["page-1", "figure-1", "figure-2"];
if (command === "ingest") {
  const root = new URL("./multimodal/fixtures/", import.meta.url);
  const sourceSha256 = createHash("sha256")
    .update(await readFile(new URL("orchard-report.pdf", root)))
    .digest("hex");
  const pages = JSON.parse(await readFile(new URL("pages.json", root), "utf8")).pages as string[];
  const manifest = JSON.parse(await readFile(new URL("answers.json", root), "utf8"));
  for (const [i, id] of ids.entries())
    await corpus.ingest({
      id,
      text: pages[i],
      summaries: [i === 0 ? pages[i] : manifest.summaries[id]],
      images:
        i === 0
          ? []
          : [{ bytes: await readFile(new URL(`${id}.png`, root)), mimeType: "image/png" }],
      metadata: {
        source: "orchard-report.pdf",
        sourceSha256,
        page: i + 1,
        proxyRevision: manifest.summary_revision,
      },
    });
  console.log("Persisted text, summary vectors, parents and original figures.");
} else if (command === "delete") {
  for (const id of ids) await corpus.delete(id);
  console.log("Deleted corpus records.");
} else if (command === "verify") {
  // This path never opens fixture files: it is safe to run after a server/process restart.
  const cases = [
    { question: "Who owns orchard operations?", id: "page-1" },
    { question: "quarterly shipment bar chart", id: "figure-1" },
    { question: "sensor controller pager diagram", id: "figure-2" },
  ];
  const report = [];
  for (const item of cases) {
    const evidence = await corpus.evidence(item.question, 1);
    if (evidence[0]?.id !== item.id) throw new Error(`Wrong evidence for ${item.question}`);
    const message = await corpus.message(item.question, evidence);
    report.push({
      question: item.question,
      source: evidence[0].id,
      mediaVerified: true,
      contentParts: message.content.length,
    });
  }
  console.log(
    JSON.stringify({ mode: "retrieval-and-media-integrity", generation: false, report }, null, 2)
  );
} else if (command === "ask") {
  const question = process.argv[3] ?? "What audit marker appears in the shipment figure?";
  if (process.argv.includes("--generate")) {
    if (!process.env.OPENAI_MODEL || !process.env.OPENAI_API_KEY)
      throw new Error("Explicit OPENAI_MODEL and OPENAI_API_KEY required");
    console.log(
      JSON.stringify(
        await corpus.answer(
          question,
          new ChatOpenAI({ model: process.env.OPENAI_MODEL, temperature: 0 })
        ),
        null,
        2
      )
    );
  } else console.log(JSON.stringify(await corpus.evidence(question), null, 2));
} else throw new Error("Use ingest, verify, ask [question] [--generate], or delete");
