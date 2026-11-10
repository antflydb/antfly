import { AntflyVectorStore } from "@antfly/langchain";
import { environment, fixture } from "./common.js";

const { client, embeddings } = environment();
const table = process.env.ANTFLY_TABLE ?? "antfly_langchain_quickstart";
const store = await AntflyVectorStore.fromDocuments(fixture, embeddings, {
  client,
  table,
  distanceMetric: "cosine",
});
const results = await store.asRetriever(1).invoke("Who owns operations?");
if (results[0]?.id !== "owner")
  throw new Error(`Expected owner; received ${results.map((doc) => doc.id)}`);
console.log(`[${results[0].id}] ${results[0].pageContent}`);
if (process.argv.includes("--cleanup")) {
  await store.delete({ ids: fixture.map((doc) => String(doc.id)) });
  console.log("Deleted the three example records; the table and other records are retained.");
}
