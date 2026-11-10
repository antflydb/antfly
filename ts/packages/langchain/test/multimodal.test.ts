import { createHash } from "node:crypto";
import { AntflyClient } from "@antfly/sdk";
import { Document } from "@langchain/core/documents";
import { FakeEmbeddings, FakeListChatModel } from "@langchain/core/utils/testing";
import { expect, it, vi } from "vitest";
import {
  AntflyByteStore,
  AntflyDocumentStore,
  AntflyMultimodalCorpus,
  AntflyVectorStore,
} from "../src/index.js";

function fixture() {
  const client = new AntflyClient({ baseUrl: "http://unused" });
  const vectors = new AntflyVectorStore(new FakeEmbeddings(), { client, table: "t" });
  const parents = new AntflyDocumentStore({ client, table: "t", namespace: "parents" });
  const media = new AntflyByteStore({ client, table: "t", namespace: "media" });
  const bytes = new Uint8Array([0, 1, 255]);
  const parent = new Document({
    id: "figure",
    pageContent: "Shipment chart",
    metadata: {
      revision: "one",
      media: [
        {
          key: "original",
          mimeType: "image/png",
          sha256: createHash("sha256").update(bytes).digest("hex"),
        },
      ],
    },
  });
  vi.spyOn(vectors, "similaritySearch").mockResolvedValue([
    new Document({ pageContent: "chart proxy", metadata: { doc_id: "figure", revision: "one" } }),
  ]);
  vi.spyOn(parents, "mget").mockResolvedValue([parent]);
  vi.spyOn(media, "mget").mockResolvedValue([bytes]);
  return {
    corpus: new AntflyMultimodalCorpus(vectors, parents, media),
    parents,
    media,
    parent,
    bytes,
  };
}
it("passes verified original bytes to a standard chat model and validates citations", async () => {
  const { corpus, bytes } = fixture();
  const model = new FakeListChatModel({ responses: ["120 crates. [figure]"] });
  const call = vi.spyOn(model, "invoke");
  expect((await corpus.answer("How many?", model)).answer).toContain("[figure]");
  expect(JSON.stringify(call.mock.calls[0][0])).toContain(Buffer.from(bytes).toString("base64"));
});
it("never calls generation after missing/corrupt image or stale/missing parent", async () => {
  const { corpus, media, parents, parent } = fixture();
  const model = new FakeListChatModel({ responses: ["answer"] });
  const call = vi.spyOn(model, "invoke");
  vi.mocked(media.mget).mockResolvedValue([new Uint8Array([2])]);
  await expect(corpus.answer("q", model)).rejects.toThrow("corrupt");
  parent.metadata.revision = "two";
  await expect(corpus.answer("q", model)).rejects.toThrow("Stale");
  vi.mocked(parents.mget).mockResolvedValue([undefined]);
  await expect(corpus.answer("q", model)).rejects.toThrow("Missing parent");
  expect(call).not.toHaveBeenCalled();
});
it("rejects uncited and invented citations, permits explicit abstention", async () => {
  const { corpus } = fixture();
  for (const response of ["120 crates.", "120 crates. [other]"])
    await expect(
      corpus.answer("q", new FakeListChatModel({ responses: [response] }))
    ).rejects.toThrow("citations");
  expect(
    (await corpus.answer("q", new FakeListChatModel({ responses: ["Insufficient evidence."] })))
      .answer
  ).toBe("Insufficient evidence.");
});
it.each([
  "summaries",
  "cleanup",
  "finalize",
])("resumes failed replacement at %s without losing old resources", async (phase) => {
  const { corpus, parents, media } = fixture();
  const documents = new Map<string, Document>(),
    images = new Map<string, Uint8Array>(),
    children = new Map<string, Document>();
  vi.mocked(parents.mget).mockImplementation(async (ids) => ids.map((id) => documents.get(id)));
  vi.spyOn(parents, "mset").mockImplementation(async (pairs) => {
    for (const [id, doc] of pairs) documents.set(id, doc);
  });
  vi.mocked(media.mget).mockImplementation(async (ids) => ids.map((id) => images.get(id)));
  vi.spyOn(media, "mset").mockImplementation(async (pairs) => {
    for (const [id, value] of pairs) images.set(id, value);
  });
  vi.spyOn(media, "mdelete").mockImplementation(async (ids) => {
    for (const id of ids) images.delete(id);
  });
  vi.spyOn(corpus.vectors, "addDocuments").mockImplementation(async (docs) => {
    for (const doc of docs) children.set(doc.id!, doc as Document);
    return docs.map((d) => d.id!);
  });
  vi.spyOn(corpus.vectors, "delete").mockImplementation(async ({ ids }) => {
    for (const id of ids) children.delete(id);
  });
  vi.mocked(corpus.vectors.similaritySearch).mockImplementation(async () => [...children.values()]);
  const source = (text: string) => ({
    id: "source",
    text,
    summaries: [text],
    images: [{ bytes: Buffer.from(text), mimeType: "image/png" as const }],
  });
  await corpus.ingest(source("A"));
  if (phase === "summaries")
    vi.mocked(corpus.vectors.addDocuments).mockRejectedValueOnce(new Error("interrupted"));
  if (phase === "cleanup") vi.mocked(media.mdelete).mockRejectedValueOnce(new Error("interrupted"));
  if (phase === "finalize") {
    const original = parents.mset.bind(parents);
    vi.mocked(parents.mset)
      .mockImplementationOnce(async (pairs) => {
        for (const [id, doc] of pairs) documents.set(id, doc);
      })
      .mockRejectedValueOnce(new Error("interrupted"));
    void original;
  }
  await expect(corpus.ingest(source("B"))).rejects.toThrow("interrupted");
  await corpus.ingest(source("B"));
  expect(children.size).toBe(1);
  expect(images.size).toBe(1);
  expect((await corpus.evidence("q"))[0].pageContent).toBe("B");
});
