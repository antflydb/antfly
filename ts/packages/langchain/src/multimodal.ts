import { createHash } from "node:crypto";
import { Document } from "@langchain/core/documents";
import type { BaseChatModel } from "@langchain/core/language_models/chat_models";
import { HumanMessage } from "@langchain/core/messages";
import { type AntflyByteStore, type AntflyDocumentStore, jsonCopy } from "./stores.js";
import type { AntflyVectorStore } from "./vectorstore.js";

export interface MultimodalSource {
  id: string;
  text: string;
  summaries: string[];
  images: { bytes: Uint8Array; mimeType: "image/png" | "image/jpeg" | "image/webp" }[];
  metadata?: Record<string, unknown>;
}
const digest = (bytes: Uint8Array | string) => createHash("sha256").update(bytes).digest("hex");
/** Persistent parent documents and original media are deliberately separate stores. */
export class AntflyMultimodalCorpus {
  constructor(
    readonly vectors: AntflyVectorStore,
    readonly parents: AntflyDocumentStore,
    readonly media: AntflyByteStore
  ) {}
  async ingest(source: MultimodalSource): Promise<void> {
    if (!source.id || !source.summaries.length || source.summaries.some((s) => !s))
      throw new TypeError("Stable source ID and nonempty summaries required");
    jsonCopy(source.metadata ?? {});
    if (
      typeof source.text !== "string" ||
      source.images.some(
        (image) =>
          !(image.bytes instanceof Uint8Array) ||
          !["image/png", "image/jpeg", "image/webp"].includes(image.mimeType)
      )
    )
      throw new TypeError("Valid text and supported image bytes required");
    const [previous] = await this.parents.mget([source.id]);
    const revision = digest(
      JSON.stringify([
        source.text,
        source.summaries,
        source.images.map((i) => [i.mimeType, digest(i.bytes)]),
        source.metadata ?? {},
      ])
    );
    const refs = source.images.map((image, i) => ({
      key: `${source.id}/${revision}/image/${i}`,
      sha256: digest(image.bytes),
      mimeType: image.mimeType,
    }));
    const childIds = source.summaries.map((_, i) => `${source.id}/${revision}/summary/${i}`);
    await this.media.mset(source.images.map((image, i) => [refs[i].key, image.bytes]));
    // Parent is committed before its searchable children. Missing/corrupt media always fails closed.
    await this.parents.mset([
      [
        source.id,
        new Document({
          id: source.id,
          pageContent: source.text,
          metadata: { ...source.metadata, media: refs, childIds, revision },
        }),
      ],
    ]);
    await this.vectors.addDocuments(
      source.summaries.map(
        (text, i) =>
          new Document({
            id: childIds[i],
            pageContent: text,
            metadata: { doc_id: source.id, revision, source_id: source.id },
          })
      ),
      { ids: childIds }
    );
    if (previous) {
      const stale = (previous.metadata.childIds as string[]).filter((id) => !childIds.includes(id));
      if (stale.length) await this.vectors.delete({ ids: stale });
      const staleMedia = (previous.metadata.media as typeof refs).filter(
        (ref) => !refs.some((next) => next.key === ref.key)
      );
      await this.media.mdelete(staleMedia.map((ref) => ref.key));
    }
  }
  async delete(id: string): Promise<void> {
    const [parent] = await this.parents.mget([id]);
    if (!parent) return;
    await this.vectors.delete({ ids: parent.metadata.childIds });
    await this.media.mdelete(parent.metadata.media.map((ref: { key: string }) => ref.key));
    await this.parents.mdelete([id]);
  }
  async evidence(query: string, k = 4): Promise<Document[]> {
    const children = await this.vectors.similaritySearch(query, k);
    const ids = [...new Set(children.map((child) => child.metadata.doc_id as string))];
    const parents = await this.parents.mget(ids);
    return parents.map((parent, i) => {
      if (!parent) throw new Error(`Missing parent ${ids[i]}`);
      if (
        children.some(
          (child) =>
            child.metadata.doc_id === ids[i] && child.metadata.revision !== parent.metadata.revision
        )
      )
        throw new Error("Stale summary revision; complete ingestion before generation");
      return parent;
    });
  }
  async message(query: string, parents: Document[]): Promise<HumanMessage> {
    const content: (
      | { type: "text"; text: string }
      | { type: "image_url"; image_url: { url: string } }
    )[] = [
      {
        type: "text",
        text: `Answer from these sources and cite their IDs. If unsupported, say Insufficient evidence.\nQuestion: ${query}`,
      },
    ];
    for (const parent of parents) {
      content.push({ type: "text", text: `[${parent.id}] ${parent.pageContent}` });
      for (const ref of parent.metadata.media as {
        key: string;
        sha256: string;
        mimeType: string;
      }[]) {
        const [bytes] = await this.media.mget([ref.key]);
        if (!bytes || digest(bytes) !== ref.sha256)
          throw new Error(`Missing or corrupt media: ${ref.key}`);
        content.push({
          type: "image_url",
          image_url: {
            url: `data:${ref.mimeType};base64,${Buffer.from(bytes).toString("base64")}`,
          },
        });
      }
    }
    return new HumanMessage({ content });
  }
  async answer(
    query: string,
    model: BaseChatModel
  ): Promise<{ answer: string; sources: Document[] }> {
    const sources = await this.evidence(query);
    if (!sources.length) return { answer: "Insufficient evidence.", sources };
    const response = await model.invoke([await this.message(query, sources)]);
    const answer = typeof response.content === "string" ? response.content : response.text;
    const citations = [...answer.matchAll(/\[([^\]]+)\]/g)].map((match) => match[1]);
    if (
      answer !== "Insufficient evidence." &&
      (!citations.length || citations.some((id) => !sources.some((s) => s.id === id)))
    )
      throw new Error("Answer omitted citations or cited unknown sources");
    return { answer, sources };
  }
}
