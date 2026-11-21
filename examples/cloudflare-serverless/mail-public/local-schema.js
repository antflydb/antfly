// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Document and query shapes for the on-device mailbox copy. Shared by the
// database worker and the page; no browser APIs, so Node tests import it too.

export const localSchema = Object.freeze({
  vectorIndex: (dimensions) => ({
    name: "mail_semantic",
    kind: "dense_vector",
    // Vectors come from the browser's EmbeddingGemma2, not from Antfly.
    config_json: JSON.stringify({ dims: dimensions, metric: "cosine", external: true }),
  }),
  vectorIndexName: "mail_semantic",
  key: (id) => "message/" + id,
  fields: ["id", "thread_id", "subject", "from", "to", "received_at", "attachments", "text_truncated"],
});

/** Text the embedding model sees for one message (the model adds its own task prefix). */
export function embeddingText(message) {
  const subject = (message.subject ?? "").trim();
  return (subject ? subject + "\n\n" : "") + (message.text ?? "");
}

/** The stored document: display fields, a lexical field and the vector. */
export function documentFor(message, vector) {
  if (!Array.isArray(vector) || !vector.length || !vector.every(Number.isFinite))
    throw Error("InvalidLocalVector");
  return {
    id: message.id,
    thread_id: message.thread_id,
    subject: message.subject,
    from: message.from,
    to: message.to,
    received_at: message.received_at,
    attachments: message.attachments ?? [],
    text: message.text,
    text_truncated: message.text_truncated === true,
    // One lexical field so keyword matches on sender and subject count too.
    content: [message.subject, message.from, message.text].filter(Boolean).join("\n"),
    _embeddings: { [localSchema.vectorIndexName]: vector },
  };
}

/** Hybrid (keyword + semantic, RRF-merged) search; either half may be absent. */
export function searchRequest(text, vector, limit = 20) {
  const request = { limit, fields: localSchema.fields };
  const query = typeof text === "string" ? text.trim() : "";
  if (query) request.full_text_search = { match: { field: "content", text: query } };
  if (vector) {
    request.embeddings = { [localSchema.vectorIndexName]: vector };
    request.indexes = [localSchema.vectorIndexName];
  }
  if (query && vector) request.merge_config = { strategy: "rrf" };
  if (!request.full_text_search && !request.embeddings) throw Error("InvalidLocalQuery");
  return request;
}

/** Flatten hosted search responses into message summaries. */
export function hitsFrom(result) {
  const hits = Array.isArray(result?.responses)
    ? result.responses.flatMap((response) => response?.hits?.hits ?? [])
    : [];
  const seen = new Set();
  const messages = [];
  for (const hit of hits) {
    const source = hit?._source ?? {};
    const id = source.id ?? String(hit?._id ?? "").replace(/^message\//, "");
    if (!id || seen.has(id)) continue;
    seen.add(id);
    messages.push({
      id,
      thread_id: source.thread_id ?? id,
      subject: source.subject ?? "",
      from: source.from ?? "",
      to: source.to ?? "",
      received_at: source.received_at ?? 0,
      attachments: source.attachments ?? [],
      score: hit?._score ?? null,
      local: true,
    });
  }
  return messages;
}
