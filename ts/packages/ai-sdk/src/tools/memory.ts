import { randomUUID } from "node:crypto";
import type { AntflyClient } from "@antfly/sdk";
import { type Tool, tool } from "ai";
import { z } from "zod";

export interface AntflyMemoryToolsOptions {
  client: AntflyClient;
  /** Memory space name. Fixed in code; never a model-controlled argument. */
  space: string;
  /** Catalog scope for the memory space. Defaults to the server's own defaults (`default`/`public`). */
  database?: string;
  namespace?: string;
  /** Memory branch. Defaults to the server's default (`""`, the trunk). */
  branch?: string;
  /** Default 20, matching the server's own default. */
  recallLimit?: number;
}

const RECALL_KIND = [
  "semantic",
  "episodic",
  "procedural",
  "decision",
  "rationale",
  "preference",
  "lesson",
  "constraint",
  "open_question",
  "alternative",
  "summary",
] as const;

/**
 * Memory tools (`recall`, `remember`, `forget`) over `/memory/v1`, built on
 * `AntflyClient.memoryApi` (the SDK's exact-integer-safe memory transport).
 * The memory space, database, namespace and branch are fixed by the caller
 * at construction time -- never exposed as tool arguments the model could
 * change -- per antfly#1505.
 *
 * `forget` is destructive and is marked `needsApproval: true`; wire an
 * approval flow (e.g. `ai`'s `toolApproval`/`addToolApprovalResponse`)
 * before enabling it for an untrusted model.
 *
 * This only covers `recall`, `observations` (as `remember`) and `forget`.
 * The rest of the memory API (assertions, corrections, supersession,
 * evaluation, lineage, entities, ...) is out of scope for this first PR.
 */
export function createMemoryTools(options: AntflyMemoryToolsOptions): Record<string, Tool> {
  const header = () => ({
    ...(options.database !== undefined ? { "X-Antfly-Database": options.database } : {}),
    ...(options.namespace !== undefined ? { "X-Antfly-Namespace": options.namespace } : {}),
    ...(options.branch !== undefined ? { "X-Antfly-Memory-Branch": options.branch } : {}),
  });

  const recall = tool({
    description: `Recall relevant memories from the Antfly memory space '${options.space}' by free-text query, optionally narrowed by kind. Returns the most relevant memories with their supporting evidence.`,
    inputSchema: z
      .object({
        query: z.string().min(1).max(4000).describe("Free-text recall query."),
        kind: z.enum(RECALL_KIND).optional().describe("Restrict recall to one memory kind."),
        limit: z.number().int().min(1).max(200).optional(),
      })
      .strict(),
    execute: async ({ query, kind, limit }) => {
      const { data, error, response } = await options.client.memoryApi.POST(
        "/memory/v1/spaces/{space}/recall",
        {
          params: { path: { space: options.space }, header: header() },
          body: { query, kind, limit: limit ?? options.recallLimit },
        }
      );
      if (!response.ok) throw new Error(`Antfly memory recall failed: ${JSON.stringify(error)}`);
      return data;
    },
  });

  const remember = tool({
    description: `Record a new memory in the Antfly memory space '${options.space}', backed by one piece of evidence text. Returns the durable operation.`,
    inputSchema: z
      .object({
        text: z.string().min(1).max(20000).describe("The memory content, as evidence text."),
        sourceUrl: z.string().url().optional(),
      })
      .strict(),
    execute: async ({ text, sourceUrl }) => {
      const { data, error, response } = await options.client.memoryApi.POST(
        "/memory/v1/spaces/{space}/observations",
        {
          params: {
            path: { space: options.space },
            header: { ...header(), "Idempotency-Key": randomUUID() },
          },
          body: { evidence: [{ id: randomUUID(), text, source_url: sourceUrl }] },
        }
      );
      if (!response.ok)
        throw new Error(`Antfly memory observation failed: ${JSON.stringify(error)}`);
      return data;
    },
  });

  const forget = tool({
    description: `Permanently delete specific memories by evidence ID from the Antfly memory space '${options.space}'. Destructive and irreversible.`,
    needsApproval: true,
    inputSchema: z
      .object({
        evidenceIds: z.array(z.string().min(1)).min(1).max(100),
      })
      .strict(),
    execute: async ({ evidenceIds }) => {
      const { data, error, response } = await options.client.memoryApi.POST(
        "/memory/v1/spaces/{space}/forget",
        {
          params: {
            path: { space: options.space },
            header: { ...header(), "Idempotency-Key": randomUUID() },
          },
          body: { evidence_ids: evidenceIds },
        }
      );
      if (!response.ok) throw new Error(`Antfly memory forget failed: ${JSON.stringify(error)}`);
      return data;
    },
  });

  return {
    antfly_memory_recall: recall,
    antfly_memory_remember: remember,
    antfly_memory_forget: forget,
  };
}
