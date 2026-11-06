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
  /** Most memories a recall can return. Default: the server's own default of 20. */
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

  // The configured limit is a ceiling; the model can only ask for fewer.
  const boundedLimit = (limit: number | undefined) =>
    options.recallLimit === undefined
      ? limit
      : Math.min(limit ?? options.recallLimit, options.recallLimit);

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
          body: { query, kind, limit: boundedLimit(limit) },
        }
      );
      if (!response.ok) throw new Error(`Antfly memory recall failed: ${JSON.stringify(error)}`);
      return data;
    },
  });

  const remember = tool({
    description: `Record a new fact in the Antfly memory space '${options.space}', as a subject-predicate-value assertion backed by free-text evidence. Returns the durable operation.`,
    inputSchema: z
      .object({
        subject: z.string().min(1).max(500).describe("What the fact is about, e.g. 'Mira Chen'."),
        predicate: z.string().min(1).max(200).describe("The relation, e.g. 'owns'."),
        value: z
          .string()
          .min(1)
          .max(2000)
          .describe("The value of the relation, e.g. 'operations'."),
        statement: z
          .string()
          .min(1)
          .max(20000)
          .describe("Free-text statement of the fact, used as recall evidence."),
        sourceUrl: z.string().url().optional(),
      })
      .strict(),
    execute: async ({ subject, predicate, value, statement, sourceUrl }) => {
      // Recall only matches memory *assertions*, not raw evidence text, so a
      // useful "remember" call must write both in the same request: evidence
      // is the source text, and the assertion is the structured, recallable
      // claim that cites it (antfly#1505 qualification).
      const evidenceId = randomUUID();
      const { data, error, response } = await options.client.memoryApi.POST(
        "/memory/v1/spaces/{space}/observations",
        {
          params: {
            path: { space: options.space },
            header: { ...header(), "Idempotency-Key": randomUUID() },
          },
          body: {
            evidence: [{ id: evidenceId, text: statement, source_url: sourceUrl }],
            assertions: [
              {
                id: randomUUID(),
                subject,
                predicate,
                value,
                statement,
                evidence: [{ id: evidenceId }],
              },
            ],
          },
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
