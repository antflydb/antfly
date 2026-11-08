import { createHash, randomUUID } from "node:crypto";
import type { AntflyClient, MemorySchemas } from "@antfly/sdk";
import { type Tool, tool } from "ai";
import { z } from "zod";

export interface AntflyMemoryToolsOptions {
  client: AntflyClient;
  /** Fixed host scope; never accepted from model arguments. */
  space: string;
  database?: string;
  namespace?: string;
  branch?: string;
  /** Existing three tools by default; agent opts into the native eight-tool profile. */
  profile?: "basic" | "agent";
  /** Globally unique host run/request ID. Reuse on retries across process restarts. */
  runId?: string;
  recallLimit?: number;
  recallTokenBudget?: number;
  briefLimit?: number;
  briefTokenBudget?: number;
  explainMaxBytes?: number;
  /** Host-selected outcome weighting, from zero (default) to one. */
  utilityWeight?: number;
}

/** Exact integers remain JSON-safe throughout model messages and chat history. */
export type MemoryToolJSON<T> = T extends bigint
  ? string
  : T extends readonly (infer Item)[]
    ? MemoryToolJSON<Item>[]
    : T extends object
      ? { [Key in keyof T]: MemoryToolJSON<T[Key]> }
      : T;

function memoryJSON(value: unknown): string | undefined {
  return JSON.stringify(value, (_key, item) => (typeof item === "bigint" ? item.toString() : item));
}
function checked<T>(
  result: { data?: T; error?: unknown; response: Response },
  action: string
): MemoryToolJSON<T> {
  if (!result.response.ok)
    throw new Error(`Antfly memory ${action} failed: ${memoryJSON(result.error)}`);
  if (result.data === undefined) throw new Error(`Antfly memory ${action} returned no JSON result`);
  return JSON.parse(memoryJSON(result.data) ?? "null") as MemoryToolJSON<T>;
}
function headers(options: AntflyMemoryToolsOptions) {
  return {
    ...(options.database !== undefined ? { "X-Antfly-Database": options.database } : {}),
    ...(options.namespace !== undefined ? { "X-Antfly-Namespace": options.namespace } : {}),
    ...(options.branch !== undefined ? { "X-Antfly-Memory-Branch": options.branch } : {}),
  };
}
function integerOption(
  name: string,
  value: number | undefined,
  fallback: number,
  max: number,
  min = 1
): number {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved < min || resolved > max)
    throw new RangeError(`${name} must be an integer in [${min}, ${max}]`);
  return resolved;
}
function validate(options: AntflyMemoryToolsOptions) {
  if (options.runId !== undefined && options.runId.trim().length === 0)
    throw new Error("runId must not be empty");
  if (
    options.utilityWeight !== undefined &&
    (!Number.isFinite(options.utilityWeight) ||
      options.utilityWeight < 0 ||
      options.utilityWeight > 1)
  )
    throw new RangeError("utilityWeight must be in [0, 1]");
  return {
    recallLimit: integerOption("recallLimit", options.recallLimit, 20, 256),
    recallTokens: integerOption("recallTokenBudget", options.recallTokenBudget, 2048, 65536),
    briefLimit: integerOption("briefLimit", options.briefLimit, 16, 64),
    briefTokens: integerOption("briefTokenBudget", options.briefTokenBudget, 1024, 65536),
    explainBytes: integerOption("explainMaxBytes", options.explainMaxBytes, 8192, 65536, 512),
  };
}
function digest(parts: readonly unknown[]): string {
  return createHash("sha256").update(JSON.stringify(parts)).digest("hex");
}
function operationKey(options: AntflyMemoryToolsOptions, action: string, eventId?: string): string {
  if (!options.runId) return randomUUID();
  if (!eventId) throw new Error("A stable tool-call/event ID is required with runId");
  return `ai:${digest(["antfly-ai-memory/v1", options.database ?? "default", options.namespace ?? "public", options.space, options.branch ?? "", options.runId, action, eventId])}`;
}
const ids = z.array(z.string().min(1).max(256)).max(100);
const kinds = [
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
const revision = z
  .union([
    z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
    z.string().regex(/^(0|[1-9]\d{0,19})$/),
  ])
  .refine((value) => BigInt(value) <= 18446744073709551615n, "revision exceeds uint64");
const distinctIds = (values: string[]) => [...new Set(values)].sort();

/**
 * Thin adapters over native memory policy. Scope and budget ceilings belong to
 * the host. Basic keeps the existing recall/remember/forget catalog; agent adds
 * brief/get/explain/correct/outcome. Maintenance and curator APIs stay separate.
 */
export function createMemoryTools(options: AntflyMemoryToolsOptions): Record<string, Tool> {
  const bounds = validate(options);
  const recall = tool({
    description: `Recall relevant evidence-backed memories from '${options.space}'. Use the brief first, then recall for task-specific detail.`,
    inputSchema: z
      .object({
        query: z.string().min(1).max(4000),
        kind: z.enum(kinds).optional(),
        limit: z.number().int().min(1).max(256).optional(),
        tokenBudget: z.number().int().min(1).max(65536).optional(),
      })
      .strict(),
    execute: async ({ query, kind, limit, tokenBudget }, { abortSignal }) => {
      abortSignal?.throwIfAborted();
      return checked(
        await options.client.memoryApi.POST("/memory/v1/spaces/{space}/recall", {
          signal: abortSignal,
          params: { path: { space: options.space }, header: headers(options) },
          body: {
            query,
            kind,
            limit: Math.min(limit ?? bounds.recallLimit, bounds.recallLimit),
            token_budget: Math.min(tokenBudget ?? bounds.recallTokens, bounds.recallTokens),
            ...(options.utilityWeight !== undefined
              ? { utility_weight: options.utilityWeight }
              : {}),
          },
        }),
        "recall"
      );
    },
  });
  const remember = tool({
    description: `Record a fact with its source evidence in '${options.space}'. Returns a durable operation receipt, admitted evidence IDs and duplicate-review signals.`,
    inputSchema: z
      .object({
        subject: z.string().min(1).max(256),
        predicate: z.string().min(1).max(200),
        value: z.string().min(1).max(2000),
        statement: z.string().min(1).max(20000),
        sourceUrl: z.string().url().optional(),
      })
      .strict(),
    execute: async (
      { subject, predicate, value, statement, sourceUrl },
      { abortSignal, toolCallId }
    ) => {
      abortSignal?.throwIfAborted();
      const key = operationKey(options, "remember", toolCallId);
      const evidenceId = digest([key, "evidence"]);
      return checked(
        await options.client.memoryApi.POST("/memory/v1/spaces/{space}/observations", {
          signal: abortSignal,
          params: {
            path: { space: options.space },
            header: { ...headers(options), "Idempotency-Key": key },
          },
          body: {
            scoped_evidence_ids: true,
            evidence: [{ id: evidenceId, text: statement, source_url: sourceUrl }],
            assertions: [
              {
                id: digest([key, "assertion"]),
                subject,
                predicate,
                value,
                statement,
                evidence: [{ id: evidenceId }],
              },
            ],
          },
        }),
        "observation"
      );
    },
  });
  const forget = tool({
    description: `Permanently forget specified evidence in '${options.space}'.`,
    needsApproval: true,
    inputSchema: z.object({ evidenceIds: ids.min(1) }).strict(),
    execute: async ({ evidenceIds }, { abortSignal, toolCallId }) => {
      abortSignal?.throwIfAborted();
      return checked(
        await options.client.memoryApi.POST("/memory/v1/spaces/{space}/forget", {
          signal: abortSignal,
          params: {
            path: { space: options.space },
            header: {
              ...headers(options),
              "Idempotency-Key": operationKey(options, "forget", toolCallId),
            },
          },
          body: { evidence_ids: distinctIds(evidenceIds) },
        }),
        "forget"
      );
    },
  });
  const basic = {
    antfly_memory_recall: recall,
    antfly_memory_remember: remember,
    antfly_memory_forget: forget,
  };
  if (options.profile !== "agent") return basic;
  const brief = tool({
    description: `Start a session with the small native memory brief from '${options.space}'. IDs support deeper get, recall and explain calls.`,
    inputSchema: z
      .object({
        limit: z.number().int().min(1).max(64).optional(),
        tokenBudget: z.number().int().min(1).max(65536).optional(),
      })
      .strict(),
    execute: async ({ limit, tokenBudget }, { abortSignal }) =>
      loadBrief(options, bounds, { signal: abortSignal, limit, tokenBudget }),
  });
  const get = tool({
    description:
      "Read one memory's current state, evidence citations and revision before making a correction.",
    inputSchema: z.object({ id: z.string().min(1).max(256) }).strict(),
    execute: async ({ id }, { abortSignal }) => {
      abortSignal?.throwIfAborted();
      return checked(
        await options.client.memoryApi.GET("/memory/v1/spaces/{space}/memories/{id}", {
          signal: abortSignal,
          params: { path: { space: options.space, id }, header: headers(options) },
        }),
        "get"
      );
    },
  });
  const explain = tool({
    description: "Explain a memory through its bounded, authorized provenance graph.",
    inputSchema: z.object({ id: z.string().min(1).max(256) }).strict(),
    execute: async ({ id }, { abortSignal }) => {
      abortSignal?.throwIfAborted();
      return checked(
        await options.client.memoryApi.POST("/memory/v1/spaces/{space}/memories/{id}/explain", {
          signal: abortSignal,
          params: { path: { space: options.space, id }, header: headers(options) },
          body: { max_bytes: bounds.explainBytes, max_nodes: 32, max_depth: 8 },
        }),
        "explain"
      );
    },
  });
  const correct = tool({
    description:
      "Correct or verify a memory using its current revision, an explicit reason and optional evidence IDs. Native ownership and revision policy apply.",
    inputSchema: z
      .object({
        id: z.string().min(1).max(256),
        expectedRevision: revision,
        lifecycle: z.enum(["active", "disputed", "retracted"]),
        stability: z.enum(["settled", "provisional"]).optional(),
        reason: z.string().min(1).max(20000),
        evidenceIds: ids.optional(),
      })
      .strict(),
    execute: async (
      { id, expectedRevision, lifecycle, stability, reason, evidenceIds },
      { abortSignal, toolCallId }
    ) => {
      abortSignal?.throwIfAborted();
      return checked(
        await options.client.memoryApi.POST("/memory/v1/spaces/{space}/corrections", {
          signal: abortSignal,
          params: {
            path: { space: options.space },
            header: {
              ...headers(options),
              "Idempotency-Key": operationKey(options, "correct", toolCallId),
            },
          },
          body: {
            id,
            expected_revision: BigInt(expectedRevision),
            lifecycle,
            stability,
            reason,
            evidence: distinctIds(evidenceIds ?? []).map((evidenceId) => ({ id: evidenceId })),
          },
        }),
        "correction"
      );
    },
  });
  const outcome = tool({
    description:
      "Report a task outcome and the memory IDs actually used. Feedback affects utility, independently of assertion truth.",
    inputSchema: z
      .object({
        taskId: z.string().min(1).max(256),
        success: z.boolean(),
        usedMemoryIds: ids,
        detail: z.string().max(20000).optional(),
      })
      .strict(),
    execute: async ({ taskId, success, usedMemoryIds, detail }, { abortSignal, toolCallId }) => {
      abortSignal?.throwIfAborted();
      const key = operationKey(options, "outcome", toolCallId);
      return checked(
        await options.client.memoryApi.POST("/memory/v1/spaces/{space}/outcomes", {
          signal: abortSignal,
          params: {
            path: { space: options.space },
            header: { ...headers(options), "Idempotency-Key": key },
          },
          body: {
            id: digest([key, "outcome"]),
            task_id: taskId,
            success,
            used_memory_ids: distinctIds(usedMemoryIds),
            ...(detail !== undefined ? { detail } : {}),
          },
        }),
        "outcome"
      );
    },
  });
  return {
    antfly_memory_brief: brief,
    ...basic,
    antfly_memory_get: get,
    antfly_memory_explain: explain,
    antfly_memory_correct: correct,
    antfly_memory_outcome: outcome,
  };
}

export interface MemoryBriefLoadOptions {
  signal?: AbortSignal;
  limit?: number;
  tokenBudget?: number;
}
async function loadBrief(
  options: AntflyMemoryToolsOptions,
  bounds: ReturnType<typeof validate>,
  request: MemoryBriefLoadOptions
): Promise<MemoryToolJSON<MemorySchemas["MemoryBrief"]>> {
  request.signal?.throwIfAborted();
  const limit = integerOption("limit", request.limit, bounds.briefLimit, 64);
  const tokens = integerOption("tokenBudget", request.tokenBudget, bounds.briefTokens, 65536);
  return checked(
    await options.client.memoryApi.POST("/memory/v1/spaces/{space}/brief", {
      signal: request.signal,
      params: { path: { space: options.space }, header: headers(options) },
      body: {
        limit: Math.min(limit, bounds.briefLimit),
        token_budget: Math.min(tokens, bounds.briefTokens),
      },
    }),
    "brief"
  );
}
export interface AntflyMemorySessionOptions extends AntflyMemoryToolsOptions {
  /** Globally unique host run ID, stable when retrying the same task. */
  runId: string;
}
export interface MemoryAccessOptions {
  eventId: string;
  signal?: AbortSignal;
}
export interface MemoryTaskOutcome {
  eventId: string;
  taskId: string;
  success: boolean;
  usedMemoryIds: string[];
  detail?: string;
}
/**
 * Explicit host lifecycle helpers: load context, report consumed IDs, and report
 * real task outcomes. No automatic ingestion, caching, access writes or success
 * inference occurs. Reuse event IDs on retries; use a new ID for each new event.
 */
export function createMemorySession(options: AntflyMemorySessionOptions) {
  const bounds = validate(options);
  if (!options.runId?.trim()) throw new Error("createMemorySession requires a stable runId");
  return {
    tools: createMemoryTools({ ...options, profile: options.profile ?? "agent" }),
    loadBrief: (request: MemoryBriefLoadOptions = {}) => loadBrief(options, bounds, request),
    async recordAccess(memoryIds: string[], request: MemoryAccessOptions) {
      request.signal?.throwIfAborted();
      const memory_ids = distinctIds(memoryIds);
      if (memory_ids.length === 0) return null;
      ids.min(1).parse(memory_ids);
      return checked(
        await options.client.memoryApi.POST("/memory/v1/spaces/{space}/accesses", {
          signal: request.signal,
          params: {
            path: { space: options.space },
            header: {
              ...headers(options),
              "Idempotency-Key": operationKey(options, "access", request.eventId),
            },
          },
          body: { memory_ids },
        }),
        "access"
      );
    },
    async recordOutcome(outcome: MemoryTaskOutcome, request: { signal?: AbortSignal } = {}) {
      request.signal?.throwIfAborted();
      ids.parse(outcome.usedMemoryIds);
      z.string().min(1).max(256).parse(outcome.taskId);
      const key = operationKey(options, "outcome", outcome.eventId);
      return checked(
        await options.client.memoryApi.POST("/memory/v1/spaces/{space}/outcomes", {
          signal: request.signal,
          params: {
            path: { space: options.space },
            header: { ...headers(options), "Idempotency-Key": key },
          },
          body: {
            id: digest([key, "outcome"]),
            task_id: outcome.taskId,
            success: outcome.success,
            used_memory_ids: distinctIds(outcome.usedMemoryIds),
            ...(outcome.detail !== undefined ? { detail: outcome.detail } : {}),
          },
        }),
        "outcome"
      );
    },
  };
}
