// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
import type { AntflyClient, MemorySchemas } from "../src/index.js";

declare const client: AntflyClient;
const resource: MemorySchemas["ResourceReference"] = {
  database: "default",
  namespace: "public",
  table: "business",
  table_id: 18446744073709551615n,
  kind: "record",
  id: "approval",
  revision: 9007199254740993n,
};
void client.memory.explainResource("team", { resource, known_at: 1 });
void client.memoryApi.POST("/memory/v1/spaces/{space}/explain", {
  params: { path: { space: "team" } },
  body: { resource },
});
// @ts-expect-error Memory endpoints require the lossless memory transport.
void client.api.POST("/memory/v1/spaces/{space}/explain", {});

void client.memoryApi.POST("/memory/v1/spaces/{space}/entities/{id}/view", {
  params: { path: { space: "team", id: "concept/a" } },
  body: { known_at: 0n },
});
void client.memoryApi.POST("/memory/v1/spaces/{space}/entity-proposals/{id}/verdict", {
  params: { path: { space: "team", id: "proposal" }, header: { "Idempotency-Key": "defer" } },
  body: { expected_revision: 18446744073709551615n, accept: null, actor: "codex/judge" },
});
void client.memoryApi.POST("/memory/v1/spaces/{space}/recall", {
  params: { path: { space: "team" } },
  body: { graph: { entity_seeds: ["concept/a"], relation_weight: 0.5 }, token_budget: 8192 },
});
