# Antfly Native Agents And A2A Integration

## Context

Antfly now has two native bounded-agent APIs in the Zig implementation:

- `POST /agents/query-builder`
- `POST /agents/retrieval`

The A2A surface is an adapter over those native agents:

- `POST /a2a`
- `GET /.well-known/agent-card.json`

MCP remains the deterministic database/tool protocol. It exposes exact table, index, document, and query operations.
Agentic natural-language planning belongs in the native agents and is advertised through A2A skills. ARD advertises the
availability of these resources; it does not execute them. See `ARD.md` for discovery and `MCP.md` for the MCP tool
surface.

This document is the canonical design and implementation note for Antfly native agents and A2A. It replaces the older
planned notes for bounded sessions, retrieval tool use, chat frontend behavior, and answer-agent enhancements.

## Goals

1. Keep the native OpenAPI agent contracts authoritative.
2. Expose A2A as a thin protocol adapter over the same native behavior.
3. Use one bounded-agent envelope across query-builder, retrieval, and future native agents.
4. Make continuation explicit and client-carried, with no hidden session state required for correctness.
5. Keep tool access configurable at the request and step level.
6. Avoid reviving deprecated answer-agent or chat-agent shapes in new clients.

## Non-Goals

- A2A is not a second agent runtime.
- MCP should not duplicate the query-builder coordinator.
- The frontend should not own native retrieval tool execution.
- `session_id` does not imply durable server-side conversation memory.
- Native agents must not run unbounded internal loops.

## Current Implementation Map

Native agent API and execution:

- `specs/openapi/antfly/metadata.yaml`
  - `QueryBuilderRequest`
  - `QueryBuilderResult`
  - `RetrievalAgentRequest`
  - `RetrievalAgentResult`
  - shared bounded-agent schemas
  - retrieval SSE event schemas
- `specs/openapi/antfly/generating.yaml`
  - `ChatToolName`
  - `ChatToolsConfig`
  - step generator/tool config
- `zig/pkg/antfly/src/api/query_builder_agent.zig`
  - query-builder coordinator, specialists, metadata context, and runtime preflight.
- `zig/pkg/antfly/src/api/retrieval_agent.zig`
  - retrieval pipeline, bounded agentic selection, tool policy, SSE events, generation, follow-up, confidence, and eval.
- `zig/pkg/antfly/src/api/http_routes.zig`
  - `/agents/query-builder`
  - `/agents/retrieval`
- `zig/pkg/antfly/src/api/httpx_handler.zig`
  - HTTPX route plumbing for the native agent endpoints.
- `zig/pkg/antfly/src/cmd/cli/agents.zig`
  - CLI entry points for retrieval and query-builder requests.
- `zig/pkg/antfly-client/src/client.zig`
  - Zig client calls for retrieval and query-builder.

Protocol adapters and discovery:

- `zig/lib/a2a/src/root.zig`
  - reusable A2A JSON-RPC dispatcher, agent card, skills, task store, message/send, message/stream, tasks/get, and
    tasks/cancel.
- `zig/pkg/antfly/src/api/protocol_adapters.zig`
  - Antfly A2A skill registration and request translation to the native agent routes.
- `zig/pkg/antfly/src/api/ard_catalog.zig`
  - ARD entries for the A2A agent card, MCP, OpenAPI, and Antfly skill artifacts.

Query request design:

- `QUERY_BUILDER.md`
  - query-builder coordinator/specialist direction and the executable `query_request` target shape.

## Native Agent Model

Antfly native agents are bounded state machines over normal Antfly APIs. Each call receives all state needed to make
progress, executes a finite amount of work, and returns an envelope that the client can display, store, replay, or pass
back on a later call.

The native API remains the source of truth because it is typed by OpenAPI, covered by API tests, and used directly by
local CLI, SDK, frontend, MCP handoff guidance, and A2A.

### Query Builder

`POST /agents/query-builder` translates a natural-language intent into query artifacts.

Inputs:

- `intent`: required natural-language retrieval intent.
- `table`, `schema_fields`, `example_documents`: optional context.
- `mode`: strategy hint. Current values are `auto`, `full_text`, `semantic`, `hybrid`, `filter`, `tree`, and `graph`.
- `output`: preferred artifact, such as `query_request`, `bleve`, or `filter_query`.
- `constraints`: execution constraints such as `limit`, `allowed_fields`, `prefer_indexes`, and `require_executable`.
- `generator`: optional generator config.
- bounded-agent fields: `session_id`, `decisions`, `interactive`, `max_internal_iterations`,
  `max_user_clarifications`, and `require_decision_after`.

Outputs:

- `query`: compatibility Bleve/filter fragment.
- `query_request`: executable Antfly `QueryRequest` for table query execution.
- `retrieval_query_request`: retrieval-only query artifact when the plan needs retrieval features such as `tree_search`.
- `specialist`: selected builder such as `full_text`, `semantic`, `hybrid`, `tree`, or `graph`.
- `plan`: machine-readable coordination details.
- bounded-agent envelope fields.

The query builder should continue to follow `QUERY_BUILDER.md`: collect table/index/example context once, select a
specialist, assemble an executable Antfly query request, preflight the result, and surface deterministic diagnostics as
warnings or clarification questions.

### Retrieval

`POST /agents/retrieval` executes retrieval workflows and optional generation.

Inputs:

- `query`: required user-facing natural-language query.
- `queries`: required list of `RetrievalQueryRequest` objects. Each query carries its own `table` and may include
  full-text, semantic, filter, graph, tree, aggregation, and other query fields.
- `messages`: optional conversational context for the current turn.
- `agent_knowledge`: optional system/domain context.
- `max_internal_iterations`: `0` for pipeline mode, `1..20` for agentic mode.
- `tools`: request-wide default tool policy.
- `steps.retrieval.tools`: retrieval-step tool policy that narrows the request-wide policy.
- `steps.classification`, `steps.generation`, `steps.followup`, `steps.confidence`, `steps.eval`: optional pipeline
  step configuration.
- `stream`: choose SSE or JSON response.
- bounded-agent fields: `session_id`, `decisions`, `interactive`, `max_user_clarifications`, and
  `require_decision_after`.

Outputs:

- `hits`: retrieved documents.
- `generation`: generated answer when `steps.generation` is enabled.
- `classification`, `followup_questions`, `generation_confidence`, `context_relevance`, `eval_result`: optional step
  outputs.
- `usage`: LLM token and retrieval/pruning statistics.
- bounded-agent envelope fields.

Pipeline mode executes the declared queries directly. Agentic mode lets the retrieval agent select, refine, evaluate,
or ask for clarification from the declared capabilities within the configured iteration and tool limits.

### Deprecated Answer Agent

The answer-agent schema is deprecated compatibility only. New code should use `RetrievalAgentRequest` with
`steps.generation` for answer generation and `steps.followup`, `steps.confidence`, or `steps.eval` for optional
post-processing.

No new frontend, SDK, CLI, or A2A surface should introduce an answer-agent-specific contract.

## Shared Bounded-Agent Envelope

The shared envelope fields are:

- `session_id`: correlation id supplied by the client and echoed by the server.
- `status`: one of `clarification_required`, `completed`, `in_progress`, `incomplete`, or `failed`.
- `steps`: ordered trace of `AgentStep` entries.
- `questions`: user-facing clarification requests.
- `decisions`: structured user answers supplied on continuation requests.
- `iteration`: internal pass count consumed so far.
- `clarification_count`: user clarification count consumed so far.
- `remaining_internal_iterations`: remaining bounded internal passes.
- `remaining_user_clarifications`: remaining bounded user turns.

Status semantics:

- `completed`: the requested artifact or retrieval result is ready.
- `clarification_required`: the agent needs one or more client-rendered `questions` before it can proceed.
- `incomplete`: the agent stopped because a limit or unavailable capability prevented completion. `incomplete_details`
  explains why for retrieval.
- `failed`: execution failed.
- `in_progress`: reserved for future async or durable task execution.

Continuation is client-carried. To continue, the client sends the same core request plus:

- the same `session_id`
- the returned or accumulated `decisions`
- any relevant `messages`
- the same or intentionally revised limits/tool policy

The server must treat `decisions` as the authoritative continuation state. `messages` may provide conversational
context, but should not replace structured decisions for bounded state transitions.

## Tool Policy

Tool names come from `ChatToolName`:

- `add_filter`
- `ask_clarification`
- `web_search`
- `fetch`
- `semantic_search`
- `full_text_search`
- `tree_search`
- `graph_search`
- `aggregate`

Rules:

1. `tools.enabled_tools` is the request-wide default policy.
2. `steps.retrieval.tools.enabled_tools` narrows the default for retrieval. It cannot expand the top-level policy.
3. If neither list is present, retrieval defaults to the available retrieval tools implied by the request.
4. `max_tool_iterations` is bounded by the same production limits as `max_internal_iterations`.
5. Web access requires explicit `web_search_connection` or `web_search_config` and should be constrained by production
   security controls.
6. Aggregations are a separate capability from filters: aggregation requests require `aggregate`, filter fields require
   `add_filter`, and filtered aggregations require both tools.
6. Use `web_search`, not `websearch` or `search`.

The Zig retrieval implementation currently enforces the narrowing policy through `ToolPolicy` in
`retrieval_agent.zig`. As more steps gain native tool use, the same pattern should move into a shared helper so each
step can accept a step-local policy without reimplementing intersection and limit rules.

## Streaming

Retrieval streaming uses `text/event-stream`. The final `done` event carries the authoritative
`RetrievalAgentResult`.

Current event names:

- `step_started`
- `step_progress`
- `step_completed`
- `classification`
- `reasoning`
- `generation`
- `followup`
- `hit`
- `tool_mode`
- `eval`
- `error`
- `done`

Clients should treat all events before `done` as progressive UI data. They may render hits, reasoning, and step traces
incrementally, but should reconcile final state from `done`.

A2A `message/stream` wraps A2A task events as SSE `message` frames, plus a terminal SSE `done` frame. The A2A event
payloads are task `status-update` and `artifact-update` objects, not the raw native retrieval SSE events.

## A2A Mapping

A2A exposes Antfly native agents as skills. The current agent card lists:

- `query-builder`
- `retrieval`

The adapter rules are:

- A2A `message/send` maps to a synchronous native agent execution.
- A2A `message/stream` maps to A2A task events over SSE.
- A2A `taskId` identifies the A2A task and task-store entry. When A2A continuation needs native bounded-agent
  correlation, the adapter should pass it through as `session_id` unless the data part provides an explicit
  `session_id`.
- A2A `contextId` maps to A2A task context, and can be included in native message metadata when needed.
- User text parts map to `intent` for query-builder and `query` for retrieval.
- The first A2A data part carries structured native request fields. The current retrieval adapter passes `queries`,
  `steps`, and `max_internal_iterations`; the query-builder adapter passes `table` and context. Future adapter work
  should forward the rest of the bounded-agent fields generically instead of adding per-field special cases.
- Query-builder results are emitted as a `query` artifact.
- Retrieval progress and results are emitted as task status/artifact events.

The adapter should stay thin:

1. Validate/normalize A2A message shape.
2. Build the native OpenAPI request.
3. Call the native Antfly route or execution boundary.
4. Translate the native result into A2A status and artifacts.

It should not duplicate query planning, retrieval strategy selection, tool policy, or generation behavior.

## Agent Card

The A2A agent card is served from:

- `GET /.well-known/agent-card.json`

The reusable A2A dispatcher emits:

- `protocolVersion`: `0.3.0`
- `preferredTransport`: `JSONRPC`
- `capabilities.streaming`: `true`
- `capabilities.stateTransitionHistory`: `true`
- input/output modes for text and data
- skills registered by `protocol_adapters.zig`

When adding a native agent, add one A2A skill only after the native OpenAPI route and tests exist. The skill descriptor
should name the capability, not an implementation detail.

## Relationship To MCP

MCP is for deterministic tool calls:

- table/index/document management
- raw `QueryRequest` execution
- table and index introspection
- `describe_query_request`
- `describe_mcp_capabilities`

A2A/native agents are for bounded natural-language workflows:

- query planning
- strategy selection
- retrieval tool selection
- clarification
- generation and answer quality steps

The handoff should be explicit:

- Agents that need exact query execution can call MCP `query` with raw `queryRequest`.
- Agents that need natural-language query planning should use A2A `query-builder` or native
  `/agents/query-builder`.
- User-facing retrieval experiences should use native `/agents/retrieval` or A2A `retrieval`.

## Frontend Guidance

Antfarm and SDK clients should build on the native retrieval endpoint, not on a separate `/agents/chat` contract.

Expected UI behavior:

- Allow full-text, semantic, hybrid, tree, graph, and metadata retrieval where the table/index context supports them.
- Use `max_internal_iterations=0` for deterministic pipeline mode.
- Use `max_internal_iterations>0` only when the user explicitly enables agentic behavior or the product flow requires
  tool selection.
- Render `questions` when `status=clarification_required`, then continue with `decisions`.
- Render `steps` and retrieval SSE events as the reasoning/tool trace.
- Treat `done` as authoritative final state.

The frontend may keep local chat history, but native continuation should still use structured `decisions` for bounded
agent state.

## Implementation Rules

1. Add native behavior to the OpenAPI schema first.
2. Regenerate generated clients and servers with `make generate`.
3. Implement behavior in the native Zig agent module.
4. Add native JSON/SSE tests before adapting A2A.
5. Add A2A skill wiring only as a thin adapter.
6. Update ARD catalog entries when a new skill should be discoverable.
7. Update SDK and frontend callers to use the generated fields instead of hand-rolled request shapes.

Do not add:

- new `websearch` or `search` tool aliases
- per-agent status enums
- hidden server-only session state as the only continuation source
- unbounded `max_internal_iterations`
- answer-agent-only behavior
- frontend-only retrieval tools that bypass native agent policy

## Test Plan

Native agent tests:

- query-builder returns legacy `query` and modern `query_request`.
- query-builder returns `retrieval_query_request` for tree plans.
- query-builder clarification can continue with `decisions`.
- retrieval pipeline mode executes full-text, semantic, hybrid, tree, graph, and metadata requests.
- retrieval agentic mode honors `max_internal_iterations`.
- retrieval tool policy intersects top-level `tools` with `steps.retrieval.tools`.
- retrieval rejects unavailable tools and invalid limits.
- retrieval streaming emits valid SSE and final `done`.

Protocol adapter tests:

- A2A agent card lists the expected skills.
- A2A `message/send` routes to query-builder and retrieval.
- A2A `message/stream` emits task status/artifact events.
- A2A `tasks/get` and `tasks/cancel` work with the configured task store.
- ARD catalog exposes the A2A agent card and relevant skill artifacts under the same auth/tenant visibility rules.

Generation checks:

- `make generate`
- `make zig-generated-check`
- focused Zig tests for `query_builder_agent.zig`, `retrieval_agent.zig`, `protocol_adapters.zig`, and OpenAPI
  contract coverage.

## Long-Term Direction

The long-term shape is one native bounded-agent API family with protocol adapters around it:

1. Keep `/agents/query-builder` as the executable query planning surface described in `QUERY_BUILDER.md`.
2. Keep `/agents/retrieval` as the retrieval, generation, clarification, and evaluation surface.
3. Add future native agents only when they need a distinct state machine or artifact type.
4. Keep A2A skills aligned one-to-one with native agents.
5. Keep MCP focused on deterministic tools and raw query execution.
6. Keep ARD as discovery over native agents, A2A, MCP, OpenAPI, and skill documents.

This gives Antfly one production agent model while still meeting clients through their preferred protocol.
