# Unified Bounded Agent Session Protocol

## Goal

Define a shared, bounded protocol for Antfly agents:

- retrieval agent
- query builder agent
- future schema builder agent

The protocol should support:

- finite internal reasoning loops
- explicit user clarification turns
- client-carried continuation
- structured intermediate state
- streaming and non-streaming responses
- agent-specific result payloads under a shared session envelope

This is a protocol and product-design change, not a freeform "chat agent" abstraction.

## Current Baseline

This document now reflects the direction already implemented for retrieval and
query-builder:

- shared `status`
- shared `questions`
- shared `steps`
- shared `decisions`
- `max_internal_iterations`
- `max_user_clarifications`
- `session_id` as correlation only

What is still pending is not the shared envelope itself, but the next consumer
of it: schema-builder.

## Why

Antfly now has multiple agent-like surfaces with overlapping needs:

- The retrieval agent already supports bounded tool loops, clarification, shared `steps`, and shared `questions`.
- The query builder should stay mostly one-shot, but it needs bounded clarification when ambiguity materially changes query structure.
- A schema builder should start with a minimal `infer` mode, then grow into bounded refinement/clarification once the base contract is proven.

Without a shared protocol, each endpoint will evolve its own fields for:

- session identity
- statuses
- clarification payloads
- continuation inputs
- reasoning/steps
- usage and iteration accounting

That produces avoidable frontend and backend complexity.

## Design Principles

1. Bounded by default

- Every agent request has finite internal iteration limits.
- Every agent request has finite user-clarification limits.
- Agents return their best draft when limits are hit instead of stalling.

2. Clarify only when output would materially change

- Ambiguity alone is not enough.
- User clarification should be requested only when two plausible outputs differ in meaningful behavior.

3. Propose before asking

- If the agent can form a reasonable draft, it should do so and ask for confirmation or targeted adjustment.

4. Same envelope, different state machines

- Retrieval, query building, and schema building should share a protocol.
- They should not share the same internal step graph.

5. Stateless-first

- Phase 1 should be stateless by default.
- `session_id` should be treated as a correlation id, not as proof of server-side persistence.
- Continuation should be explicit and structured, not inferred from chat history alone.

## Common Session Envelope

### Shared Status Enum

```yaml
AgentSessionStatus:
  type: string
  enum:
    - in_progress
    - clarification_required
    - completed
    - incomplete
    - failed
```

Semantics:

- `in_progress`: Only used for streaming lifecycle or internally persisted state.
- `clarification_required`: The agent cannot safely proceed without a user decision.
- `completed`: The agent reached a usable final result.
- `incomplete`: The agent stopped early but can provide partial work.
- `failed`: Execution failed unexpectedly.

### Shared Limits Object

```yaml
AgentExecutionLimits:
  type: object
  properties:
    max_internal_iterations:
      type: integer
      minimum: 0
      maximum: 20
      default: 0
      description: Maximum agent self-revision or tool-use rounds before stop.
    max_user_clarifications:
      type: integer
      minimum: 0
      maximum: 10
      default: 0
      description: Maximum number of clarification turns the agent may request from the user.
    require_decision_after:
      type: integer
      minimum: 0
      maximum: 20
      description: Force a user-facing decision after this many unresolved internal passes.
```

Notes:

- `max_internal_iterations` and `max_user_clarifications` are the baseline boundedness knobs.
- New agents should not introduce a separate overloaded `max_iterations` field.

### Shared Question Schema

```yaml
AgentQuestionKind:
  type: string
  enum:
    - confirm
    - single_choice
    - multi_choice
    - free_text
    - field_policy

AgentQuestion:
  type: object
  required:
    - id
    - kind
    - question
  properties:
    id:
      type: string
    kind:
      $ref: "#/components/schemas/AgentQuestionKind"
    question:
      type: string
    reason:
      type: string
      description: Why this decision blocks safe completion.
    options:
      type: array
      items:
        type: string
    default_answer:
      type: string
    affects:
      type: array
      items:
        type: string
      description: High-level result areas affected by this decision.
```

### Shared Decision Schema

```yaml
AgentDecision:
  type: object
  required:
    - question_id
  properties:
    question_id:
      type: string
    answer:
      description: User answer, scalar or structured depending on question kind.
      nullable: true
    approved:
      type: boolean
      description: Used for confirm/review steps where the draft may be accepted as-is.
```

### Shared Step Schema

```yaml
AgentStep:
  type: object
  required:
    - id
    - kind
    - name
    - action
  properties:
    id:
      type: string
    kind:
      type: string
      enum:
        - tool_call
        - planning
        - classification
        - generation
        - validation
        - clarification
    name:
      type: string
    action:
      type: string
    status:
      type: string
      enum:
        - success
        - error
        - skipped
    details:
      type: object
      additionalProperties: true
```

### Shared Session Metadata

```yaml
AgentSessionState:
  type: object
  required:
    - session_id
    - status
  properties:
    session_id:
      type: string
      description: Correlation identifier echoed by the server. In Phase 1 this does not imply server-side storage.
    status:
      $ref: "#/components/schemas/AgentSessionStatus"
    iteration:
      type: integer
      description: Current internal iteration count.
    clarification_count:
      type: integer
      description: Number of user clarification turns already consumed.
    remaining_internal_iterations:
      type: integer
    remaining_user_clarifications:
      type: integer
    assumptions:
      type: array
      items:
        type: string
    warnings:
      type: array
      items:
        type: string
    questions:
      type: array
      items:
        $ref: "#/components/schemas/AgentQuestion"
    steps:
      type: array
      items:
        $ref: "#/components/schemas/AgentStep"
```

This matches the current retrieval/query-builder direction more closely than the
older `phase/summary` shape. If we need more detailed phase data later, it can
live in `kind` plus `details`.

### Shared Continuation Contract

Every bounded agent should accept:

```yaml
session_id:
  type: string
  description: Correlation identifier for a bounded interaction.

decisions:
  type: array
  items:
    $ref: "#/components/schemas/AgentDecision"
  description: Decisions or answers provided by the user as part of client-carried continuation.
```

If `session_id` is absent, the request starts a new bounded interaction.

If `session_id` is present, the client is continuing a prior interaction by
sending back structured continuation inputs such as `decisions`, `messages`, or
a future opaque continuation token. Phase 1 should not assume the server has
persisted session state.

## Shared Response Shape

Each agent should return a common session envelope plus an agent-specific `result`.

```yaml
AgentSessionResponse:
  allOf:
    - $ref: "#/components/schemas/AgentSessionState"
    - type: object
      properties:
        result:
          type: object
          additionalProperties: true
```

This should be specialized per agent rather than exposed as a totally generic
object in generated clients.

## Query Builder Mapping

### Why Query Builder Needs This

The query builder should remain mostly one-shot. Multi-turn behavior is useful
only for cases where ambiguity changes the Bleve query materially:

- time windows like "recent", "latest", "active"
- field ambiguity like `published` as status vs date existence
- exclusion or intent uncertainty like "Apple docs" across multiple candidate fields/types

### Request Shape

```yaml
QueryBuilderAgentRequest:
  allOf:
    - $ref: "#/components/schemas/AgentExecutionLimits"
    - type: object
      required:
        - intent
      properties:
        session_id:
          type: string
        decisions:
          type: array
          items:
            $ref: "#/components/schemas/AgentDecision"
        interactive:
          type: boolean
          default: true
        intent:
          type: string
        table:
          type: string
        schema_fields:
          type: array
          items:
            type: string
        example_documents:
          type: array
          items:
            type: object
            additionalProperties: true
        existing_query:
          type: object
          additionalProperties: true
        generator:
          $ref: "#/components/schemas/GeneratorConfig"
```

### Result Shape

```yaml
QueryBuilderAgentResult:
  allOf:
    - $ref: "#/components/schemas/AgentSessionState"
    - type: object
      properties:
        result:
          type: object
          properties:
            query:
              type: object
              additionalProperties: true
            explanation:
              type: string
            confidence:
              type: number
            warnings:
              type: array
              items:
                type: string
```

### Query Builder Defaults

- `max_internal_iterations: 1` or `2`
- `max_user_clarifications: 1`
- `interactive: true`

### Query Builder States

- `infer`
- `validate`
- `clarify`
- `finalize`

## Retrieval Agent Mapping

### Why Retrieval Should Use This Too

Retrieval now shares the common bounded envelope, but it remains the richest
agent shape because it combines planning, retrieval, optional generation, and
clarification.

### Request Shape

```yaml
RetrievalAgentSessionRequest:
  allOf:
    - $ref: "#/components/schemas/AgentExecutionLimits"
    - type: object
      required:
        - query
        - queries
      properties:
        session_id:
          type: string
        decisions:
          type: array
          items:
            $ref: "#/components/schemas/AgentDecision"
        interactive:
          type: boolean
          default: true
        query:
          type: string
        queries:
          type: array
          items:
            $ref: "#/components/schemas/RetrievalQueryRequest"
        messages:
          type: array
          items:
            $ref: "../../lib/ai/openapi.yaml#/components/schemas/ChatMessage"
        accumulated_filters:
          type: array
          items:
            $ref: "../../lib/ai/openapi.yaml#/components/schemas/FilterSpec"
        agent_knowledge:
          type: string
        generator:
          $ref: "#/components/schemas/GeneratorConfig"
        steps:
          $ref: "#/components/schemas/RetrievalAgentSteps"
        stream:
          type: boolean
```

### Result Shape

```yaml
RetrievalAgentSessionResult:
  allOf:
    - $ref: "#/components/schemas/AgentSessionState"
    - type: object
      properties:
        result:
          type: object
          properties:
            classification:
              $ref: "../../lib/ai/openapi.yaml#/components/schemas/ClassificationTransformationResult"
            hits:
              type: array
              items:
                $ref: "#/components/schemas/QueryHit"
            generation:
              type: string
            generation_confidence:
              type: number
            context_relevance:
              type: number
            followup_questions:
              type: array
              items:
                type: string
            strategy_used:
              $ref: "#/components/schemas/RetrievalStrategy"
            applied_filters:
              type: array
              items:
                $ref: "../../lib/ai/openapi.yaml#/components/schemas/FilterSpec"
            usage:
              $ref: "#/components/schemas/RetrievalAgentUsage"
```

### Retrieval Defaults

- `max_internal_iterations: 2` for agentic mode
- `max_user_clarifications: 1`
- pipeline mode can be represented by `max_internal_iterations: 0`

### Retrieval States

- `classify`
- `plan`
- `retrieve`
- `generation`
- `validate`
- `clarify`
- `finalize`

### Retrieval Baseline

The retrieval agent cleanup should be treated as done enough for schema-builder
work:

- shared `status`
- shared `questions`
- shared `steps`
- shared boundedness fields
- stateless continuation model

Further retrieval changes should be incremental cleanup or refactoring, not
another protocol redesign.

## Schema Builder Mapping

### Why Schema Builder Needs This Most

Schema building is naturally iterative:

- field type inference
- indexing policy decisions
- enum vs open string
- strict vs permissive validation
- dynamic vs explicit modeling
- validation against sample docs

This agent is the next consumer of the shared bounded envelope, but it should
not start life as a fully open-ended multi-turn agent. The first version should
be minimal and safe.

### Request Shape

```yaml
SchemaBuilderAgentRequest:
  allOf:
    - $ref: "#/components/schemas/AgentExecutionLimits"
    - type: object
      properties:
        session_id:
          type: string
        decisions:
          type: array
          items:
            $ref: "#/components/schemas/AgentDecision"
        mode:
          type: string
          enum:
            - infer
            - refine
            - explain
        table:
          type: string
        existing_schema:
          type: object
          additionalProperties: true
        example_documents:
          type: array
          items:
            type: object
            additionalProperties: true
        sample_limit:
          type: integer
          default: 25
        goals:
          type: object
          additionalProperties: true
          description: User goals such as searchable fields, exact-match fields, sort fields, or validation strictness.
        generator:
          $ref: "#/components/schemas/GeneratorConfig"
```

### Result Shape

```yaml
SchemaBuilderAgentResult:
  allOf:
    - $ref: "#/components/schemas/AgentSessionState"
    - type: object
      properties:
        result:
          type: object
          properties:
            proposed_schema:
              type: object
              additionalProperties: true
            field_summaries:
              type: array
              items:
                type: object
                additionalProperties: true
            conflicts:
              type: array
              items:
                type: object
                additionalProperties: true
            validation_summary:
              type: object
              additionalProperties: true
            warnings:
              type: array
              items:
                type: string
```

### Schema Builder Defaults

Phase 1:

- default `mode: infer`
- `max_internal_iterations: 1` or `2`
- `max_user_clarifications: 0` or `1`

Later bounded refinement:

- `max_internal_iterations: 3` to `5`
- `max_user_clarifications: 2` to `4`

### Schema Builder States

- `gather_context`
- `infer`
- `validate`
- `revise`
- `clarify`
- `finalize`

## Streaming Alignment

Streaming agents should emit shared lifecycle events where possible:

- `session_started`
- `step_started`
- `step_progress`
- `step_completed`
- `clarification_required`
- `done`
- `error`

Retrieval should keep its richer search/generation events as agent-specific extensions:

- `classification`
- `reasoning`
- `generation`
- `hit`
- `followup`

Query builder and schema builder can start simpler:

- `step_started`
- `step_completed`
- `clarification_required`
- `done`

## Continuation Model

Phase 1 should be stateless:

- request context stays with the client
- current draft/result stays with the client
- assumptions, questions, and decisions are sent back by the client on continuation
- iteration counters are echoed by the server for UX and client bookkeeping

An optional later phase may introduce:

- opaque continuation tokens
- external cache-backed session storage

but Antfly should not assume server-side session persistence as the default architecture.

## What Not To Do

1. Do not make every agent an unconstrained chat loop.
2. Do not treat `messages` as the only source of truth for continuation.
3. Do not require clarification when the agent can safely propose a good default.
4. Do not use one overloaded `max_iterations` field to mean every kind of boundedness.

## Suggested Implementation Order

1. Commit the current cleanup baseline

- retrieval/query-builder/A2A/CLI/SDK naming aligned
- generated-type usage cleaned up
- generator pinning made consistent in submodules

2. Freeze the shared bounded-agent envelope

- treat the current `status/questions/steps/decisions` model as the baseline
- avoid introducing new parallel protocol concepts before schema-builder

3. Schema builder introduction

- add new endpoint on top of the shared envelope
- start with minimal `infer` mode
- add bounded clarification/refinement only after base inference works

4. Frontend unification

- Use one session driver for all agent UIs.
- Agent-specific renderers only for result payloads and question types.

## Open Questions

1. Should schema-builder `field_summaries` and `conflicts` start as flexible objects or strongly typed structs?
2. How much sample data should schema-builder pull by default when `example_documents` are not provided?
3. At what point should schema-builder start asking bounded clarification questions instead of returning an `infer` draft plus warnings?
4. If server-side storage is ever added, should it live in an external cache/service rather than Antfly itself?

## Recommendation

Use:

- one shared bounded-agent envelope
- one shared clarification/decision protocol
- one shared status model
- agent-specific contexts and results
- schema-builder as the next implementation target, starting with minimal `infer`

This keeps the product coherent without flattening retrieval, query building,
and schema building into the same implementation.
