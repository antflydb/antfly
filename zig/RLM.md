# Recursive Language Model Design

## References

- Recursive Language Models paper: https://arxiv.org/abs/2512.24601
- RLM repository: https://github.com/alexzhang13/rlm

## Context

Recursive Language Models (RLMs) treat large context as an external object that
the model can inspect, slice, summarize, and recursively query instead of
placing the whole context in one prompt. The useful idea for Antfly is not a
Python REPL specifically. It is a bounded execution model where an agent can:

- keep large evidence outside the model context window;
- operate on symbolic handles to tables, documents, artifacts, memories, and
  query results;
- decompose a task into smaller model subcalls over selected context slices;
- merge and verify subcall results before producing an answer or artifact.

The production design must stay compatible with Antfly's existing boundaries:

- native agents remain bounded state machines over normal Antfly APIs;
- MCP remains the deterministic tool/query surface;
- A2A and ARD remain protocol and discovery adapters, not separate runtimes;
- extensions can contribute domain-specific agents and memory behavior, but
  should not own the generic recursive execution semantics.

## Placement

RLM-style recursion should be a native Antfly agent capability.

Memoryaf should be a consumer of that capability, not the owner of it. Memoryaf
can specialize recursive execution for memory research, session summarization,
procedural-memory creation, and trace reuse. The same recursive substrate should
also work for docsaf corpora, table retrieval, graph traversal, query-builder
inspection, warehouse-backed corpora, and generated artifacts.

The initial implementation should not embed a general Python REPL in the Zig
server. A Python RLM sidecar is useful for experiments, but the native product
surface should be a constrained, typed agent VM over Antfly operations:

- query;
- fetch;
- slice;
- map over chunks, documents, entities, or query variants;
- call a configured generator;
- write or read artifacts;
- emit a final answer with trace metadata.

WASM extension runtimes may later provide extension-owned recursive handlers,
but core recursion should first be inspectable, bounded, and typed.

## Generic Agent Semantics

`recursive` should be an execution mode, not a retrieval strategy.

A retrieval strategy describes how evidence is selected or combined inside a
retrieval step: full-text, semantic, hybrid, graph, tree, aggregation, rerank,
or fusion. Recursive execution changes control flow. It can use any retrieval
strategy inside each subproblem.

The same distinction applies outside retrieval. Query builder already uses
`mode` as a query-strategy hint (`auto`, `full_text`, `semantic`, `hybrid`,
`filter`, `tree`, `graph`). Future recursive support should not overload that
field. Use a separate execution-control field in new schema work, such as
`execution_mode` or `agent_mode`, and leave domain-specific `mode` fields to
mean what they already mean.

Suggested shape:

```json
{
  "execution_mode": "pipeline | agentic | recursive",
  "queries": [],
  "steps": {
    "retrieval": {
      "strategy": "hybrid",
      "tools": {}
    },
    "generation": {}
  },
  "recursive": {
    "max_depth": 1,
    "max_subcalls": 8,
    "max_concurrency": 4,
    "split_policy": "auto | by_document | by_entity | by_section | by_query",
    "merge_policy": "synthesize | vote | rank | verify"
  }
}
```

The conceptual split is:

- `execution_mode`: who controls the workflow;
- `strategy`: how retrieval is performed inside a step;
- `tools`: what operations the workflow may call;
- `steps`: which pipeline stages run;
- `recursive`: depth, fanout, concurrency, split, and merge policy.

Expected modes:

- `pipeline`: execute declared retrieval and generation steps directly.
- `agentic`: run a bounded tool-selection and refinement loop.
- `recursive`: decompose the task into bounded subcalls over external context
  objects, then merge and verify results.

Recursive mode must obey the same tool policy and iteration limits as agentic
mode, plus recursion-specific limits for depth, total subcalls, concurrency,
tokens, and wall time.

## Generic Extensibility

The substrate should be generic over domain objects and agent outputs. Retrieval
answers are only one use case.

Candidate native use cases:

- retrieval over very large corpora;
- query-builder planning over many tables, indexes, examples, and candidate
  query shapes;
- docsaf document research over sections, attachments, and extracted artifacts;
- graph algorithms that need model-assisted interpretation of neighborhoods or
  paths;
- Memoryaf research over persistent and ephemeral memory;
- artifact extraction where each source document, page, or chunk can be handled
  independently and then merged;
- warehouse-backed corpora where one task spans many hydrated business slices;
- codebase or repository understanding through file, symbol, and dependency
  context handles.

To stay generic, recursive execution should depend on narrow interfaces rather
than retrieval-specific structs:

- context handles identify readable external state;
- splitters partition a context handle into bounded child handles;
- subcall builders construct child prompts or structured requests;
- mergers combine child outputs;
- verifiers validate or preflight the merged result;
- trace writers emit portable execution events.

Each native agent can provide its own splitters, mergers, and verifiers while
sharing the same budget, scheduling, policy inheritance, and trace model.

## Query Builder Role

The query builder should use recursive execution selectively.

It should not become recursive by default. Most query-builder requests are
single-table, single-intent plans where the current coordinator/specialist model
is cheaper, more predictable, and easier to validate.

Recursive query-builder mode is useful when the planning problem itself is too
large or ambiguous for one prompt:

- many candidate tables or indexes;
- large schema and example-document context;
- multi-table or multi-corpus retrieval planning;
- join planning across table pairs, join keys, and execution strategies;
- aggregation planning with nested aggregations, grouping fields, filters, and
  bounded cross-table aggregation descriptors;
- graph/tree query construction that benefits from candidate-plan comparison;
- ambiguous intent where several query families should be explored in parallel;
- repair loops where multiple candidate plans should be generated and preflighted.

In that shape, recursive query builder should decompose planning, not execute
retrieval:

1. collect table, schema, index, graph, and example context once;
2. split the planning problem into candidate strategies or table/index groups;
3. run bounded subcalls that each produce a candidate `QueryRequest` or
   `RetrievalQueryRequest`;
4. run deterministic query-builder validation and runtime preflight for each
   candidate;
5. merge by choosing, repairing, or asking for clarification;
6. return one executable artifact plus a trace of rejected alternatives.

The current native slice implements this as a deterministic candidate planner:
recursive subcalls are specialist planning frames, not retrieval execution.
Each candidate reuses the existing query-builder pipeline, validation, and
preflight path, then the recursive merge selects the highest-scoring executable
candidate and records candidate metadata in `result.plan.candidate_plans`.
Candidate metadata now includes typed `QueryRequest` and
`RetrievalQueryRequest` artifacts when available, warning and preflight error
counts, runtime preflight estimate signals, and join runtime validation flags
so recursive merge decisions are machine-checkable.

Query-builder recursive support should therefore reuse the core recursive
execution substrate, but keep query-builder-specific validation in
`query_builder_agent.zig`. The recursive merge policy for query-builder should
prefer deterministic preflight diagnostics over model preference whenever an
executable result is required.

Join and aggregation support do not change the execution-mode split. They make
query-builder planning richer, but they remain query families or specialist
outputs, not agent execution modes. A future query-builder planner can select
or compare specialists such as `join`, `aggregation`, `join_aggregation`,
`graph`, `hybrid`, and `projection_sort` while `execution_mode` still answers
whether the planner is running as a direct pipeline, a bounded agentic loop, or
a recursive candidate planner.

The important constraint is that join and aggregation candidates must be typed
artifacts, not opaque prompt text. A join-capable candidate should surface the
primary `QueryRequest`, join descriptor, table/key choices, join strategy hints,
and preflight diagnostics. An aggregation-capable candidate should surface the
aggregation tree, grouping paths, bounded materialization/proof assumptions, and
any second-pass scan risk. Recursive mode can then compare those artifacts using
the same candidate-plan contract instead of adding a separate recursive branch
for every query family. The first native shape accepts explicit
`constraints.join` and `constraints.aggregations` objects and emits `join`,
`aggregation`, or `join_aggregation` candidate plans from them. The native
planner also handles conservative natural-language synthesis for common
aggregation and join descriptors when it can bind fields from schema context.
Runtime-backed contexts can validate join-capable `QueryRequest` artifacts
before recursive merge. The HTTP runtime validator uses the distributed join
parser, rewrites and preflights the left side, and preflights the right side
with a bounded result window. Recursive candidate scoring can then prefer
validated join candidates while lightly penalizing high latency heuristics from
runtime preflight estimates. Runtime-backed recursive join candidates can also
ask the concrete distributed join planner for a bounded execution-plan summary
before merge. The HTTP validator prefers a real bounded left-side sample query,
capped to the planner sample limit, and falls back to synthetic
preflight-derived hits when the sample is unavailable. Candidate artifacts
surface the planner strategy, cost, row and memory estimates, stats usage,
shuffle feasibility, broadcast fallback, sample size, and sample source so
recursive scoring can compare its own join-strategy hint against the execution
planner. Aggregation candidates combine runtime second-pass preflight signals
with table and field-catalog statistics for costing. Join candidates combine
related-table metadata, primary-key hints, document counts, shard placement,
runtime preflight, and concrete planner probes so recursive merge can compare
typed join plans using machine-checkable statistics.

## Native Execution Model

The native recursive substrate should expose a small set of structured runtime
concepts.

`ContextObject` identifies an external context source:

- Antfly table or query result;
- document, section, chunk, or artifact;
- graph node neighborhood;
- Memoryaf memory set;
- extension-owned shape or corpus object.

`AgentFrame` is the bounded execution state for one recursive agent call:

- input request and user-visible query;
- current context handles;
- accumulated observations;
- subcall results;
- budget counters;
- decisions and continuation state.

`Subcall` is a child generator invocation over a derived prompt and selected
context object. A subcall may be plain generation, structured extraction,
classification, summarization, or query planning.

`MapSubcalls` runs a bounded set of subcalls across a partitioned context, such
as document sections, graph neighborhoods, candidate query plans, or retrieved
hits. It must require explicit fanout and concurrency limits.

`TraceArtifact` records the structured execution:

- decomposition plan;
- context handles used;
- tool calls;
- subcalls;
- observations;
- merge and verification steps;
- final answer or generated artifact.

The trace should be renderable through retrieval SSE events and storable as an
artifact for evaluation, debugging, and future Memoryaf learning.

## Tool Policy And Safety

Recursive execution has a different failure mode than a linear tool loop:
unbounded fanout. The policy surface must therefore include:

- `max_depth`;
- `max_subcalls`;
- `max_concurrency`;
- `max_internal_iterations`;
- `max_tool_iterations`;
- token budget;
- wall-clock timeout;
- allowed context object types;
- allowed tools per step;
- whether child subcalls inherit, narrow, or override parent tool policy.

Child calls should default to narrowed policy inheritance. A child frame cannot
expand permissions beyond the parent request. Web access remains explicit
through configured web-search connections. Writes, including Memoryaf memory
writes, require explicit enabled tools and normal Antfly authorization.

The runtime should fail closed when limits are reached:

- `incomplete` when budget prevents completion;
- `clarification_required` when the agent needs user input;
- `failed` for execution errors;
- `completed` only when merge and verification produce an answer or artifact.

## Memoryaf Role

Memoryaf is the first natural application of recursive mode.

Memoryaf can add a domain-specific recursive research agent that:

1. searches visible persistent and ephemeral memories;
2. partitions results by source, entity, session, or memory type;
3. runs bounded subcalls over selected memory sets;
4. expands through the memory entity graph when allowed;
5. merges answers with citations back to memory IDs and source references;
6. optionally stores durable procedural or semantic memories from verified
   traces.

Memoryaf should own:

- memory record shape;
- session and agent scoping;
- source-reference semantics;
- memory-specific graph expansion;
- memory write/update/delete policy;
- deciding which traces are worth storing as memories.

Antfly core should own:

- recursive agent mode;
- execution budget accounting;
- subcall scheduling;
- trace event schema;
- common context-object handles;
- authorization and tool-policy inheritance.

This split lets other extensions use the same recursive substrate without
depending on Memoryaf.

## Initial Implementation Path

1. Prototype with an external RLM sidecar that calls Antfly's OpenAI-compatible
   inference endpoint and MCP tools. Use this only to validate workflows and
   collect examples.
2. Add an execution-control field such as `execution_mode: "recursive"` to the
   native retrieval agent request schema behind an experimental flag. Do not
   reuse query-builder's `mode` field for this.
3. Add recursive budget fields and reject recursive mode unless all limits are
   explicit or defaulted to conservative values.
4. Implement a first split policy over retrieved hits: `by_document` or
   `by_section`.
5. Emit recursive trace steps through retrieval SSE before adding persistence.
6. Add a Memoryaf recursive research agent that consumes recursive mode through
   normal extension and MCP visibility rules.
7. Persist trace artifacts only after the event schema is stable enough for
   evaluation.

## Branch Implementation Status

This branch starts the native path by making recursive execution a typed,
bounded agent contract instead of a retrieval strategy:

- `specs/openapi/antfly/metadata.yaml` defines `AgentExecutionMode`,
  `RecursiveAgentConfig`, split/merge policy enums, child tool-policy
  inheritance, and portable context-object kinds.
- `RetrievalAgentRequest` and `QueryBuilderRequest` both expose
  `execution_mode` and `recursive`; query-builder `mode` remains a
  query-family strategy hint, so future `join`, `aggregation`, and
  `join_aggregation` planners do not become execution modes.
- `zig/pkg/antfly/src/api/recursive_agent.zig` contains the shared native
  substrate types (`ContextObject`, `AgentFrame`, `Subcall`,
  `TraceArtifact`) plus conservative recursive-budget normalization, shared
  wall-clock budget accounting, context-object allow-list checks, fanout and
  concurrency calculations, incomplete-reason selection, and typed trace
  artifact construction.
- Retrieval and query-builder entry points validate execution mode before
  doing work. Existing omitted-mode behavior is preserved, contradictory
  controls fail closed, and recursive budgets are normalized before execution.
- Retrieval recursive mode has a first native scheduler: retrieved hits become
  document `ContextObject` child frames, child generation calls summarize each
  frame, and a merge generation call produces the final answer through the
  existing generation runner.
- Child generation runs in bounded concurrent waves capped by
  `recursive.max_concurrency`; trace metadata reports the scheduled concurrency
  while preserving stable subcall ordering in JSON/SSE traces.
- Recursive retrieval now fails closed when scheduler budgets prevent complete
  processing. `max_subcalls` truncation, per-child prompt budget skips, and
  cooperative wall-clock exhaustion return `status: incomplete` with
  `incomplete_details.reason` while still exposing any partial merged answer
  and recursive trace details.
- Per-child prompt budgeting uses Antfly's built-in fixed tokenizer when
  available and records `token_count_method` in subcall traces; a heuristic
  counter remains as a fallback if tokenizer initialization or encoding fails.
- `merge_policy: verify` now performs deterministic retrieval merge
  verification: child context IDs cited by child summaries must be preserved in
  the merged answer. Verification emits a `validation` step and fails the run
  when an otherwise complete merge drops required citations.
- Recursive retrieval results now include a typed `trace_artifact` containing
  the root frame, final status, context object handles, child subcalls, and the
  ordered agent steps. This stabilizes the artifact shape for durable storage
  and evaluation.
- Query-builder recursive mode now has a first native candidate planner.
  Candidate subcalls reuse existing deterministic specialists (`full_text`,
  `filter`, `semantic`, `hybrid`, `graph`, and `tree` when metadata or
  constraints make them applicable), run the existing validation/preflight
  path, and return one selected executable artifact.
- Query-builder recursive candidate plans now carry typed selected artifacts
  (`query_request` or `retrieval_query_request`) plus warnings and preflight
  error counts. Explicit `constraints.join` and `constraints.aggregations`
  produce `join`, `aggregation`, or `join_aggregation` candidates without
  turning those query families into execution modes.
- Query-builder aggregation candidates can also synthesize conservative typed
  aggregation artifacts from intent and schema context for common shapes such
  as `count by status` and `sum amount by customer`. Explicit
  `constraints.aggregations` still win, and inference fails closed with
  warnings when it cannot bind a grouping or metric field. When field catalog
  metadata is available, aggregation synthesis uses groupable/metric field
  roles, field kinds, and aliases such as `revenue` for `total_cents` before
  falling back to field-name heuristics.
- Query-builder join candidates can synthesize conservative typed `JoinClause`
  artifacts from intent and schema context for common shapes such as
  `orders with customers` when a left key like `customer_id` can be bound.
  Explicit `constraints.join` still wins. When related-table catalog metadata
  is available, join synthesis uses it to bind right-table primary keys,
  choose matching left-side foreign keys, and constrain projected right-side
  fields before falling back to heuristics. Recursive `join_aggregation`
  candidates can now carry both inferred join and inferred aggregation
  artifacts.
- `constraints.require_executable` fails closed when a selected query-builder
  artifact has preflight errors or when requested join/aggregation artifacts are
  absent. Metadata-only join candidates still expose typed descriptors and
  diagnostics, but runtime-backed query-builder contexts can now validate a
  join-capable `QueryRequest` through `QueryBuilderRuntimeQueryRequestValidator`;
  when that validator accepts the request, recursive `require_executable` join
  plans can pass preflight with zero candidate errors. The HTTP runtime
  validator is wired to Antfly's distributed join parser for supported
  `JoinClause` shapes, rewrites the left query without the join, and preflights
  both the left and right tables through the normal table-read runtime. The
  returned join preflight summary preserves the left query's selectivity
  meaning while folding right-side shard, result-window, stored-projection,
  vector, rerank, and remote-shard work into the latency signal used by
  recursive candidate scoring.
- Query-builder table context now carries optional left-table and related-table
  document-count hints. HTTP-loaded query-builder contexts derive left-table
  and related-table document counts from metadata snapshot range/group status,
  load related-table schema fields and primary-key hints for join-like tables,
  and recursive join candidate plans expose `join_strategy_hint`,
  `join_recommended_strategy`, `join_strategy_options`,
  `join_left_doc_count`, `join_right_doc_count`, and
  `join_cardinality_heuristic`. Candidate scoring uses those hints to compare
  lookup, broadcast, and shuffle strategy cost scores, prefer low-risk
  small-right lookup or broadcast joins, and penalize unvalidated large
  bilateral lookup joins.
- Runtime-backed recursive join candidates also promote filtered left-side
  result estimates and placement signals from preflight into candidate
  metadata: `join_left_result_doc_estimate`,
  `join_left_result_doc_upper_bound`, `join_runtime_shard_count`,
  `join_runtime_remote_shard_count`, and `join_runtime_feasibility`.
  Candidate scoring rewards runtime-validated low-cardinality joins and
  penalizes remote-shard fanout.
- Runtime-backed recursive join candidates now call an optional concrete join
  planner hook before merge. The HTTP implementation reuses Antfly's
  distributed join planner with bounded real left-side sample hits when a cheap
  runtime query is available, falling back to synthetic hits from runtime
  preflight when sampling is unavailable. Candidate plans expose
  `join_planner_strategy`,
  `join_planner_estimated_cost`, `join_planner_estimated_rows`,
  `join_planner_estimated_memory_bytes`, `join_planner_used_stats`,
  `join_planner_shuffle_candidate`,
  `join_planner_forced_broadcast_fallback`, and
  `join_planner_left_sample_row_count`, and
  `join_planner_left_sample_source`. Candidate scoring rewards agreement
  between recursive join hints and the concrete planner and penalizes forced
  broadcast fallback.
- Related-table join metadata now includes right-side shard count, whether the
  right table is co-located with the left table, and a right-key selectivity
  heuristic derived from primary-key metadata. Recursive join candidate plans
  expose `join_right_shard_count`, `join_right_key_selectivity`, and
  `join_shard_colocation`; each `join_strategy_options` entry also carries
  `feasible` and `feasibility` fields so lookup, broadcast, and shuffle
  strategies can be compared on more than raw document counts.
- Recursive join candidates now promote richer right-key statistics into
  candidate metadata: `join_right_key_field`, `join_right_key_unique`,
  `join_right_key_estimated_distinct`,
  `join_right_key_avg_rows_per_key`, `join_right_key_stats_source`, and
  `join_right_key_stats_confidence`. Primary-key metadata and the `_id`
  document-key convention produce high-confidence unique-key stats, while
  non-primary keys get conservative schema/name-based distinct-count and
  fanout estimates. Join strategy scoring uses those estimates to price
  lookup fanout and distinguish unique lookup keys from lower-confidence
  selective keys.
- Recursive aggregation candidates now promote runtime aggregate-cost signals
  from preflight into candidate metadata, including
  `aggregation_may_scan_full_results`,
  `aggregation_second_pass_doc_estimate`,
  `aggregation_second_pass_doc_upper_bound`, and
  `aggregation_cost_heuristic`. Candidate scoring penalizes full-result
  second-pass aggregation work and larger second-pass document bounds.
- Recursive aggregation candidates now also promote table and field-catalog
  statistics into candidate metadata: `aggregation_group_field`,
  `aggregation_metric_field`, `aggregation_field_catalog_backed`,
  `aggregation_table_doc_count`, `aggregation_bucket_size`, and
  `aggregation_stat_heuristic`. HTTP-loaded query-builder contexts derive
  conservative field metadata from table schema/index metadata, so aggregation
  candidates can prefer catalog-backed group/metric fields and account for
  table size even before runtime second-pass preflight runs.
- Query-builder recursive traces emit `recursive_decomposition`,
  `recursive_subcall`, and `recursive_merge` steps through the existing
  `QueryBuilderResult.steps` envelope; candidate metadata is exposed in
  `QueryBuilderResult.plan.candidate_plans`.
- `AgentStepKind` now has recursive decomposition, subcall, and merge step
  kinds, and both retrieval recursive mode and query-builder recursive mode
  emit those steps through the existing JSON/SSE trace vocabulary.

The branch now covers the production gaps called out in this design: native
recursive execution mode, bounded retrieval recursion, query-builder recursive
candidate planning, typed join and aggregation artifacts, runtime preflight and
planner-backed merge signals, table/catalog aggregate costing, and richer
right-key join statistics.

## Non-Goals

- Do not make `recursive` a retrieval strategy.
- Do not overload query-builder's existing `mode` field with execution-control
  semantics.
- Do not bypass MCP or native Antfly APIs for deterministic query execution.
- Do not give child subcalls broader permissions than the parent request.
- Do not require Memoryaf for generic recursive retrieval.
- Do not run arbitrary local code or a host-process REPL in production core.
- Do not add training or fine-tuning loops before trace capture and evaluation
  are useful.
