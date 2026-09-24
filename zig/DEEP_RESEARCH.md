# Deep Research Agent

The research agent (`POST /agents/research`, `zig/pkg/antfly/src/api/research_agent.zig`) plans a question into
sub-questions, researches them in parallel with the retrieval agent, reflects on coverage, and writes a long-form
report whose `[E#]` citations resolve to a deduplicated evidence registry. It follows the native-agent rules in
`A2A.md`: a bounded state machine, client-carried continuation, OpenAPI as the contract, and A2A as a thin adapter.

## Why a separate agent

`A2A.md` adds a native agent only for a distinct state machine or artifact. Research has both:

- **State machine:** plan, research, reflect (repeat), write, verify. A run can take minutes and must be resumable.
- **Artifact:** a sectioned report plus an evidence registry and citation map, not `hits` plus a short answer.

The retrieval agent is the researcher. Each researcher is an ordinary retrieval-agent run over the caller's
`queries`, so authorization, mandatory predicates, tool policy, and prompt-injection framing are exactly those of
`/agents/retrieval`.

## State machine

| Phase | Work | Model calls | Next |
|---|---|---|---|
| `plan` | Planner writes a brief, up to `max_sub_questions` sub-questions, and success criteria. In interactive mode it may return a `research_scope` clarification instead. | 1 | `research` |
| `research` | Runs every pending sub-question as a retrieval researcher, `max_parallel` at a time, and merges findings and evidence. | per researcher | `reflect` or `write` |
| `reflect` | Judges coverage against the success criteria and adds new sub-questions for the next round. | 1 | `research` or `write` |
| `write` | Writes the report from findings and the evidence index, then the server resolves citations. | 1 | `verify` or `done` |
| `verify` | Optional model check that cited evidence supports each statement. | 1 | `done` |

Reflection is on by default when `max_rounds > 1`. It also runs after the last round, so a run that ends with
coverage judged insufficient reports `incomplete` with reason `max_rounds` and still writes the report.

A planner reply that is not a valid plan still researches the original question as one sub-question.

## Researchers

A researcher request is built from the caller's raw request JSON, so caller predicates reach every researcher
byte-for-byte:

- `query` is the sub-question; `queries`, `accumulated_filters`, `tools`, `max_context_tokens`, and
  `reserve_tokens` are copied unchanged.
- `agent_knowledge` gets the research brief appended.
- `steps.generation.system_prompt` asks for one JSON object: `summary`, `claims` with `sources` (hit `_id`s or URLs),
  and `open_questions`.
- `max_internal_iterations` and the tool-call budget are carved from what the run has left after reserving the model
  calls that later phases need (reflections, the writer, and the verifier). A sub-question the budget cannot fund is
  marked `skipped` and the run reports `max_llm_calls` or `max_tool_calls`.
- A successful researcher is charged the usage it reports; a failed one is charged its whole allocation, because it
  may have spent all of it before failing. Retries are funded only from what remains after those charges, so reported
  usage never undercounts and never exceeds the budget.

Researchers run through `retrieval_agent.executeWithOptions` with a lent `agent_tools.Budget`. With a server runtime
(`QueryRunner.io`) they run concurrently in a `std.Io.Group`; each job owns an arena backed by the thread-safe
allocator, and its encoded result is copied into the request arena after the batch joins. Without a runtime they run
sequentially with identical results.

`queries` are best given as table scopes (table, indexes, fields, filters). Researchers then plan their own query for
each sub-question with `build_query`. A caller query with fixed search text is executed as written, which is right for
a scoped corpus and wrong for broad research.

A researcher whose tool loop fails at the model level (for example a small local model emitting tool-call syntax the
inference runtime cannot parse) is retried once without tools. The retry keeps the caller's tool policies with web
tools removed; if removing them would empty an `enabled_tools` list (which would mean "no restriction"), there is no
retry. It runs the declared queries directly, and a bare table scope gets a full-text `match` on the sub-question text
only when both policies allow `full_text_search`. A failed retry is a failed finding, not a request error.

Retrieval reports each hit's source table (`ExecuteOptions.hit_tables`) and deduplicates by table and key, so equal
keys from different tables stay separate evidence.

## Evidence and citations

The registry dedups by `(table, doc_id)` for table hits (a bare key resolves to the first table's document;
`table/key` is always unambiguous) and by URL for web and fetched pages; a fetched page replaces
the same URL's search snippet. Evidence IDs (`E1`, `E2`, ...) are stable within a run and across resumes. Claim
sources resolve through hit `_id`, URL, or evidence ID, and unknown references are dropped.

The writer sees findings and a bounded evidence index (per-snippet length shrinks as evidence grows). After writing,
the server rewrites every `[E1, E9]` group to keep only IDs in the registry, records the rest in
`verification.unresolved_markers`, lists sections with no resolvable citation in `verification.uncited_sections`,
and renders a Sources list of the cited evidence. Ordinary brackets such as `[link](url)` are untouched.

## Budgets and deadlines

`ResearchBudget` declares every limit up front and is validated against its schema ranges. Counters are cumulative
over `research_state.usage`, so resuming a run, or advancing a job, cannot exceed the declared worst case.
`deadline_ms` bounds one synchronous call or one job advance; the handler sets it as the request context deadline, so
every model call, query, web search, and fetch checks it. A run that hits its deadline returns `incomplete` with
reason `deadline` and a resumable `research_state`.

## Execution modes

- **Synchronous:** `POST /agents/research` as JSON or SSE. SSE reuses the retrieval event names; `step_progress`
  carries `phase` values `plan`, `sub_question_started`, `finding`, `reflection`, `section`, and `verification`.
  An incomplete run (deadline, budget, `max_rounds`) ends with `done` only, carrying its `research_state` and any
  partial report; `error` is reserved for failures.
- **Client-carried continuation:** the result's `research_state` (plan, findings, evidence, reflections, report,
  usage, and never raw transcripts or credentials) resumes the run at its phase. Every resumed request is
  re-authorized.
- **Durable jobs:** `POST /agents/research/jobs` stores the request (streaming and interactivity forced off) and
  optionally runs phases immediately. `POST .../advance` runs up to `max_phases` phases and checkpoints after each
  one while holding its lease, so a crash in a later phase keeps the completed ones;
  `GET` returns the job with its latest result; `POST .../cancel` stops it. The client or the CLI (`--job`) drives
  advances, which keeps every pass bounded and restart-safe without a background scheduler.

Durable job details (`research_jobs.zig`):

- Jobs are owned by the authenticated username. Other principals get 404, and IDs are 128-bit random.
- Each advance claims an attempt with a lease that covers the longest permitted pass. A concurrent advance gets
  409; a pass whose lease expired is superseded and its late checkpoint is discarded.
- Cancellation during a pass wins over a non-terminal checkpoint.
- Stored requests must not carry inline API keys; generators reference the secret store, the environment, or server
  connections.
- An owner may hold 16 active jobs. Records expire after seven days.
- Local standalone mode stores jobs in `<replica_root>/api-research-jobs`, and an interrupted attempt is queued again
  on restart. Lite keeps jobs in memory until it has a reserved namespace for them. In a multi-node cluster a job
  lives on the node that created it.

## Fetch

Deep web research needs full pages, so the retrieval agent's `fetch` tool (`web_fetch.zig`) is implemented:

- Opt-in: `fetch` in `enabled_tools` or a `fetch_config`.
- A URL is admitted only if `web_search` returned it in the same run or its host is under
  `fetch_config.allowed_hosts`. Model-invented URLs, including returned URLs with an added query string, are
  rejected, so injected text cannot exfiltrate data through a fetch.
- Downloads use the shared remote-content client with private-address blocking forced on, per-hop redirect
  validation, and size and time ceilings. Requests cannot disable the blocking or pass object-store credentials.
- HTML is reduced to visible text (scripts, styles, and templates dropped; entities decoded); plain text, markdown,
  JSON, and XML pass through; binary content is rejected.

## Retrieval changes this required

- `agent_tools.Budget` replaces the literal 20-call caps in the model-directed loop; `Conversation.limit_bytes` lets
  a nested run lend a smaller history ceiling.
- `executeModelTools` takes a `ModelToolContext` and a `ModelToolState` instead of sixteen positional parameters.
- Tool calls within one assistant turn stay sequential on purpose. Whether a result fits the shared context
  budget depends on earlier results, and a call the budget stops must never reach the provider. Concurrency comes
  from running researchers in parallel, each with its own budget.
- Tool-result context is budgeted in estimated tokens (`agent_tools.estimateTokens`: about four ASCII bytes per
  token, one token per non-ASCII code point). For ASCII payloads the limit is the same as the former byte budget.
- The HTTP generation runner checks cancellation and the deadline before every model round, and agent handlers can
  set the deadline from a declared budget (never beyond a transport deadline).

## Clients

- CLI: `antfly agents research` (synchronous, SSE or JSON) and `--job` / `--resume-job` for durable jobs.
- Go SDK: `ResearchAgent` with typed stream callbacks, job helpers, and `RunResearchJob`.
- TypeScript SDK: `researchAgent`, `streamResearchAgent`, job helpers, and `runResearchJob`; `@antfly/components`
  adds `useResearchStream` and `ResearchReport`.
- Python and Rust SDKs are generated from the spec.
- evalaf: research evaluators for citation coverage, citation precision, sub-question coverage, and evidence
  diversity.
- A2A: the `research` skill maps onto the same native agent.

## Open work

- A server-owned driver that advances queued jobs in the background, with per-tenant concurrency limits in
  `request_admission_policy.zig`.
- A reserved Lite namespace for research jobs, and cluster-wide job visibility.
- A tokenizer-backed budget when the generator's tokenizer is available locally.
- Writing reports back into a table so research compounds across runs.
