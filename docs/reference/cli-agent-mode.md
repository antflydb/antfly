# CLI agent mode

Agent mode gives external coding agents a versioned output contract across
Antfly's remote client commands. It is independent of the built-in `antfly
agents` workflows. Server startup, Lite administration, hot standby, inference
and the separate Cloud CLI retain their own interfaces; presentation options
on those routes fail explicitly.

## Selecting the contract

```sh
antfly --agent capabilities
antfly --agent schema --database default --namespace public --table docs
antfly --agent sql --statement 'SELECT count(*) FROM docs'
antfly --agent query --table docs --full-text-search antfly --limit 10
antfly --agent lookup --table docs --key doc:1
```

Global options belong before the command, or immediately after it and before
its local options. They are never extracted from SQL, document values, or
other command-local arguments. For example, `antfly sql --agent --statement
'SELECT 1'` is supported; put `--agent` before `--statement`, not after it.
Duplicate global options are errors.

Selection precedence is:

1. `--agent` or `--no-agent`.
2. `ANTFLY_AGENT_MODE=1|true|0|false` (case-insensitive words).
3. A nonempty, non-false `CODEX_THREAD_ID`, `CLAUDECODE`, `CURSOR_AGENT`, or
   `CLINE_ACTIVE`, combined with stdout being a pipe or file.
4. Ordinary output.

Ordinary output uses human-readable tables by default across remote CLI
commands. SQL uses its declared columns, search shows IDs, scores and selected
source fields, lookup shows a field/value view, and schema lists qualified
tables, columns and indexes. Retrieval workflows print the answer and a source
table. `COLUMNS` sets the terminal width (default 100, bounded to 40–240).
Scores use four decimal places. Long values are abbreviated and omissions are
marked; SQL NULL and JSON null remain distinct. Mutation outcomes and recovery
receipts remain visible in full.

`--output table` explicitly selects the human presentation. An explicit
`--output json` preserves the original result shape and pretty encoding;
`--output compact` preserves that shape with compact encoding. These JSON
formats bypass presentation budgets. Agent error handling remains enabled
when an agent explicitly selects another format. `--output auto` selects the
default presentation for the chosen mode. Existing command-local `--output
json` and `--verbose` selections on table/index commands, and `backup --list
--output json` (or `-o json`), retain their original JSON shape. Local selections
use the same policy validation as global flags, including fingerprint conflicts. Outside recognized agent contexts, pipes also receive human output;
scripts should request `--output json` explicitly.

File-producing commands use a separate artifact contract. `internal store-root
proof` always writes the original signed JSON document, without a table, agent
envelope, fingerprint, or display sampling. Its stdout can be redirected directly
to the file consumed by `internal store-root enroll --file` in every presentation
mode. Stdout delivery failures still exit nonzero and use the agent stderr
error contract when enabled.

Human tables share the row, byte and cell display settings below. Values may
be visibly shortened to fit the terminal; `--output json` retrieves complete
received values. Opaque administrative responses preserve unknown recovery
fields even when those exceed display budgets.

`--help` remains concise human-readable usage and never needs a server.
`capabilities` provides the small, deterministic local CLI contract; it does
not assert that a remote deployment implements a feature.

## Results and completeness

Default agent results are compact JSON followed by a newline:

```json
{"agent_contract":1,"execution":{"status":"complete","scope":"sql","result_scope":"sql"},"display":{"omitted":false,"returned_rows":1,"displayed_rows":1,"limits":{"rows":20,"bytes":16384,"cell_bytes":2048},"budget_exempt":false,"retrieve":null},"control":{"rows_affected":0,"command_tag":"SELECT"},"data":{"columns":[{"name":"count","type":"integer"}],"rows":[["42"]]}}
```

Read `execution` and `display` independently:

- `execution.status=complete` means the requested operation completed. SQL
  results cover the admitted statement result. Search uses
  `result_scope=requested_top_k`, **not exhaustive enumeration of all matches**.
- `execution.status=partial` marks failed search operations or reported shard
  failures. The process also exits nonzero. Successfully received results may
  still be present.
- `execution.status=unknown` is used for operations without an authoritative
  completeness adapter. The formatter never infers engine completeness from
  result size.
- `display.omitted=true` means presentation omitted otherwise received data.
  It does not mean the server stopped scanning or that a query LIMIT applied.
- `returned_rows` counts received display units, not all matching records.
  `displayed_rows` counts units retained in `data`. Opaque operations use null
  counts. Search's original `hits.total.value` and `relation` are retained
  when their containing payload fits; `gte` remains a lower bound.

SQL `--limit` still bounds **admitted result rows**, defaults to 128, and fails
on overflow. It never inserts a SQL LIMIT. Display defaults are independently
20 rows/documents, 16,384 encoded bytes, and 2,048 encoded bytes per cell:

```sh
antfly --agent --display-rows 8 --display-bytes 8192 --display-cell-bytes 1024 \
  sql --statement 'SELECT id, title FROM docs'
```

The byte ceiling includes the envelope and newline. The minimum is 1,024
bytes. Oversized cells cause their whole row/document to be omitted; retained
values are never shortened or replaced with fake values. SQL integer strings,
JSON values, SQL NULLs and their ordinal `sql_nulls` masks remain aligned and
exact. Budgets preserve a prefix, so a single large first document can yield
zero displayed documents. Exceptionally large metadata may leave `data=null`.

Retrieval, research, query-builder and durable research-job results use the
`workflow` scope. Answers, reports, source arrays and context are display data:
oversized values are omitted whole and source arrays retain a bounded prefix.
Workflow lifecycle controls (`status`, `job_id`, `state`, `phase`, `advances`)
and session identities remain in `control`, even when the report is omitted.
Workflow responses do not claim SQL-style row totals or result fingerprints.

For read-only retrieval, narrow SQL projections or use query `--fields`.
`--output json` retrieves the original complete received response without
presentation caps. Do not replay a mutation to retrieve omitted RETURNING
rows: reconcile the receipt, then issue a separate read. `display.retrieve`
repeats this guidance.

`control` carries SQL command tags, row mutation counts, transaction/session
identities, mutation outcomes and DDL receipts. These fields are never
sampled. Receipt size can exceed the requested byte ceiling, with
`budget_exempt=true`. Opaque administrative/mutation responses also remain
whole and explicitly exempt, because arbitrary response shape cannot prove
which fields are safe to discard. An acknowledgement confirms only the API
operation; committed-pending or repair-required outcomes remain authoritative
in the response. The remote CLI disables automatic transport retries and redirect following.
There is no automatic replay of uncertain mutations.

## Errors and delivery

Agent failures emit one JSON error envelope on stderr and exit nonzero:

```json
{"agent_contract":1,"error":{"code":"HttpFailure","category":"http","http_status":503,"execution":"unconfirmed","retryable":false,"hint":"Inspect diagnostics and reconcile mutation receipts before retrying; antfly --help describes global options.","diagnostic":{"code":"40003","transaction_id":"00000000000000000000000000000000","retryable":false}}}
```

Argument, transport, HTTP and SQL failures share this envelope. SQLSTATE,
source position, transaction state, receipts and authoritative retryability
are retained when supplied. Null retryability means unknown, not retryable.
Authoritative SQLSTATE `57014` (cancellation/deadline) and `54000` (program
budget) mark execution `stopped`; transport timeouts remain `unconfirmed`.
Partial query failures use `partial`. Only explicit input-validation boundaries
use category `argument` and execution `rejected`; generic runtime failures remain
`unconfirmed`. Error-name prefixes such as `Invalid` or `Unknown` are not proof
that an operation was rejected. Catalog commands preserve HTTP status and safe
server recovery fields through the same HTTP diagnostic path as other commands.
Server prose, parameter values and credential-bearing URLs are not echoed
into error hints. Argument templates may contain redacted format placeholders.
A transport failure never asserts that a mutation did not commit. Failed
stdout writes terminate with failure; a pipe closing is not successful delivery.
For a received structured response, a delivery error preserves its known
execution status, sets `retryable=false`, and includes unsampled recovery
fields in `error.diagnostic.control`. Opaque operation responses are also kept
whole in `error.diagnostic.outcome`. These fields are available with all output
formats in agent mode. Execution completion describes the received request;
`mutation_outcome` still determines whether a transaction is committed-pending
or needs reconciliation. Delivery failure never causes replay.

Status messages also use JSON Lines on stderr. Requested `--help` output and
interactive external authentication programs keep their own human interface.
Agent SQL is one-shot; interactive SQL is rejected. Built-in agent workflows
default to non-streaming responses, rendered as prose and source tables for
humans or JSON in agent mode. An explicit `--streaming` preserves
its SSE transport rather than applying JSON display budgets. Bulk load emits
complete batch acknowledgements and final statistics as separate JSON records,
so consumers of multi-result commands must read stdout as JSON Lines.

## Schema and plans

`schema [--table NAME]` projects qualified database/namespace/table identity,
JSON schema column types, index names/types, and the server's effective field
capabilities. It excludes provider configurations and prompts. Tables, columns
and indexes are sorted deterministically. Counts are not manufactured when
metadata does not provide them. Existing `table` and `index` reads use the same
agent writer and display policy. Table names remain literal; dots do not select
a namespace.

```sh
antfly --agent sql --explain --statement 'SELECT id FROM docs WHERE id=$1' \
  --parameters '["doc:1"]'
```

`--explain` requests the existing `EXPLAIN (FORMAT JSON)` dry-run planner.
Agent output unwraps the plan JSON into `data.plan`, marks
`planning_only=true`, and uses execution scope `plan`. Explicit JSON output
retains the original SQL response.

```sh
antfly --agent sql --estimate --statement 'SELECT id FROM docs ORDER BY id'
antfly --agent --progress sql --analyze --statement 'SELECT id FROM docs LIMIT 10'
```

`--estimate` requests `EXPLAIN (FORMAT JSON, COSTS ON)` for a read-only SELECT.
The engine calls a dedicated metadata-only provider, which cannot open
cursors, execute search, activate tracking, publish state, count or sample rows.
The server reuses its bounded optimizer planning generation, verifies table
identity, and labels the resulting status cardinalities and sizes as estimates.
Native status does not price external-lake inventories or ranked search;
those remain unknown unless a provider supplies suitable metadata. Each
logical plan node contains `estimates`; absent source
statistics and unknown selectivity stay null. The versioned `cost_model`
reports its formulas and excluded work. Units are logical tuple visits or
comparisons: scan N, hash-input N+M, nested-loop N×M, and sort N×ceil(log2 N).
These are neither a calibrated time prediction nor a physical I/O estimate.
Row bounds appear only when structurally proved, such as an identity point
lookup. Metadata estimates never become scan or admission limits. A source
without metadata remains supported with explicit unknowns.

`--analyze` requests read-only `EXPLAIN ANALYZE`, executes SELECT exactly once,
and sets `planning_only=false` in both plan data and execution metadata; the
execution flag survives display omission. The logical plan and execution share
one
immutable authorized binding. Its `runtime` includes measured result rows,
inclusive elapsed nanoseconds, peak execution-allocator memory, spill bytes
written, and bounded per-node pull calls/rows/times. The memory peak includes
execution/result arenas and transient native pages, excluding pre-owned
parameter frames, compiled plans and remote worker allocations. Ordinary scans,
composed
joins, grouping, sorts and recursion retain their normal admission,
cancellation and work controls. Runtime node IDs identify this execution's
operators; they are not stable cross-run IDs. Inclusive times overlap and
must not be summed. Source rows emitted through a SQL boundary do not measure
storage rows scanned; physical row/byte I/O and parallel/provider internals
are explicitly unmeasured. Missing clocks produce null timing, never zero.
A metadata count is recorded separately from rows pulled. Operator exhaustion
flags mean an end was observed for that SQL stream; they do not establish
exhaustive table enumeration across repeated iterator invocations.

Both extended modes reject mutations before row capture or native admission.
`--analyze` and `--estimate` are mutually exclusive: a separate planning capture
and subsequent execution can observe different data. Default `--explain`
retains its non-executing behavior without opening a cursor. No timeout or
measurement enables mutation replay.

## Execution bounds and measured progress

Display budgets only affect presentation. These separate controls require a
server implementing the execution-limit headers introduced with this mode:

| Option | Server control |
| --- | --- |
| `--deadline-ms N` | Remaining request duration; positive, at most 1,800,000 ms |
| `--max-scan-rows N` | Tighten SQL's logical scan-work allowance |
| `--memory-bytes N` | Tighten SQL's retained execution memory allowance |
| `--spill-bytes N` | Tighten SQL's spill allowance |

Work options are SQL-only and fail on other commands. Limits can tighten the
server's existing engine defaults but cannot raise them. SQL preparation and
encoded-result admission retain their independent existing bounds. Budgeted
interactive/session SQL is rejected rather than silently losing controls on
session forwarding.

The CLI sends `x-antfly-timeout-ms`, `x-antfly-sql-scan-rows`,
`x-antfly-sql-memory-bytes`, and `x-antfly-sql-spill-bytes`. Ingress converts the
relative deadline using its own monotonic clock and preserves an earlier
admitted deadline. Public and internal service budgets compose by the earliest
deadline; a public timeout cannot bypass internal-header validation. Routing
and inference consumers translate remaining durations when crossing clocks.
The existing request context carries cancellation and the
deadline into routing, remote shard requests and expensive downstream work.
`X-Antfly-Execution-Limits: 1` acknowledges an accepted timeout header. Old
servers may ignore these headers; capability discovery is local, so do not
use it as proof of remote enforcement.

The deadline is per HTTP request, not a wall-time cap on local file reading,
a bulk load, or a readiness loop. Those commands retain their operation-specific
bounds. Deadlines are cooperative: a mutation already admitted to durable
commit must finish safely, and an interrupted response can require receipt
reconciliation. No timeout authorizes replay.

`--progress` emits start, periodic running, and finish events on stderr with
measured elapsed milliseconds. Query profiles, when available, supply measured
query/shard statistics. A finish event indicates the command ended, not that
it succeeded; inspect the process status and result/error envelopes. No
percentages or ETAs are invented. Analyze progress adds a bounded final
`query_statistics` event with measured runtime totals; per-operator detail
stays in the result. MCP continues to own its existing transport and budget contract;
the CLI reuses authoritative public metadata, not MCP's wire envelope.

## Validation and benchmark

```sh
cd zig
zig build antfly-cli-agent-test antfly-api-execution-budget-test public-limits-test
# Optional paired report using a model tokenizer:
zig build antfly-cli-contract-bin
cd ..
uv run --with tiktoken python scripts/ci/test_cli_agent_contract.py \
  --binary zig/zig-out/bin/antfly-cli-contract --report /tmp/cli-agent-benchmark.json
```

The fixture invokes the real remote CLI runtime and parser. It deliberately
fails local backup staging operations; storage suites exercise those separately.
Tests cover mode/format precedence, argument errors, schema, exact SQL values,
SQL admission, search scope, long documents, output caps, partial/shard failures,
uncertain writes without replay, preserved mutation outcomes, compact plans,
execution headers, progress separation, and failed stdout delivery. API tests
also verify ingress clock conversion, tightened engine budgets, remote scan
cancellation, routing clock translation and deadline/cancellation propagation
across remote shard phases.

The paired benchmark includes schema discovery, aggregation, hybrid search,
long documents, invalid queries, oversized results, partial shards and uncertain
writes. It checks interpretation against known fixture answers and records
output bytes, tokenizer counts when installed, HTTP requests, CLI invocations
and latency. One run is not a latency performance claim. Coding-agent turns,
end-to-end input tokens and cost remain null because this fixture does not run
a coding agent. A deployment-level agent evaluation should measure those
separately against live data and injected shard/commit failures before claiming
end-to-end savings.

## Optional fingerprints

```sh
antfly --agent --fingerprint ordered sql --statement 'SELECT id FROM docs ORDER BY id'
antfly --agent --fingerprint multiset query --table docs --full-text-search antfly
```

Fingerprints cover the complete **received** response before display sampling.
The record identifies `complete_sql_result`, `requested_top_k`,
`lookup_document`, or `discovery_response`; it never claims enumeration of all
search matches. Partial or unknown execution, opaque operations, and plan
outputs receive an unavailable record with no digest. Hashing uses BLAKE3-256
with a versioned, tagged, length-prefixed encoding. Object keys sort by UTF-8
bytes; array order and number-token spelling are preserved. SQL columns/types
and each cell's SQL NULL flag participate, distinguishing SQL NULL from JSON
null and the string `"null"`. Integer strings remain exact. Ordered mode
preserves row order; multiset mode sorts unit digests and retains duplicate
multiplicity. Search response/table boundaries and aggregations participate;
transport timing and match-total estimates do not.

The snapshot scope is one received response with no cross-request snapshot
guarantee. Different digests can reflect changed data, ranking, ordering,
projection or types. They are comparison aids, not mutation reconciliation
receipts. Fingerprint selection requires default/table output so explicit
`--output json|compact` keeps the original response shape unchanged.

## Model-backed evaluation

```sh
uv run --with tiktoken python scripts/ci/eval_cli_coding_agent.py \
  --runner claude --binary zig/zig-out/bin/antfly-cli-contract \
  --artifacts .recordings/cli-agent-eval --report .recordings/cli-agent-eval/report.json
```

The evaluator supports installed Codex and Claude CLIs using their configured
models and authentication. It executes the actual Antfly runtime against
isolated fixtures, then asks the coding agent to invoke a read-only replay of
that result exactly once and interpret it. It grades exact answers, omission
and failure recognition, and single-command compliance. It records CLI calls,
requests, tool-output bytes/tokens, runner-reported input/cache/output usage,
turns, retries and latency. Claude-reported cost is a runner estimate; Codex
cost remains null when its events do not supply a monetary amount. Failed
runner startup supplies no model evidence and must not be scored as a model
interpretation failure.

This is a controlled interpretation evaluation with fixed fixture state and
one sample per case. It does not measure autonomous database exploration,
production shard behavior, cache-independent latency, or billed cost. The
engine/API cancellation suites separately verify remote execution. Preserve
raw local event traces for auditing and publish only aggregate fixture results.

## Roadmap coverage

| Issue milestone | Implementation and validation |
| --- | --- |
| Output contract | Shared policy, human tables, activation/precedence, bounded compact envelope, exact values, receipt preservation, process failure checks |
| Discovery/recovery | Qualified schema, local capabilities, compact plans, structured diagnostics, admission/display guidance |
| Execution visibility | Tightening headers, ingress/remote cancellation tests, measured progress on stderr |
| Follow-on | Metadata-based algebraic estimates, read-only runtime plan statistics, scoped typed fingerprints, model-backed interpretation evaluator |

Remaining measurement limits are explicit in each contract: unknown statistics
are null; no calibrated ETA, physical I/O attribution, mutation ANALYZE or
cross-request snapshot identity is advertised.

The [local paired report](cli-agent-benchmark.json) compares human tables with
the agent envelope. These are delivery checks against fixtures, not evidence
of end-to-end agent cost savings. Envelope overhead can increase small outputs.

The [recorded model-backed run](cli-agent-evaluation.json) uses the installed
Claude CLI and its configured model. Raw event traces remain local under
`.recordings/`. The initial Codex attempt was rejected because its configured
model was unavailable through its login, and contributes no model measurements.

| Recorded interpretation metric | Human tables | Agent envelope |
| --- | ---: | ---: |
| Requested answers recovered | 8/8 | 7/8 |
| Completeness/omission classified correctly | 8/8 | 8/8 |
| Tool-output tokens (`o200k_base`) | 670 | 1,029 |
| Runner input tokens (including cache traffic) | 96,025 | 96,726 |
| Runner turns | 24 | 24 |
| CLI invocations / retries | 8 / 0 | 8 / 0 |

The missing answer is the long-document field-name question: the agent-mode
response omitted the entire document, and Claude correctly declined to guess
its hidden fields. The evaluation permits one command, so it does not perform
the documented explicit-JSON follow-up. This demonstrates information loss
and the potential need for another turn, rather than a parsing or certainty
failure. Human tables retain abbreviated field labels in that case.

The runner reported list-price estimates of $0.254848 for human output and
$0.131174 for agent output, with substantially different cache creation/read
traffic. Each pair ran human first and agent second, so cache effects are
confounded with mode. These are not billed costs or evidence that agent mode
is cheaper.
Here the agent envelope used more tool-output and input tokens than human
tables. Its benefit is explicit machine-readable execution and omission state.
