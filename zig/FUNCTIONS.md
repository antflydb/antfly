# Functions in the query DSL and SQL

Status: initial implementation on `design/decision-functions`, 2026-10-02.
Antfly and Jev providers, DSL evaluation, SQL decision expressions, completed
MATCH evaluation, and versioned decision asset enrichment are implemented.
Traversal inference, cross-request caching, error-to-NULL policies, and native
SQL record types remain future extensions.

## Goal

Introduce typed expressions shared by JSON queries and SQL. Functions produce
values consumed by projections, predicates, ordering, and aggregations. The
first external functions are typed decisions, described in [DECIDE.md](DECIDE.md).
Full-text and vector retrieval retain their indexed abstractions. Graph queries
evaluate expressions over existing node, edge, and tuple bindings.

Start with registered built-ins and a closed expression vocabulary. Arbitrary
scripts and user-defined functions are outside the initial scope.

Initial decision providers are `antfly` (Antfly inference) and `jev`; both ship
with the initial implementation. OpenAI Decisions is deferred because it is in
private preview. The shared provider interface permits future adapters without
adding them to the initial implementation scope.

## Preferred names

| Concept | Name |
| --- | --- |
| Structured decision built-in in both JSON and SQL | `ai_decide` |
| Single-question built-ins | `ai_choice`, `ai_score`, `ai_probability` |
| Named computed expressions | `compute` |
| Predicate over computed values | `where` |
| Function invocation / computed reference | `call` / `ref` |
| Configured decision service reference | `decider` |
| Implementation inside configuration | `provider` |
| Provider interface / configuration | `DecisionProvider` / `DeciderConfig` |
| Physical decision operator | `DecisionEval` |
| Evaluation scopes | `candidates`, `matches` |

Use identical built-in names across query languages. Preserve `/decide`, the
public proxy route `/ai/v1/decide`, and `noul` in the inference contract.
Query users access Boolean probability through `ai_probability`.

## Shared expressions and binding

```text
Expression = Literal | FieldRef | BindingRef | GetMember | Call
Predicate  = Compare(Expression, Expression) | And | Or | Not | IsNull
```

Distinguish literal strings, stored fields, and computed references. Named
bindings form a dependency DAG; reject cycles, undefined names, and excessive
depth. Add arithmetic and further expression forms as concrete uses require.

Function descriptors declare argument/result types, nullability, required
columns, volatility, execution kind, cost, batching, and concurrency constraints.
Decision binding also validates question names, choice IDs, ordered levels,
provider capabilities, and configured limits before retrieval or model work.

SQL already represents calls in `pkg/antfly/src/sql/ast.zig` and binds typed
programs in `pkg/antfly/src/sql/scalar.zig`. Reuse those semantics where suitable,
while keeping shared expressions independent of SQL parsing. Existing local
built-ins can migrate incrementally to descriptors.

## JSON query surface

Illustrative candidate-scoped query:

```json
{
  "full_text_search": { "match": "refund", "field": "transcript" },
  "filter_query": { "term": "open", "field": "status" },
  "evaluate": {
    "scope": "candidates",
    "candidate_count": 200,
    "compute": {
      "refund_probability": {
        "call": "ai_probability",
        "input": { "field": "transcript" },
        "statement": "The customer asks for a refund.",
        "decider": "support-decider"
      }
    },
    "where": {
      "gte": [{ "ref": "refund_probability" }, { "literal": 0.8 }]
    }
  },
  "limit": 20
}
```

`evaluate` is the explicit stage container. JSON named arguments bind to the
same signature as SQL positional arguments.
Return selected computed values under `_computed`, separate from stored fields.
Required input fields may be fetched without being projected, subject to field
authorization.

Existing `filter_query` promises filtering before scoring. Begin with an explicit
evaluation stage. Later expression predicates can lower to the same operators
once scope and Boolean query-tree interactions are defined.

## Scope and execution ordering

| Scope | Population | Result meaning |
| --- | --- | --- |
| `candidates` | Explicit globally merged retrieval window | Decisions/statistics cover that window |
| `matches` | All rows surviving ordinary predicates | Decisions cover the full qualifying relation |

```text
Indexed retrieval + ordinary filters
  -> global merge and explicit candidate window
  -> fetch authorized inputs
  -> DecisionEval
  -> computed predicate and ordering
  -> offset and final limit
```

Expose the stage's order relative to fusion and reranking. Apply the window
globally, rather than independently per shard. Vector hit evaluation supports
nonzero final offsets: retrieve the candidate window from offset zero and page
after global evaluation. Graph evaluation leaves ordinary vector hit paging
restrictions unchanged. A filtered candidate window may
return fewer than `limit`; initially do not refill implicitly. Report scope,
evaluated population, and whether the window was truncated. Computed counts and
aggregations must identify candidate scope rather than claim table-wide totals.

SQL matches evaluation streams qualifying rows in bounded pages and provider
batches. DSL matches evaluation collects the complete relation within an
explicit `max_rows` budget (at most 10,000) and a 64 MiB expression memory
budget; incomplete retrieval fails before inference. Budget exhaustion
fails the query by default; any future partial mode must expose incompleteness.
Final LIMIT does not become an inference candidate cap. Filtering can stop once
a correct unordered SQL page is determined; ordering and aggregation evaluate
the complete qualifying relation. Projection-only calls can move after LIMIT when semantics permit.

Push independent ordinary conjuncts ahead of inference. Preserve OR, NOT, NULL,
CASE, and conditional semantics. Pulling a cheap predicate from an OR branch
can change results. SQL textual predicate order does not govern inference input.

## SQL functions and lowering

Signatures:

```text
ai_decide(input_text, questions, decider) -> structured answers
ai_choice(input_text, instructions, criteria, decider) -> text
ai_score(input_text, instructions, ordered_levels, decider) -> double
ai_probability(input_text, statement, decider) -> double
```

Initially require statement-constant question specifications and decider
references, supplied as literals or parameters. Bind parameter-dependent shapes
per execution, without mutating shared prepared plans. Changed shapes require
rebinding. JSON is a practical initial SQL representation for structured results
and specifications; still validate the question schema. A native record type can
later improve member access and metadata. Convenience calls return scalar types.

```sql
WITH classified AS (
  SELECT id, ai_decide(transcript, $1, 'support-decider') AS decision
  FROM conversations
  WHERE status = 'open'
)
SELECT id, decision
FROM classified
WHERE CAST(decision -> 'answers' -> 'asks_for_refund' ->> 'noul'
           AS DOUBLE PRECISION) >= 0.8;
```

This example uses matches scope. SQL never introduces a hidden candidate cap;
bounded input requires an explicit limited relation or search operator. A CTE
alone does not guarantee physical materialization.

Extract external calls into `DecisionEval`, which appends result columns for
ordinary scalar evaluation. Avoid blocking provider I/O inside per-row
`Program.evaluate()`. Preserve row identity and join multiplicity through batches.
Evaluate join-dependent calls once referenced columns exist. Initially exclude
external calls from schema/index expressions, constraints, and row policies.

CTE and derived-table expressions evaluate bounded pages with row and byte
ceilings, including predicates and projections. Projection inference runs only
for rows surviving the relation's predicate and OFFSET/LIMIT. Streaming execution
validates nested decision specifications before opening reads. Ordering
expressions evaluate in bounded batches over qualifying rows, including grouped
and windowed results. Reusing a computed CTE column does not re-evaluate it. Native SELECT, aggregate
input, and pull-stream inference also split cursor pages at byte boundaries and
release inference scratch between pages. Independent SELECT projection calls, including grouped and windowed outputs,
run after sorting and pagination; calls used as sort keys remain before sorting
and direct sort aliases reuse the same evaluated output. Window arguments,
partition keys, and window ordering still evaluate over the full input relation.
Top-K owns the required deferred input cells under its existing memory quota.
Tableless SELECT uses temporary evaluation storage for each predicate and
projection, copying only final output values into statement storage. Discarded
provider metadata and normalization scratch do not accumulate across columns;
retained JSON and text outputs remain owned by the statement.

Decision functions also work in ordinary UPDATE/DELETE predicates, UPDATE
assignments, and mutation expressions including MERGE arms,
conflict-update predicates and assignments, and RETURNING. Validate every bound
decision specification and provider before starting mutation reads. MERGE keeps
ordered arm selection lazy, and conflict updates evaluate assignments only for
existing owner rows whose predicates pass. INSERT VALUES evaluates heterogeneous cell programs in bounded decision pages,
releasing provider responses after copying final mutation values. External mutation work uses
row- and byte-bounded decision pages and retains the existing native row and
read-set fences. Assignment inference runs only for rows passing the predicate.
RETURNING uses the same page bounds and releases provider scratch after each
page. Resolve all decision results, including RETURNING, before publishing the native commit;
provider failure never publishes a partial mutation batch.

DSL provider preflight traverses the parsed expression and predicate domains.
Aggregation names and literal JSON contents cannot change validation behavior.
Every call is validated before reads, including calls in untaken branches.
Direct calls in predicates, sorting, and aggregations evaluate in batches;
Boolean branches retain per-row short-circuiting and three-valued NULL logic.
Expression documents use public source visibility for both document and graph
queries: storage revision markers cannot be projected through `_computed`.

Antfly SQL routing uses trusted bound catalog identities. Single-source reads,
including CTEs, self-joins, grouped queries, windows, and pull streams, send the
physical table identity through `X-Antfly-Source-Table`. Multi-table reads and
tableless queries use general inference routing without a table header. Mutation
expressions route through the target table; an INSERT SELECT input is a read
and uses its own source scope. Synthetic relation tables inherit their enclosing
query scope. Routing does not depend on catalog resolution order, client JSON,
or mutable provider state; concurrent statements keep independent scopes. Jev
requests never receive Antfly table-routing headers.

## Runtime contract

- NULL input propagates SQL NULL without invoking a provider. Empty/oversized
  input follows validation; never silently truncate it.
- Provider errors fail the query by default. A future opt-in NULL-on-error mode
  must retain diagnostics and usage. Failure is never a false decision.
- Remote inference is not immutable by default. Volatility, batching, external
  I/O, and concurrency are separate descriptor properties.
- A named computed binding retains one logical result per input occurrence per
  execution for all consumers. Deduplicating equal inputs requires an explicit
  deterministic or execution-stable contract. Do not deduplicate arbitrary
  volatile calls or lose row multiplicity.
- PREPARE, planning, and ordinary EXPLAIN never invoke providers. The existing
  SQL engine does not support EXPLAIN ANALYZE.
- Apply authorization and row policies before sending content. Use shared
  deadlines, cancellation, bounded queues/concurrency, row/token limits, and
  resource admission. Never hold storage locks across provider I/O.
- DSL evaluation metadata reports provider/model identity, rows, batches,
  latency, usage, cache hits, evaluation scope, and candidate truncation. SQL
  EXPLAIN displays DecisionEval without executing providers. Calls remain
  distinct per input occurrence; cross-request cache hits are currently zero.

Cross-request caching needs an explicit policy. Keys cover tenant isolation,
rendered inputs, canonical questions, resolved model/version, configuration, and
rendering version. Prepared plans contain descriptors, not answers or secrets.
Decision-based pagination retains evaluated membership and sort values, or uses
versioned materialization; re-inference per page can change membership.

## Graph and materialization

Reuse expressions over node, edge, and matched-tuple bindings; no new graph node
type is needed. First evaluate completed matches. Traversal-time evaluation is
a later extension requiring frontier batching, depth/work limits, and explicit
reachability semantics. Any future tuple cache must cover all referenced inputs.

Versioned decision enrichment uses an asset producer, preserving the existing
source revision and artifact publication lifecycle. Its frozen configuration
contains `version`, an inline `DeciderConfig`, and `questions`. Outputs retain
source fingerprint, specification hash, requested version, actual model,
answers, and usage. Source/specification changes invalidate the generated
artifact through the existing lifecycle. Model alias upgrades require changing
the version or frozen specification explicitly; they do not silently rewrite
published decisions. Use pinned models and secret references for durable specs.

```json
{
  "name": "support_decision",
  "kind": "asset",
  "field": "transcript",
  "content_type": "application/json",
  "producer": {
    "type": "decision",
    "config": {
      "version": "support-v1",
      "decider": {"provider": "antfly", "model": "your-decision-model"},
      "questions": {
        "refund": {"type": "noul", "instructions": "The customer asks for a refund."}
      }
    }
  }
}
```

For completed graph bindings, set `evaluate.graph_query` to the named MATCH,
request documents, and refer to fields such as `customer.document.transcript`.
Canonical graph results return `computed` parallel to `rows`; compatibility
pattern rows return `_computed`. Evaluation runs after named graph operations
complete; it does not alter traversal or dependencies between graph operations.
Existing graph aggregates cannot be combined with this stage; use its computed
aggregations instead.

Graph evaluation widens only the named MATCH collection. Ordinary retrieval
hits keep their requested offset, limit, and count behavior, including shard
collection and coordinator merging.

## Configuration and supported boundaries

```json
{
  "deciders": {
    "support-decider": {
      "provider": "antfly",
      "model": "your-decision-model",
      "max_rows": 10000,
      "max_input_tokens": 1000000,
      "batch_size": 32
    },
    "jev-decider": {"provider": "jev", "model": "jev-latest"}
  }
}
```

Jev defaults to `https://api.typesafe.ai/v1/systemone` and resolves credentials
from configured `api_key` (including secret references) or `TYPESAFE_API_KEY`.
Antfly inherits the inference URL or uses the embedded provider callback;
explicit URLs select HTTP. Provider rate limits reuse shared provider quotas.
Both adapters preserve row alignment and validate complete distributions. The
input token budget reserves the serialized request byte count conservatively
before inference; reported usage retains the provider's actual token count. The
portable subset uses text input and instructions, string choice descriptions,
and 2–64 ordered score levels for Antfly or 2–10 for Jev.

DSL evaluation rejects cursor pagination, simultaneous reranking/pruning,
and ordinary aggregations. Approximate vector retrieval cannot use matches
scope. These combinations fail explicitly; indexed predicates remain in their
existing retrieval stage. A candidates stage does not refill rejected rows.
NULL fields skip inference; empty text is invalid. Provider errors fail rather
than filtering out rows. Provider HTTP failures expose bounded diagnostics.

Worker requests carry the retrieval projection required by coordinator-owned
evaluation, independently of the caller's final projection. Deferred projection
fetches complete stored inputs from local and remote shards; the coordinator
applies the original output fields. Graph MATCH collection uses the evaluation row ceiling independently of its
public return limit, with hydration admission checked against that ceiling.
After evaluation, the original return limit bounds the emitted bindings.
Graph MATCH hydration follows the same rule:
evaluation inputs are fetched independently of returned document fields, which
are applied during response encoding. Text-only source templates are supported for
materialized decisions, including rendered neighbor context. Media and malformed
content parts remain unsupported.

Request admission is bounded, with up to 32 concurrent single-state requests
per batch, shared deadlines/cancellation, per-query row/token budgets, and
8 MiB per transport job and a 1 MiB HTTP response ceiling enforced before
downloading the body. The conservative token reservation happens before
inference; actual usage is checked after each response as well. A failing batch
may already have consumed upstream tokens. No automatic retries or provider
fallback occur.

Implementation lives in `pkg/antfly/src/functions/`, SQL demand evaluation in
`sql/decision_eval.zig`, and the query coordinator in `api/table_reads.zig`.
OpenAPI sources and generated Zig, Go, TypeScript, and Python contracts include
the new evaluation surface.

Validation: `zig build functions-test`, `zig build sql-test`, and the API query
contract tests. The function tests cover both HTTP protocols, credentials,
usage, budgets, cancellation, binding reuse, NULL inputs, candidate analytics,
incomplete matches, graph tuples, public parsing, and materialized provenance.

## Delivery sequence and evidence

1. Expressions/descriptors: type checking, dependencies, capabilities, no I/O
   during planning.
2. Antfly and Jev adapters/DecisionEval: row alignment, batch cardinality,
   cancellation, NULLs, malformed outputs, capability parity, and bounded memory.
3. Candidate DSL/SQL projection and filtering: global windows, binding reuse,
   authorization, Boolean logic, and three-valued SQL semantics.
4. Matches analytics with Antfly and Jev: full population, budget failure,
   aggregation, joins, and unsupported probability operations.
5. Graph-match expressions/materialization: tuple dependencies, invalidation,
   provenance, and pagination. Traversal inference follows.

## References

- [DECIDE.md](DECIDE.md): inference contract and provider integration.
- [MotherDuck prompt_jev](https://motherduck.com/blog/motherduck-supports-jev/):
  structured classification composed with SQL.
- [Postgres function attributes](https://www.postgresql.org/docs/current/sql-createfunction.html)
  and [volatility](https://www.postgresql.org/docs/current/xfunc-volatility.html):
  function metadata guiding planning.
- [Elasticsearch scripts/runtime fields](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-script-query)
  and [reranker retrievers](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/retrievers/text-similarity-reranker-retriever):
  computed values and explicit expensive candidate windows.
