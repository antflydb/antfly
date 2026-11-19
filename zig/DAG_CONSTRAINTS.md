# DAG constraint implementation boundary

Issue: https://github.com/antflydb/antfly/issues/1637

## Status

This branch implements **validation and authority protocol primitives only**. It
is not an implementation of the complete public DAG feature. `topology: "dag"`
remains rejected by the runtime parser. Do not add it to the public enum until
the integration described below is implemented and qualified.

Implemented sources:

- `pkg/antfly-embedded/src/graph/dag_validation.zig`: final visible-state overlay,
  iterative cycle detection, shared traversal state across batch additions,
  physical-request/work/memory/deadline budgets, bounded witness storage and
  all-or-nothing witness authorization.
- `pkg/antfly-embedded/src/graph/dag_authority.zig`: immutable domain identity,
  exclusive attempt reservation, accepted revision and owner epoch checks,
  retained transaction identity through owner changes, terminal outcome
  transitions, and a checksummed, canonical state codec.
- `pkg/antfly-embedded/src/graph/dag_test.zig`: composition tests demonstrating
  why independently successful traversals cannot authorize opposing commits.

The authority module also provides transactional staging for the accepted
adjacency, owner counts, validation proof and terminal receipts. The proof
binds the staged adjacency and count updates to the reserved attempt and
accepted revision across owner changes. The count reconciler takes effective
changes after source precedence, so an edge remains present when another
effective owner survives. The contribution adapter diffs complete effective
owner manifests against accepted storage, binds those replacements and producer
versions to the attempt, and writes the resulting manifests with the terminal
receipt. Committed replay recomputes the delta at the reserved accepted revision
without a new admission deadline. The local transaction manager can prepare
an ordered set of authority domains atomically with its participant vote.
It persists a versioned candidate packet and resolves the recorded domains in
the same batch as the participant decision, including authority-only participants
and resolution after authority owner changes. Ordinary transactions keep their
original record format and skip authority lookups. The hosted coordinator can
carry an explicitly routed authority participant supplied by the caller. Its
private HTTP and ordered Raft prepare envelopes have distinct version tags and
carry binary contribution identifiers. Prepared graph artifact replay can derive
canonical per-record replacements for a selected index and edge type after the
publisher has resolved contenders. The prepared command can construct a
transaction participant vote bound to the complete publication digest,
constrained domain and transaction identity. A co-located writer can stage
validation, the terminal authority receipt, and publication effects in one
physical transaction. The ordered graph publication writer invokes this path
after its source and contender guards when a co-located binding exists for the
producer incarnation. Binding installation and publication both require a
visible active authority and its activation certificate. Binding provisioning
checks that the activation proofs cover exactly the declared DAG types and
stages every activation certificate with the binding. The private ordered
artifact catalog command can carry these proofs and install them with the
catalog watermark. Catalog changes without new proofs can reuse a binding
only when it covers the declared types and their certified authorities.
The authority persists the scanner's snapshot before its first page, so a
new owner can resume even when no page has committed yet. Physical writers
reject source and artifact changes while a root's activation fence is
installed. Scan progress and completion require that fence. Each root installs
its fence only after draining prepared writers, in the same serialized writer
that excludes new votes. Topology admission and activation fences exclude each
other, and topology drain includes authority-only prepared participants.
The private ordered source-fence command commits acquisition, release, scan
initialization, takeover, or a bounded scan page with its apply receipt. It
rejects delayed acquisitions for released attempts and checks the full fence
identity before staging scan work. An owner takeover preserves the pinned scan
cursor and fences subsequent proposals from the old owner. Pages advance in
physical source order, can contain no relationships, and cannot replace a
contribution imported by an earlier page. A private native lookup returns the
current owner epoch and cursor from one storage snapshot.
A private source-page lookup reads generation-bound graph artifact rows from a
fenced physical root using its owned key range, with bounded row and byte
counts. The caller supplies the source catalog generation and must establish
the owner read barrier before either lookup. The scan driver commits one
page at a time and persists a root-completion cursor even for an empty root,
so resumption uses the authority checkpoint. The fence snapshot digest binds
the ordered roots and source catalog generations for every activated domain.
The ordered writer pins each domain's root count and requires the last
root's terminal cursor before certification.
An active source fence allows an ordered catalog change only when activation
proofs match its pinned snapshot. Raft protocol version 31 gates these commands,
DAG-bearing transaction prepare, and catalog activation proofs. The source-fence
command currently requires a root without asynchronous standby mirroring. The
activation coordinator still needs to execute the protocol across roots,
enumerate the pinned graph, and certify end of input. Regular index admission
and direct graph-index open still reject DAG.
An existing distributed reservation remains under the original coordinator,
and the publication coordinator still needs to enlist the authority under
the source and contender guards. Document-derived and explicit writers still
need to supply their final contributions.

## Constraint domain and authority

The proposed immutable domain is `(table identity, graph index incarnation,
edge type)`. `dag_authority.domain` length-delimits names and excludes shard IDs.
An index replacement gets a new domain. Different edge types have independent
constraints; their union need not be acyclic. Source shard splitting, movement,
or merge must not create new constraint domains.

Use a durable table control owner for the domain, with an explicit metadata
binding to the active consensus owner and epoch. A process mutex on a data shard
cannot substitute for this owner. The existing
`IndexManager.beginGraphPrimaryMutation` fence protects shard-local graph
preparation and publication, but cannot serialize a table-wide constraint.

The domain needs its own authoritative accepted adjacency, maintained alongside
accepted contribution manifests. This avoids scanning a lagging private graph
index and avoids a cross-shard query round trip for every visited edge. The
adjacency is a transaction participant's durable integrity data, not an
asynchronously rebuilt query index. If unavailable or unproven, fail with
`GraphConstraintNotReady`.

The initial concurrency policy serializes all mutations in each domain. Multiple
domain reservations must be acquired in canonical domain-ID order. Later rank
certificates, conditional read sets, or partitioning require their own integrity
proof; neither local reachability nor an arbitrary partition key is sufficient.

## Transaction coupling still required

All relationship producers must pass the same coordinator:

1. Pin index/configuration incarnations and producing source versions. Construct
   complete contribution replacements for the atomic mutation, including all
   records in the batch and field-derived relationships.
2. Reserve the affected domains, binding a stable attempt ID, transaction ID,
   accepted revision and complete candidate digest. Persist reservations before
   relying on them. Retained per-attempt receipts must be checked before starting
   a duplicate attempt; the authority state itself is not an idempotency history.
3. Reconcile contribution ownership and source precedence at the authority.
   Compute the final **visible** adjacency delta over accepted state. Deleting
   one owner cannot remove an edge still supplied by another owner. Reject
   ambiguous/conflicting deltas rather than depending on application order.
4. Validate that final delta. `dag_validation.validate` needs an already certified
   DAG snapshot. Its reader must pin the reserved accepted revision and supply
   complete adjacency. Storage adapters page and cache adjacency, charge actual
   physical requests, cap response bytes before allocating, and honor the
   deadline during blocking I/O. A snapshot/revision change invalidates the proof.
5. Persist the validated reservation before the domain participant votes yes.
   Prepare source changes, accepted contribution changes and artifact reference
   changes in the same distributed transaction. Never let a preflight traversal
   authorize an unrelated ordinary write.
6. Apply the durable terminal decision. Acceptance advances the domain revision
   and logically publishes the matching graph/artifact generation. Rejection or
   abort leaves the previous accepted revision intact. Retain an attempt receipt
   atomically with the resulting state and logical publication references.
7. Install private query projections through managed replay. A lagging projection
   delays readiness; it does not alter which graph generation was accepted.

A synchronous explicit or field-derived violation rejects the entire source
mutation. Generated candidates are asynchronous: their source already committed,
so rejection fails publication without undoing that source.

Reservations have no expiry. After an owner change, recover the original
transaction's authoritative decision. An unknown decision is not an abort. The
value protocol preserves pending reservations and invalidates old owner tokens;
actual ingress must enforce active ownership and actual storage must atomically
persist transitions. Replay must apply the original decision without rerunning a
fresh admission that could strand a committed participant.

## Generated candidates and inspection still required

Graph publication reserves a bounded slot for the latest cycle rejection per
producer/source. Replaying that exact candidate is rejected inside the same
guarded write that would publish it. Transient responses and newer successful
publications preserve the cycle record. The authority still needs to drive this
outcome from an actual cross-shard validation decision and retain inspectable
candidate history under the configured policy. Co-located capacity and
authority readiness failures are recorded as retryable ordered outcomes, so
the apply watermark advances and immutable uploads remain available.

Generated output must first enter private candidate storage, outside the current
accepted artifact namespace. A candidate binds source revisions, producer recipe,
index incarnations, complete contribution replacement, and stable attempt ID.
If one generated artifact feeds several constrained graph domains, all required
acceptances must participate in the same logical publication decision.

The current `artifact_graph_publication.zig` boundary authenticates graph effects
and supplies managed replay, but is source-shard-local. Extending only that path
would leave generated/explicit and generated/generated cross-shard races open.
The asset publication and graph publication boundaries must be coordinated so a
new current artifact cannot become a successful downstream input before its
validated graph generation is logically accepted.

For semantic rejection, retain the candidate and bounded diagnostics under an
operator-defined byte/count/age policy. Inspection must expose attempt identity,
source/config versions, terminal semantic failure, and repairability. Previous
valid output remains available only as explicitly stale. Required current
coverage must fail while this publication is missing. Capacity, ownership and
concurrency failures are retryable and must remain distinct from `GraphCycle`.
Reprocessing creates/fences a new attempt; replay must not resurrect an older
rejected contribution. Garbage collection must retain anything referenced by an
unresolved transaction or an inspectable candidate.

The validator stores at most 64 node IDs and 4096 identifier bytes for a cycle
witness; it omits an oversized path completely. The response layer must check
every node against current authorization and disclose no path if any node is
inaccessible. Do not copy raw witness data to public logs, metrics or errors.

## Activation, lifecycle and runtime admission still required

Existing-state activation needs a bounded, resumable scan of authoritative
contributions, a stable snapshot and persisted traversal/cursor state. Fence
writes or retain/revalidate all admitted deltas during that scan. Do not expose
an enforcing state until the complete existing domain and concurrent mutations
are covered. Invalid existing state needs actionable authorized diagnostics,
without silently deleting edges. Exhaustion leaves activation incomplete.

Index creation/replacement, source deletion, contribution retirement, source
precedence updates and graph TTL cleanup must use the same authority. A logical
removal must consider surviving owners and any promoted source. Index retirement
must wait for pending decisions and fence generation reuse. Query-time visibility
filters cannot define integrity membership.

Backup/restore must include the accepted domain ledger, owner/incarnation
bindings, unresolved reservations, terminal transaction decisions, accepted
artifact references and retained rejection records. Raw import must remain
non-enforcing until bounded activation proves the complete imported graph.
Shard movement must carry source participants while keeping the table-wide
constraint domain intact. Cross-version protocol activation must ensure old
writers cannot bypass admission.

HTTP, native embedded and Lite must share these semantics, including transactions
and transforms. Serverless/lake, replication/import and other runtimes must
explicitly reject DAG admission until they support the complete authority and
publication protocol. Public OpenAPI and generated clients should change only
when an enforcing runtime exists.

## Verification

From `zig/`, using Zig 0.17:

```sh
zig build --build-file build.focused.zig dag-constraint-test
zig build --build-file build.focused.zig dag-constraint-bench -Doptimize=ReleaseFast
```

The tests are also dependencies of the root `unit-test` aggregate. The focused
build avoids constructing the full server/inference build graph.

The tests compare cycle results with an independent Kahn oracle for every
four-node graph and every three-node replacement from an acyclic initial graph.
They also cover self-loops, multi-parent graphs, final-state reparenting,
10,000-node depth, shared descendants, budget dimensions, allocator failures,
reader unavailability, non-progressing cursors, post-read deadline expiry,
witness authorization, codec corruption, and value-level restart/owner changes.

The benchmark emits JSON for deep/wide graphs and small/large replacement
inputs, with median time and tracked work/memory. It uses in-memory accepted
adjacency. It does **not** measure domain queueing, durable commit latency,
network cost, generated publication or real contention. Those require integrated
multi-producer benchmarks once the transaction coordinator exists.

Production qualification remains necessary: opposing cross-shard writes,
generated/generated and generated/explicit races, contribution ownership and
source-precedence integration, invalid-candidate inspection and downstream
blocking, crash injection at every publication boundary, active-owner changes,
shard split/move/merge, resumable activation, backup/restore, runtime admission,
authorization and HTTP/embedded parity. Primitive tests do not establish these
acceptance criteria for issue #1637.

## Local kernel benchmark sample

Measured on 2026-11-17 on the development macOS arm64 host using Zig 0.17
`ReleaseFast`: one warmup followed by seven runs, reporting the median. Fixture
allocation is outside the measured interval; validation and its cleanup are
inside. Physical requests are zero because adjacency is already in memory.
Tracked peak memory covers validator allocations, not the caller's input
buffers, fixture or process RSS. These are observations, not latency targets.

| Input | Size parameter | Visited nodes | Examined edges including delta | Median ms | Peak MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| deep | 100,000 | 100,000 | 99,999 | 16.37 | 13.09 |
| wide | 100,000 | 100,000 | 99,999 | 15.53 | 7.05 |
| replacement | 100,000 | 100,000 | 100,001 | 16.12 | 13.09 |
| shared_replacement | 100,000 | 200,002 | 300,002 | 42.17 | 22.97 |

The shared replacement adds 100,000 branches that converge onto one 100,001-node
chain, while removing the old root-to-tail contribution. Shared traversal state
visits that chain once. This fixture measures final visible-delta validation; it
does not include manifest/precedence reconciliation or generated artifact work.
