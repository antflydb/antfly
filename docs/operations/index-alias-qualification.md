# Index alias deployment qualification

Follow-up to #1680 and #1687. The deployment tests are in
`zig/e2e/antfly/test_index_alias_qualification.py`.

The result oracle uses two external two-dimensional embedding indexes with
opposite nearest-neighbor sets. Each query must report one concrete target and
return exactly that target's independently specified document IDs. An alias
name in the resolved targets, a mixed result set, or a stale target after an
acknowledged swap fails the test. This isolates catalog/routing correctness
from model inference quality; model-owned query embedding remains covered by
the native alias planning tests.

## Scenarios

- Three metadata nodes and three data nodes, with three replicated shards:
  seed with a full-index barrier, then sixteen A/B/A cutovers overlap independent query and full-index ingestion
  loops. Kill the metadata leader during the workload, query every data gateway
  while it is absent, restart it, then replay an earlier idempotent operation
  after rollback and verify the replay does not reactivate its old target.
- A native object table backed by an actual MinIO S3 service: six cutovers
  overlap reads and ingestion, followed by a crash/restart and another swap.
  Verify objects exist in the bucket independently of the process cache.
- An empty portable whole-instance HA seed with synchronous RemoteApply:
  create and populate the table after bootstrap, replicate alias creation,
  conditional deletion and recreation, restart the standby, fence the primary,
  promote without data loss, query the retained target, and reject another
  mutation at the fenced primary.

Read retries are bounded and require explicit retryable availability codes or
an explicit catalog-binding conflict. Swap retries use the same idempotency
key and only retry documented pre-admission readiness/catalog conflicts.
Transport errors and unknown mutation outcomes fail the qualification.

## Running

Build the current native executable and focused regression suites from `zig/`:

```sh
ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" python3 tools/run_bounded_zig_build.py --zig zig -- build antfly antfly-common-config-test antfly-index-alias-provenance-test antfly-index-alias-http-test antfly-index-alias-hierarchy-test runtime-error-identities-test -Doptimize=debug
```

From the repository worktree, with `minio` and `aws` on PATH and permission to
bind loopback sockets:

```sh
ANTFLY_BIN="$PWD/zig/zig-out/bin/antfly" PYTHONDONTWRITEBYTECODE=1 uv run --project zig/e2e/antfly pytest zig/e2e/antfly/test_index_alias_qualification.py -x -s --durations=5
```

MinIO uses disposable local credentials and storage. This checks a real S3
service's object requests and process recovery; it does not certify a specific
cloud account, IAM policy, network topology or model's retrieval quality.

## Regressions exposed

- A coordinator's second public-ID conversion cleared nested `artifact_ref`.
  Decode the public identity and reconcile retained provenance, including
  payload/derivation revisions. Allocation-failure coverage checks cleanup.
- Shard HTTP projection discarded `IndexRebuilding` and
  `IncompletePublishedSnapshot`, turning temporary unavailability into 500s.
  Preserve these typed errors through operations, HTTP and remote clients.
- Public native-object index list and GET requests searched resident shard status and reported
  a permanently unavailable runtime. Observe the incarnation-fenced object
  runtime that owns those indexes; alias admission shares this observation.
- HA classified alias mutations as unknown writes. Admit PUT/DELETE only for
  the whole-instance synchronous metadata stream; legacy table-scoped streams
  and unrelated catalog mutations retain their rejection policy.
- Invalid configurations containing multiple storage engines could overflow
  one-bit intermediate arithmetic. Count in `u8` and reject invalid configs.
- Broad config compilation also exposed a non-exhaustive public-ingestion codec
  after private prepared/catalog WAL kinds were added. Reject all private
  publication kinds at that codec, matching the ingestion receiver, and cover
  that boundary directly.
- Alias metadata was part of native owner descriptors, causing swaps to retire
  warm physical owners and reject delayed captures. Owner descriptors now use
  the physical recipe, preferring the publication's compact query projection
  instead of parsing receipt history. Legacy committed descriptors normalize
  at owner admission; catalog watermarks still reject actual index regressions.
  A regression retains an active lease across swaps and older observations and
  proves the same owner also serves committed apply.
- Native object recipe equality omitted alias pointers and receipts but still
  compared the lifecycle epoch. Exclude that control field too while retaining
  rejection of real recipe changes. Preserve typed descriptor/definition errors
  across independent runtime error domains.
- Object search responses discarded their selected sources when adapting the
  serverless plan to the shared response encoder. Pass borrowed concrete names
  from the pinned execution plan through response metadata; the encoder copies
  and deduplicates them. This avoids another catalog lookup and gives native,
  object and standalone serverless responses the same attribution contract.
