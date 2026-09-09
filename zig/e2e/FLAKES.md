# Zig E2E flakes

Track intermittent failures with their original evidence, the contract being
tested, and repeated validation. A passing soak reduces uncertainty; it does not
establish the cause of a failure that was not reproduced locally. Keep resolved
entries so later failures can be compared with the original signature.

## Known cases

| Test | CI evidence | Fix commit | Status |
| --- | --- | --- | --- |
| `test_retrieval.py::test_retrieval_agent_streaming_fallback_progress` | [PR #657, run 34176604388, job 101914807099](https://github.com/antflydb/antfly/actions/runs/34176604388/job/101914807099?pr=657), head [`bc8f8a20d`](https://github.com/antflydb/antfly/commit/bc8f8a20d34534969decc90813fbcb8f390164f1) | [`47106c1fd`](https://github.com/antflydb/antfly/commit/47106c1fd09e9be5f1e3333363fd77d007813632) | Teardown recovery fixed; original reset cause unknown; 30/30 soak runs passed. |
| `test_backup_restore.py::test_three_by_three_cluster_backup_restore_through_metadata_public_api` | [PR #658, run 34177703845, job 101916669107](https://github.com/antflydb/antfly/actions/runs/34177703845/job/101916669107?pr=658), head [`96bee1e80`](https://github.com/antflydb/antfly/commit/96bee1e80cf115c2dc636ed065a0378d8cfb27f3) | [`1bf7230cc`](https://github.com/antflydb/antfly/commit/1bf7230cc74c37ba4263964719542c210ff9473d) | Write-admission handling fixed; 30/30 soak runs passed. |
| `test_cli.py::test_cli_inline_create_load_wait_query_image_and_rag_pipeline` | [PR #658, run 34177703845, job 101916669107](https://github.com/antflydb/antfly/actions/runs/34177703845/job/101916669107?pr=658), head [`96bee1e80`](https://github.com/antflydb/antfly/commit/96bee1e80cf115c2dc636ed065a0378d8cfb27f3) | [`1bf7230cc`](https://github.com/antflydb/antfly/commit/1bf7230cc74c37ba4263964719542c210ff9473d) | Readiness assertion fixed; 30/30 soak runs passed. |
| Same CLI pipeline, retry-exhaustion phase (`settled_failure is not None`) | [PR #659, run 34182855053, job 101932868141](https://github.com/antflydb/antfly/actions/runs/34182855053/job/101932868141), head [`51ec7a551`](https://github.com/antflydb/antfly/commit/51ec7a551aa3ac5713eb243155a9e2c9d8cfa0ac) | [`19b988108`](https://github.com/antflydb/antfly/commit/19b9881080af1dbc805f9ad5bda096b62bf063b1) | Reproduced in 3/3 concurrent runs with real retry sleeps; corrected-budget soak passed 9/9. |
| Same three-by-three backup test, initial table create | [PR #664, run 34263167199, job 102199089027](https://github.com/antflydb/antfly/actions/runs/34263167199/job/102199089027?pr=664), merge `d6108b73b85a8e77dfcb740d5518279b2a51d826` | This change | Read waiter clock and pre-admission handling fixed; 100/100 Debug soak runs passed. |
| `test_quickstart.py::test_public_quickstart_query_string_boolean_controls` | [PR #657, run 34296218257, job 102299245250](https://github.com/antflydb/antfly/actions/runs/34296218257/job/102299245250?pr=657), head `292e5ec9c` | This change | Deterministic fixture mismatch reproduced 9/9; fresh stateful restart fixture passed 30/30 final soak runs. |
| `test_standby.py::test_standby_streams_public_writes_restarts_and_rejects_writes` | Same #657 job | This change | Live replication startup wait passed 30/30 ordinary and 30/30 delayed-fetch runs. Delayed first fetch reproduces the pending-durability 503 without the wait; original CI delay was not observed locally. |

### Quickstart restart fixture and HA replication startup (#657)

The job reported 376 passed, five skipped, and two failures. The quickstart
Boolean-query assertions passed, then accessing `backup_api.supports_restart`
raised `AttributeError`: that fixture does not expose a restart lifecycle.
The test now uses the existing `stateful_api` restart contract and requests a
fresh process. All query assertions still run before and after the local
restart, without restarting a module-shared runtime.

The HA case failed at the first document write after the bootstrapped standby
restarted with continuous replication enabled. The primary returned HTTP 503,
`write committed locally; standby durability acknowledgment pending`. That is
a post-commit outcome and must not be retried as an unadmitted write.

The fixture waited for `/readyz`, but that endpoint does not promise a completed
upstream replication round. Bootstrap and restart also restore `received_lsn`
and `applied_lsn` before the background replication loop connects. The test now
waits for a successful live round (`last_success_ns`), no current replication
error, and the expected applied LSN before issuing synchronous writes after
either restart. A successful round includes the upstream status acknowledgement.
The wait uses the existing 20-second observation budget, caps each read request
by its remaining time, fails on process exit, and reports the last snapshot
plus both nodes' logs. Write success, applied data, remote durability, restart
recovery, and rejection of standby writes remain required. Production policy
and the two-second synchronous acknowledgement budget are unchanged.

The new `test_standby_replication_startup.py` regression forwards authenticated
HA requests through a local proxy that delays only the first replication fetch
by three seconds. Disabling only the live-round requirement reproduces the
exact pending-durability 503; enabling it passes the complete original HA case.
Fast harness tests distinguish restored progress from a live round, retain
applied-LSN requirements, reject unsuccessful replication, bound requests, and
fail immediately on process exit.

This establishes the missing startup precondition. The original Linux CI log
contained only primary logs, so it cannot establish what delayed that standby's
first acknowledgement. All 69 unmodified local HA repetitions passed. A passing
soak or injected startup delay does not prove the original CI stall's internal
cause. HA fixture failures now emit both nodes' logs for future comparison.

Validation on 2026-09-08 (America/Los_Angeles), macOS ARM64, based on merged
`origin/main` commit `50e923cb5`, using one unchanged native Debug executable:

- Native build: 27/27 steps passed. Executable SHA-256:
  `051121877ef7fe6c5230c00138cc9f0b1b990ffac68a6a4c8a93387bfa0e26e9`.
- Baseline mixed soak: three workers × three repetitions; quickstart failed
  9/9 with `AttributeError`, while HA passed 9/9. Additional HA baseline:
  six workers × ten repetitions, 60/60 passed.
- Corrected proxy comparison: one failure with the live-round check disabled,
  one pass with it enabled, using the same executable and three-second delay.
- 133 fast harness and scheduler checks passed.
- Final mixed soak: **90/90 passed**, three workers × ten repetitions of each
  of the two original cases and the delayed-fetch regression (30 per case).
- `make fmt`, Ruff checks on the three HA test files, and `git diff --check`
  passed. The quickstart file has an unrelated pre-existing broad-exception
  lint finding outside this change.

The final mixed soak uses the repository regression loop from the worktree root:

```sh
SKIP_BUILD=1 ANTFLY_E2E_ENV_LOADED=1 \
ANTFLY_E2E_REGRESSION_WORKERS=3 ANTFLY_E2E_REGRESSION_REPEATS=10 \
ANTFLY_E2E_PRESERVE_FAILURE_LIMIT=2 \
scripts/ci/zig-e2e-regression-loop.sh \
  e2e/antfly/test_quickstart.py::test_public_quickstart_query_string_boolean_controls \
  e2e/antfly/test_standby.py::test_standby_streams_public_writes_restarts_and_rejects_writes \
  e2e/antfly/test_standby_replication_startup.py::test_standby_waits_for_delayed_first_replication
```

Local evidence is retained in `/private/tmp/antfly-pr657-*.log`, including
`baseline-soak`, `ha-baseline-soak`, `delayed-before-corrected`,
`delayed-fixed-proxy`, and `fixed-soak`. The initial proxy prototype omitted
the GET identity handshake and its failed runs are excluded from the comparison.
Linux CI remains the cross-platform validation.

### Retrieval streaming teardown

The retrieval assertions passed, then the reusable fixture's DELETE failed with
`ConnectionResetError(104, 'Connection reset by peer')`. The old cleanup made one
attempt and discarded the server diagnostics. The CI artifact contained only the
executable, so it cannot establish whether that reset was a transport failure or
a server failure.

Cleanup now retries an idempotent DELETE at most three times within its existing
30-second request budget. A retry may return 404 when the original DELETE
succeeded but its response was lost. Process-exit checks run before and after
requests; exited servers and HTTP errors still fail. Exhausted transport errors
include bounded server logs and process status. Recovered transport failures are
printed in the soak log rather than silently discarded.

Deterministic harness tests cover resets, successful deletion before response
loss, deadline/attempt exhaustion, HTTP errors, and server crashes. The original
CI reset has not been reproduced naturally in the local soak.

The review of #660 also found that lock contention could escape this cleanup
deadline. Follow-up [`5e05d2314`](https://github.com/antflydb/antfly/commit/5e05d2314a6555a73f5dfa20766f5587b42d6f6e)
bounds lock acquisition by the remaining time and rechecks the deadline before
DELETE. Regression cases cover lock timeout, late acquisition, and retry-sleep
overshoot, including lock release on failure. A further 30/30 retrieval teardown
soak runs passed with the existing main-based executable.

### Three-by-three backup seeding

The first document batch returned HTTP 503 with `write unavailable`, after the
test observed three healthy voters and a known leader for each shard. That
metadata observation does not hold a lease on the current data leader or routing
catalog. The public write API explicitly distinguishes this pre-commit
unavailability from ambiguous and post-commit outcomes.

The fixture now seeds documents through a bounded admission loop that retries
only the exact `503 write unavailable` response. Transport failures, other HTTP
errors, ambiguous transactions, and pending durability acknowledgements remain
failures. The original assertions still verify every seeded document, all three
shard payloads in the backup, and restored documents through every data node.

Harness regressions cover eventual admission, retry classification, deadline
diagnostics, and process exit. The specific CI rejection has not been
reproduced naturally in the local soak.

### Three-by-three backup table creation: unknown outcome

The #664 recurrence failed earlier than the seeding case above: the initial
`POST /db/v1/tables/metadata_leader_backup_<unique suffix>` returned HTTP 409,
`table mutation outcome is unknown; observe table state before retrying`.
The job reported 352 passed, five skipped, and this one failure. Its failure
log did not contain the cluster diagnostics needed to locate the failure.
The test now attaches all six server log tails when that initial create fails.

Investigation found that `MetadataHttpService.ensureLinearizableReadWithContext`
ran a **ticking** Raft round on each iteration of its 1 ms polling loop. The
runtime also has a dedicated cadence driver. Read traffic could therefore
advance elections, heartbeats, and virtual time independently of elapsed time.
A captured stack showed a routing read executing this path; the accompanying
runtime diagnostics reported virtual time well ahead of elapsed time.

The fix uses the existing progress-only Raft operations in
that read loop, including pending-update synchronization. The dedicated ticker
continues to own election and heartbeat time. ReadIndex requests, quorum
requirements, request deadlines, and the E2E create-success assertion remain
unchanged. Unknown outcomes remain non-retryable; this fix does not turn a 409
into success or replay a possibly committed mutation.

The local investigation also exposed socket pressure under three concurrent
six-node clusters: `AddressUnavailable` in data control rounds and Python
`EADDRNOTAVAIL`, with 45,747 TCP sockets in `TIME_WAIT`. Changing the fixture's
5 ms ticks to the runtime's 100 ms defaults did not solve the issue: that probe
reproduced the create 409 near the forwarding deadline. No cadence override
change is included in the fix.

A deterministic regression starts without a leader and gives a read waiter a
50 ms deadline. Before the fix, that waiter advanced virtual time from zero to
3,800 ms. With the fix, it times out without advancing time or electing itself.
The test then advances the dedicated cadence driver, verifies leader election,
and completes a ReadIndex request without any further virtual-time advance.
This proves the clock ownership bug; the original CI log alone cannot establish
which internal timeout produced its 409.

The first Debug soak then exposed a distinct initial-create failure:
`503 metadata_leader_unavailable`. The server's
`metadataMutationNotAdmittedResponse` marks this response with
`X-Antfly-Metadata-Mutation-Not-Admitted: true`, a stronger guarantee than a
leader-routing hint. Even a quorum-backed leader observation cannot reserve
mutation authority for a subsequent request. The fixture now honors this
pre-admission contract within the original 30-second create budget, using a
one-second backoff and the remaining budget for each request.

Retry requires HTTP 503, that explicit non-admission marker, the exact
`metadata_leader_unavailable` code, and `retryable: true`. A contradictory
unknown/committed outcome marker forbids retry. Transport errors, unmarked
503s, ambiguous 409s, and process exits still fail. The create must return a
successful response, and every replication, backup payload, and restore
assertion remains. Recovered admission attempts are printed in the soak log;
failures retain the last status, headers, body, and all server log tails.

### CLI image readiness

`index wait --until searchable-artifacts=1` succeeded, but the following source
coverage assertion saw `covered == 0`. Query-visible vectors and the asynchronous
source census have independent publication points. The searchable-artifact wait
contract checks queryability and visible vectors, not source coverage.

The test now checks the matching milestone at each stage: at least one queryable
vector after the searchable-artifact wait, then exact source outcomes after
complete readiness. The later assertions still require one covered source, two
skipped sources, zero failures, and a successful image query. No wait deadline
was increased and no source-coverage assertion was removed from final completion.

The specific premature assertion did not fail naturally in the local soak.

### CLI provider retry exhaustion with real backoff

This is a different phase of the CLI pipeline; #660 did not fix it. The test
keeps returning HTTP 503 for the ClipClap provider and expects the request to
exhaust its budget, become a per-source failure, and leave the shared enrichment
worker and sibling text index healthy. CI still reported one pending source,
zero failed sources, eight retryable errors, and a live retrying worker when the
30-second assertion expired.

PR #659 restores real sleeps in `enrichment_runtime.zig`. Previously, the
`@hasDecl(std.Thread, "sleep")` guard skipped inline sleeps with Zig 0.16. The
default policy allows six worker attempts, each with six provider attempts:

- Inline delays per worker attempt: 0.25 + 0.5 + 1 + 2 + 4 = 7.75 seconds.
- Five worker backoffs: 0.5 + 1 + 2 + 4 + 8 = 15.5 seconds.
- Total scheduled delay before exhaustion: 6 × 7.75 + 15.5 = **62 seconds**,
  before HTTP, storage, or scheduler overhead.

The assertion now has a named, finite 90-second allowance for that policy. It
still requires a terminal source failure, repeated provider calls, a healthy
shared worker and text index, and later successful image work. The test reports
elapsed retry time and provider-request count. The CLI fixture stops the server
during teardown and defers the directory cleanup decision until the completed
teardown report, so the regression script can retain failed runtime roots and
logs, including failures in the module's last fixture teardown.

The unchanged 30-second test failed in all three workers against the existing
PR #659 executable, reproducing the CI signature. This demonstrates a test budget
that was incompatible with the real retry schedule, not a need to remove
production backoff or weaken per-source failure isolation.

With the fix, all nine runs passed against that same executable. Each exhausted
36 provider requests in 62.63–63.21 seconds, matching the scheduled backoff.

## Related unit failure

The same main-based branch also fixes
`db repair issue list exposes algebraic generation debt as repairable` from
[run 34177703845, job 101910688344](https://github.com/antflydb/antfly/actions/runs/34177703845/job/101910688344?pr=658)
in [`d9e095ed9`](https://github.com/antflydb/antfly/commit/d9e095ed9a96dd764ff3967b18bf812a08579b86).
A temporary 300 ms activation hook reproduced the exact `indexes_rebuilt`
assertion on main. The functional test now uses the existing 5-second completion
budget; the 250 ms production policy is unchanged. The injected case passed
before removing the temporary hook. Twenty subsequent repetitions of the repair
test and the production deadline test passed (40 test executions), using
`zig/tools/run_bounded_zig_build.py` while the E2E soak ran.

## Soak record

2026-09-07 (America/Los_Angeles): fixes are based on `origin/main`
[`43fda0ba4`](https://github.com/antflydb/antfly/commit/43fda0ba4684163a3ee563f18fd4ad61849003cf).
Local validation ran on macOS ARM64; the cited CI jobs ran on Linux x86_64.
The native ReleaseSafe `antfly` build passed all 27 build steps. The fixed E2E
tests are committed at [`1bf7230cc`](https://github.com/antflydb/antfly/commit/1bf7230cc74c37ba4263964719542c210ff9473d).

- Initial mixed-load check using the existing PR #658 executable: three workers,
  three repetitions of each case, **27/27 passed**. This included the teardown
  recovery change; the backup and CLI tests were still unchanged.
- Fixed main-based checkout: three workers, ten repetitions of each case,
  **90/90 passed** (30 per case), with no recovered cleanup transport errors.
- Fast harness, scheduler, and metadata leader-discovery regressions:
  **101 passed**.

From the repository root, after building `zig/zig-out/bin/antfly`:

```sh
SKIP_BUILD=1 \
ANTFLY_E2E_ENV_LOADED=1 \
ANTFLY_E2E_REGRESSION_WORKERS=3 \
ANTFLY_E2E_REGRESSION_REPEATS=10 \
ANTFLY_E2E_PRESERVE_FAILURE_LIMIT=2 \
scripts/ci/zig-e2e-regression-loop.sh \
  e2e/antfly/test_backup_restore.py::test_three_by_three_cluster_backup_restore_through_metadata_public_api \
  e2e/antfly/test_cli.py::test_cli_inline_create_load_wait_query_image_and_rag_pipeline \
  e2e/antfly/test_retrieval.py::test_retrieval_agent_streaming_fallback_progress
```

The script preserves failed worker logs and the first two failed runtime roots
per worker with this configuration. Record the tested commit, worker/repetition
counts, failing node IDs, and preserved diagnostics when adding a new result.

### Follow-up validation after #660

2026-09-07 (America/Los_Angeles), macOS ARM64, based on #660's squash merge
[`3ce736ee6`](https://github.com/antflydb/antfly/commit/3ce736ee67a547e196423480ed6a079576a93582):

- Cleanup deadline fix [`5e05d2314`](https://github.com/antflydb/antfly/commit/5e05d2314a6555a73f5dfa20766f5587b42d6f6e):
  **105 fast regressions passed**; the retrieval case passed **30/30**
  (three workers × ten repetitions) using the existing main-based executable
  from `.worktrees/fix-ci-repair-retrieval/zig/zig-out/bin/antfly`.
- CLI retry budget fix [`19b988108`](https://github.com/antflydb/antfly/commit/19b9881080af1dbc805f9ad5bda096b62bf063b1):
  **3/3 failed before**, **9/9 passed after** (three workers × three repetitions
  after the fix). Both used the existing PR #659 executable from
  `.worktrees/std-io-migration-audit/zig/zig-out/bin/antfly`, exercising real
  retry sleeps. The 105 fast regressions also passed with this change.
- Review correction [`d058ddb2f`](https://github.com/antflydb/antfly/commit/d058ddb2ff4e478f1b539774cf3938a3d711cf9d):
  **113 fast regressions passed**, including eight real-pytest lifecycle cases
  covering setup, call, final teardown, earlier-test failures, preservation
  settings, successful cleanup, and directory cleanup errors. These tests use
  the real CLI fixture and server shutdown method without launching a server.

Both soaks used `scripts/ci/zig-e2e-regression-loop.sh` with `SKIP_BUILD=1`,
`ANTFLY_BIN` set to the executable above, and the corresponding test node ID.
These are local macOS results; Linux CI remains the cross-platform check.

### Follow-up validation for #664

2026-09-08 (America/Los_Angeles), macOS ARM64, based on `origin/main`
[`fea3e6611`](https://github.com/antflydb/antfly/commit/fea3e66111a3f39f8cc95a8c71e31d6e30f2dc5f),
in `.worktrees/fix-metadata-backup-create-flake`:

- The native **Debug** build passed. Executable SHA-256:
  `65576850a6cbb3d934c8d158138a39dfa7d16f3cab79d80d7c5e0dcadf03611f`.
- **75 metadata service tests passed** in Debug, with zero leaks, including
  the read waiter clock regression. **114 Python harness, scheduler, and
  leader-discovery checks passed**.
- Original 5 ms fixture cadence, three workers × ten repetitions:
  **29/30 passed**. No unknown-outcome 409 recurred. Worker 2, iteration 2
  failed at initial create with the explicit pre-admission JSON response
  `503 metadata_leader_unavailable` for
  `metadata_leader_backup_1788900153771408000` through data node 4.
  This exposed the admission handling gap addressed next; this initial result
  is not a clean soak.
- A temporary probe removed only the fixture's `--raft-tick-ms` and
  `--control-tick-ms` overrides, retaining the original test assertions.
  At the runtime's 100 ms defaults, **9/9 passed** (three workers × three
  repetitions). Before the fix, this probe failed 9/9 with five initial-create
  unknown-outcome 409s. That earlier executable was ReleaseSafe, so the E2E
  comparison also changes optimization mode; the deterministic clock
  regression supplies the isolated evidence for the production bug.
- With the admission helper, **132 fast checks passed**, including 18 new
  cases covering safe admission retry, successful 200/202 responses, deadline
  exhaustion, backoff overshoot, process exit, and refusal to replay unmarked,
  malformed, conflicting, transport-failed, or ambiguous outcomes.
- Final original-cadence Debug soak with the admission helper:
  **100/100 passed**, four workers × 25 repetitions. No initial-create retries
  occurred in this batch; the deterministic harness cases exercise the
  recovered 503 path. The executable is unchanged from the initial Debug soak.

Build and correctness commands, from `zig/`:

```sh
python3 tools/run_bounded_zig_build.py --zig zig -- build antfly -Doptimize=Debug -fincremental
python3 tools/run_bounded_zig_build.py --zig zig -- build lib-metadata-test -Doptimize=Debug -- metadata.service.
```

Final original-cadence soak, from the worktree root (the initial 30-run batch
used three workers and ten repetitions):

```sh
SKIP_BUILD=1 ANTFLY_E2E_ENV_LOADED=1 \
ANTFLY_E2E_REGRESSION_WORKERS=4 ANTFLY_E2E_REGRESSION_REPEATS=25 \
ANTFLY_E2E_PRESERVE_FAILURE_LIMIT=2 \
scripts/ci/zig-e2e-regression-loop.sh \
  e2e/antfly/test_backup_restore.py::test_three_by_three_cluster_backup_restore_through_metadata_public_api
```

Local logs, the temporary comparison sources, and the failed six-node runtime
root are retained under the worktree's ignored
`.benchmark-results/metadata-backup-flake/` directory. In particular,
`soak-debug.log` records all 30 original-cadence outcomes and
`soak-debug-runtime-cadence.log` records the nine comparison outcomes.
`soak-debug-100.log` records all 100 final passing executions.
The probe sources are not part of test collection. Linux CI validation remains
outstanding.
