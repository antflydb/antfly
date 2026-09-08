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
