# Zig E2E flakes

## 2026-09-11: main merge and peer endpoint review follow-up (#694)

The main merge preserves the regression targets in the new build modules.
Review reproduced a peer endpoint change being ignored while placement stayed
unchanged; the fix now includes owned node IDs and Raft URLs in the cache
inputs without invalidating on heartbeat telemetry. The merged data-runtime
suite passes 179/179 and Python harness checks pass 73/73. See `../FLAKES.md`
for the deterministic before/after evidence. Fresh 100-per-scenario Linux
acceptance is required; the 300/300 result below belongs to the pre-merge code.

## 2026-09-11: mixed Linux acceptance before the main merge (#694)

The fresh run completed **300/300**, with **100/100 each** for CLI quickstart,
three-by-three metadata backup/restore, and CLI retry exhaustion/restart.
Four workers each ran 25 iterations of all three scenarios, from 02:21:01 to
03:17:04 UTC. There were zero failures, errors, skips, or failed-case retries;
the driver exited 0. These results are from one revision and do not combine
passes from the earlier failed runs.

Production commit `05f96814e` includes the atomic coverage batch, lifecycle and
placement authority fixes, durable Raft replacement/abort fixes, and monotonic
replication progress. Test revision `b63468094` includes complete publication
before exact semantic ranking. The subsequent `574af3586` formatter correction
has an identical Python AST. The Linux x86_64 ReleaseFast executable was built
with a fresh compiler cache and verified SHA-256:
`dfdbe000a2712b11d0919a4dc66888c51f905030d3d14a8a360bee4d5cbc3265`.
The driver used eight allowed CPUs (`0-6,8`) on a pod requesting four CPUs and
limited to eight; these were not dedicated cores. The retry scenario retained
all 36 provider attempts and the real production backoff.

Complete logs, runner script, and machine-checked acceptance manifest are in
`/private/tmp/ci694-replication-acceptance-results.tar.gz`, SHA-256
`f78936d40adbf5f6b96519bdc3a8c871e96531b0f168a838192531326515a9cd`,
verified against the runner archive. Historical failure archives remain
preserved separately. This acceptance result does not establish that unrelated
flakes cannot occur; deterministic regressions and evidence limits are recorded
below. GitHub CI is tracked separately from this controlled Linux soak.

## 2026-09-11: delayed Raft responses amplify metadata replication (#694)

The full `42cb81c6a` run completed **297/300**: quickstart 99/100,
backup/restore 98/100, retry exhaustion 100/100. Complete preserved evidence is
in `/private/tmp/ci694-durable-complete-results.tar.gz`. Its second restore
timeout also showed amplified Raft batches and one lagging metadata replica.

The new mixed Linux run on `42cb81c6a` failed backup/restore immediately in
worker 1. Metadata node 1 quarantined its group after one Ready batch exceeded
the existing hard outbound ceiling at 1,144,753,225 bytes. Preserved logs/state
and native stacks: `/private/tmp/ci694-durable-first-failure.tar.gz`, SHA-256
`70658edd776376a2c5dc2a966b59d6941b0ea9a37b3030200e8d7fe835350657`.

A deterministic regression shows an early acknowledgement resending 31 entries
already in flight. The leader now preserves monotonic replication progress,
ignores stale rejections and prior-term acknowledgements, and uses empty
heartbeat probes to recover a lost append/ack without enlarging its window.
Quorum commit also requires a current-term entry. All 401 Raft library tests
pass; the corrected snapshot-abort history matches the etcd oracle. See
`../FLAKES.md` for before/after evidence and protocol details. This is another
failed acceptance run; the fresh corrected run recorded above passes 100/100
for all three scenarios.

The standalone quickstart failure also exposed an invalid test assumption:
one searchable artifact does not guarantee that Alpha has published ahead of
Beta. A controlled provider gate proves the partial milestone can succeed with
only Beta searchable; complete publication then returns Alpha first. The test
retains its partial milestone check and uses `complete` before exact ranking,
with unchanged wait bounds and no query retries. Complete-query failures now
include readiness and query diagnostics. This probe does not reproduce the
original empty-hit result itself; see `../FLAKES.md` for the evidence limits.

## 2026-09-11: unit CI retention assertion races legitimate consumer progress (#694)

Run `34543627560`, x86 job `103092055515`, failed the provider-restart storage
test because its negative index-progress assertion raced a healthy derived
consumer. The revised test explicitly waits for that consumer, then verifies
that enrichment's independent durable checkpoint retains the failed source
record and that restart generates both searchable embeddings. Three focused
retention checks pass; see `../FLAKES.md` for the original log and contract.

## 2026-09-10: 299/300 identifies replacement persistence and abort outcome defects (#694)

The `9e5b558ce` mixed run finished at 23:57 UTC with quickstart **100/100**,
backup/restore **99/100**, and retry exhaustion **100/100**. Complete logs and the
one preserved failure root are in
`/private/tmp/ci694-authority-complete-results.tar.gz`; the downloaded archive's
SHA-256 matches the runner copy. This remains failed acceptance.

The failed shard's durable Raft history contains an overwritten term-2 prepare
at index 4 on node 4, while nodes 5 and 6 retain the new leader's term-3 no-op.
Review reproduced the missing persistence-watermark invalidation. All three
transaction records are aborted; a second regression reproduced an unknown
prepare outcome escaping despite a confirmed coordinator abort. The fixes bind
persistence to entry identity and preserve the proven abort decision, with
existing bounded stateless retries and unchanged deadlines. See `../FLAKES.md`
for the before/after regressions and exact history. Fresh 100-per-scenario
acceptance must include these fixes as well as the forwarding and placement
review changes below.

## 2026-09-10: review closes forwarding and placement authority gaps (#694)

The `9e5b558ce` mixed soak had a backup seed-write `409 write outcome unknown`
and therefore failed acceptance. The first failure is preserved in
`/private/tmp/ci694-first-final-failure.tar.gz`; the durable transaction records
show abort on all three groups. The subsequent investigation is recorded above.

Independent deterministic review probes reproduced Raft transport starvation
from sharing its executor with forwarded writes, and changed placement bypassing
authority when two peers return the same lifecycle counter. Forwarding now has
separate bounded whole-request admission. Placement reuse compares the exact
plan inputs, including remote member rows and split bootstrap inputs. See
`../FLAKES.md` for the production contract, resource budget, and before/after
regressions. Fresh 100/100 runs for each of the three split scenarios must use
these completed changes; earlier passing subsets do not count.

## 2026-09-10: data-Raft placement requires authority before retirement (#694)

The preserved `sjng4qip` backup failure contained repeated restored-group
admissions followed by `ConflictingDataApplyBatch`. A deterministic data-runtime
regression reproduced destructive retirement from an older empty catalog whose
process-local metadata epoch was larger: group 77 changed from active to absent.
The fix requires a coherent linearizable snapshot for changed local placement,
serializes its acquisition with reconciliation, and leaves unchanged placement
on the cached path. Equal epoch counters from different metadata processes do
not suppress a genuinely authoritative deletion. See `../FLAKES.md` for the
production contract and before/after artifacts.

The prior 294/300 mixed run and 57/60 old-runtime backup diagnostic remain failed
historical runs. Fresh 100-per-scenario acceptance must use the completed
placement-authority fix together with the resident-owner, CDC, backup-ABI, and
Raft-cadence fixes; no old passing subset counts toward that acceptance.

## 2026-09-10: bounded coverage follow-up acceptance (#694)

The `00dd1e4aef` Linux ReleaseFast executable
(`7ef883c0c2928ece1562e62f6518b57316cd0a66628988ec59ae883ce3e3a033`)
ran 100 of each split scenario with four workers and no failed-case retries.
The run ended at 20:42 UTC with **294/300**, not passing acceptance:
quickstart 99/100, 3x3 backup/restore 95/100, retry exhaustion 100/100.
Failures: post-restart RAG resident availability, fatal restore apply conflict,
fatal metadata `NotLeader`, backup timeout, repository teardown collision, and
DELETE conflict. Preserved archive:
`/private/tmp/ci694-bounded-soak-failures.tar.gz`; extracted state:
`/private/tmp/ci694-bounded-soak-state/`.

The next diagnostic run retained that runtime and used the corrected repository
lifetime and native stack capture. It completed **57/60** backup cases. Failures
were metadata `NotLeader`, metadata outbound Ready growth past its hard ceiling
followed by `GroupLeaderUnavailable`, and a seed write with an unknown outcome.
All three native stack captures succeeded. Archive:
`/private/tmp/ci694-diagnostic-failures.tar.gz`; extracted state:
`/private/tmp/ci694-diagnostic-state/`. These diagnostic runs are not combined
with acceptance for subsequent production changes.

Cold repair ownership, CDC scheduling admission, and backup ambiguity transport
have deterministic regressions and fixes recorded in `../FLAKES.md`. Fresh
100/100 acceptance for the complete implementation remains outstanding.

Track intermittent failures with their original evidence, the contract being
tested, and repeated validation. A passing soak reduces uncertainty; it does not
establish the cause of a failure that was not reproduced locally. Keep resolved
entries so later failures can be compared with the original signature.

## Known cases

| Test | CI evidence | Fix commit | Status |
| --- | --- | --- | --- |
| Same CLI pipeline, completion regresses between `index get` and `index list` | [PR #696, run 34428885099, job 102726530711](https://github.com/antflydb/antfly/actions/runs/34428885099/job/102726530711?pr=696) | PR #694 | Reproduced with the original Linux CI executable; delayed source callbacks now recognize completed observations within the same catalog epoch. See below. |
| Same three-by-three backup test, seed batch `409 write outcome unknown` | [PR #694, run 34423487352, job 102714559943](https://github.com/antflydb/antfly/actions/runs/34423487352/job/102714559943) | This change | Reproduced control-executor exhaustion; review follow-up isolates forwarding from Raft transport with bounded admission. Earlier merged-runtime soak: 90/90 passed (60 ordinary, 30 stalled-route); current 100-per-scenario acceptance remains outstanding. |
| `test_index_lifecycle.py::test_serverless_named_embedding_indexes_report_publication_actions` | [PR #692, run 34420585088, job 102704104941](https://github.com/antflydb/antfly/actions/runs/34420585088/job/102704104941?pr=692) | This change | Filesystem GET keeps metadata and payload on one open descriptor across atomic publication; see [deterministic reproduction and validation](../FLAKES.md#serverless-build-status-preconditionfailed-during-publication-692). |
| `test_resolution.py::test_multinode_autograph_resolves_promotes_and_hydrates_entities` | [PR #690, run 34395199129, job 102623777993](https://github.com/antflydb/antfly/actions/runs/34395199129/job/102623777993?pr=690) | This change | Resolver work moved out of refresh; per-group Raft apply deferral preserves healthy progress; see [runtime investigation and validation](../FLAKES.md#autograph-second-document-write-timeout-690). |
| `test_retrieval.py::test_retrieval_agent_streaming_fallback_progress` | [PR #657, run 34176604388, job 101914807099](https://github.com/antflydb/antfly/actions/runs/34176604388/job/101914807099?pr=657), head [`bc8f8a20d`](https://github.com/antflydb/antfly/commit/bc8f8a20d34534969decc90813fbcb8f390164f1) | [`47106c1fd`](https://github.com/antflydb/antfly/commit/47106c1fd09e9be5f1e3333363fd77d007813632) | Teardown recovery fixed; original reset cause unknown; 30/30 soak runs passed. |
| `test_backup_restore.py::test_three_by_three_cluster_backup_restore_through_metadata_public_api` | [PR #658, run 34177703845, job 101916669107](https://github.com/antflydb/antfly/actions/runs/34177703845/job/101916669107?pr=658), head [`96bee1e80`](https://github.com/antflydb/antfly/commit/96bee1e80cf115c2dc636ed065a0378d8cfb27f3) | [`1bf7230cc`](https://github.com/antflydb/antfly/commit/1bf7230cc74c37ba4263964719542c210ff9473d) | Write-admission handling fixed; 30/30 soak runs passed. |
| `test_cli.py::test_cli_inline_create_load_wait_query_image_and_rag_pipeline` | [PR #658, run 34177703845, job 101916669107](https://github.com/antflydb/antfly/actions/runs/34177703845/job/101916669107?pr=658), head [`96bee1e80`](https://github.com/antflydb/antfly/commit/96bee1e80cf115c2dc636ed065a0378d8cfb27f3) | [`1bf7230cc`](https://github.com/antflydb/antfly/commit/1bf7230cc74c37ba4263964719542c210ff9473d) | Readiness assertion fixed; 30/30 soak runs passed. |
| Same CLI pipeline, retry-exhaustion phase (`settled_failure is not None`) | [PR #659, run 34182855053, job 101932868141](https://github.com/antflydb/antfly/actions/runs/34182855053/job/101932868141), head [`51ec7a551`](https://github.com/antflydb/antfly/commit/51ec7a551aa3ac5713eb243155a9e2c9d8cfa0ac) | [`19b988108`](https://github.com/antflydb/antfly/commit/19b9881080af1dbc805f9ad5bda096b62bf063b1) | Reproduced in 3/3 concurrent runs with real retry sleeps; corrected-budget soak passed 9/9. |
| Same three-by-three backup test, initial table create | [PR #664, run 34263167199, job 102199089027](https://github.com/antflydb/antfly/actions/runs/34263167199/job/102199089027?pr=664), merge `d6108b73b85a8e77dfcb740d5518279b2a51d826` | This change | Read waiter clock and pre-admission handling fixed; 100/100 Debug soak runs passed. |
| `test_quickstart.py::test_public_quickstart_query_string_boolean_controls` | [PR #657, run 34296218257, job 102299245250](https://github.com/antflydb/antfly/actions/runs/34296218257/job/102299245250?pr=657), head `292e5ec9c` | This change | Deterministic fixture mismatch reproduced 9/9; fresh stateful restart fixture passed 30/30 final soak runs. |
| `test_standby.py::test_standby_streams_public_writes_restarts_and_rejects_writes` | Same #657 job | This change | Live replication startup wait passed 30/30 ordinary and 30/30 delayed-fetch runs. Delayed first fetch reproduces the pending-durability 503 without the wait; original CI delay was not observed locally. |

### Completed CLI readiness regresses after publication (#696)

The CI job reported 397 passed, five skipped, and two failures: this CLI
readiness regression and the backup seed 409 described below. The CLI test
had already observed complete readiness through `index get`, then found
`observation_complete=false` through `index list` without another source
mutation. Revision and coverage counts remained intact. The
[runtime entry](../FLAKES.md#completed-cli-index-readiness-regresses-after-a-delayed-notification-696-694)
records the callback ordering, epoch-qualified fix, and deterministic tests.

Linux validation uses the original CI artifact from head `5c54729b6` as the
baseline and PR #694 with main merged as the fixed source. The repository's
`scripts/ci/zig-e2e-regression-loop.sh` runs both failing node IDs with three
workers and ten repetitions per worker, pinned to eight CPUs on a disposable
runner with a fresh filesystem. The baseline reproduces both CI signatures.

The baseline finished **47/60 passed**: CLI **29/30**, with one matching
readiness regression; backup **18/30**, with ten seed-write 409s, one completed
restore-progress retirement timeout, and one 30-second HTTP read timeout.
The latter two failures are separate observations, not evidence for the
forwarding executor cause. The original binary lacks the added underlying
transport-error diagnostics. Raw worker logs were retained for comparison.

The exact `3ab5f6aba` executable from passing Linux CI run `34439124254`
then finished **59/60 passed** under the same mixed load: CLI **29/30** and
backup/restore **30/30**. The remaining CLI failure matched the same completed
target-6 signature. This exposed the independent exact-index callback path;
the follow-up fix preserves its completed observation while retaining source
and delete watermarks. Metadata also now owns atomic restore-progress
retirement and rejects stale incarnation reports at apply time; see the
[runtime entry](../FLAKES.md#restore-completion-owns-progress-retirement-694).

Final acceptance requires **100/100 for each affected test**, using the branch
with the native-storage main merge and all fixes, without failure retries.

The follow-up Linux soak of `70e0b11869` is **not a passing acceptance run**.
It finished **292/300 passed**: quickstart **98/100**, backup/restore **96/100**,
and independent retry exhaustion **98/100** (the four backup non-passes include
one fixture setup error). The final retry failure occurred during its healthy
seed and exposed [coverage reads across an atomic commit](../FLAKES.md#coverage-reads-straddle-the-first-atomic-outcome-commit-694).
It exposed thumbnail activation without a runtime owner observation, two
metadata exits after slow successful WAL sync, and a seed batch with unknown
write outcome. Failure roots and raw worker logs were retained. See the
[runtime diagnosis](../FLAKES.md#slow-raft-sync-kills-the-runtime-targeted-activation-joins-sibling-work-694)
for the production changes and remaining write-timeout investigation. A fresh
100-per-scenario soak is required after those changes. The later partial-source
replay failure and stale follower job read now have separate
[production regressions](../FLAKES.md#partial-source-replay-and-stale-follower-restore-job-observations-694).

The CLI quickstart now separates retry exhaustion into
`test_cli_index_wait_survives_retry_exhaustion_and_restart`. The quickstart
retains the 10.5-second maintenance observation, completed list/detail
readiness, restart, image query, and RAG assertions. The dedicated test owns
its initial healthy corpus, exhausts the unchanged provider retry policy,
then checks isolated failure, later progress, and partial-generation restart.
Both tests restore their mock provider state during cleanup. A Linux run of
both tests passed: quickstart **13.37 s**, retry exhaustion **65.73 s**, including
**62.31 s / 36 provider requests** before exhaustion. The earlier integrated
quickstart averaged 77.2 s. Focused readiness soaks use the quickstart node ID;
the retry-policy test remains in the ordinary E2E suite.

Running the new test independently then exposed an additional initial-build
availability defect: three of four exploratory executions failed while one
passed. The durable repair checkpoint recorded terminal
`RepairSourceCoverageIncomplete` for catalog admission, despite a previously
published healthy image. The [runtime entry](../FLAKES.md#initial-catalog-admission-quarantines-a-healthy-generation-on-shadow-coverage-lag-694)
records the fix and deterministic before/after regression; fixed-runtime Linux
validation remains pending.

### Three-by-three backup seed batch: unknown outcome (#694)

The run reported 394 passed, five skipped, and one failure. Table creation and
three-shard, three-voter replication checks succeeded, but the initial batch
seeding three fixed document keys returned HTTP 409 `write outcome unknown`.
The test failed before starting backup. Both routing-watch unit tests and the
inference E2E suite passed in the same run. The failed aggregate checks merely
report their child-job failures; they are not additional flakes.

This is distinct from the earlier 503 `write unavailable` admission rejection.
The seeding helper correctly refuses to replay an ambiguous generic batch.
Its immediate error path now includes the six server log tails, status, response
body, and chained exception, just like the deadline-exhaustion path. Fast tests
cover this exact 409, transport failures, and other non-admission errors, require
diagnostics, and verify that each fails after one POST.

The failure reproduced on current main with #692 included: 1/30 initial runs,
then 7/60 instrumented runs, using three concurrent soak workers. Every
instrumented failure reported `ConcurrencyUnavailable` in the group batch
forwarder. The [runtime investigation](../FLAKES.md#backup-seed-forwarding-exhausts-the-control-executor-694)
records the executor fix and deterministic before/after regression.

The fixture was also missing from the scheduler's legacy process-fixture list,
which still named its predecessor `multi_metadata_backup_cluster`. It now
declares `@e2e_resource("antfly_process")` directly, so future fixture renames
retain the declaration. Actual pytest collection changes from `light--test--`
to `antfly-process--test--`; the six-process cluster now consumes a process
resource slot. The concurrent soak uses independent pytest workers and still
stresses multiple clusters simultaneously.

Validation on 2026-09-09 (America/Los_Angeles), macOS ARM64, native Debug:

- 128 harness and scheduler tests passed.
- The saturated-control forwarding regression failed before the executor fix
  and passed afterward; borrowed I/O and error-classification checks passed.
- All 114 focused forwarding, HTTP-client, and Raft checks passed after updating
  three stale expectations for the distinct internal transport-ambiguity error.
- Fixed-runtime soak: **59/60 passed**, three workers × twenty repetitions.
  No seed-write 409 occurred; one run failed before seeding at table-create
  admission, detailed below. This is not a clean full-test soak.
- Pinned Ruff lint/format checks, Zig formatting, and diff checks passed.
- Linux CI remains cross-platform validation.

The fixed Debug executable SHA-256 was
`ad61b7bdfd43fd6e425e44caab2e8c8a9d2252f7b366356fe7b12b6eb96454a8`.
The final soak log is `/private/tmp/pr694-backup-outbound-fixed-soak.log`.

Original CI diagnostics were insufficient to prove that CI hit the same internal
error; the local reproduction and deterministic regression establish a concrete
cause of the matching failure signature. Baseline soak logs are retained in
`/private/tmp/pr694-backup-fixed-soak.log` (the earlier scheduler/diagnostics-only
change) and `/private/tmp/pr694-backup-transport-soak.log` (underlying error added).

#### Table-create admission timeout during #694 validation

Worker 1, iteration 11 of the first forwarding-fix soak exhausted the existing
30-second create-admission budget after five HTTP 503 responses with
`metadata_leader_unavailable`, `X-Antfly-Metadata-Mutation-Not-Admitted: true`,
and `X-Antfly-Metadata-Not-Leader: true`. It had not reached seed writes or the
changed batch forwarder. The original six logs did not identify the internal
cause, so they cannot prove which discovery defect occurred in that run.

The [runtime investigation](../FLAKES.md#metadata-mutation-discovery-exhausts-admission-time-694)
reproduced both a first-endpoint status probe consuming the entire mutation
budget and a returned Raft role referencing a freed response buffer. Bounded
endpoint probes, stable endpoint coverage, and role stabilization before
response release fix those defects. Public retry policy, production deadlines,
and the prohibition on replaying ambiguous writes remain unchanged.

The live stalled-status reproduction failed before the fix with the same five
pre-admission 503 responses, then passed the full backup/restore case afterward.
The retained proxy test keeps every direct metadata node address available
beside its stalled alternate route and activates the fault after bootstrap.
This preserves a discoverable leader across elections. An earlier proxy version
replaced one node address; elections could make that hidden node the only
leader, violating the test's healthy-leader assumption.

Exploratory validation is retained separately from the final soak:

- A diagnostic baseline had one `AddressInUse` startup collision in 60 runs,
  before the first public request; this was not the table-create failure.
- Overlapping three-worker ordinary and three-worker proxy soaks with other
  local builds raised host load above 100. The ordinary run passed 56/60:
  three restore-progress retirement timeouts and one seed 409 after Raft apply
  timeouts and thousands of transport send failures. The proxy run was stopped
  to correct its endpoint assumption and reduce concurrent load. These results
  do not establish that the discovery changes fix the separate stress failures.
- Logs: `/private/tmp/pr694-create-full-diagnostic-soak.log`,
  `/private/tmp/pr694-create-proxy-before.log`,
  `/private/tmp/pr694-create-proxy-after.log`,
  `/private/tmp/pr694-create-fixed-soak.log`, and
  `/private/tmp/pr694-stalled-overlap-workers/`.

Final validation on macOS ARM64, native Debug, with remote PR commits through
`d0aa27d49` merged and the discovery/ownership and resolver-drain fixes applied:

- **90/90 full backup/restore runs passed**: 60 ordinary and 30 with a stalled
  alternate metadata status route. Three workers total; each ran ten rounds of
  two ordinary tests followed by one faulted test. No table-create, seed-write,
  backup, restore, or retirement failure occurred.
- 100 metadata service, four data discovery/status, five resolver-backfill,
  and 173 derived-coverage checks passed without leaks. All 134 Python
  harness/scheduler checks passed, as did pinned Ruff, Zig format, and diff checks.
- Log: `/private/tmp/pr694-final-merged-backup-soak.log`.
- Executable SHA-256:
  `bae3921f715c8e2f0e3a0d0aeb40088391d4600d611b473a79c3c618d87f28df`.

After merging `origin/main` at `8211fc92c4`, the rebuilt native Debug executable
passed a further **20/20** serial runs: ten ordinary backup/restore runs and ten
with the stalled alternate metadata status route. The affected Zig suites
passed 513 tests without leaks, and all 67 Python harness tests passed. The
merge retained both sets of transport diagnostics and corrected an upstream
empty-create assertion to include the newly persisted default storage setting.
Log: `/private/tmp/pr694-main-merge-backup-soak.log`. Executable SHA-256:
`bbf4146d631ee247fccde9deb3f6de995c68a429ec66179dd8aa15aecaf2f7dd`.

The matching local reproductions establish concrete discovery defects; the
original failed run's logs do not prove which one it encountered. The clean
final soak is evidence of the merged behavior, not proof that unrelated
higher-load or port-handoff failures are eliminated.

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
