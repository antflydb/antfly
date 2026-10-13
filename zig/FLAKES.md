# Zig runtime flakes

Intermittent failures seen in Zig unit, VOPR, build, and CI-infrastructure lanes, newest first.
E2E test flakes are tracked in [e2e/FLAKES.md](e2e/FLAKES.md).
When a flake shows up, add a row (or update the existing one) with where it was seen and links to the failing run, issue, or PR.
Mark a row `Closed` only once its fix is merged and verified (a soak or later CI passes); anything still flaky, only mitigated, or unexplained stays `Open`.
Past investigation notes are in git history (`git log -p -- zig/FLAKES.md`) and the linked PRs.

| Flake | Where | First seen | Status | Links |
| --- | --- | --- | --- | --- |
| Snapshot-publication fixture races Ready-drain compaction | zig unit-test (storage gate, local full run) | 2026-10-01 | Closed | [run 36961250636](https://github.com/antflydb/antfly/actions/runs/36961250636) |
| Data-runtime test process hangs in `DataServer` teardown (`drainShutdown`) | zig unit-test (data-runtime, UNIQUE/FK merge/split history) | 2026-10-01 | Closed | [run 36903575966](https://github.com/antflydb/antfly/actions/runs/36903575966) |
| Replicated merge overflows the VOPR fiber stack | zig unit-test (data-runtime, local Debug) | 2026-10-01 | Closed | |
| Full E2E binary build hits its 90-minute deadline | `e2e-full` build job | 2026-10-01 | Open | [run 36903575966](https://github.com/antflydb/antfly/actions/runs/36903575966) |
| CUDA E2E build cancelled near its 90-minute limit | CUDA build job (PR #885) | 2026-09-27 | Open | #885, [job 108695038652](https://github.com/antflydb/antfly/actions/runs/36345949010/job/108695038652) |
| Model prefetch step fails before server startup | L4 GPU smoke | 2026-09-26 | Open | [job 108309588813](https://github.com/antflydb/antfly/actions/runs/36203548073/job/108309588813) |
| `TestInferenceRuntimeContract/lazy` times out during cold model download | operator inference job | 2026-09-25 | Open | [job 108198671285](https://github.com/antflydb/antfly/actions/runs/36168024127/job/108198671285) |
| Determinism audit fails on a raw pointer in teardown diagnostics | VOPR `qualify` | 2026-09-25 | Closed | #889, [run 36189107289](https://github.com/antflydb/antfly/actions/runs/36189107289) |
| `qualify` job cancelled at its 120-minute limit (duplicate cold compile) | VOPR `qualify` | 2026-09-25 | Closed | #889, [run 36189107289](https://github.com/antflydb/antfly/actions/runs/36189107289) |
| Runtime-cache fixture picks an arbitrary compile artifact | zig unit-test (full lane, runtime cache) | 2026-09-21 | Closed | #832, [run 35641562828](https://github.com/antflydb/antfly/actions/runs/35641562828) |
| Full-lane runner filesystem fills with compiler outputs | zig full lane | 2026-09-21 | Closed | #832, [run 35641562828](https://github.com/antflydb/antfly/actions/runs/35641562828) |
| Raft trace producers truncate a shared trace file (invalid JSON) | TLA Raft trace validation | 2026-09-21 | Closed | #828, #832, [job 106181715054](https://github.com/antflydb/antfly/actions/runs/35549453714/job/106181715054) |
| `MissingSourceVectorStatus` after owner reopen under load | zig storage-owner test | 2026-09-21 | Closed | #845 |
| `native_chat` request-cancellation test hangs until the idle watchdog | zig unit-test (x86_64) | 2026-09-18 | Closed | #801, [job 105721669088](https://github.com/antflydb/antfly/actions/runs/35382288964/job/105721669088) |
| Anonymous unit test executable stalls until the idle watchdog | zig unit-test (x86_64, outside DB partitions) | 2026-09-17 | Open | [job 105456824907](https://github.com/antflydb/antfly/actions/runs/35298670343/job/105456824907) |
| `ProductionStandbyBaselineDrainTimeout` | VOPR standby-scaling shards | 2026-09-17 | Closed | [run 35247508165](https://github.com/antflydb/antfly/actions/runs/35247508165) |
| `db async replay truncation retains journal behind generated enrichment` times out | zig unit-test (x86_64, PR #691) | 2026-09-16 | Closed | [job 105060523520](https://github.com/antflydb/antfly/actions/runs/35176786855/job/105060523520) |
| Metadata ownership fixture aborts on scheduler reentrancy | zig unit-test (x86_64, PR #704) | 2026-09-16 | Closed | #704, #737, [job 104656431732](https://github.com/antflydb/antfly/actions/runs/35052615288/job/104656431732) |
| Self-hosted runner lost or preempted mid-job (GKE preemption / memory pressure) | zig-base, build-cache, VOPR qualification/corpus, E2E x86_64 | 2026-09-15 | Open | #889, [job 104276235667](https://github.com/antflydb/antfly/actions/runs/34927431365/job/104276235667), [run 35446473661](https://github.com/antflydb/antfly/actions/runs/35446473661), [run 36200131260](https://github.com/antflydb/antfly/actions/runs/36200131260), [job 108307916600](https://github.com/antflydb/antfly/actions/runs/36207700839/job/108307916600), [run 36819027473](https://github.com/antflydb/antfly/actions/runs/36819027473) |
| Graph repair yield drops work count; resolver reopen not idle | zig unit-test (db partition, PR #704) | 2026-09-15 | Closed | #704, [job 104631027746](https://github.com/antflydb/antfly/actions/runs/35044256661/job/104631027746) |
| `VoprIndexMaterializationIncomplete` during exact replay | VOPR full soak (distributed-data) | 2026-09-15 | Open | [run 34927431365](https://github.com/antflydb/antfly/actions/runs/34927431365) |
| Compiler RSS exceeds declared VOPR build reservation | VOPR qualification | 2026-09-14 | Closed | #704, [job 104248350481](https://github.com/antflydb/antfly/actions/runs/34927431365/job/104248350481) |
| Lookup returns HTTP 500 (`ReadIndexTimeout`) after sibling merge | VOPR adversarial scaling | 2026-09-14 | Closed | #704, [job 104181278895](https://github.com/antflydb/antfly/actions/runs/34899531048/job/104181278895) |
| `PromotionRequiresForce` treated as a harness error | VOPR full soak (standby shards) | 2026-09-14 | Closed | #704, [run 34878521929](https://github.com/antflydb/antfly/actions/runs/34878521929) |
| Combined unit build/test budget terminates a progressing DB-core run | zig unit-test (Linux) | 2026-09-12 | Open | #716, [job 103609868433](https://github.com/antflydb/antfly/actions/runs/34714680318/job/103609868433) |
| Physical-churn benchmark `FileNotFound` (obsolete-file deletion race) | zig unit-test (db partition, PR #503) | 2026-09-12 | Closed | #716, [job 103497870209](https://github.com/antflydb/antfly/actions/runs/34672950333/job/103497870209) |
| DB-core `munmap_chunk(): invalid pointer` abort | zig unit-test (db partition) | 2026-09-11 | Open | [job 103457081854](https://github.com/antflydb/antfly/actions/runs/34658598331/job/103457081854) |
| Last dense bulk-lease fixture races physical publication | zig unit-test (db partition, local Linux) | 2026-09-11 | Closed | #694 |
| `db dense finalization owner drains requests queued during publication` | zig unit-test (x86_64) | 2026-09-11 | Closed | #694, [job 103392541552](https://github.com/antflydb/antfly/actions/runs/34638056167/job/103392541552) |
| Prefetch queue restart hits `ConcurrencyUnavailable` | zig unit-test (inference prefetch) | 2026-09-11 | Closed | #694, [job 103355428154](https://github.com/antflydb/antfly/actions/runs/34627106321/job/103355428154) |
| Provider-restart replay test races an independent index consumer | zig unit-test (x86_64) | 2026-09-10 | Closed | #694, run 34543627560 |
| Dense reset fixture aborts while native publication holds the catalog | zig unit-test (x86_64) | 2026-09-10 | Closed | #694, [job 103018251656](https://github.com/antflydb/antfly/actions/runs/34520726158/job/103018251656) |
| Online vector-publication test counts unrelated posting progress | zig unit-test (dense lifecycle) | 2026-09-10 | Closed | #694 |
| No-op `upsertResolver` returns before queued backfill applies (`NotFound`) | zig unit-test (resolver backfill) | 2026-09-10 | Closed | #694, [job 102732550441](https://github.com/antflydb/antfly/actions/runs/34432995411/job/102732550441) |
| Dense generation rollback exceeds the activation budget | zig unit-test (x86_64) | 2026-09-10 | Closed | #694, #695, [job 102704099568](https://github.com/antflydb/antfly/actions/runs/34423487352/job/102704099568) |
| Standalone routing watch returns `retry` instead of absence | zig unit-test (standalone runtime, PR #689) | 2026-09-09 | Closed | #694, [job 102687782332](https://github.com/antflydb/antfly/actions/runs/34418061842/job/102687782332) |
| Repair issue list misses algebraic generation debt (`indexes_rebuilt`) | zig unit-test (PR #658) | 2026-09-08 | Closed | [job 101910688344](https://github.com/antflydb/antfly/actions/runs/34177703845/job/101910688344) |
