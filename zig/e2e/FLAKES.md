# Zig E2E flakes

Intermittent failures seen in E2E tests (pytest lanes, recovery shards, and E2E soaks), newest first.
Unit, VOPR, build, and CI-infrastructure flakes are tracked in [../FLAKES.md](../FLAKES.md).
When a flake shows up, add a row (or update the existing one) with the test, lane, and links to the failing run, issue, or PR.
Mark a row `Closed` only once its fix is merged and verified (a soak or later CI passes); anything still flaky, only mitigated, or unexplained stays `Open`.
Past investigation notes are in git history (`git log -p -- zig/e2e/FLAKES.md`) and the linked PRs.

Soak a selector against a prebuilt server from the repository root:

```sh
SKIP_BUILD=1 ANTFLY_BIN="$PWD/zig/zig-out/bin/antfly" \
ANTFLY_E2E_REGRESSION_WORKERS=4 ANTFLY_E2E_REGRESSION_REPEATS=25 \
  scripts/ci/zig-e2e-regression-loop.sh '<pytest node id>'
```

| Flake | Where | First seen | Status | Links |
| --- | --- | --- | --- | --- |
| FK merge admission waits on a follower's full-text initial build | e2e: `test_online_fk_merge_preserves_shadow_claims_and_retained_references[snapshot-child_owner]` | 2026-10-02 | Closed | [run 36961250636](https://github.com/antflydb/antfly/actions/runs/36961250636) |
| Truncated HTTP response (`UnexpectedEof`) is fatal to standby restart | e2e: standby restart | 2026-10-02 | Closed | [run 36961250636](https://github.com/antflydb/antfly/actions/runs/36961250636) |
| Private restore provisioning loses the source-generation proof | e2e: three-by-three restore | 2026-10-02 | Closed | [run 36961250636](https://github.com/antflydb/antfly/actions/runs/36961250636) |
| Progressive index first-result gate stalls on a provider call | e2e: quickstart progressive indexing | 2026-09-29 | Open | #918, run 36648393361 |
| Progressive index completion stalls with `EnrichmentLeaseFenceLost` | e2e soak: quickstart progressive indexing | 2026-09-29 | Closed | #918 |
| Autograph emits a live provisional event beside its canonical event | e2e: `test_label_routed_autograph_promotes_events_and_entities` | 2026-09-29 | Closed | #919, [run 36609343247](https://github.com/antflydb/antfly/actions/runs/36609343247) |
| FK merge `snapshot-owner` and schema rewrite `publication-reply_loss` recovery stall under slow metadata WAL | e2e: recovery shards | 2026-09-29 | Closed | #918, #919, [run 36609343247](https://github.com/antflydb/antfly/actions/runs/36609343247) |
| Child table never writable (`503 write unavailable`) after release reply loss | e2e recovery-2: `test_online_fk_merge_preserves_shadow_claims_and_retained_references[release-reply_loss]` (PR #885) | 2026-09-27 | Open | #885, [job 108702992295](https://github.com/antflydb/antfly/actions/runs/36345949010/job/108702992295) |
| Restore stuck `running` with `OnlineSourcePinMissing` | e2e recovery-2: `test_schema_rewrite_recovers_dependency_cohort[publication-coordinator]` (PR #885) | 2026-09-27 | Open | #885, [job 108702992295](https://github.com/antflydb/antfly/actions/runs/36345949010/job/108702992295) |
| Replacement parent rejected with false `UniqueConstraintViolation` after cascade | e2e soak: `test_fk_cascade_recovers_claims_references_and_rows[transaction_resolve-owner]` | 2026-09-27 | Closed | #891 |
| Post-merge UNIQUE probe returns `500 MetadataSnapshotUnavailable` | e2e soak: FK merge `release-reply_loss` | 2026-09-27 | Closed | #891 |
| Schema rewrite row read returns 404/500/503 after the job succeeded | e2e soak: `test_schema_rewrite_recovers_dependency_cohort[publication-*]` | 2026-09-26 | Closed | #891, #918, #919 |
| Data node exits on `SocketUnconnected` in store-report worker | e2e soak: FK merge recovery | 2026-09-26 | Closed | #891 |
| Secret follower forwarding fixture fails during slow WAL sync | e2e: secret follower forwarding integration | 2026-09-26 | Closed | #891, [run 36274408215](https://github.com/antflydb/antfly/actions/runs/36274408215) |
| Label-routed Autograph failure with no traceback (job cancelled) | e2e: `test_label_routed_autograph_promotes_events_and_entities` | 2026-09-26 | Open | [run 36271592595](https://github.com/antflydb/antfly/actions/runs/36271592595) |
| Restore fixture accepts another server's readiness after `AddressInUse` | e2e scheduled production soak: standalone restore | 2026-09-26 | Closed | [run 36247286560](https://github.com/antflydb/antfly/actions/runs/36247286560) |
| Live Autograph promotion replay erases curator redirects | e2e scheduled production soak: Autograph | 2026-09-26 | Closed | [run 36247286560](https://github.com/antflydb/antfly/actions/runs/36247286560) |
| FK merge / schema rewrite / FK cascade recovery stalls (seed admission, forward capacity, slow persistence) | e2e recovery shards: `release-reply_loss`, `publication-coordinator`, `transaction_resolve-owner` | 2026-09-25 | Closed | #891, [job 108198671138](https://github.com/antflydb/antfly/actions/runs/36168024127/job/108198671138), [run 36271592595](https://github.com/antflydb/antfly/actions/runs/36271592595), [run 36274408215](https://github.com/antflydb/antfly/actions/runs/36274408215) |
| Second index stays `runtime_unavailable` past the activation deadline | e2e: `test_progressive_index_is_semantically_queryable_before_full_coverage` | 2026-09-25 | Closed | [job 108330133910](https://github.com/antflydb/antfly/actions/runs/36213445604/job/108330133910) |
| Fenced promotion returns 500 after `MetadataHABindingBusy` | e2e HA: `test_empty_seed_then_first_table_replication_and_fenced_promotion` | 2026-09-25 | Closed | [run 36200131260](https://github.com/antflydb/antfly/actions/runs/36200131260) |
| Dictate cleanup times out after 600 s with capacity errors | e2e inference (CPU) | 2026-09-25 | Open | [run 36200131260](https://github.com/antflydb/antfly/actions/runs/36200131260) |
| Schema rewrite requeues attempts on `GroupLeaderUnavailable` | e2e recovery-0: `test_schema_rewrite_recovers_dependency_cohort[publication-reply_loss]` (PR #882) | 2026-09-25 | Closed | #882, [job 108211921323](https://github.com/antflydb/antfly/actions/runs/36173392068/job/108211921323) |
| Data node exits on store-report `ReadFailed` | e2e soak: schema rewrite recovery | 2026-09-25 | Closed | #882 |
| Data-group proposal stuck one index above commit at write timeout | e2e recovery-1: FK merge `snapshot-child_owner`, schema rewrite `publication-coordinator` | 2026-09-24 | Open | [job 107754773721](https://github.com/antflydb/antfly/actions/runs/36028793026/job/107754773721) |
| Executable chunk embedding restart cases skip coverage | e2e: executable embedding artifact restart (PR #868) | 2026-09-23 | Closed | #868, [run 35937420037](https://github.com/antflydb/antfly/actions/runs/35937420037) |
| Schema rewrite exhausts terminal wait on `RestoreValidationPending` | e2e: schema rewrite recovery (PR #868) | 2026-09-23 | Closed | #868, [run 35937420037](https://github.com/antflydb/antfly/actions/runs/35937420037) |
| Seed write times out during slow metadata persistence | e2e: schema rewrite recovery (PR #868) | 2026-09-23 | Open | [run 35937420037](https://github.com/antflydb/antfly/actions/runs/35937420037) |
| Constraint-status poll fails on transient 409 | e2e: constraint lifecycle | 2026-09-23 | Closed | [job 107446985962](https://github.com/antflydb/antfly/actions/runs/35936641093/job/107446985962) |
| Catalog and foreground-traffic failures during 12 s metadata WAL commits | e2e base lanes (catalog resilience) | 2026-09-23 | Open | [job 107446985962](https://github.com/antflydb/antfly/actions/runs/35936641093/job/107446985962) |
| Replayed older owner descriptor fails with `SchemaVersionRegression` | e2e: `test_concurrent_tenant_schema_migrations_preserve_documents` | 2026-09-23 | Closed | [job 107389740209](https://github.com/antflydb/antfly/actions/runs/35916384702/job/107389740209) |
| Progressive index loses published vectors across process restart | e2e: `test_progressive_publication_remains_queryable_across_process_restart` | 2026-09-22 | Closed | #867, #918, [run 35780419851](https://github.com/antflydb/antfly/actions/runs/35780419851), [job 107069241819](https://github.com/antflydb/antfly/actions/runs/35823581381/job/107069241819), [job 109286926408](https://github.com/antflydb/antfly/actions/runs/36526714836/job/109286926408) |
| Cutover readiness requeues attempts until `RestoreValidationPending` timeout | e2e: `test_schema_rewrite_recovers_dependency_cohort[publication-coordinator]` | 2026-09-22 | Closed | [job 107039234761](https://github.com/antflydb/antfly/actions/runs/35813900420/job/107039234761) |
| Recovery test reports no successor leader within 30 s | e2e recovery-1: `test_schema_rewrite_recovers_dependency_cohort[publication-coordinator]` | 2026-09-22 | Closed | #856, [run 35671650044](https://github.com/antflydb/antfly/actions/runs/35671650044) |
| Restore job stays in validation (`job=None` on followers) | e2e recovery-0: `test_schema_rewrite_recovers_dependency_cohort[publication-reply_loss]` | 2026-09-22 | Open | #856, [run 35780419851](https://github.com/antflydb/antfly/actions/runs/35780419851) |
| Standby `capture_catalog()` times out; catalog-authority write gets 503 | e2e: `test_standby_replication_startup.py`, `test_standby_empty_bootstrap.py` | 2026-09-22 | Open | #856, [run 35780419851](https://github.com/antflydb/antfly/actions/runs/35780419851) |
| Constrained session commit returns `409 decision conflict` after lost BEGIN reply | e2e: `test_constrained_sessions_defer_reject_and_rollback_across_owners` | 2026-09-22 | Closed | #856, [run 35688632936](https://github.com/antflydb/antfly/actions/runs/35688632936) |
| Restore with concurrent observers stays `running` past 120 s (attempt 15) | e2e: `test_cluster_restore_modes_with_concurrent_observers` | 2026-09-21 | Open | #846, [run 35780419851](https://github.com/antflydb/antfly/actions/runs/35780419851), [job 107069241819](https://github.com/antflydb/antfly/actions/runs/35823581381/job/107069241819), [job 107085992226](https://github.com/antflydb/antfly/actions/runs/35826961268/job/107085992226) |
| Concurrent restore observer gets 404/409 during binding replacement | e2e: `test_cluster_restore_modes_with_concurrent_observers` | 2026-09-21 | Closed | #832, [run 35641562828](https://github.com/antflydb/antfly/actions/runs/35641562828) |
| Donor group reports `leader_known=false` | e2e: `test_online_fk_merge_preserves_shadow_claims_and_retained_references[snapshot-owner]` | 2026-09-21 | Closed | #842, #832 |
| Online merge misses release window / stays in `freeze` after owner-link outage | e2e recovery-1: `test_online_merge_recovers_after_owner_link_outage_and_crash` (PR #832) | 2026-09-21 | Open | #832, [job 106465550834](https://github.com/antflydb/antfly/actions/runs/35633482490/job/106465550834) |
| Child-table seed exhausts its 30 s write budget on `LeaderUnavailable` | e2e: `test_schema_rewrite_recovers_dependency_cohort` | 2026-09-21 | Closed | #841, #832, [job 106507526474](https://github.com/antflydb/antfly/actions/runs/35647014305/job/106507526474) |
| Reporter registration fails with `500 RuntimeStatusProtocolUnavailable` | e2e: `test_catalog_resilience.py` (large inventory, baseline fragments) | 2026-09-21 | Closed | #828, #831, #832, [job 106186967472](https://github.com/antflydb/antfly/actions/runs/35549453714/job/106186967472), [job 106229248643](https://github.com/antflydb/antfly/actions/runs/35564629013/job/106229248643) |
| Foreground batch write `503 DeadlineExceeded` during skewed migrations | e2e: `test_skewed_schema_migrations_keep_foreground_traffic_available` | 2026-09-21 | Closed | #831, #832, [job 106215258019](https://github.com/antflydb/antfly/actions/runs/35557807565/job/106215258019) |
| Diagnostic snapshot admission returns `snapshot transfer capacity exhausted` | e2e catalog soak (`nofile=256`) | 2026-09-21 | Open | |
| Data node `ReadFailed` after deliberate leader termination | e2e: catalog resilience module (local) | 2026-09-21 | Open | |
| Full-text count 2001 instead of 2002 | e2e: catalog resilience module (local) | 2026-09-21 | Open | |
| Cluster-restore soak restore stuck on `CatalogRoutingSnapshotTimeout` | e2e scheduled cluster-restore soak | 2026-09-19 | Closed | |
| Aggregation readers exceed request deadline; rerun gets `IdentityReadGenerationChanged` 409 | e2e: `test_aggregations_remain_exact_during_concurrent_inserts` | 2026-09-18 | Closed | #801, [job 105482406606](https://github.com/antflydb/antfly/actions/runs/35304355356/job/105482406606) |
| Savepoint rollback returns 503 (recovery overlaps foreground commit) | e2e: `test_session_transform_savepoint_rollback` | 2026-09-18 | Closed | #801, [job 105482406606](https://github.com/antflydb/antfly/actions/runs/35304355356/job/105482406606) |
| Large catalog database create returns 503 during fast elections / slow WAL | e2e: `test_large_inventory_uses_bounded_control_and_diagnostic_transfers` | 2026-09-17 | Closed | [job 105470606625](https://github.com/antflydb/antfly/actions/runs/35298670343/job/105470606625), [run 36247286560](https://github.com/antflydb/antfly/actions/runs/36247286560) |
| Restore job poll returns HTTP 500 past a stalled status route | e2e: `test_backup_restore_discovers_leader_past_stalled_status` (PR #781) | 2026-09-17 | Open | #781, [job 105310030026](https://github.com/antflydb/antfly/actions/runs/35247545305/job/105310030026) |
| Restore completion timeouts and seed failures under proposal supersession | e2e production soak (VOPR workflow) | 2026-09-17 | Closed | #781, [job 105291344795](https://github.com/antflydb/antfly/actions/runs/35247508165/job/105291344795) |
| Restore owner admission blocks its own bootstrap (`GenerationTransitionActive`) | e2e: `test_three_by_three_cluster_backup_restore_through_metadata_public_api` (PR #771) | 2026-09-16 | Closed | #781, [job 105028671150](https://github.com/antflydb/antfly/actions/runs/35163796459/job/105028671150) |
| Data node exits with `StorageBusy` in committed Raft apply | e2e soak: `test_multinode_autograph_recovers_after_data_restart` (constrained) | 2026-09-16 | Closed | #704, [job 104951437450](https://github.com/antflydb/antfly/actions/runs/35126679231/job/104951437450) |
| Data node SIGSEGV in teardown (source quiescence frees borrowed activity) | e2e: `test_multinode_exact_candidates_follow_redirects_across_entity_shards` (PR #691) | 2026-09-16 | Closed | [job 105044672774](https://github.com/antflydb/antfly/actions/runs/35168905132/job/105044672774) |
| Promoted entities missing; lookup 500 on untransportable `ReadIndexTimeout` | e2e: `test_multinode_autograph_resolves_promotes_and_hydrates_entities` | 2026-09-15 | Closed | #704, [job 104259435053](https://github.com/antflydb/antfly/actions/runs/34928784180/job/104259435053) |
| Cluster restore fails with `StorageBusy` during publication | e2e: `test_cluster_restore_modes` (PR #704) | 2026-09-14 | Closed | #704, [job 104174381057](https://github.com/antflydb/antfly/actions/runs/34899514358/job/104174381057) |
| Session stage transform commit returns 503 | e2e: `test_session_stage_transform_commit` (PR #704) | 2026-09-14 | Open | #704 |
| Producer counter stays zero / `source_observation_incomplete` | e2e: `test_embedding_producer_registry_rejects_orphans_and_owner_mismatches` | 2026-09-14 | Closed | #723, [job 103853126220](https://github.com/antflydb/antfly/actions/runs/34801864332/job/103853126220) |
| Partial-policy index `runtime_unavailable` / `missing_group` after restart | e2e: `test_artifact_coverage_terminal_outcomes_by_policy_after_restart` | 2026-09-13 | Closed | #722, #723, #626, [job 103815319126](https://github.com/antflydb/antfly/actions/runs/34789270919/job/103815319126), [job 103794277696](https://github.com/antflydb/antfly/actions/runs/34781521711/job/103794277696) |
| Full-text query returns 500 `RuntimeBoundaryFailure` | e2e: `test_table_chunker_full_text_index_routes_template_chunks[stateful]` | 2026-09-13 | Closed | #722, #723, [job 103815319126](https://github.com/antflydb/antfly/actions/runs/34789270919/job/103815319126) |
| Semantic query returns unexpected 504 | e2e: `test_semantic_timeout_budget_survives_embedding_cache` | 2026-09-13 | Open | #722, [job 103815319126](https://github.com/antflydb/antfly/actions/runs/34789270919/job/103815319126) |
| Packed-ingest batch returns 500 after `IndexNotFound` in derived worker | e2e: `test_stateful_external_embeddings_index_detail_supports_packed_ingest_and_query` | 2026-09-13 | Closed | #626, #723, [job 103794277696](https://github.com/antflydb/antfly/actions/runs/34781521711/job/103794277696) |
| Heap corruption in progressive-index quickstart | e2e: quickstart progressive index | 2026-09-11 | Closed | #705 |
| Serverless publication helper fails on retryable 503 (`WorkLeaseLost`) | e2e: `test_table_chunker_full_text_index_routes_template_chunks[serverless]`, `test_semantic_query_embedding_template_supports_remote_text[serverless]` | 2026-09-11 | Closed | #503, [job 103470234534](https://github.com/antflydb/antfly/actions/runs/34659490727/job/103470234534) |
| Quickstart loses serving text index after image index creation | e2e soak: CLI quickstart | 2026-09-11 | Closed | #694 |
| Semantic ranking indexes an empty result after the partial milestone | e2e soak: CLI quickstart | 2026-09-11 | Closed | #694 |
| Create returns unknown-outcome 409 after metadata leadership loss | e2e soak: three-by-three backup/restore | 2026-09-11 | Closed | #694 |
| Duplicate restore jobs for one request after an admission timeout | e2e soak: three-by-three backup/restore | 2026-09-11 | Closed | #694 |
| Metadata Ready batch exceeds its 1 GiB ceiling and quarantines the group | e2e soak: three-by-three backup/restore | 2026-09-10 | Closed | #694 |
| Overwritten Raft prepare / unknown outcome after confirmed abort | e2e soak: three-by-three backup/restore | 2026-09-10 | Closed | #694 |
| Restored group exits with `ConflictingDataApplyBatch` | e2e soak: three-by-three backup/restore | 2026-09-10 | Closed | #694 |
| RAG returns `StorageReadTemporarilyUnavailable` right after restart | e2e soak: CLI quickstart | 2026-09-10 | Closed | #694 |
| Metadata follower exits with `NotLeader` (CDC scheduling) | e2e soak: three-by-three backup/restore | 2026-09-10 | Closed | #694 |
| Backup timeout surfaces untransportable `BackupOutcomeAmbiguous` | e2e soak: three-by-three backup/restore | 2026-09-10 | Closed | #694 |
| Repository cleanup collides with the reclaimer's lock file | e2e soak: three-by-three backup/restore | 2026-09-10 | Closed | #694 |
| Backup DELETE returns a conflict | e2e soak: three-by-three backup/restore | 2026-09-10 | Open | #694 |
| Healthy seed fails with `InvalidDerivedCoverageCounter` | e2e soak: `test_cli_index_wait_survives_retry_exhaustion_and_restart` | 2026-09-10 | Closed | #694 |
| Partial-source replay target stalls behind a pending source | e2e soak: CLI retry exhaustion | 2026-09-10 | Closed | #694 |
| Retry test reports `pending` instead of `queryable_partial` | e2e: `test_cli_index_wait_survives_retry_exhaustion_and_restart` | 2026-09-10 | Closed | #694 |
| Restore job poll serves a stale follower row until timeout | e2e soak: three-by-three backup/restore | 2026-09-10 | Closed | #694 |
| Completed restore progress not retired within 30 s | e2e soak: three-by-three backup/restore | 2026-09-10 | Closed | #694 |
| Thumbnail index has no runtime observation within the activation deadline | e2e soak: CLI quickstart | 2026-09-10 | Closed | #694 |
| Metadata exits with `RaftProgressDriverStalled` after slow WAL sync | e2e soak: CLI quickstart / backup | 2026-09-10 | Closed | #694 |
| Completed CLI index readiness regresses between `index get` and `index list` | e2e: `test_cli_inline_create_load_wait_query_image_and_rag_pipeline` (PR #696) | 2026-09-10 | Closed | #694, [job 102726530711](https://github.com/antflydb/antfly/actions/runs/34428885099/job/102726530711) |
| Seed batch returns `409 write outcome unknown` (control executor exhaustion) | e2e: `test_three_by_three_cluster_backup_restore_through_metadata_public_api` | 2026-09-10 | Closed | #694, [job 102714559943](https://github.com/antflydb/antfly/actions/runs/34423487352/job/102714559943) |
| Build status returns 500 `PreconditionFailed` during publication | e2e: `test_serverless_named_embedding_indexes_report_publication_actions` (PR #692) | 2026-09-10 | Closed | #696, [job 102704104941](https://github.com/antflydb/antfly/actions/runs/34420585088/job/102704104941) |
| Second document write times out (refresh/Raft apply cycle) | e2e: `test_multinode_autograph_resolves_promotes_and_hydrates_entities` (PR #690) | 2026-09-09 | Closed | #692, [job 102623777993](https://github.com/antflydb/antfly/actions/runs/34395199129/job/102623777993) |
| Standby write gets pending-durability 503 after restart | e2e: `test_standby_streams_public_writes_restarts_and_rejects_writes` | 2026-09-09 | Closed | #657, #832, [job 102299245250](https://github.com/antflydb/antfly/actions/runs/34296218257/job/102299245250) |
| Table create exhausts admission on `503 metadata_leader_unavailable` | e2e: `test_three_by_three_cluster_backup_restore_through_metadata_public_api` | 2026-09-08 | Closed | #694 |
| Initial table create returns unknown-outcome 409 (read waiter advances Raft time) | e2e: `test_three_by_three_cluster_backup_restore_through_metadata_public_api` (PR #664) | 2026-09-08 | Closed | [job 102199089027](https://github.com/antflydb/antfly/actions/runs/34263167199/job/102199089027) |
| Seed batch returns `503 write unavailable` | e2e: `test_three_by_three_cluster_backup_restore_through_metadata_public_api` (PR #658) | 2026-09-08 | Closed | [job 101916669107](https://github.com/antflydb/antfly/actions/runs/34177703845/job/101916669107) |
| Source coverage asserted before complete readiness (`covered == 0`) | e2e: `test_cli_inline_create_load_wait_query_image_and_rag_pipeline` (PR #658) | 2026-09-08 | Closed | [job 101916669107](https://github.com/antflydb/antfly/actions/runs/34177703845/job/101916669107) |
| Retry-exhaustion phase exceeds its 30 s budget | e2e: CLI pipeline retry exhaustion (PR #659) | 2026-09-08 | Closed | [job 101932868141](https://github.com/antflydb/antfly/actions/runs/34182855053/job/101932868141) |
| Fixture DELETE gets `ConnectionResetError` in teardown | e2e: `test_retrieval_agent_streaming_fallback_progress` (PR #657) | 2026-09-08 | Open | [job 101914807099](https://github.com/antflydb/antfly/actions/runs/34176604388/job/101914807099) |
| Local multi-cluster soaks exhaust TCP ports (`TIME_WAIT`, `EADDRNOTAVAIL`) | local macOS E2E soaks | 2026-09-08 | Open | |
