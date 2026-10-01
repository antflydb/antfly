# STATUS.md Dated E2E Observations (2026-05)

> Relocated verbatim from `zig/STATUS.md` (lines 81–110 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`STATUS.md`](../../../zig/STATUS.md), specifically the "Dated E2E Observations" section. Durable decisions from this log were folded into that document before the move.

### Dated E2E Observations

Observed on 2026-05-01:

- `e2e/antfly/test_distributed_status.py::test_non_host_api_reports_remote_index_status_from_metadata_heartbeat`
  is the clearest status-subsystem failure. The data owner publishes runtime
  status into metadata heartbeat, but the API-only process still reports
  `runtime_source = "synthetic_config"` with `expected_groups = 0` and
  `reported_groups = 0`. That means the in-process/unit-level distributed
  status contract is not yet proven in the real split-process heartbeat path.
- Managed embedding index lifecycle failures are status-plane publisher
  failures until proven otherwise. The indexes can often answer queries or make
  progress, but index detail readiness does not reliably reflect that progress
  after rate-limit recovery, provider pacing, delete/recreate, or artifact
  corruption recovery.
- The schema migration full-text rebuild failure has the same status-plane
  shape: `full_text_index_v1` is created, but readiness does not reach the
  expected state in the public status path before timeout. The next diagnostic
  step is to determine whether rebuild work is missing, stuck, or complete but
  unpublished.
- CDC failures are metadata status-summary failures, not table runtime-status
  failures. Snapshot import and streaming changes succeed, but `/status`
  counters such as `projected_replication_source_statuses_streaming` and
  `projected_replication_source_statuses_terminal_failed` do not match the
  projected replication source records exposed elsewhere.
- `test_occ_conflict_detection` returned HTTP 500 on the first stateless commit
  in the full E2E run, but focused transaction reruns now pass. If it recurs,
  it belongs to transaction correctness/error mapping, not runtime status
  publishing.

