# TODO.md Resolved E2E Failures (2026-05-11 Run)

> Relocated verbatim from `zig/TODO.md` (lines 7–77 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`TODO.md`](../../../zig/TODO.md), specifically the "Current Bugs" section. Durable decisions from this log were folded into that document before the move.

## Current Bugs

Observed on 2026-05-11:

Latest full-suite status:

- Antfly inference E2E is green:
  - command: `bash go/pkg/termite/scripts/debug_metal_command.sh command --timeout 1800 -- e2e/inference/.venv/bin/pytest -q -s e2e/inference`
  - result: `63 passed, 31 skipped in 825.24s (0:13:45)`
  - debug bundle: `go/pkg/termite/.debug/metal-command-20260510-195358`
- Antfly Python E2E is green:
  - command: `UV_CACHE_DIR=/tmp/uv-cache ANTFLY_E2E_PRESERVE_ROOT=1 uv run --project e2e/antfly pytest -q e2e/antfly`
  - result: `192 passed, 10 skipped in 1256.92s (0:20:56)`
  - note: the sandboxed Antfly run failed with localhost port bind permission
    errors and is not considered meaningful

### Antfly E2E Status

Status: green in the latest full Antfly run on 2026-05-11.

Latest full-suite result:

- command: `UV_CACHE_DIR=/tmp/uv-cache ANTFLY_E2E_PRESERVE_ROOT=1 uv run --project e2e/antfly pytest -q e2e/antfly`
- result: `192 passed, 10 skipped in 1256.92s (0:20:56)`

Focused verification after the lookup/transaction fixes:

- command: `UV_CACHE_DIR=/tmp/uv-cache ANTFLY_E2E_PRESERVE_ROOT=1 uv run --project e2e/antfly pytest -q -s e2e/antfly/test_transactions.py`
  - result: `21 passed in 145.58s (0:02:25)`
- command: `UV_CACHE_DIR=/tmp/uv-cache ANTFLY_E2E_PRESERVE_ROOT=1 uv run --project e2e/antfly pytest -q -s e2e/antfly/test_index_lifecycle.py`
  - result: `27 passed in 203.23s (0:03:23)`

Passing / skipped Antfly E2E areas in the latest full run:

- passed: `192`
- skipped: `10`
- the full CDC file is passing in the current Antfly suite
- previously failing backup/restore managed chunked semantic, managed
  embedding pacing, quickstart chunked semantic, and schema migration full-text
  rebuild cases are no longer failing in the latest full run

### Recently Resolved / Superseded E2E Failures

- Stateful lookup / full-text derived index race:
  - fixed by making lookup prefer live local writer DB leases and fall back only
    to lightweight primary/status opens, plus transaction torn-state conflict
    mapping and WAL directory retry handling
  - verified by focused transaction/index-lifecycle runs and the latest full
    Antfly suite
- CDC distributed apply and projected status summary counters:
  - current full Antfly run includes the CDC file passing
  - focused verification previously passed with `8 passed in 36.61s`
- API-only remote index status:
  - `test_non_host_api_reports_remote_index_status_from_metadata_heartbeat`
    is no longer a current full-suite failure
- Backup / restore managed chunked semantic restore:
  - no longer a current full-suite failure
- Managed embedding pacing:
  - no longer a current full-suite failure
- Schema migration full-text rebuild:
  - no longer a current full-suite failure
- Stateless OCC conflict 500:
  - transaction focused coverage and the latest full Antfly run are green after
    mapping torn participant state to conflict responses
- Chunked full-text materialization:
  - `test_mutable_table_chunker_full_text_index_persists_chunks` is no longer a
    current full-suite failure
- Data-raft multinode scaling:
  - `test_autoscaling_finalizes_shard_split_from_size_threshold` is no longer a
    current full-suite failure

