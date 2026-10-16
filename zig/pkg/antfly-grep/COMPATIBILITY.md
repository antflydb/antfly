# Compatibility

Target: ripgrep 15.2.0 / PCRE2 10.45. Implementation: native Zig.

Checked-in differential contracts: 292 local-search cases and 76 PCRE2 dialect cases. Normal tests require no oracle executable.

| Area | Status | Evidence and limits |
|---|---|---|
| Regex, Unicode and output combinations | tested subset | [local_fixtures.json](tests/local_fixtures.json), [pcre2_dialect_fixtures.json](tests/pcre2_dialect_fixtures.json). Golden fixtures establish the enumerated cases; complete upstream dialect/JIT parity is not claimed. |
| Discovery, binary input, encoding, JSON/statistics | tested subset | [test_cli.py](tests/test_cli.py). Includes Git ignore/config, late NUL, transcoding and platform-specific contracts; exhaustive gitwildmatch/platform parity is not claimed. |
| Python, Go and Rust source understanding | pinned grammar coverage | [README.md](../../lib/syntax/README.md). 328 upstream valid named trees; no macro expansion or semantic type resolution. |
| Decision relevance and source evidence | fixture pipeline + opt-in provider evaluation | [relevance.json](tests/relevance.json), [evaluate.py](bench/evaluate.py). Fixture precision/recall/nDCG test plumbing and independent-line discovery, not model quality. |
| Decision budgets, cache, retries and cancellation | fixture tested | [test_cli.py](tests/test_cli.py). Provider checks use HTTP/native fixtures. POSIX interrupt cancellation is cooperative; Windows signal behavior is not runtime-verified. |
| Antfly indexed tables | fixture tested exact scan | [document-v1.json](schema/document-v1.json), [tables-v1.json](schema/tables-v1.json), [test_cli.py](tests/test_cli.py). Snapshot-bound ID cursors are required. Exact scans are bounded by request/document/source/output budgets. No connector ingestion or direct remote-file downloads. |
| Unified local/indexed results | shared matcher, decision and renderer | [root.zig](src/root.zig), [document.zig](src/document.zig), [test_cli.py](tests/test_cli.py). JSON events expose schema version, provenance, source revision, index time, snapshot and observation time. Local sources are live; indexed sources represent stored revisions. |
| Performance | reproducible measurements | [compare.py](bench/compare.py). Tracks startup, first stdout byte, elapsed/CPU, RSS and throughput on generated large files and 1000-file repositories. Host, binary hashes and per-run results are recorded; no general ripgrep performance parity claim. |
