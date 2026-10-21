# Benchmark qualifications

Keep compact qualification summaries (`*-summary.json`), artifact hashes,
limitations, and reproduction commands here. Write raw trials, profiler samples,
server logs, and exploratory runs under the repository's ignored `output/`
directory. CI runs should publish those raw files as downloadable artifacts;
summaries should record their archive checksum and durable location when available.
An unpublished local artifact must be identified as local, not as downloadable
evidence. Benchmarks are measurements, not universal performance guarantees.

The [historical archive manifest](archive-manifest.json) preserves the names,
sizes, SHA-256 checksums, and Git blob IDs of the earlier reports and samples.
Their original bytes remain reachable through the manifest's source commit.
The numerical qualifications in the package README and compatibility document
refer to these historical runs unless they name a newer summary. Restore one
report, or omit `--file` to restore all of them:

```sh
python3 zig/pkg/antfly-grep/bench/restore_results.py \
  --file grep-execution-followup-validation.json \
  --output output/grep-benchmark-archive
```

Restoration verifies every checksum and refuses to overwrite different bytes.
A shallow checkout may need to fetch the manifest's source commit first. This
cleanup reduces checkout and review clutter; existing Git history retains the
original blobs. Correctness fixtures under `tests/` remain tracked and unchanged.
