# Qualification, lifecycle, and corrected TTL evidence

The corrected actual-Metal snapshot/listing/TTL selection passed **11/11 tests** using the same frozen executable and unchanged guards as its earlier admission-denied attempt. This archive also retains all six failed command checkpoints and the separate nine passing model-free teardown probes. It is local implementation evidence, not GA, release, remote CI, model quality, or performance qualification.

| Checkpoint | Result | Interpretation |
| --- | --- | --- |
| Qualification/lifecycle v1 | Compilation failed | Missing overlap `.allow` case; no main inference suite ran. |
| Qualification/lifecycle v2 | 49 selected: 47 pass, 1 skip, 1 fail | Incorrect warm request-buffer ownership assertion. |
| Qualification/lifecycle v3 | 50 selected: 48 pass, 1 skip, 1 fail | Fixture five-second observation deadline; production thirty-second ticket did not expire. |
| Final-focused v1 | Compilation failed | Learned-window test watchdog optional-type inference. |
| Final-focused v2 | 67 selected: 65 pass, 1 skip, 1 fail | Actual TTL failure exposed metrics snapshot release incorrectly refreshing idle-use time. |
| Observation/TTL v1 | 11 selected: 10 pass, 1 fail, no skips/leaks | Unchanged live-memory admission rejected the first inference. This is **not a TTL execution result**. |
| Observation/TTL v2 | **11 pass, no skips/failures/leaks** | Direct retry of the exact frozen binary passed actual model retention, eviction, reload, metrics observation, and cleanup checks. |

The observation fix separates read-only model snapshots from actual inference-use release. Its exact reviewed patch, pre-application proposal, applied receipt, and three changed source files are preserved. The successful selection covers three new snapshot regressions, the existing retired-owner regression, six model-listing tests, and the actual pinned-small Metal TTL test. The abandoned post-scrape timestamp-only workaround is not recorded as applied.

The failed observation attempt requested 2,415,919,104 bytes while 536,870,912 bytes were pending against live capacity 2,807,284,480 bytes. The sum exceeded capacity by 145,505,536 bytes and correctly produced HTTP 503 `MODEL_RESOURCE_BUSY` before inference. That raw failure remains intact. The retry changed no source, model path, filters, resource caps, or process guards and performed no rebuild. Both attempts bind the 82,720,136-byte executable SHA-256 `4a7af5cb45c5202b51ddfd57065d866a67c521be00bba85667f71b02d749fc64` and the same 2,588-file source inventory. The original executable and complete source tar were rehashed but are not included.

The retry's observed runtime working directory was `zig`; the build-target test's was `zig/pkg/inference`. The fixture resolver explicitly supports both. The retry completed in 74.023516584 seconds with sampled child-tree RSS peak 1,001,390,080 bytes. Whole eligible-eviction and final reload-eviction maintenance took 15.871518 and 20.680144 seconds, both below the existing thirty-second boundary; held-handle, before-expiry, and empty-cache observations each reported one microsecond. These whole-maintenance observations include post-close memory reclamation and are **not isolated driver destructor timings or performance benchmarks**. Exact post-metrics idle timestamps, final caches, leases, tickets, and transient cleanup assertions passed. Source inventories stayed unchanged and every observed owned child was reaped.

The nine earlier fresh-process fake-destructor probes remain tied to their own `7e9519…` executable. They verify expected watchdog exit 86, bounded reaping, active close tickets before cache cleanup, and two blocked-stderr fatal paths. The escaped raw-session probe deliberately has no admission lease; it proves ticket/owner lifetime rather than lease-order retention. Those process probes are separate from the actual-model TTL result. The learned multi-window native/Metal tests and allocation regressions passed individually in final-focused v2, but its aggregate stays failed. Their predeclared exercise profiles do not claim an upstream whole-document quality oracle.

`prior/` is the unchanged prior evidence directory, including both immutable earlier ledgers, all raw failures, nine child streams, the two 203-test Python passes, and the failed clean-dependency reproduction. New raw build/retry receipts, observations, source inventories, stdout/stderr, and both archive bindings are under `observation/`. Selected exact source bytes are shared when their hashes match. Only the 1.4 MB `server.zig` snapshot is stored as deterministic gzip with its original byte size and SHA-256 retained; the verifier bounds inflation at 2 MiB. All prior source bytes and every raw receipt remain unchanged. No binaries, full tars, or model weights are packaged, and the archive stays below 8 MiB. Source inventories are selections, not complete dependency closures.

Run the offline standard-library verifier from any location:

```sh
python3 /absolute/path/to/qualification_lifecycle_execution_v1/verify.py \
  /absolute/path/to/qualification_lifecycle_execution_v1 --self-test
```

It rehashes every file, recursively checks the immutable historical evidence, verifies the same-binary/source/guard retry, checks the applied source pins and five exact maintenance phase records, and safely validates the compressed source. Adversarial checks reject admission denial relabeled as a TTL result, a relaxed retry guard, and a different retry executable. No model, GPU, child-process probe, or build runs during verification.
