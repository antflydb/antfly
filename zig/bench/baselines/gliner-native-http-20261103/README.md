# Native GLiNER HTTP qualification, 2026-11-03

Status: **partial**. `summary.json` records source revisions, limits, tests,
statistics and remaining work. The runtime fix passed six selected Debug checks
and an optimized native HTTP request at exactly 16,384 tokens in one window.
The unchanged request heap is 512 MiB. Execution capacity does not establish
long-context extraction accuracy.

## Corrected timing and results

Commit `29570dcf9924bce387c3d824af955f9f47956144` separates transport capture
from parsing/validation. Every concurrent response is captured and timestamped
before any JSON or oracle work starts. The native allocation-failure regression
also runs independently after adding its test-root import. Eight Python checks
and the standalone Debug regression pass; their logs are included as text.

`http-isolated-c4.json` contains the corrected run. Short and reference-window
cases completed 100 pairs each, with all outputs matching their oracles and
upper paired 95% median-cost bounds below 1.05. The composed-window phase failed
after 53 pairs with candidate live-host admission refusal. Its four 512 MiB
reservations exceeded the observed 1,932,735,283-byte live capacity. This shared
host had approximately 22 GiB of swap in use. Server logs identify the refusal;
no branch-induced admission regression is established. Binaries, model files,
and cases were independently rehashed unchanged after the failed campaign.

No overall throughput, tail-latency or four-client full-word pass is claimed.
The original single-client short-request slowdown and single-client full-word
confidence interval remain unresolved. CI also requires maintainer approval.

## Evidence format and superseded runs

The `http-fixed-*.json` reports precede the timing isolation fix. Retain their
single-client evidence, but treat their concurrent latency/throughput statistics
as superseded: one client's validation could delay another client's completion
timestamp. The original full-word concurrent report failed after seven pairs.

Reports deduplicate identical extraction outputs without dropping raw samples.
Each file contains `report`, `raw_report_sha256` and
`responses_by_canonical_json_sha256`. Recursively replace each `output_sha256`
field with an `output` field containing its referenced response to reconstruct
the original parsed report. Every compact report was round-trip checked against
its original before publication. These are public synthetic cases, not private
conversation inputs.

The case files and protocol preserve the common pre-feature baseline and fixed
candidate comparison. Absolute artifact paths identify the local experiment;
replace them with the rebuilt artifact/model paths when reproducing. The two
native services use two CPU threads each, `-Doptimize=fast -Dmetal=false`, four
request slots, work budget 32 and 4096 MiB aggregate scratch. Per-request limits
are unchanged. No SDK changes are required.
