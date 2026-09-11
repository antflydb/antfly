# GLiNER2.5 extraction monitoring

The repository provides an optional Grafana dashboard, recording rules and
example alerts for the fixed-enum V2 extraction metrics. These files do not
change a running deployment, load a model, enable a capability or configure a
paging receiver. Review the targets, resource ceilings and example thresholds
before importing them.

## Artifacts and explicit opt-in

All artifacts live in [devops/monitoring/gliner25](../devops/monitoring/gliner25):

| File | Purpose |
| --- | --- |
| `dashboard.json` | Twenty metric panels and a scope note; Prometheus datasource, job and instance selectors; direct metric queries so recording rules are optional. |
| `recording.rules.json` | Thirteen rates, ratios and durations, retaining job/instance identity. |
| `alerts.rules.json` | Seven optional warning/info alerts with minimum-duration checks and runbook references. No receiver or critical/page severity is configured. |
| `prometheus.example.json` | Bounded example scrape and rule-file configuration. Its `.invalid` hostname must be replaced explicitly. |
| `compose.dashboard.json` | Optional read-only dashboard mount for either existing local compose example. It adds no service, scrape target or alert rules. |
| `rules.test.json` | Offline native Prometheus rule evaluation fixtures. |
| `tooling.json` | Versioned official promtool 3.14.0 archive sizes/hashes for Linux amd64/arm64 and macOS arm64, plus preparation ceilings. |

The JSON files are accepted by Prometheus's YAML configuration parser. The
existing `devops/prometheus.yml` and both compose deployments remain unchanged.
Each rule and dashboard query requires the static scrape label
`gliner25_monitor="enabled"`. Apply that label only to reviewed inference
metrics endpoints, not to every Antfly scrape.

The inference node registers `/ml/v1/metrics`. A dedicated inference listener
defaults to port 8090; embedded inference registers the same path on the
public listener, commonly port 8080 in the compose examples. The existing
compose scrape of `antfly:4200` serves different process/data metrics. It does
not replace a scrape of the inference route. Confirm the selected deployment
actually exposes the V2 metric family; the dashboard deliberately displays
missing data when it does not. Scrape the worker handling remote extraction,
not just the caller's host. Avoid scraping the same node through multiple
aliases and counting it twice.

Merge the example scrape into the deployment's reviewed Prometheus config;
preserve its existing jobs, authentication/TLS and rule files. Set the real
target, mount the selected rule files read-only, and update `rule_files` to
those mounted paths. The example has a 15-second scrape interval, 10-second
timeout, 20,000-sample ceiling and bounded label sizes. Its rule groups cap
output at 10,000 series. Reconcile those ceilings with the entire endpoint and
target count before enabling them. Scrape/rule-limit errors are failures,
not evidence of zero load; monitor Prometheus's own scrape and rule-evaluation
health through the deployment's established monitoring.

Import `dashboard.json` into Grafana and select its Prometheus datasource.
For the local compose examples, this command only renders an optional combined
configuration; it does not start or update services:

```sh
docker compose -f devops/docker-compose/docker-compose.yml -f devops/monitoring/gliner25/compose.dashboard.json config
```

The S3 example uses `devops/docker-compose-s3/docker-compose.yml` as the first
file. Relative mounts resolve against that first compose file's directory.
The overlay uses the existing dashboard file provider and mounts the same
source dashboard for both examples. No Grafana server/rendering or compose
deployment has been exercised by the offline checks below.

## Interpret the signals

Every chart and recording rule retains `job` and `instance`. Request-derived
labels are limited to the native transport/outcome/stage/solver enums; model
names, schemas, document IDs, entity labels and extracted values are absent.
The dashboard has a fixed panel count, 1,000 data points per query and a
30-second refresh. Deployment target cardinality and Prometheus query-resource
limits still need operator admission.

Counter rates are calculated before aggregation so one worker or transport
reset does not corrupt another series. Missing series are never replaced with
zero, and graph gaps are not connected. Idle counters are observed zeros;
unavailable metrics remain unavailable. Ratio/quantile panels may have no
meaningful value when traffic is absent. The variable selectors use `up`, so
an opted-in target remains selectable even when its V2 metrics disappear.

Latency histograms and phase sums originate in nanoseconds. The queries
convert them to seconds after computing a quantile or mean. Dispatch begins
after recognizing V2 and ends after owned cleanup; HTTP body collection,
version probing and response publication lie outside that interval. Example
dispatch thresholds are not an end-to-end service SLO. Review task and document
length mix rather than treating a cross-model process aggregate as a model
benchmark.

Parsed, decoded and returned item rates are separate. A decoded item or
completed document window can belong to an atomic request that later fails.
Returned items count successful dispatches, not delivery acknowledgments.
Neither subtracting those rates nor subtracting planned/completed windows
establishes queue depth. Prompt-token work includes overlapping windows.

Strict exhausted searches appear in `outcomes_total{outcome="search_exhausted"}`.
The separate solver exhaustion/status/node counters describe validated
witnesses from decoded items, including best-effort results; they do not
account for all work performed by strict failed searches. A feasible witness
does not establish global optimality or successful request publication.

The host-memory gauge is a process-lifetime maximum of request-owned capped
heaps. It excludes model residency, device allocations, transport and
caller-owned buffers; it is not RSS. Hard worker termination can prevent an
in-process completion and reset all worker counters. Use parent supervision,
worker-exit and process/resource monitoring for that boundary. These optional
rules do not add durable hard-kill accounting or detect a target removed from
service discovery; expected-target inventory needs separate monitoring.

See the [metric contract](GLINER25_OPERATIONS.md#inspect-extraction-lifecycle-metrics)
for every family and its recording boundary.

## Example alerts and response

The workload alerts require at least 20 completed calls in the preceding five
minutes and a sustained ten-minute condition. Thresholds below are examples
to calibrate before use. They are not a claimed production error budget.
Client validation, unsupported features, infeasibility and caller cancellation
are visible outcomes, not server-failure alerts. No Alertmanager configuration
or notification destination is included.

### GLiNER25ScrapeUnavailable

The opted-in endpoint has `up=0` for five minutes. Check listener, TLS/auth,
network and parent/worker lifecycle. A stopped worker cannot complete its own
request trace. Keep client failures and parent termination evidence; do not
interpret a missing completion as success.

### GLiNER25MetricsMissing

A successful scrape lacks the V2 active-gauge family for five minutes. Confirm
the `/ml/v1/metrics` path, actual binary and source revision. A legacy or
different endpoint can be healthy while lacking V2 telemetry. Metric presence
itself does not assert a qualified model capability. Do not fill the missing
series with zeros to make the alert disappear.

### GLiNER25ServerFailureRatio

Backing OOM, model errors and internal errors exceed 5% of completed calls.
Inspect bounded outcome and failure-stage charts, immutable artifact identity,
backing allocation and worker logs. The outcome and stage counters are separate
families, so their charts alone do not prove a joint per-request correlation.
Pause a canary/backfill when its reviewed error budget is exceeded and preserve
the failed request's bounded diagnostic receipt.

### GLiNER25AdmissionPressure

Transient admission denials exceed 20%. Reduce offered concurrency or backfill
load and check capacity leases. Use the existing bounded retry policy only for
transient admission pressure. Changing constraints or truncating text is not
an admission recovery policy.

### GLiNER25FixedLimitRejections

Explicit memory/resource ceiling rejections exceed 10%. Identify the failing
input/window/output/host ceiling. Repeating an unchanged fixed-cap request
cannot make it fit. Increase admitted capacity or route to a larger reviewed
worker while preserving request semantics. Genuine backing OOM remains a
separate server failure.

### GLiNER25DispatchLatency

Dispatch p95 exceeds five seconds. Check phase duration, admission, cold-model
work and document-window counts, then compare against the reviewed workload
and client-observed SLO. Sparse histograms and very different task/length
mixes limit this example threshold. It does not measure network publication.

### GLiNER25StrictSearchExhaustion

Strict search rejection exceeds 10%. This informational rule asks for review
of the declared search ceiling and constraints. Never silently drop
constraints or enable best effort. If callers explicitly choose best effort,
retain validity/exhaustion metadata and inspect the separate witness charts.

## Offline validation and provenance

The standard-library contract tests are discovered by the existing GLiNER2.5
CI suite. Without an explicitly supplied promtool binary, the six structural
tests run and the two native-tool tests report a skip:

```sh
python3 zig/pkg/inference/scripts/gliner25/test_monitoring.py -v
```

The two Linux amd64/arm64 GLiNER2.5 CI steps now run through
`scripts/gliner25/bootstrap_monitoring.py`. It chooses the exact host archive
from `tooling.json`, verifies the pinned published checksum list and archive,
and extracts only the exact regular `promtool` member into a private temporary
directory. Links, duplicate tool members, changed bytes and excessive archive
inventory are rejected. The bootstrap sets `ANTFLY_GLINER25_PROMTOOL` only for
the child contract command and drains its temporary owner after completion or
failure. Tool acquisition/verification failure fails the step; it does not
silently skip native rule tests. Remote CI execution is not yet claimed.

Preparation caps archive downloads at 128 MiB, the tool at 256 MiB, declared
unpacked inventory at 768 MiB and members at 64. Network operations use a
30-second I/O timeout and a 300-second overall preparation budget checked
between chunks; a blocking read can consume its remaining I/O timeout before
that check runs. The child contract command has a 15-minute timeout. TLS
verification remains enabled and redirects cannot downgrade HTTPS. No global
PATH, installed package or production configuration is changed.

The official Linux archive SHA-256 pins are
`f665c6da19eb7ba399c915d30c7d9793c9b417bf8a749b504bc470678631478d`
(amd64, 107,111,714 bytes) and
`077f3781ab7245dc04c9a3c9b78ba120fc8e41aa0dc97489b0af67247e50ba83`
(arm64, 98,015,563 bytes). Both agree with the same official release metadata
and published checksum list used for local validation.

Native validation uses the official
[Prometheus v3.14.0 release](https://github.com/prometheus/prometheus/releases/tag/v3.14.0),
with the macOS arm64 archive checked against both its
[published checksum list](https://github.com/prometheus/prometheus/releases/download/v3.14.0/sha256sums.txt)
and GitHub release asset digest. Only `promtool` was extracted under
`/private/tmp/antfly-gliner25-promtool-3.14.0`; no package or service was installed.
The archive SHA-256 is
`a9623f7f4fe65b1b171b423c1a72bbf23dfdf41a171dcb33e7dd302af80dc01c`;
the extracted promtool SHA-256 is
`8273d9b5f8a5fc624f503d667055d5f1e542046405548491246e095b9da818f6`.
Release/checksum metadata and `provenance.json` are retained alongside it.

```sh
ANTFLY_GLINER25_PROMTOOL=/private/tmp/antfly-gliner25-promtool-3.14.0/promtool python3 zig/pkg/inference/scripts/gliner25/test_monitoring.py -v
```

The local macOS validation also exercised the actual bootstrap using
`--existing` with the verified binary. This path copies and rehashes the binary
into a new private owner; it does not trust the supplied path or skip identity
verification. All 17 monitoring/bootstrap tests pass without skips, including
injected download/size/hash/header/deadline failures, archive ownership and
cleanup, child environment/failure propagation, and both CI step bindings.
The retained receipt is
`/private/tmp/antfly-gliner25-monitoring-validation-v1/validation.json`, SHA-256
`a8e20a55b743bafd2f5fa63c0f92e30b3fcddbaf3d0d88672f33b29eea14ec7b`;
its `checks.log` SHA-256 is
`da6a7f9d21f42d88cc8bbc11a8479860d81bb888d94c49d0a184fdbd3ded9a0b`.

The native tests parse the example config, all 20 rules and every dashboard
PromQL expression without starting a server. Five scenario groups exercise
56 positive/negative alert expectations and 14 numeric/presence expectations:
missing versus idle/absent/opted-out targets; per-instance isolation; traffic
and duration gates; failure recovery; resource/admission/strict-search versus
witness signals; counter resets; time units; and decoded versus returned work.
The official [rule-test format](https://prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/)
supports `fuzzy_compare`; the fixture enables its last-mantissa-bit tolerance
for a one-ULP rate arithmetic difference, without changing alert thresholds.

Offline rule evaluation does not prove a live HTTP metrics scrape, Grafana
rendering, alert routing or concurrent production service behavior. The next
service qualification should exercise the real `/ai/v1/extract` and
`/ml/v1/metrics` routes against a pinned worker: exact atomic errors, explicit
unsupported capability handling, cancellation/recovery, and matching attempt,
completion and returned-item observations. Actual-model success, concurrent
load, eviction and hard termination require their separate guarded receipts.
