# Continuous writable-lake qualification

`continuous_lake.py` exercises continuous HN-derived changes, automatic text
publication, catalog-history growth and vacuum concurrently. It complements the
static archive search comparison in `poc.py`; it does not replace full-archive,
regional Cloud, external Iceberg writer, or DuckDB qualification.

The default bounded profile uses two dedicated object tables:

- `hn_catalog_history` receives paced, single-row commits to cross the native
  100-small-manifest merge threshold, a burst of transactions to observe commit
  grouping, then continuous updates. It has no search indexes, isolating catalog
  costs from sidecar construction.
- `hn_search_churn` receives a small HN seed, upserts and deletes while published
  and accepted highlighted searches run. It has a stored-source text index and
  scalar indexes. A stable seed anchor must survive every query. Final accepted
  SQL and published search must match acknowledged writes exactly, including
  deletions, before and after restart/maintenance.

Both tables use the Antfly-managed Iceberg catalog. Vacuum runs with its normal
minimum ten-minute retention and a bounded deletion budget. Incomplete operations
keep the same operation ID across turns and restart. The driver resumes unfinished
jobs every second (or the shorter configured maintenance interval), while spacing
new jobs for each table by `--maintenance-interval`. It separately checks
that some observed expired snapshot manifest-list objects are actually absent.
Manifest merging is not Parquet compaction; deleting metadata objects does not
establish reclamation of all superseded row data.

## Running

Build the engine from the revision under test. Use Debug to investigate engine
failures and ReleaseFast for final performance measurements. Keep build caches
inside the worktree, as required by `AGENTS.md`.

From `examples/hackernews`, inspect a plan first:

```sh
uv run --group dev --frozen python continuous_lake.py \
  --binary /absolute/path/to/antfly \
  --engine-commit FULL_40_HEX_SOURCE_REVISION \
  --fixture /absolute/path/to/hn.parquet \
  --state /absolute/path/to/fresh-state
```

Add `--run` to execute locally. For real GCS add `--provider gcs`, `--project`,
`--bucket`, and `--prefix hn-qualification/UNIQUE_ATTEMPT`. The bucket must already
exist and the prefix must be empty. Use only an exclusively owned qualification
namespace: this workload explicitly authorizes vacuum to delete its unreachable
objects. The driver obtains a short-lived bearer using the existing `poc.py`
credential helper, places it only in the engine environment, and stores only a
secret reference in configuration. It refreshes the credential on restart; this
is not an unattended credential-lifetime test.

The driver neither creates VMs nor exports BigQuery data nor changes IAM. Default
limits are 2,700 seconds per run, 128 seed rows, 120 paced history commits,
32 burst transactions, and at most 256 subsequent updates per table. The 780-second
continuous phase allows retention to elapse naturally. Search uses one worker by
default; `--query-workers` permits up to eight for separate contention probes.
GCS inventory is sampled about once a minute; slow observation can extend the
interval. A sampled count above 50,000 objects or 1 GiB stops the engine, with
bounded termination grace. These checks are not billing or hard provider
spending controls. Use an approved project/budget and reuse an
existing export.

Each attempt requires a fresh state directory and, for GCS, a fresh prefix.
`--duration-seconds 10 --history-commits 2 --burst-transactions 8 --seed-rows 8
--max-updates 4 --max-seconds 300` is a functional smoke test. Short profiles are
explicitly unqualified for manifest-threshold and retention behavior.

## Evidence and interpretation

The private state directory retains engine logs, redacted structured events,
pending write/maintenance intent, fixture and executable hashes, source revision,
paired request-start/completion IDs, query successes and failures, catalog
observations, periodic/pre-restart phase counters, and `report.json`. Do not
commit raw logs, bearer tokens, or environment dumps. `complete` describes the
functional run; `qualified` additionally requires the full profile, successful
queries, observed manifest merging and commit grouping, retention aging,
deletions reported during the continuous phase and independently observed
deletion. Failed attempts retain evidence and do not count as successful
qualification. Even short smoke profiles exit unsuccessfully after
search failures. An independent deadline owner stops the engine if foreground
observation stalls; teardown allows bounded termination/drain grace. Credential
helper execution is bounded separately.

Transport records distinguish failures before response headers from failures
reading a response body. A known header status is stored as `response_status`,
separately from an HTTP-error `http_status`, so a broken 200 body cannot be
mistaken for an acknowledged write. Error-body timeouts and truncated responses
retain the same structured evidence; response bodies and exception messages are
discarded. Older records without these fields cannot establish the transport
stage retrospectively.

Catalog and publication lag are measured from the acceptance ACK to first sampled
visibility. They are sampling upper bounds. Superseded changes and deletes without
an observed row image remain censored, rather than receiving zero latency.
Observed snapshot and published-revision counts are lower bounds between samples.
Direct manifest diagnostics hold no engine reader lease; a concurrent retirement
404 is recorded as an unleased-observer race, while other failures still fail the
run.

Object-operation counters cover the API runtime during registration, background
publication, foreground traffic and maintenance, ending before restart. They
exclude direct observer reads and separately owned runtimes. They are logical
operations, not exact provider requests, billable operations, or isolated
publication amplification. Observer calls are counted separately. Restart metrics
are retained separately. Managed vacuum deletion counts sum acknowledged turns;
a lost response may censor work, so these are not an exact durable job total.

The fixture intentionally normalizes nulls, limits body size, and puts a common
search term in every row. It tests correctness under dense matches and small
continuous changes, not the archive's natural term distribution or scale. Host
load is sampled; results from a contended developer machine must retain that
label. Do not compare these timings directly to earlier static fixtures or claim
an isolated speedup without a matched control run.

The [2026-11-04 UTC baseline](../../docs/operations/hackernews/continuous-lake-qualification-20261104.md)
records the successful catalog checks and remaining availability/maintenance gaps.
Both full provider profiles failed the strict qualification criteria.
