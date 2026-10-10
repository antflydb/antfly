# Local real-provider qualification — 2026-10-09

Nessie **0.109.0**, Polaris **1.7.0**, and versioned RustFS
**1.0.0-alpha.81** ran as real local Docker services. Provider catalogs, commits,
object operations and native Antfly HTTP calls were executed. No cloud resources
were provisioned and no full-archive performance result is claimed.

The final package suite passed **33 tests**, with **four provider-specific skips**.
Both native HTTP cases ran against a binary built from the merged branch with the
negotiated-prefix fix. Coverage includes:

* Real table create/append/overwrite and row scans through lease-aware clients.
* Antfly HTTP delegated vacuum and exact replay after daemon restart.
* Multi-turn durable owner replacement and receipts after metadata advancement.
* Writer exclusion during physical deletion and retained external readers.
* Lost vendor commit response and positive marker-based recovery.
* Polaris expiration and rejection of retired snapshot/file resurrection.
* Native pins arriving during planning, retirement-before-final-pin-check, and
  revoked planner epochs that cannot be reused after reader admission reopens.
* Nessie native branch creation, branch/history root preservation and actual
  v2 reference pagination.
* Exact S3 version deletion preserving concurrent replacements; independent
  progress for all three versions of an orphan URI across owner replacement.
* Credentials/idempotency sanitization, authority-prefix separation, minimum-age
  policy typing and release of obsolete SDK leases while live streams retain them.

A separate qualification ran each final gateway image on both an internal backend
network and a client network. Client containers reached the authenticated gateway
but could not reach Nessie, Polaris or object administration by DNS or IP. Merely
separating ordinary bridge networks failed the IP probe; an **internal** backend
was required. The image builds with pinned, hash-checked runtime dependencies and
runs without root, with dropped capabilities and a read-only filesystem.

Retention fixtures advance the controller clock by 21 minutes; synthetic native
pin deadlines use that same clock. The native HTTP tests qualify the wire protocol
and restart behavior, rather than a production node/controller clock-skew race.
GCS generation handling is implemented but was not real-cloud qualified here.

The focused Zig negotiated-prefix test passed. The native binary built successfully.
After initializing the sparse-predicate test fixture's `overlay` and `active_recent`
fields, `zig build lake-api-test` completed with **48/48 steps successful**:
**131 API tests and 124 local lake tests passed**, with no skips, failures or leaks.
The local executable also passed directly with exit code zero. The previous
optional-unwrapping panic was caused by undefined fixture fields selecting the
recent-data fallback; production predicate behavior is unchanged. Concurrent SQL
and SDK changes in the shared workspace were preserved.

Production IAM/network enforcement, broader shared-data/delete-file combinations,
larger catalogs and archive-scale performance remain additional qualifications.

## Extended implementation qualification — 2026-10-09

The earlier results above describe the previous implementation. The following
qualification covers durable accepted SQL cuts, managed adapters, checkpoint
reuse and incremental vacuum planning added subsequently:

* The native binary builds with **46/46 successful steps**. The full lake API
  suite passes **136 API + 124 local tests**, with no skips, failures or leaks
  (**48/48 steps**). Validation used an isolated checkout to preserve concurrent
  SQL and SDK edits in the shared workspace.
* Focused accepted SQL tests pass: a durable session reuses the same metadata
  snapshot and WAL images after publication and queue collection; an aggregate
  and self-join remain unchanged, while a fresh accepted read sees the added row.
* Focused remote recovery tests pass: removing local chunk hints does not force
  another upload, and a recovered replacement with new file seals can reuse its
  verified remote checkpoint chunks without transferring them again.
* A real four-process native cluster passes the original public-cursor test
  across finalized split, finalized merge and restart of all data owners, using
  an S3 snapshot repository. This tests an original pre-split cursor; **fresh
  ordered snapshot capture after splitting still needs distinct physical
  generation identities for ranges that preserve a shared document namespace**.
* A real PostgreSQL 18 logical replication fixture passes managed provisioning,
  initial copy, subsequent update and owned physical slot/publication teardown
  against a distributed native cluster.
* A real GCS fixture in `antfly-dev-01` passes owned bucket notification,
  Pub/Sub topic/subscription and publisher IAM setup, event receipt,
  acknowledgement and teardown. Temporary cloud resources were removed. Its
  native handoff is a test double; native catalog reconciliation is validated
  independently. This is not an end-to-end cloud search qualification.
* The final local provider suite passes **22 tests**, with eight skips for
  unavailable native HTTP or provider-specific cases. A separate opt-in Nessie
  retention test passes: a leased old reader survives retirement, and its
  expired data files are actually deleted after the lease is released. The
  shortened-history test uses an isolated catalog because it intentionally
  removes files that conservative-history fixtures require.
* The local authority, managed source, incremental planning and retention suites
  pass **25 tests**. Python lint/format and staged whitespace checks pass.

Real AWS/SQS provisioning, archive-scale throughput and cloud-provider vacuum
performance remain unqualified. Incremental marking retains bounded metadata
and persists reachability across turns, but oversized individual manifests may
require replay rather than resuming at a byte offset. Nessie retention shortens
file reachability under the enforced gateway contract; it does not compact the
vendor's commit database. Cross-source graph/global aggregation execution,
pgwire accepted selection and serializable accepted external SQL remain outside
the supported query coverage described in the design documents.
