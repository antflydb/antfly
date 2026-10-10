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
