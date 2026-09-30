# Local database source separation

This refactor targets main before the Lite/inference licensing and release PR
(#893). Existing source licenses remain unchanged; moved files keep their
original headers. License-header tooling records ELv2 files that moved into
otherwise Apache directories. Package naming, artifact publication, and CLI
behavior changes belong to #893.

## Source owners

- `zig/pkg/antfly-embedded` owns embedded facades, shared schema types, and
  Antfly inference provider adapters (including Vertex and Bedrock).
- `zig/pkg/inference/src/host` owns inference host/worker implementation and
  portable request/execution contracts.
- `zig/lib/runtime` owns borrowed runtime ABIs, cancellation, cache budgets,
  filesystem helpers, and threaded I/O limits.
- `zig/pkg/antfly-server-api` owns generated server routers and extractors;
  shared generated types live in embedded. Authored schemas stay in
  `specs/openapi`, with the generator importing shared type modules.
- Local API helpers and catalog/index reconciliation contracts remain under
  `zig/pkg/antfly` until the local database owner can move as a complete unit.
  Server replica catalogs, provisioning summaries, and coordination remain
  with the server.

## Database and hot standby contracts

`storage/db` owns apply receipts, durable outbox storage, replication policy
values, effect codecs, and publication sequencing. Borrowed publisher and
write-gate interfaces keep concrete hot standby runtimes outside DB production code.
`storage/hot_standby` supplies the primary, standby, fencing, policy/metrics,
and synchronous wait adapters. These interfaces preserve durable frame formats
and acknowledge writes only after publication/wait and authority revalidation.
Writer-cache identity includes adapter identity and borrowed lock identity.

## Engine boundary

DB executes typed replicated mutations; `storage/db/replication_ingress.zig`
owns envelope decoding and temporary payload allocation. Apply receipts remain
atomic with primary mutations and derived effects. The engine also owns record
and effect formats, durable outbox recovery, and local snapshot maintenance.
The runtime library owns the shared/exclusive mutation barrier. Hot standby
adapters own seed capture, authenticated replica restore coordination, and
remote acknowledgement waits.

Runtime names use `hot_standby_*` or `HotStandby*`; engine contracts use generic
replication names. Existing persisted key bytes, record encodings, error names,
and C ABI symbols/tags remain compatible. Server runtime integration tests
live under `storage/hot_standby`, with test-only hooks for white-box engine
assertions. Production DB sources do not import those fixtures or runtimes.

`zig build embedded-source-boundary-check` follows authored production imports
from the embedded source root and physical DB, excluding test bodies. It rejects
server coordination, missing sources, and imports outside the source owner, and
runs with the existing storage test ownership audit. Native and WASM builds
validate named module dependencies separately.

Local index reconciliation has its own result summary; server provisioning
keeps group/root counts separately. Local range observation limits and catalog
route identity are portable contracts. Server catalog command envelopes stay
with server coordination. Backup materialization stays with local storage;
replica-catalog restore admission stays under server Raft storage.

## Remaining separation

The physical DB and its complete local source closure must still move into
`antfly-embedded`. The local source owner now uses shared APIs directly rather
than server facades. The native C API's private server owner operations also
need separate ownership, while its public local DB/inference surface and WASM
build must remain independently buildable. Source-boundary checks and staged
build verification will enforce that separation.

## Review and merge order

Merge this refactor into main first. The licensing PR applies its Apache
boundary, packaging, and release changes on top. Its source moves and DB
refactors should then disappear from its diff against main.
