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

## Database and HA contracts

`storage/db` owns apply receipts, durable outbox storage, replication policy
values, effect codecs, and publication sequencing. Borrowed publisher and
write-gate interfaces keep concrete HA runtimes outside DB production code.
`storage/hot_standby` supplies the primary, standby, fencing, policy/metrics,
and synchronous wait adapters. These interfaces preserve durable frame formats
and acknowledge writes only after publication/wait and authority revalidation.
Writer-cache identity includes adapter identity and borrowed lock identity.

## Remaining separation

The DB is not yet independently housed in embedded. Replay/seed adapters and
runtime-dependent test fixtures still need extraction before moving its owner.
The source layout here is a prerequisite for that work, not completion of the
full embedded package separation.

## Review and merge order

Merge this refactor into main first. The licensing PR applies its Apache
boundary, packaging, and release changes on top. Its source moves and DB
refactors should then disappear from its diff against main.
