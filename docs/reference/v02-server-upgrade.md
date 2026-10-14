# Upgrading released v0.2.1–v0.2.5 servers

The v0.3 server can open directories and restore backups written by the
released v0.2.1–v0.2.5 Zig servers. This contract covers their writer outputs;
it does not extend to every older encoding those releases could read.
Serverless and libantfly have no previous release compatibility contract.

Before upgrading, take a backup with the old server and retain a complete
stopped copy of its data directories, replica catalogs and snapshot roots.
Stop all metadata, data and hot-standby nodes before replacing binaries. Use
the same v0.3 build throughout the deployment; a mixed-version rolling upgrade
is not the upgrade procedure. Start metadata nodes, then data nodes, then
standbys. Check table/index status and exercise reads and writes before
resuming application traffic.

The first open atomically advances `ANTFLY_FORMAT` to storage format 2 and
minimum reader 2, and logs the rollback boundary. This admission happens before
server writes, even if a later table open fails. Retrying with v0.3 is safe;
v0.2 servers reject the upgraded directory before opening storage. To roll
back, restore the pre-upgrade stopped copy with the old binary. Editing the
marker is not a rollback procedure.

| Released output | v0.3 behavior |
| --- | --- |
| ASCH v12 runtime schemas and public JSON | Recover additive column/full-text path metadata from the matching public schema. Preserve stored active and immutable historical bytes. Reject changes to fields written by v0.2. |
| JSON documents, identities, TTL/tombstone records | Retain document storage mode and existing primary records. Do not convert these tables to relational packed rows. |
| LSM manifest v9, SSTable v11, full-text AFSM v4 | Existing readers admit these formats. New writes/checkpoints use current writers, subject to the directory rollback gate. |
| Dense index projections | Existing generation migration rebuilds the native projection from authoritative sources. Index status exposes rebuild availability. |
| Dense embedding source ownership | Preserve the persisted `primary_lsm` default. Moving sources into the shared vector store remains an explicit storage migration, independent of physical index migration. |
| AFB2 portable backups, reader level 2 | Authenticate metadata before validating document rows, including released document-before-metadata archives. Verify the source fingerprint across passes. |
| Descriptorless Raft batches | Replay through the prepared resident owner with the atomic term/index receipt, preserving exactly-once increments across restart. |
| HA v1 records/manifests, topology v3, KV stream v2, capture receipt v2 and published seed receipt v4 | Retain existing readers, verify captured file digests, reopen old WAL/progress state, continue streaming, and materialize/activate the released seed. |
| Native-generation manifest v3 | Recover canonical install paths absent from the released writer; retain ownership, digest and inventory checks. Rebuild incompatible dense/full-text projections from the authenticated primary before publication. |

Legacy public/runtime schemas must agree on every field the released writer
persisted. Recovering new derived metadata does not authorize a user to change
an immutable schema epoch. Modern archives retain their stricter ordering and
schema requirements. For reader levels 3–8, validation and restore retain the
consumed manifest in the original authenticated pass; they never reset an
incomplete archive reader.

For release qualification, build the server and run:

```sh
python3 scripts/ci/qualify-v02-server-upgrade.py \
  --old-bin v0.2.1=/path/to/v0.2.1/antfly \
  --old-bin v0.2.2=/path/to/v0.2.2/antfly \
  --old-bin v0.2.3=/path/to/v0.2.3/antfly \
  --old-bin v0.2.4=/path/to/v0.2.4/antfly \
  --old-bin v0.2.5=/path/to/v0.2.5/antfly \
  --new-bin zig/zig-out/bin/antfly \
  --work-dir /path/to/retained-evidence
```

The harness uses each released server to make its own fixtures and validates
same-binary controls before attempting upgrade. It retains binary/file hashes,
logs, clean and crashed directories, and portable/native backups. It tests
lookup, full-text, dense/sparse queries, writes/deletes, second restart,
restoration and old-reader rejection. `--seed-only` and `--verify-only` split
fixture generation from upgrade qualification. Run the same command with
`--swarm-only` for stopped metadata/data Raft upgrades and an exactly-once
increment, `--schema-only` for TTL and immutable schema history, and `--ha-only`
for primary/standby WAL and progress reopen, released seed publication, current
restore/physical activation, and query/write/restart on the materialized seed.
Use `--managed-only` for embeddings generated through a deterministic local
provider, including portable/native restoration and fresh generation after
restart. Add `--chunked` with a separate evidence directory to qualify released
chunk artifacts and regenerated embeddings. Portable backups regenerate derived embeddings from their retained
source documents, so the configured provider must remain available. Restore
quiesces background enrichment and explicitly resumes synchronous provider
admission before draining.

Those checks do not qualify fenced failover or mixed-version rolling operation.
