# Codex migration profile

This profile generates all relational mappings and a history-safe metadata
overlay for the pinned `antfly/integration` schema. It does not change the
generic Zig tool into a Codex application or switch the active store.

The [design and preservation gates](../../../../../docs/design/codex-sqlite-migration.md)
are part of the plan. In particular, the existing Codex importer rewrites
fallback history timestamps; a lossless full cutover requires a
Codex-owned timestamp-preserving replay path and application acceptance.

Stop Codex before preparing a bundle intended for a full migration:

```sh
python3 prepare.py \
  --codex-repo /path/to/antfly-integrated-codex \
  --codex-home /path/to/codex-home \
  --sqlite-home /path/to/configured-sqlite-home \
  --out /path/to/new-private-bundle

python3 capture_history.py \
  --bundle /path/to/new-private-bundle \
  --codex-home /path/to/codex-home \
  --source-quiesced
```

Python 3.9+ and the standard library suffice. The SQLite files are captured
through the backup API into standalone immutable inputs. `control.sqlite`
merges v1/v2 memory state using versioned keys. The script rejects target schema
drift and unclassified source tables/columns. Paths in generated plans resolve
relative to the bundle, not the shell's working directory.

To regenerate plans from a prior captured bundle, pass `--existing-snapshots`
with `--sqlite-home` pointing at that bundle. Every input is checked against
the previous manifest's hash. This reuses immutable captures without reading
live SQLite again and preserves the original log disposition.

The history capture can be about as large as the entire source history. It
copies selected files and rebases `rollout_path` only in the importer's copy
of the state DB, preserving the original relational snapshot.

The bundle contains:

- `relational.json`: 31 target mappings and authoritative target DDL/indexes.
- `metadata-overlay.json`: an upsert into a stable snapshot of the
  history-populated baseline, preserving importer-owned fields.
- `manifest.json`: coverage, schema pin, snapshot hashes, source counts,
  phase ownership and acceptance gates.
- `rollouts.json`: source file inventory and capture validation evidence.
- `schema-contract.json`: exact exported target schema and provenance.
- Original SQLite snapshots, `control.sqlite`, and (when captured) a separate
  `captured-codex-home` for the history importer.

Run the stages in order, using a recent main build of libantfly:

```sh
antfly-migrate migrate --plan /path/to/bundle/relational.json --dry-run
antfly-migrate migrate --plan /path/to/bundle/relational.json

# Requires the timestamp-preserving replay contract from the design document
# before accepting a lossless cutover. --replace rebuilds the projections.
codex-antfly-import \
  --codex-home /path/to/bundle/captured-codex-home \
  --to /path/to/bundle/01-relational.aflite \
  --replace --no-semantic

antfly-migrate migrate --plan /path/to/bundle/metadata-overlay.json
```

The examples directory shows the full plan shape for the captured local schema.
Those plans require the corresponding captured databases/control.sqlite; run
`prepare.py` to generate actual inputs and a current manifest. The example
schema receipts use zero as the unspecified application time.

Every produced aflite file is a new artifact. Intermediate publication is not
Codex configuration cutover. The manifest does not certify completed history
or application checks. Keep the source data until acceptance; handle logs and
unsupported board templates according to the recorded dispositions.
