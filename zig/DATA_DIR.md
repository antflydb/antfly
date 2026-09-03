# Zig Data Directory Layout

The Zig runtime treats `--data-dir` as the root of all durable local Antfly
state. When `--data-dir` is omitted, the root comes from the common storage
configuration and defaults to `~/.antfly`.

The data directory is versioned by the `ANTFLY_FORMAT` marker at the root. The
marker applies to the whole directory tree, not to an individual node mode.

```text
<data-dir>/
  ANTFLY_FORMAT
  secrets.json

  metadata/
    replicas/
    catalog.txt
    snapshots/
    auth/
    local-metadata.json

  data/
    replicas/
    catalog.txt
    snapshots/

```

## Design

The top-level directories are durable storage domains, not process modes.
`standalone` is a way to run Antfly locally, so it must not create a durable
`<data-dir>/standalone` namespace by default. Standalone should use the same domain
directories as standalone metadata, data, and inference nodes.

`metadata/` owns metadata raft state:

- `metadata/replicas/` stores metadata replica apply state.
- `metadata/catalog.txt` stores the metadata replica catalog.
- `metadata/snapshots/` stores metadata raft snapshot transport payloads.
- `metadata/auth/` stores local auth users, roles, and policy state.
- `metadata/local-metadata.json` is used by local standalone mode when metadata raft
  is disabled.

`data/` owns data-node state:

- `data/replicas/` stores hosted data group table state.
- `data/catalog.txt` stores the data replica catalog.
- `data/snapshots/` stores data raft snapshot transport payloads.

Table database snapshots are a lower-level DB artifact and remain adjacent to
the database path as `<db_path>.snapshots/<snapshot-id>/...`.

Inference assets are deliberately independent of the database data root.
AI model discovery defaults to `~/.antfly/inference/models`, and Traditional
ML predictor discovery defaults to `~/.antfly/inference/ml`.
`ANTFLY_INFERENCE_MODELS_DIR`/`--models-dir` and
`ANTFLY_INFERENCE_ML_DIR`/`--ml-dir` override those locations.

`secrets.json` is rooted at `<data-dir>/secrets.json` because secrets are
runtime-wide process configuration, not metadata replica state or data replica
state.
