# Status API

`GET /api/v1/status` is the public summary endpoint used by health checks and
operational tooling. `GET /api/v1/cluster` is the richer topology endpoint used
by Antfarm cluster views.

The canonical Zig topology shape is a summary plus typed `data` sections. The
old Go dashboard shape exposed `stores.statuses` and `shards.statuses` through
OpenAPI `additionalProperties`; new Zig responses should use `data` instead.
Antfarm may continue accepting the old fields during migration, but new code
should not add to them.

## Summary

`GET /api/v1/status` returns only top-level fields about the cluster as a whole:

```json
{
  "health": "healthy",
  "message": null,
  "auth_enabled": false,
  "swarm_mode": true,
  "secret_store": null
}
```

`health` is derived from metadata projection and placement state. It should be
usable even when detailed topology is unavailable.

## Data Section

`GET /api/v1/cluster` returns the same top-level summary fields plus `data`,
which contains the table-data runtime view. Use `data` terminology in public API
and UI surfaces:

- `data.nodes`: data-node records, replacing the old dashboard "stores" name.
- `data.ranges`: table range records, replacing the old dashboard "shards"
  name for Zig status surfaces.
- `data.replicas`: intended range replicas on data nodes.
- `data.groups`: merged runtime/raft state for each range group.

Example:

```json
{
  "health": "healthy",
  "swarm_mode": true,
  "auth_enabled": false,
  "data": {
    "nodes": [
      {
        "data_id": 1,
        "node_id": 1,
        "api_url": "http://127.0.0.1:8080",
        "raft_url": "",
        "role": "data",
        "state": "healthy",
        "live": true
      }
    ],
    "ranges": [
      {
        "group_id": 7001,
        "range_id": 0,
        "table_id": 42,
        "table_name": "docs",
        "start_key": "",
        "end_key": null,
        "state": "healthy",
        "leader_data_id": 1,
        "voter_count": 1,
        "doc_count": 120,
        "disk_bytes": 1048576
      }
    ],
    "replicas": [
      {
        "group_id": 7001,
        "data_id": 1,
        "node_id": 1,
        "replica_id": 1
      }
    ],
    "groups": [
      {
        "group_id": 7001,
        "leader_known": true,
        "leader_data_id": 1,
        "voter_count_known": true,
        "voter_count": 1,
        "healthy_voter_reports": 1,
        "transition_pending": false,
        "doc_identity_lifecycle": "ready"
      }
    ]
  }
}
```

## Compatibility

The TypeScript dashboard should handle three states:

- summary only: render cluster health/auth/swarm information and empty topology;
- Zig topology: use `data.nodes`, `data.ranges`, and `data.replicas`;
- legacy topology: fall back to `stores.statuses` and `shards.statuses`.

Transport errors, authentication errors, and non-2xx responses are the only
states that should produce the Cluster page failure screen.
