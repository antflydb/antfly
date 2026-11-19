# Stock PostgreSQL CDC client

This runner pins its build/runtime images by digest and Debezium **3.3.0.Final** and uses its unchanged PostgreSQL
connector with `pgoutput`. It supplies configuration, writes JSONL events, and
persists the connector's own offsets. It contains no Antfly catalog, replication,
SQL, snapshot, or decoding adapter.

Build from this directory:

```sh
docker build -t antfly-debezium-qualification:3.3.0 .
```

Copy `connector.properties.example` into `.local/connector.properties`, configure
the target and credentials, and create `.local/state`. Mount both into the runner:

```sh
docker run --rm --init --stop-timeout 30 \
  -v "$PWD/.local/connector.properties:/connector.properties:ro" \
  -v "$PWD/.local/state:/state" \
  antfly-debezium-qualification:3.3.0 /connector.properties
```

Stdout contains one JSON object per event with the topic, original decoded key,
and original decoded envelope (including null tombstones). Diagnostics go to
stderr. The runner reports connector failures with a nonzero exit. A normal stop
closes the engine and flushes its offsets; an abrupt kill exercises the stock
connector's restart behavior. Retain the same state directory and slot for resume.
The publication and table must exist before starting this configuration.

The configuration and lifecycle use the official [Debezium Engine documentation](https://debezium.io/documentation/reference/3.3/development/engine.html)
and [PostgreSQL connector documentation](https://debezium.io/documentation/reference/3.3/connectors/postgresql.html).

## Qualification driver

Run a disposable PostgreSQL 17.6 baseline first:

```sh
python3 qualify.py
```

The driver checks an initial snapshot, lossless `numeric(38,8)` values, before
images, transaction identity/order/end counts, primary-key deletion/tombstone/new
key, persisted offsets, graceful reconnect after offline writes, and reconnect
after an abrupt client kill. It uses isolated uniquely named containers on an
existing Docker network (`bridge` by default); it creates no daemon-wide networks
or published host ports and cleans up only its own containers.

To run the same stock client against an existing target, set
`QUALIFICATION_DATABASE_PASSWORD` and use `--external --host HOST --port PORT
--database DATABASE --user USER`. The target must support the ordinary SQL setup
and catalog/replication operations used by the connector. Each run uses a unique
source table, publication, and slot and reports any cleanup failures.

`--artifacts DIRECTORY` retains JSONL event logs, diagnostics, connector offsets,
and the credential-bearing properties file in a mode-0700 directory (properties
are mode 0600). Without this flag the driver removes its local temporary state.
The final JSON summary distinguishes a PostgreSQL baseline from an external run;
it does not infer which product serves an arbitrary external endpoint.


## Antfly qualification

Debezium 3.3.0.Final with pgjdbc 42.7.7 passed the driver against Antfly
`2f5d6563a5c6fc52b1788178f9a2ddd5863f621d`, which advertised `16.0-antfly`,
using `pgoutput` protocol version 1. The run used Temurin 21.0.10 on macOS,
one metadata server, one data server, and a provisioned filesystem spool.
The client used UTC, `snapshot.mode=initial`, exact decimal handling, ordered
event processing, and a file-backed offset store. Antfly served the imported
snapshot and the role, namespace, relation, column, type, primary-key,
publication, and slot lookups used by the connector.

The driver observed 23 events and verified the snapshot, streamed
`numeric(38,8)` before and after values, transaction identity and order,
reconnect after offline writes, primary-key deletion and tombstone, and
reconnect after killing the connector. Stock `pg_recvlogical` 18.6 separately
covers Antfly restart for streaming-only and exported-snapshot slots. The
Debezium run reported an unknown outcome for the final fixture `DROP TABLE`.

The qualified publication contains one native relational table on one retained
root, with an integer primary key, text, numeric, and boolean columns and
`REPLICA IDENTITY FULL`. Use a replication principal authorized for the
whole table. The stream delivers committed text-format inserts, updates, and
deletes with transaction boundaries and durable per-slot positions.
Binary tuples, in-progress transaction streaming, two-phase replication,
native PostgreSQL failover slots, and physical replication are unsupported.
Logical-message options produce the same row stream. Multi-table and
multi-root publications, shard movement, TRUNCATE, and schema evolution
remain outside this qualification.
