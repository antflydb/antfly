# pgaf

PostgreSQL extension for [Antfly](https://github.com/antflydb/antfly). Provides a custom index access method, search functions, and row sync triggers — so Antfly-powered search feels native to Postgres.

## Features

- **Custom Index AM** — `CREATE INDEX ... USING antfly` for planner-integrated search
- **`@@@` operator** — full-text, semantic, and hybrid search via ParadeDB-style query builders
- **`antfly_search()`** — query an Antfly table from SQL and join results back to your tables
- **`antfly_sync_trigger()`** — trigger that pushes row changes to Antfly on INSERT/UPDATE/DELETE

## Requirements

- PostgreSQL 13–18
- Rust (edition 2024)
- [cargo-pgrx](https://github.com/pgcentralfoundation/pgrx) 0.17.0

## Developer Setup

```bash
# Install cargo-pgrx (one-time)
cargo install cargo-pgrx --version 0.17.0 --locked

# Initialize pgrx — point it at your Postgres installation
# Homebrew (Apple Silicon):
cargo pgrx init --pg18 /opt/homebrew/opt/postgresql@18/bin/pg_config
# Homebrew (Intel):
cargo pgrx init --pg18 /usr/local/opt/postgresql@18/bin/pg_config
# System Postgres (adjust path as needed):
cargo pgrx init --pg18 $(which pg_config)
```

### macOS Linker Flags

pgrx extensions link against Postgres server symbols (`palloc`, `pfree`, etc.)
that live in the server binary, not a shared library. On macOS you must allow
unresolved symbols at link time:

```bash
export RUSTFLAGS="-C link-arg=-undefined -C link-arg=dynamic_lookup"
```

The `Makefile` sets this automatically, so `make test` / `make build` just work.
If you run `cargo pgrx test` directly, set `RUSTFLAGS` first.

## Quick Start

```bash
# Build and install
cargo pgrx install

# Or run in a temporary dev instance
cargo pgrx run
```

Then in psql:

```sql
CREATE EXTENSION pgaf;
```

## Usage

### Index Access Method

Create an Antfly-backed index on a text column:

```sql
CREATE INDEX idx_content ON docs USING antfly (content)
  WITH (url = 'http://localhost:8080/api/v1/', collection = 'my_docs');
```

Query naturally — the planner uses the Antfly index:

```sql
SELECT * FROM docs WHERE content @@@ 'fix my computer';
```

The `@@@` operator delegates search to Antfly. On `CREATE INDEX`, the table is auto-created in Antfly and all existing rows are pushed. Subsequent inserts are synced automatically via the index AM.

**WITH options:**

| Option | Default | Description |
|--------|---------|-------------|
| `url` | `http://localhost:8080` | Antfly server URL (include `/api/v1/` prefix) |
| `collection` | table name | Target Antfly table |

### Query Builders

pgaf provides ParadeDB-style query builder functions in the `pgaf` schema. These return JSON strings that the `@@@` operator sends as structured queries to Antfly.

**Full-text search:**

```sql
SELECT * FROM docs WHERE content @@@ pgaf.search('fix computer');

-- With filter prefix
SELECT * FROM docs WHERE content @@@ pgaf.search(
    'fix computer',
    filter_prefix => 'tenant:acme:'
);
```

**Semantic (vector) search:**

```sql
SELECT * FROM docs WHERE content @@@ pgaf.semantic(
    'fix my broken computer',
    indexes => ARRAY['embedding_idx']
);
```

**Hybrid search (full-text + semantic via RRF):**

```sql
SELECT * FROM docs WHERE content @@@ pgaf.hybrid(
    full_text => 'computer repair',
    semantic => 'fix my broken computer',
    indexes => ARRAY['embedding_idx']
);
```

Plain strings passed to `@@@` are treated as full-text search queries. Query builder functions return JSON that the index AM passes through as structured Antfly query bodies.

### Search Function

For cases where you need scores or want to join search results explicitly:

```sql
SELECT t.*, s.score
FROM my_table t
JOIN antfly_search(
    'http://localhost:8080/api/v1/',
    'my_table',
    'fix my computer'
) s ON t.id = s.id
ORDER BY s.score DESC;
```

```sql
antfly_search(base_url TEXT, collection TEXT, query TEXT, limit INT DEFAULT NULL)
RETURNS TABLE (id TEXT, score FLOAT8, data JSONB)
```

### Triggers

Automatically sync row changes to Antfly (useful when not using the index AM):

```sql
CREATE TRIGGER sync_to_antfly
  AFTER INSERT OR UPDATE OR DELETE ON my_table
  FOR EACH ROW
  EXECUTE FUNCTION antfly_sync_trigger(
    'http://localhost:8080/api/v1/',  -- Antfly server URL
    'my_table',                       -- target Antfly table
    'id'                              -- column to use as document ID
  );
```

### Status Check

```sql
SELECT antfly_status('http://localhost:8080/api/v1/');
```

## Architecture

pgaf depends on `antfly-client`, a sibling crate in the `rs/` workspace that
generates a typed Rust SDK from `openapi.yaml` via [Progenitor](https://github.com/oxidecomputer/progenitor).
pgaf uses the shared types (e.g. `QueryResponses`, `QueryHit`) for
deserialization but keeps its own blocking HTTP client (Postgres extensions
cannot run an async runtime).

```
rs/
├── Cargo.toml          # Workspace root
├── antfly-client/      # Generated async SDK (types shared with pgaf)
│   ├── build.rs        # Progenitor codegen + OpenAPI preprocessing
│   └── src/lib.rs
└── pgaf/               # This extension
```

## Project Structure

```
src/
├── lib.rs            # Extension entry point + _PG_init
├── client.rs         # Antfly HTTP client (batch API, query API)
├── query.rs          # ParadeDB-style query builders (pgaf.search, pgaf.semantic, pgaf.hybrid)
├── functions.rs      # SQL functions (antfly_search, antfly_status)
├── e2e_tests.rs      # End-to-end tests (require running Antfly server)
├── triggers.rs       # Trigger function (antfly_sync_trigger)
└── index_am/
    ├── mod.rs        # AM handler (IndexAmRoutine)
    ├── ctid.rs       # ctid ↔ document ID encoding
    ├── options.rs    # WITH clause parsing (url, collection)
    ├── build.rs      # ambuild, ambuildempty, aminsert
    ├── scan.rs       # ambeginscan, amrescan, amgettuple, amendscan
    ├── vacuum.rs     # ambulkdelete, amvacuumcleanup
    ├── cost.rs       # amcostestimate
    └── operator.rs   # @@@ operator and SQL registration
```

## Testing

```bash
# Unit tests (no server needed)
make test

# E2E tests against a running Antfly server
make test-e2e

# Or manually point at a running server
ANTFLY_TEST_URL=http://localhost:8080/api/v1/ cargo pgrx test pg18
```

The e2e tests check the `ANTFLY_TEST_URL` environment variable and skip automatically when no server is available.

## License

See [LICENSE](LICENSE).
