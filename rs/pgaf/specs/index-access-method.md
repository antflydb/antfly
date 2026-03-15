# pgaf Custom Index Access Method

## Context

pgaf already has SQL functions (`antfly_search()`) and triggers (`antfly_sync_trigger()`) for integrating Antfly with PostgreSQL. The index AM is the third piece: it makes the PostgreSQL planner aware of Antfly so queries like `SELECT * FROM docs WHERE content @@@ 'fix my computer'` use the index automatically, without explicit `antfly_search()` calls.

This is a **remote index** (like ZomboDB → Elasticsearch). No index data is stored locally in PostgreSQL pages — all indexing and search is delegated to a remote Antfly server via HTTP.

## Target UX

```sql
CREATE INDEX idx ON docs USING antfly (content)
  WITH (url = 'http://localhost:8080', collection = 'my_docs');

SELECT * FROM docs WHERE content @@@ 'fix my computer';
```

## New Files

```
src/index_am/
├── mod.rs            # AM handler (IndexAmRoutine const + palloc0 pattern)
├── ctid.rs           # ctid ↔ "block_offset" string encoding
├── options.rs        # Reloptions: WITH (url, collection), _PG_init registration
├── build.rs          # ambuild (heap scan → push all rows), ambuildempty, aminsert
├── scan.rs           # ambeginscan/amrescan/amgettuple/amendscan
├── vacuum.rs         # ambulkdelete/amvacuumcleanup (no-op v1)
├── cost.rs           # amcostestimate
└── operator.rs       # @@@ operator + extension_sql! for CREATE ACCESS METHOD/OPERATOR CLASS
```

## Modified Files

- `src/lib.rs` — add `mod index_am`, add `_PG_init` calling `index_am::options::init()`
- `src/client.rs` — no changes required for v1 (existing `search`/`sync_document`/`delete_document` suffice)

## Key Design Decisions

### ctid as Document ID
Encode PostgreSQL ctids as `"{block}_{offset}"` strings, stored as the document ID in Antfly. On scan results, parse them back to `ItemPointerData`. ctids are stable across regular VACUUM; VACUUM FULL rebuilds indexes (calls `ambuild`), so the mapping stays consistent.

### Scan Flow
1. `ambeginscan` — allocate `AntflyScanState` in scan memory context
2. `amrescan` — extract query text from scan key (the RHS of `@@@`)
3. `amgettuple` (first call) — HTTP POST to Antfly `/query`, cache all results as `Vec<(ctid, score)>`
4. `amgettuple` (subsequent) — return next cached ctid
5. `amendscan` — cleanup

### @@@ Operator
- `text @@@ text → bool`, operator function returns `true` unconditionally
- The index AM does real filtering; the operator function is only a fallback for sequential scans
- Same pattern as ZomboDB/ParadeDB

### Reloptions (WITH clause)
- `url` (string, default `http://localhost:8080`)
- `collection` (string, defaults to table name)
- Registered via `add_string_reloption` in `_PG_init`, parsed via `build_reloptions` in `amoptions`
- Uses `OnceLock` for the relopt kind (VectorChord pattern)

### AM Handler
- VectorChord pattern: `const AM_HANDLER: IndexAmRoutine` + `palloc0` + write (confirmed working with pgrx 0.17.0)
- `amstrategies = 1` (one operator: `@@@`)
- `amcanorderbyop = false`, `amcanmulticol = false`, `amoptionalkey = true`
- All callbacks use `#[pg_guard] extern "C-unwind"`

### SQL Registration (via `extension_sql!`)
```sql
CREATE OPERATOR @@@(PROCEDURE=antfly_match, LEFTARG=text, RIGHTARG=text);
CREATE ACCESS METHOD antfly TYPE INDEX HANDLER antfly_amhandler;
CREATE OPERATOR CLASS antfly_text_ops DEFAULT FOR TYPE text USING antfly AS
  OPERATOR 1 @@@(text, text), STORAGE text;
```

### Vacuum (v1)
No-op. Stale docs in Antfly are harmless — PostgreSQL's heap visibility checks filter out dead ctids. Future: query Antfly for all doc IDs, check against vacuum callback, delete dead ones.

### PG Version Compatibility
- `aminsert` signature: PG14+ added `index_unchanged` param — needs `#[cfg]` gating
- `relopt_parse_elt`: PG18 added `isset_offset` field — needs `#[cfg(feature = "pg18")]`

## Implementation Order

1. `ctid.rs` — pure Rust, no dependencies
2. `options.rs` — reloption infrastructure
3. `cost.rs` — simple heuristic costs
4. `vacuum.rs` — no-op stubs
5. `build.rs` — ambuild/aminsert (depends on ctid, options, client)
6. `scan.rs` — search execution (depends on ctid, options, client)
7. `operator.rs` — SQL glue (depends on mod.rs)
8. `mod.rs` — AM handler wiring everything together
9. `lib.rs` — add `mod index_am` + `_PG_init`

## Verification

```bash
# Build
cargo pgrx run

# Then in psql:
CREATE EXTENSION pgaf;
CREATE TABLE docs (id serial PRIMARY KEY, content text);
INSERT INTO docs (content) VALUES
  ('how to fix a broken computer'),
  ('best recipes for chocolate cake'),
  ('introduction to machine learning');
CREATE INDEX idx_content ON docs USING antfly (content)
  WITH (url = 'http://localhost:8080', collection = 'test_docs');
SELECT * FROM docs WHERE content @@@ 'fix computer';
```

## Future Work (not in this PR)
- `amcanorderbyop = true` + score operator for `ORDER BY` by relevance
- `amgetbitmap` for bitmap index scans
- `amcanmulticol = true` for multi-column indexes
- Bulk sync in `ambuild` (batch HTTP instead of per-row)
- Vacuum reconciliation with Antfly
- Async HTTP / connection pooling
- Custom `AfQuery` type for structured queries (semantic + full-text + filters)
