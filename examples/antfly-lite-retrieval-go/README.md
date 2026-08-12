# Antfly Lite Retrieval Template

This template is a small embedded retrieval app for Antfly Lite. It creates a
single `.aflite` file on first run, opens that existing file on later runs,
initializes schema and retrieval indexes for new databases, writes documents
with caller-supplied embeddings, runs full-text, dense, and hybrid searches, and
writes a portable `.afb` backup.

## Run

Build the Antfly C ABI first:

```bash
cd ../../zig
zig build capi
cd ../examples/antfly-lite-retrieval-go
```

Run the template:

```bash
GOWORK=off go run . --reset --db retrieval.aflite --backup retrieval.afb
```

`--reset` removes the template files before running so the example exercises
`antflylite.Create`. Without `--reset`, the template reopens `retrieval.aflite`
with `antflylite.Open` and keeps the existing schema and indexes.

Use `retrieval.aflite` as the live embedded database. Use `retrieval.afb` for
promotion, restore, or archival backup.

Promote the live Lite database into a normal Antfly table:

```bash
antfly restore \
  --input retrieval.aflite \
  --table notes \
  --location file:///tmp/antfly_backups \
  --url http://localhost:8080
```
