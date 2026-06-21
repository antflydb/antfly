# Antfly Lite Retrieval Template

This template is a small embedded retrieval app for Antfly Lite. It opens a
single `.aflite` file, creates schema and retrieval indexes, writes documents
with caller-supplied embeddings, runs full-text, dense, and hybrid searches, and
writes a portable `.afb` backup.

## Run

Build the Antfly Lite C ABI first:

```bash
cd ../../zig
zig build lite-capi
cd ../examples/antfly-lite-retrieval-go
```

Run the template:

```bash
GOWORK=off go run . --reset --db retrieval.aflite --backup retrieval.afb
```

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
