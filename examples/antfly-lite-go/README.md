# Antfly Lite Go Example

This example embeds Antfly Lite in a Go application. It creates a live
`.aflite` database on first run, opens that existing database on later runs,
writes a JSON document, reads it back, prints Lite status, and exports a
portable `.afb` backup. It also checks the `.aflite` file directly without
opening another database handle.

## Run

Build the Antfly C ABI first:

```bash
cd ../../zig
zig build capi
cd ../examples/antfly-lite-go
```

Run the example:

```bash
GOWORK=off go run . --reset --db demo.aflite --backup demo.afb
```

`--reset` removes the demo files before running so the example exercises
`antflylite.Create`. Without `--reset`, the example reopens `demo.aflite` with
`antflylite.Open`.

The generated files have different meanings:

- `demo.aflite` is the live embedded database.
- `demo.afb` is the portable backup archive for restore or promotion.

Promote the Lite database to a running Antfly service:

```bash
antfly restore \
  --input demo.aflite \
  --table docs \
  --location file:///tmp/antfly_backups \
  --url http://localhost:8080
```

Or restore the portable backup back into Lite:

```bash
antfly lite restore demo.afb --out restored.aflite
```
