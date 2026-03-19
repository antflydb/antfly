# Simulator CLI

`cmd/debugging/sim` is a small entrypoint for running the deterministic simulator without going through individual `go test` invocations.

It is intended for:

- simulator-focused validation
- multi-seed soak runs
- collecting replayable failure artifacts from randomized DST runs

It is not a replacement for the real `e2e/` chaos suite. The simulator is faster and deterministic, but it exercises the in-process DST harness, not the full real-network deployment path.

## Quick Start

From the repo root:

```bash
make sim-validate
make sim-soak
make sim-validate-repo
```

Direct CLI usage:

```bash
go run ./cmd/debugging/sim -action validate -scope sim
go run ./cmd/debugging/sim -action validate -scope repo
go run ./cmd/debugging/sim -action soak -json
go run ./cmd/debugging/sim -action soak -modes split_failover -seeds 521,523
```

## Actions

### `validate`

Simulator-focused validation:

```bash
go run ./cmd/debugging/sim -action validate -scope sim
```

This runs:

```bash
go test -run '^$' ./src/metadata ./src/sim
go test ./src/sim
```

Broader repo validation:

```bash
go run ./cmd/debugging/sim -action validate -scope repo
```

This adds:

```bash
go test ./...
```

### `soak`

Run seeded randomized simulator scenarios and write JSON artifacts for any failures:

```bash
go run ./cmd/debugging/sim -action soak
```

Emit the final summary as JSON:

```bash
go run ./cmd/debugging/sim -action soak -json
```

Run only specific modes or seeds:

```bash
go run ./cmd/debugging/sim -action soak -modes documents,splits
go run ./cmd/debugging/sim -action soak -modes split_failover -seeds 521,523,541
```

Override runtime knobs:

```bash
go run ./cmd/debugging/sim \
  -action soak \
  -modes transactions \
  -steps 24 \
  -action-settle 1500ms \
  -stabilize-every 5 \
  -max-failures 3
```

## Soak Modes

Supported values for `-modes`:

- `all`
- `documents`
- `splits`
- `split_failover`
- `transactions`

Default seeds currently come from [src/sim/soak.go](/Users/ajroetker/go/src/github.com/antflydb/antfly/src/sim/soak.go).

## Paths And Artifacts

Defaults:

- `-base-dir` defaults to `./tmp/sim-cli`
- `-artifact-dir` defaults to `./tmp/sim-cli/artifacts`

Each soak run uses a per-seed directory under:

```text
./tmp/sim-cli/<mode>/seed-<seed>
```

Failure artifacts are written as JSON files under:

```text
./tmp/sim-cli/artifacts
```

Artifact filenames look like:

```text
split_failover-seed-521-split_liveness.json
```

Artifacts include:

- scenario mode and kind
- seed
- failure category
- full action list
- reduced action list when reduction succeeds
- compact trace output
- original error text

## Useful Flags

- `-action`: `soak` or `validate`
- `-scope`: `sim` or `repo`
- `-modes`: comma-separated soak modes
- `-seeds`: comma-separated seed override
- `-steps`: override per-scenario step count
- `-action-settle`: override settle duration after each action
- `-stabilize-every`: force periodic stabilization cadence
- `-base-dir`: base temp/output directory
- `-artifact-dir`: explicit artifact directory
- `-max-failures`: stop soak after this many failures
- `-json`: emit JSON summary

## Current Limits

- The simulator focuses on the DST harness in `src/sim`, not the full external deployment path.
- Real network behavior is approximated through the simulated transport and RPC adapters.
- `make sim-validate-repo` is useful for confidence, but it is broader and slower than simulator-only validation.
- The most valuable workflow is still: run soak, inspect minimized artifacts, then replay or convert interesting failures into focused tests.
