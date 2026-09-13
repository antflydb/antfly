# GLiNER2.5 regression fixtures

This directory retains small regression inputs, expected outputs, model metadata,
provenance, and regeneration contracts. Larger numerical payloads are omitted
from Git pending a separate object-storage integration.

`reference_manifest.json` distinguishes two inventories:

- `files`: checked-in fixtures. Missing files or changed bytes fail verification.
- `external_files`: exact names, SHA-256 hashes, and sizes of 27 omitted payloads.
  Tests skip only when these declared inputs are absent. Restored payloads must
  match the original identity; corrupt files fail. Their paths are ignored so
  local restoration does not accidentally add the large files back to Git.

The omitted set includes larger training forward/gradient/optimizer captures,
training-step and dropout tensors, inactive-adapter captures, selected head
weights, and larger preprocessing/decoding references. Original generators and
numerical assertions remain. No cloud bucket or automatic downloader is wired
up in this PR. Default CI has reduced numerical parity coverage until these
inputs are restored; skips do not establish correctness or qualification.

Small inputs for assignment, relation proposals, tokenization, schemas, request
outputs, and PEFT remain available. The shared 334-tensor model inventory retains
explicit small/multilingual shape overrides. Its metadata stays in Git despite
being slightly over 50 KiB. `tiny/capture.json` uses compact JSON with identical
values. Tiny tokenizer files stay under `training_step` for dataset preflight.
The independent CrossNER scorer retains its exact upstream bytes and MIT license.

From the repository root:

```sh
python3 zig/pkg/inference/scripts/gliner25/oracle.py verify-fixtures
python3 zig/pkg/inference/scripts/gliner25/oracle.py verify-references
python3 zig/pkg/inference/scripts/gliner25/fixture_support.py
```

The first two commands verify the checked-in configuration/reference inventory.
The third also verifies any restored external payloads and lists missing ones.
Use `fixture_support.py --require-all` to reject incomplete restoration. Restore
files at their manifest-relative paths beneath this directory, then run that
strict check before claiming full fixture coverage. Capture commands expose
`--help`; regeneration requires the original pinned source and environment.

Captures preserve independent source values and provenance. Shared tensor and
metadata references resolve to exact immutable values before tests create their
own mutable state. Numerical tolerances must not be adjusted to fit new results.

Keep build logs, executables, model/adapter outputs, copied source trees, virtual
environments, and run reports outside Git. Use the ignored
`.benchmark-results/gliner25/` directory or an external artifact store. Preserve
source, model, executable, and output identities with each run.

See the [completed work log](../../../../../work-log/completed/gliner2.5.md)
for implementation scope, reproduction commands, and qualification limits.
