# GLiNER2.5 regression fixtures

This directory contains the small, checked-in fixtures used by normal CI for
inference and finetuning regression coverage. `reference_manifest.json` pins
their paths, sizes, hashes, retained helper identities, and upstream source
revision. Missing or changed files fail verification.

From the repository root:

```sh
python3 zig/pkg/inference/scripts/gliner25/oracle.py verify-fixtures
python3 zig/pkg/inference/scripts/gliner25/oracle.py verify-references
```

Numerical tolerances must not be adjusted to fit new results.

Keep build logs, executables, downloaded models, adapter outputs, virtual
environments, and run reports outside Git. Use
`.benchmark-results/gliner25/` or an external artifact store for local evidence.

The CUDA family has a small, curated snapshot in
[`family/evidence/`](family/evidence/README.md): final benchmark comparisons,
compressed holdout replay oracles, and diagnostic result summaries. Its separate
`family/manifest.json` pins the retained files. Intermediate campaign histories
and duplicated prediction reports stay outside Git.
