# Historical qualification and lifecycle checkpoints

This private archive retains three failed local Metal build/test commands and the local Python dependency reproduction. It grants no runtime qualification, release readiness, remote CI, quality, or performance claim. A corrected run belongs in additive evidence; the failures here remain unchanged.

| Checkpoint | Main inference result | Failure retained |
| --- | --- | --- |
| Qualification/lifecycle v1 | Compilation failed; no main selected-suite result | Extractor qualification omitted the `.allow` overlap enum case. |
| Qualification/lifecycle v2 | 49 selected: 47 passed, 1 skipped, 1 failed, 0 leaked | The TTL fixture expected request MetalTensor buffers to stay owned after HTTP completion. |
| Qualification/lifecycle v3 | 50 selected: 48 passed, 1 skipped, 1 failed, 0 leaked | TTL eviction returned after the fixture's five-second observation deadline. The thirty-second production teardown ticket did not expire. This was exit 1, not watchdog exit 86. |

The separate ancillary build summary reported 23/23 tests passed in each command. It is not the main inference suite. The skipped main test is the optional published-small full/head export snapshot test. Passing individual qualification, allocation, restore, window, or ownership tests does not make these failed aggregates pass.

Raw `start.json`, `process.json`, all executable observations, full source inventories, stdout, stderr, and v2/v3 archive receipts are preserved without reserialization under `runs/`. The v2/v3 main test binaries and complete source tars were streamed and rehashed against their original receipts. Their bytes are not copied here. Thirteen selected source files per checkpoint are stored by content hash under `sources/`; identical files are shared. The 2,587-file inventories are source selections, not dependency closures or proof that every listed file was imported.

The v1 model policy source is the unchanged Add File payload of an existing private patch, and its SHA-256 exactly matches v1's inventory. That patch is retained under `source_recovery/`. Its failing extractor policy comes from the matching pre-existing private proposal. Other v1 selected sources match the retained v2 archive byte for byte. Current corrected source was not substituted for historical source.

All three process receipts record direct-child reaping, observed child identities gone, no survivors or cleanup errors, and unchanged selected source inventories. Optional executable-observation errors remain in the raw receipts; mandatory RSS/identity cleanup inspection reported no errors. RSS is the sampled sum for tracked process identities, potentially double-counting shared pages. It is diagnostic evidence only. These runs used a 1,800-second deadline, 6 GiB aggregate child RSS cap, 8 MiB stream caps, and bounded cleanup.

The Python evidence under `contracts/` preserves:

- Oracle-environment handoff: **203 tests passed**, 4.487 seconds reported.
- Clean `python -S` reproduction: **201 tests ran, 25 errors**, with missing `packaging`, `psutil`, and `pytest`. The failed run is retained.
- Isolated minimal environment: **203 tests passed**, 4.602 seconds reported; the log records six installed packages.

The environment roles come from the parent run context. These logs do not contain full argv or interpreter identities, so this archive does not invent environment receipts. It includes current source snapshots, the local HEAD baseline, and the exact proposed CI diff. Both `zig-base-tests` and `zig-full-tests` invoke the model-free suite through isolated `uv` with no project, builds, or Python downloads. The six exact minimal dependencies are separate from the unchanged numerical-oracle requirements. No remote CI execution is claimed.

Verify the saved evidence without any original temporary paths, binaries, models, builds, or child process:

```sh
python3 verify.py /absolute/path/to/this-directory --self-test
```

The verifier rehashes every saved artifact and checks source/receipt/executable bindings, main-versus-ancillary outcomes, failed aggregate counts, cleanup facts, and dependency-test outcomes. Its three in-memory adversarial checks reject a mismatched executable, a mismatched source, and promotion of a failed aggregate. `helpers/stage_evidence.py` records the bounded assembly procedure; it requires original artifacts and a fresh destination, and never runs a model or build.

This archive is under 8 MiB. No binaries, full source tars, model weights, or mutable-current-source claim are included. The final focused follow-up is separate and pending at this checkpoint.
