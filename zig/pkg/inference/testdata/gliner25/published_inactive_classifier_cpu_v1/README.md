# Published-small inactive classifier CPU evidence

This compact ledger records six completed public production CLI invocations:
uninterrupted, paused after the first microbatch, and fresh-owner resumed runs
for LoRA and DoRA. The actual published checkpoint/tokenizer and ordinary
production constructor were used. Five authored rows alternate active and
inactive classification supervision; all rows retain entity supervision.

`manifest.json` binds the exact build, source, authored data, configuration,
process/progress/result reports, typed checker policy, final owned-state hashes
and final tensor-file identities. Both modes complete five microbatches and
three updates with exact final result/checkpoint/export bytes after resume.
Native checkpoint payloads were independently audited at their original
private paths; this ledger stores their digests rather than raw tensor data.

The first paused process exited successfully. Its original checker failed on
declared-f32 JSON representation and is preserved under `helpers/v1` with the
original failure log. `helpers/v2` contains the corrected checker and its tests;
the existing phase was validated offline without rerunning or rewriting it.
The Controller fingerprint and full owned-state hash are independently derived
from exact configuration tokens and ordered checkpoint values. Resource and
process ownership constraints are unchanged.

The stored helper sources are historical evidence, not self-contained commands
to execute from this directory: they refer to the original private workspace.
This is CPU policy and durable-continuity evidence, not a published Fastino
numerical oracle, model-quality result, GPU job or release qualification.
