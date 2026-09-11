# Published-small inactive classifier Metal evidence

This compact ledger records six completed public production CLI invocations:
uninterrupted, paused after the first microbatch, and fresh-owner resumed runs
for classifier-only rank-2 LoRA and DoRA. They use the original published small
checkpoint/tokenizer, ordinary production initialization and strict resident
Metal execution in the frozen `9f0d349e…786f9` executable.

Both modes complete the five authored rows and three optimizer updates at
`[2,4,5]`. The inactive fallback sequence is `[false,true,false,true,true]`;
positive raw frozen-task terms remain recorded while inactive optimizer losses
are zero. All four/six selected slots reach three Adam steps. Stitched progress
semantics and final result/checkpoint/all four export-file bytes agree exactly
between uninterrupted execution and fresh-owner resume after microbatch one.

`manifest.json` binds the exact configuration, source, build, helpers, process
ownership receipts, progress and final artifacts. The Controller fingerprint
and owned-state SHA256 were independently reconstructed from the checkpoint
bytes at the original private paths. This ledger stores their digests and
state summaries rather than the raw checkpoint or adapter tensor payloads.
Both final validation reports and all four continuation phase reports were
independently regenerated read-only and matched the existing receipts exactly.

The enforced owner profile is 128 MiB host, 1 GiB backend including 64 MiB
metadata, and 2 GiB combined. Source/job/dataset owners remain separately
admitted. The outer supervisor samples the owned process tree every 50 ms
under a separate 3 GiB RSS limit, with all six processes cleanly reaped. The
resident admission bound, allocator peaks and sampled RSS are distinct values;
the RSS sample can count shared pages more than once.

The stored helpers are historical evidence, not self-contained commands to
execute from this directory. They refer to the original private workspace.
The earlier CPU ledger and its checker revisions remain immutable. This is
same-backend update-policy and durable-continuity evidence, not published
Fastino loss/VJP parity, CPU–Metal numerical parity, useful trained quality,
long-context qualification, performance or general release readiness.
