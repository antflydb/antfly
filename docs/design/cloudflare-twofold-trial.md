# Disposable latency rehearsal and results

The re-sealed pair uses the previously approved corrected control `c44896ac8c`
and candidate production source `e8591354d3`. The revised policy passes local
correctness checks: 499 Node tests, three guarded workerd checks, 188 shared native
checks (two live-provider skips), 34 focused journal tests, 18 app cases and all
108 candidate metadata observations. Its metadata recovery gain does not prove
an overall app gain or a gate pass. The original eager-prefetch proposal is
preserved without credential files at
`dist/qualification/twofold-original-hosted-proposal` under the example.
The named disposable tokens were approved and installed for this run.

This is a short diagnostic
rehearsal with the original offered workload, exact-body/receipt checks,
independent whole-window evidence, and unchanged qualification thresholds.
It cannot substitute for the three hour-long trials and cold samples.
The approved pair and observer were deployed and the rehearsal completed.

The locally built and verified configuration/bundle closure is in
`examples/cloudflare-serverless/dist/qualification/gate-mvvfqcgjb124/gate.json`.
Both stacks pass `verifySealedRun` with no errors. The control full module is
`563936db3958bdab50c404db4a6ccef15e095e798e49198ab2304d87322dac97`;
candidate full/compact/observation modules are respectively
`9b86730b4cf0941db1adf22c8876d61c90a0d03643cde2e87fe03d7f2c33d367`,
`fbeac401afd8dc59bd7eabefc5cf6368306b972bd7af277863b3bbbdb6b31e61`,
and `fa7e6af392149a884443f1bf27ffad5336d40ff46a7ed9c4ad6f892ef8d35c49`.
The candidate retains the previous hosted feature configuration; affinity and
write read-ahead remain disabled because local comparisons show no consistent benefit.

The independent observer was sealed and deployed from
`examples/cloudflare-serverless/dist/qualification/antfly-1278-observe-mvvfupn619ef/run.json`.
Its bundle SHA-256 is
`2eec19d4eb0f24170f5fc73c012732f14686795f592ade0a480af087100418e1`.
Its dedicated reader and receiver use that same observer's newly generated
disposable token, following the existing scoped lifecycle. Existing account
credentials are not replaced or copied into the experiment's evidence.

The concrete scope is:

- Stacks `antfly-1278-g-mvvfqcgjb124-1-b` and
  `antfly-1278-g-mvvfqcgjb124-1-c`: each app Worker and its `-writer`,
  `-reader`, `-builder` Workers, R2 bucket, Queue and `-dead` Queue.
- Observer Workers `antfly-1278-observe-mvvfupn619ef`,
  `antfly-1278-observe-mvvfupn619ef-reader`, and
  `antfly-1278-observe-mvvfupn619ef-receiver`, plus R2 buckets
  `antfly-1278-observe-mvvfupn619ef` and its `-archive` bucket.
- Three newly generated API tokens: one on each app Worker and one shared by
  the three observer Workers. Token values stay out of reports and Git.
- After testing, remove this scope's 11 Workers, six Durable Object namespaces,
  four Queues/DLQs, four R2 buckets and their disposable data, and its temporary
  export destinations/attachments. Retain sanitized reports and bundles locally
  and independently verify the final resource inventory.

The user approved the named disposable-token uploads and cleanup. The historical
live `SQLITE_NOMEM` remains unreproduced and is not relabeled as fixed.

## Hosted result, 2026-10-30

The unchanged 180-second offered workload completed under execution
`d27f844b-432f-403f-81be-386b47087cea`, driver source `7728dfb982`.
Both sides verified 2,052 exact bodies and eight receipt replays. Each side has
72 logical measured reads and 24 writes; six candidate reads retried, producing
78 physical read attempts. Final client errors/resource errors are empty.
Retries remain included in the reported logical latency.

| Hosted p95 | Corrected control | Candidate | Original target | Result |
| --- | ---: | ---: | ---: | --- |
| Read | 9,766 ms | 1,922 ms | 1,000 ms | Fail |
| Write | 11,723 ms | 1,507 ms | 3,000 ms | Pass |

Candidate read/write p50 are 896/1,004 ms. This is substantially faster than the
frozen corrected control, but does not establish another twofold improvement
over the previous candidate. Its earlier hosted p95 was 1,508/2,087 ms: reads
are slower here, writes about 28% faster, and deployment/placement differ.
The rehearsal and the overall gate remain failed; this short run does not
replace the three hour-long trials or 100 physical cold samples.

The independent collector honored its original ten-minute aggregate deadline.
It saved six first-pass source catalogs but did not complete all eight sources
and a second observation. The first writer catalog alone took 215,658 ms.
There was no forced interruption. Missing candidate app/builder sources were
preserved afterward as explicitly non-qualifying postmortem evidence. That
late preservation does not amend the original report or repair completeness.
Captured observer resource failures are empty, but incomplete coverage cannot
rule out resource errors or close the historical live `SQLITE_NOMEM`.

The original report is retained at
`examples/cloudflare-serverless/dist/qualification/antfly-1278-observe-mvvfupn619ef/rehearsal-d27f844b-432f-403f-81be-386b47087cea/report.json`,
SHA-256 `8a2924acbce630790a9c74a1ebd301aca4f0a5cf302fd42e1f3d4ee55e598eeb`.
`deployment-snapshot` under the observer directory retains tested versions,
configurations and bundled modules before cleanup changes lifecycle files.

The separate placement probe restored all four reviewed candidate configurations
before the workload. App colo was LAX; actual bucket/actor geography remains
unverified. Five storage HEAD probes per role had app/writer/reader/builder
median 84/87/86/64 ms and p95 214/139/96/75 ms. These are diagnostic probes,
not proof of colocation or physical-cold qualification samples.

Selected first/last measured-minute archives retain 86 sanitized native Tail
objects and 264 derived events, with explicit incomplete-window labeling.
An expired/rebased metadata observation took 1,444 ms, all in native I/O, with
zero observation queue wait, seven followed slots, 18 physical GETs and 11 I/O
waves. Nine reported transport errors include expected missing end/speculative
slots; they are not nine network failures. Other selected expired observations
needed five to seven serial waves; cold observations fetched eight speculative
slots after only two or three positive slots. Another read spent 832 ms in
admission and 773 ms in native lookup, with zero point-kernel queue wait.
Do not attribute this hosted bottleneck to the shared WASM queue merely because
that queue dominated an earlier local case. The native `read_prefix` hint only
selects an existing inline row; it does not scan the metadata tree.

The next implementation should reduce dependent storage work: repair checkpoint
lag, evaluate a shared native authenticated serving projection/seekable recovery
index, and bound short-tail cold/expired speculation using measured fixtures.
Keep owner/epoch/GC fences, typed revisions, the pre-I/O clock anchor, fresh end
validation and cumulative memory/I/O budgets. Point lookup I/O is a separate
remaining cost. Measure placement separately. Resumable observation is useful
where queue traces warrant it, but alone cannot remove these serial I/O waves.
For diagnostics, improve closed-window evidence preparation/serving throughput
under the existing memory and page caps; raising deadlines or promoting selected
minutes would not pass the original gate. None of these additional architectural
changes is claimed as implemented or measured by this rehearsal.


Cleanup is complete. Independent inventory verifies absence of all 11 Workers,
six Durable Object namespaces, four Queues/DLQs and four R2 buckets; no other
`antfly-1278-*` test resources remain. Two export destinations were deleted.
The observer drain was resumed after two connection resets; both failure logs
remain preserved. A subsequent exact-name cleanup confirmation resolved the
automatic review's inability to associate the earlier approval with this scope.

The hosted closure is independently sealed at
`examples/cloudflare-serverless/dist/qualification/twofold-hosted-1793401520945/evidence.json`:
516 verified artifacts / 309,329,416 bytes, including all 224 referenced window
files, frozen deployed bundles/configurations, original report, selected archives,
postmortem captures, lifecycle failures and cleanup inventories. Manifest SHA-256
`b3a3ffdeb04f5f552386ed94c0c7de44ee22f1d60f9a595c168c4b20797fa718`.
Credential files are excluded; the pre-cleanup scan found no disposable token
values in 335 report/source/log files. Original local and historical hosted
closures remain separate and unchanged.


## Local follow-up before another deployment

Shared native changes now bound speculative windows to the WASM transport,
repair checkpoints from fresh native certificates without changing strict GC,
avoid obsolete facts roots for fresh WAL points, and accept optional physical
commit locations only after native frame/floor/owner/end validation. The final
60-case component matrix passes 240 observations; 32-entry recovery at 100 ms
binding delay falls from 1,225–1,327 ms / 37 GETs to about 310 ms / four GETs.

All twelve local app cases pass correctness, but they do not establish another
twofold gain. At 512 documents/tenant and 100 ms delay, read p95 ranges are
939–1,000 ms control, 992–1,655 ms current and 902–934 ms with opt-in hints.
The smaller workload has hint regressions. These are local closed-loop cases,
not the hosted gate; no new profile is promoted on their basis.

Bounded pipelined evidence retrieval completes the historical eight-source
replay within five minutes, but still fails six source reconciliations. A
separate explicitly synthetic complete fixture passes all eight sources in
7m44s under the unchanged ten-minute budget. Positive both-plane preflight,
retained failed startup attempts and explicit RPC data-group disposal are now
implemented for the next rehearsal. They have not been proven to fix live
export startup or the historical `SQLITE_NOMEM`.

[The native design](serverless-metadata-recovery.md) contains the full performance
ranges, invariants and final module hashes; [the memory investigation](cloudflare-sqlite-memory.md)
separates throughput, startup evidence and allocator qualification. No additional
Cloudflare resources were created for this follow-up. The hosted read target,
complete live coverage, three hour-long trials and physical cold samples remain
unqualified; another deployment should follow a justified candidate selection.
