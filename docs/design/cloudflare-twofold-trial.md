# Next disposable latency rehearsal

The re-sealed pair uses the previously approved corrected control `c44896ac8c`
and candidate production source `e8591354d3`. The revised policy passes local
correctness checks: 499 Node tests, three guarded workerd checks, 188 shared native
checks (two live-provider skips), 34 focused journal tests, 18 app cases and all
108 candidate metadata observations. Its metadata recovery gain does not prove
an overall app gain or a gate pass. The original eager-prefetch proposal is
preserved without credential files at
`dist/qualification/twofold-original-hosted-proposal` under the example.
Resource names and the pending disposable-token authorization scope are unchanged.

This is a short diagnostic
rehearsal with the original offered workload, exact-body/receipt checks,
independent whole-window evidence, and unchanged qualification thresholds.
It cannot substitute for the three hour-long trials and cold samples.
No resources have been deployed by this preparation.

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

The independent observer is prepared and locally sealed at
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

The older named deployment has already been deleted; its token and cleanup
approval does not name these new resources. Credential upload and permanent
cleanup need authorization for this concrete scope before starting the hosted
rehearsal. Local qualification continues independently. The historical live
`SQLITE_NOMEM` remains unreproduced and is not relabeled as fixed.
