# Next disposable latency rehearsal

The proposed pair uses the previously approved corrected control `c44896ac8c`
and candidate production source `edd57e464c`. This is a short diagnostic
rehearsal with the original offered workload, exact-body/receipt checks,
independent whole-window evidence, and unchanged qualification thresholds.
It cannot substitute for the three hour-long trials and cold samples.
No resources have been deployed by this preparation.

The locally built and verified configuration/bundle closure is in
`examples/cloudflare-serverless/dist/qualification/gate-mvvfqcgjb124/gate.json`.
Both stacks pass `verifySealedRun` with no errors. The control full module is
`563936db3958bdab50c404db4a6ccef15e095e798e49198ab2304d87322dac97`;
candidate full/compact/observation modules are respectively
`352c3e98742b5330f47f2fe8f8c47b0fde79e55c45f677609ec44a7702ba68ce`,
`5d45cfc3292cfc9a13a4dc96b0d506ee14304ae8bb50251a3412522e5d367269`,
and `1a010d53a7debdcefeb58c45d2167c7491461997a811f1a1544bdbb8dee1a151`.
The candidate retains the previous hosted feature configuration; affinity and
write read-ahead remain disabled pending local evidence.

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
