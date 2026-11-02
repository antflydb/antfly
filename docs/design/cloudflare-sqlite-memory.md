# Unresolved SQLite memory failure

The compatibility profile reported a real `SQLITE_NOMEM` during live collection
after 591 completed tasks. Its precise allocation boundary/root cause remains
unreproduced. Native Antfly R2 metadata removes the application SQL journal from
the database path, but does not establish a Cloudflare platform allocator fix.
See [architecture](cloudflare-serverless.md) and [durability](antfly-object-storage-backend.md).

## Evidence and existing protections

A dedicated SQLite soak completed 1,340,160 statements without reproducing the
failure. Later local fixtures intentionally throw `SQLITE_NOMEM`; those are
injection tests, not recovered provider failures. Synthetic pressure fixtures omit
the full Antfly engine and cannot prove the live problem resolved. Partial Tail
with zero observed errors also cannot establish zero actual failures.

The native R2 workerd fixture makes application `ctx.storage.sql` and
`ctx.storage.kv` throw `UnexpectedPlatformDatabaseAccess`. Initialization, writes,
queries, receipt replay, publication, both collectors, reindex and recovery must
work under that guard. Compatibility migration retains its real SQL source.
This checks application independence; provider-local durable bootstrap uses the
platform storage API and remains a distinct additional durable tier.

Memory accounting must include colocated actors, JS objects, JSON expansion,
compressed/decompressed buffers, responses, native linear memory and compiled code.
WASM metrics are not whole-isolate peaks. Sampled provider metrics can miss spikes.
Original role ceilings and aggregate input/cache/admission limits remain unchanged.

Diagnostic ingestion has aggregate heavy-parse admission, bounded waiting callers,
streamed gzip with wire/expanded bounds, and JSON node/depth preflight. It avoids
keeping duplicate compressed/decoded trees. Source rejection prefixes and reader
resource failures survive collection. A rejection, timeout, dropped event or
complexity truncation prevents complete evidence; it cannot become an empty clean log.

A logical deadline retains physical reservations until all dispatched work settles.
Late resource errors, nested trace flags and recovered `error_message` values
survive canonical compaction and fail the run even when the final outcome is `ok`.
No retry can erase the failure ledger. Resource boundary logs contain fixed safe
component/operation labels, never credentials, object keys or arbitrary exceptions.

## Next reproduction

1. Run the actual split Antfly runtime, not only synthetic retained bytes. Establish
   complete observation for app/writer/reader/builder and receiver/reader failures.
2. Increase one dimension at a time: colocated tenants, overlapping read/write,
   reconstruction, body size, publication/collection, then diagnostic upload load.
   Keep native limits, workload identities and failed populations fixed/retained.
3. Capture exact Worker/module/config versions, time range, actor lifecycle,
   sanitized boundary/error code, active physical work and available isolate metrics.
   Verify both foreground settlement and observer export settlement.
4. Attribute recurrence: application SQL means compatibility or regression; native
   allocation means working-set pressure; archive/proof R2 means diagnostic binding;
   storage startup/reset before Antfly work requires a minimal provider reproducer.
5. Reduce the reproducer, implement the demonstrated fix and repeat the exact failure
   workload plus pressure/reconstruction checks. A run without recurrence is evidence
   of stability for that workload, not retrospective root-cause proof.

Keep the gate failed on any real memory/CPU/resource event, including recovered ones.
Complete canonical/native windows and provider delivery history are required before
claiming a clean hosted trial. The original historical diagnosis stays open until
an actual root-cause fix is validated; current qualification status is in
[the qualification procedure](cloudflare-independent-http.md).
