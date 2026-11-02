# Shared image operations: implementation and Debug qualification

The opt-in `-Dshared-image-operations=true` moves complete image operations to
Antfly's existing enrichment/media owner. It also enables retained PDF sessions.
Default builds continue to use the source image implementation. Native transport
is restricted to the supported 64-bit macOS/Linux profiles and validates the
compiler signature, counted layout table and participating value layouts before each operation.

## Ownership

The codec-free `lib/image/src/values.zig` contains shared image, GIF, PNG and VP8
values. Source codecs alias those declarations. Pixel processing is a separate
named module shared by source and native clients.

The provider owns encoded image inspection/decode, GIF frame collection and
streaming, PNG encoding, fused JPEG preprocessing, MJPEG profile inspection,
and retained VP8 decoding. Callers receive results allocated through their
original allocator; decoded pixels are not copied through a serialization layer.
GIF visitor failures and work-control failures preserve caller-origin error
identity. VP8 decoder metadata uses the session's parent allocator; frame and
reference buffers use the existing per-session budget allocator, including
failure and close paths. No borrowed per-call allocator is retained as decoder
metadata ownership.

Production graphs substitute the image facade and PDF facade recursively while
retaining independent source graphs for codec tests. Platform options and link
configuration must be complete before that substitution. Cloned source catalogs
must bind to their own consumer's imports during late build finalization.

## Evidence

`compiled-image-native-test` runs independently compiled native and source-oracle
executables. Each has seven tests. Across them, 111 successful single-frame image
fixtures produce identical RGBA hashes. Four checked-in VP8 sequence families
cover 26 packets, including interframes and alternate stream versions/partitions.
The tests also cover PNG stride/output limits, GIF early stop and partial
progress, caller-only callback errors, JPEG fused preprocessing and cancellation,
MJPEG profile facts, failed VP8 decoding and allocator failure cleanup.
Guard regressions reject old endpoint versions, differing table lengths before
dereferencing the table, and a same-size reordered inspection-result layout.

The Debug cache receipt
`.benchmark-results/compile-costs/image-owner-debug-v2-guards-cache.json` passed
all nine phases with frozen build-construction inputs:

| Edit | Provider | Native client | Source oracle |
| --- | --- | --- | --- |
| None, warm build | cached | cached | cached |
| Reachable provider implementation | rebuilt | cached | cached |
| JPEG or WebP codec | rebuilt | cached | rebuilt |
| Facade | cached | rebuilt | cached |
| Shared values, operation contract, or transport protocol | rebuilt | rebuilt | rebuilt |

The reachable implementation edit changes the provider archive hash. Every
phase repeats all 111 source/native fixture comparisons and four VP8 sequences.
No storage, inference, API, query or CLI compiler owners enter this focused
matrix. Logs and receipt include source/build hashes; the private overlay is
removed when the script finishes. Parser and attribution tests pass.

This establishes focused functional equivalence and compilation isolation.
Both Debug products, C API/CLI smoke checks, inference ABI checks, and query/
storage-owner regressions pass in the combined opt-in image/lake build. The
final replay passes all 225 steps, including the linked lake fixture, and all
13 PDF/OCR and query-deadline HTTP regressions against the installed binaries.

The installed archive inventory
`.benchmark-results/compile-costs/media-owner-final-symbols.json`
records exact archive hashes and definition counts. All ten installed archives were scanned. Media owns BMP, GIF, JPEG, PNG, WebP,
JPEG2000, JBIG2 and CCITT implementations, PDF Reader/render operations, all 13
image endpoints and all 24 PDF endpoints. No consumer, including libantfly's
storage-engine and inference-core archives, defines those raw codecs or PDF
Reader/render operations. The existing small `lib/chunker/src/png.zig`
uncompressed encoder remains in storage-engine, inference-core, API and
serverless. Archive sizes describe placement, not installed binary savings.

It does not
establish smaller installed binaries, lower full-build compiler memory, or a
cold-build speedup. Those measurements require stable final product builds;
archive byte counts are not substitutes for either installed product size.

The shared build-graph rewriter is implemented inside the standalone inference
package and re-exported by the parent build support. This preserves that
package's import boundary. Standalone build-graph resolution succeeds; the
source document suite passes 7/7 steps. A subsequent product replay passes
103/103 steps, with all production compilers cached, and its installed archives
receive the final ownership scan above. The broad native video suite passes
318 tests with 2 skips when macOS native services are available; the sandboxed
run reports unavailable Metal/VideoToolbox services.
