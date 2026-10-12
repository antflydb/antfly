# Request-local projected video frame cache qualification

Native CPU and Metal cache complete per-picture projections across overlapping
video parts and groups in one request. Exact clip hashes and sampled picture
identities distinguish entries, and one loaded model session owns the cache.
CPU arrays own their storage; dense F32 Metal tensors retain device buffers
across temporary backend teardown, without intermediate host downloads.

Capacity is bounded to 128 entries and 64 MiB of retained payload/buffer spans.
Shared admission leases outlive per-group execution permits. Device views
charge the full underlying buffer span. Fixed cache metadata and process RSS
are separate from this payload quota. Capacity/admission/allocation denial
falls back to uncached execution. Queue tickets protect borrowed entries from
eviction and return ownership under cancellation, failure and backpressure.
Only model consumers mutate Metal buffer references. Device cleanup retains
its process protection guard through the last cached buffer release.

The final macOS ReleaseSafe suite passes 34 tests. It includes CPU ownership,
shared admission denial, allocation denial, eviction with outstanding loans,
fully cached producer runs for every container/codec path, abandoned queue
cleanup, and Metal value/offset preservation across wrapper teardown without
intermediate downloads. A fake hard-cancellation monitor verifies that device
cache protection remains armed through cleanup.

Actual Linux ARM64 passes 174 decoder tests with 27 platform skips and 31
model/cache tests with three skips. This closes the prior progressive H.264
reference-storage runtime follow-up. Docker's orphaned backend was reset after
a supported restart and graceful termination failed; the actual runtime and
image identities are retained with the artifacts.

Pretrained CPU and Metal qualification checks eight pinned corpus documents,
18 ordered groups (repeated windows and mixed cached/missing pictures), two
multipart groups and a subsequent independent request. All 18 cached group
vectors match the pinned uncached production binary exactly, and the multipart
vectors also match. PyTorch vector gates pass, retrieval rankings are exact
for both queries, and top-1/3/5 overlap is 1.0. Maximum cosine score drift is
1.10e-7 on CPU and 1.80e-7 on Metal. This establishes reference preservation on
the recorded corpus, not end-user relevance or broad codec conformance.

The corpus is `tools/embeddinggemma2/testdata/sampling/corpus.json`, with its
pinned oracle alongside it. Weights remain outside the repository. The
qualification tool is `tools/embeddinggemma2/video_cache_qualification.py`.

```sh
# From zig/pkg/inference, using the qualified Zig development compiler:
zig build test-video-model install-video-model-tests bench-server -Doptimize=ReleaseSafe --prefix /tmp/frame-cache
zig build install-video-model-tests bench-server -Dtarget=aarch64-linux-musl -Doptimize=ReleaseSafe --prefix /tmp/frame-cache-linux
# From the repository root, with the pinned bundle already acquired:
python3 -B tools/embeddinggemma2/video_cache_qualification.py --backend native --binary /tmp/frame-cache/bin/antfly-inference-bench-server --baseline-binary /path/to/pinned/uncached-server --model-dir /path/to/model --corpus tools/embeddinggemma2/testdata/sampling/corpus.json --reference tools/embeddinggemma2/testdata/sampling/pytorch.json --output /tmp/cache-native.json
# Repeat with --backend metal on macOS. Execute the Linux binaries on Linux.
```

Timings are diagnostic. The baseline predates progressive H.264 reference
sharing and executes its batch before standalone warmups; the cached server
executes standalone checks before its batch. The shared host also runs other
work. Runtime differences therefore cannot be attributed solely to the cache,
and this is not sustained-load or statistically qualified throughput evidence.

Container/header validation still runs. Missing interframes still replay
necessary dependencies; cached targets skip preparation/projection, and a
fully cached selection skips reconstruction. Wider scheduling, persistent
stream-window reuse, conformance/fuzzing and display qualification remain in
the full active non-CUDA goal.

The same pretrained qualification also passes on actual Linux ARM64: all 18
ordered cached groups and both multipart groups match its pinned uncached ELF
binary exactly, with exact reference rankings/top-1/3/5 overlap. Maximum score
drift is 2.91e-07. Linux HTTP execution uses recorded Docker proxy wrappers;
their reports hash the wrappers, and this manifest also binds the underlying
ELF binaries. The proxy port is exposed only on host loopback. Process-memory
fields in the Linux report observe the proxy, not the VM server, and are not
claimed as Linux model memory measurements.
