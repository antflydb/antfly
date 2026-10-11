# Logical pictures and explicit model windows

The v4 video recipe qualifies completed logical AVC pictures and explicit rational
timestamp windows on the pinned EmbeddingGemma 2 checkpoint. The default remains
the reference average-FPS/index policy. The eight video inputs cover VFR, PAFF
field pairs, VP9/AV1 timestamps and two independent windows from a 52-second
Sintel clip; two text queries compare reference cosine scores and retrieval order.

| Executed gate | Result |
|---|---|
| macOS video library, actual Metal/VideoToolbox | 154 passed, 2 skipped |
| Linux ARM64 portable video library | 133 passed, 23 platform skips |
| CPU/Metal model unit tests | 16 passed |
| Focused HTTP parser tests | 2 passed |
| Pretrained native CPU vs pinned PyTorch | 10 vectors; maximum coordinate error 4.62e-7 |
| Pretrained Metal vs pinned PyTorch | 10 vectors; maximum coordinate error 3.85e-7 |
| Pretrained Linux ARM64 vs pinned PyTorch | 10 vectors; maximum coordinate error 5.30e-7 |
| Rankings and top-k sets | Exact for both queries on CPU, Metal and Linux ARM64 |
| Malformed/empty policy and clean retry | 11 policies per backend, HTTP 400; clean retry parity |
| Sampling on non-video content | HTTP 400 on CPU and Metal |
| Separate window groups | Two ordered vectors, each agrees with its independent reference |

The final source-bound campaign also completes eight requests at concurrency two
on each macOS backend and the actual Linux ARM64 native server. The earlier operational run completed sixteen per macOS backend.
A deliberately disconnected request is followed by a matching clean retry; HTTP
alone does not establish when the cancelled GPU work completed.

The HTTP campaign exposed a union-tag conversion panic. The parser now reads the
original video bytes and validates the policy before assigning the sampled-video
variant. The focused borrowed-attachment regression and actual CPU/Metal HTTP
campaigns exercise this conversion. The first attempted unit command used runtime
filters and compiled the entire inference suite; it was stopped and superseded by
the successful compile-time-filtered command recorded in parser-tests.log.

The logical AVC library gate includes inter-picture field fragments, data
partitions, separate planes and geometry-changing configuration epochs. These
are not all pretrained cases. FFmpeg cannot decode the JM partition fixture
correctly; the four-picture independent JM library oracle is used for that case.
The pretrained PAFF fixture has eight packets and four completed pictures.

[Corpus, reports, hashes and reproducible commands](../../../../tools/embeddinggemma2/testdata/sampling/README.md)
bind the exact source, binary, checkpoint, independent frame selections and
result vectors. Timing/RSS were collected under shared build load and are not
isolated performance comparisons. Actual Linux pretrained timestamp/window
execution now passes in the offline ARM64 VM. Its report binds the statically
linked musl server and the same pinned checkpoint/reference; x86_64 runtime
execution remains unverified. This phase
does not close device normalization, cross-request scheduling, cross-window
frame reuse, streaming inheritance or the broader codec/performance gates.
