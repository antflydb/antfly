# Pretrained selective replay qualification

Four late two-second WebM windows from the pinned full-length Sintel VP9/AV1
encodings exercise codec-key restart and original logical-picture IDs. Each
window selects two frames through independent FFprobe presentation timestamps.
Two queries compare vector cosine scores, exact rankings and top-k overlap
against pinned PyTorch. Source attribution and encoding/pixel receipts remain
in `zig/lib/video/testdata/benchmarks/webm`. This measures reference preservation,
not end-user relevance. CPU, Metal and actual Linux ARM64 match the unchanged pinned PyTorch vectors,
exact rankings/top-k and load/retry checks. Full source-bound receipts are in
`zig/lib/video/testdata/replay-validation`. Recipe v5 includes nonaligned-row
RGB24 integer quantization; failed before-fix reports retain the observed gap.
