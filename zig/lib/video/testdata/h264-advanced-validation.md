# Advanced portable H.264 validation

Validated on 2026-10-08 with Zig 0.17.0 against the checked-in tool and advanced
oracle receipts. The advanced receipt contains 71 vectors; the PAFF packet
receipt adds 30 vectors confirmed by FFmpeg at native depth. This is generated
coding-tool qualification, not full H.264 conformance or production-stream coverage.

| Check | Result |
| --- | --- |
| macOS native Debug `test-video` | 85 passed, 2 platform skips, 0 failures |
| Linux ARM64 musl ReleaseSafe, executed in Alpine 3.22 | 68 passed, 19 platform skips, 0 failures |
| WASI Debug, executed with Wasmtime | 68 passed, 19 platform skips, 0 failures |
| Linux ARM64 / x86-64 musl ReleaseSafe | Video tests, import checks and benchmark compile |
| Inference consumer `check-video test-media test-audio` | Imports compile; 26 media and 3 audio tests pass |
| CABAC normative generator | Regenerates checked-in tables without changes |
| Zig formatting, Python Ruff formatting/lint, diff whitespace | Pass |

Native tests include existing VideoToolbox/Metal preparation and cache coverage.
Portable tests compare samples against independent FFmpeg or known normative
oracles, exercise allocation failures, cancellation, resource bounds, slice
coverage, scaling fallback rules and shared reservation resizing. Linux execution
uses a read-only binary mount and no network; no codec library is linked.

See [README.md](README.md) for fixture generation and oracle distinctions, and
[VIDEO.md](../VIDEO.md#implemented-independent-efficiency-and-portable-h264-work)
for the supported profiles, native sample layout and explicit remaining limits.
Complete PAFF fields in consecutive packets and field MMCO 1–6 are qualified.
Tests cover bottom-first pairs, I/P/B presentation, long-term reordering, either
field selection, duplicate callback slots, bounded lookahead, missing/mismatched
complements, cancellation after a first field and exhaustive allocation failures.
Non-consecutive/non-complementary pairs and fields split across multiple packets
remain rejected. Higher-depth/chroma Metal imports and CUDA/model integration
remain separate work.
