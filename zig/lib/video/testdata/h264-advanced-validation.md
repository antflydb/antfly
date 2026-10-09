# Advanced portable H.264 validation

Validated on 2026-10-08 with Zig 0.17.0 against the checked-in tool and advanced
oracle receipts. The advanced receipt contains 71 vectors; the PAFF packet
receipt adds 30 vectors confirmed by FFmpeg at native depth. The assembly receipt
adds 15 vectors with canonical FFmpeg sample qualification and separately tested
fragmented/inter-picture packet ordering. This is generated
coding-tool qualification, not full H.264 conformance or production-stream coverage.

| Check | Result |
| --- | --- |
| macOS native Debug `test-video` | 90 passed, 2 platform skips, 0 failures |
| Linux ARM64 musl ReleaseSafe, executed in Alpine 3.22 | 73 passed, 19 platform skips, 0 failures |
| WASI Debug, executed with Wasmtime | 73 passed, 19 platform skips, 0 failures |
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
Complete PAFF fields and field MMCO 1–6 remain qualified. The bounded assembly
extension supports fields split across packets, metadata gaps and buffering pairs
across unrelated coded pictures. H.264 complementary pairs normally occupy
consecutive coded access units; these reordered packet cases are explicitly an
extension, not conformant-order oracle coverage. Tests compare every member
selection with independently known samples at 8/10/14-bit depths and validate
member-only timestamps, unordered callback slots, pending/slice/dependency limits,
continuation sync flags, missing/duplicate fragments, cancellation, frozen
prediction after DPB eviction, copy-on-write completion and exhaustive allocation
failures for snapshots and multiple pending workspaces. Non-complementary pairs
remain rejected. Higher-depth/chroma Metal imports and CUDA/model integration
remain separate work.
