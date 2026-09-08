# Shared checksums

Import `antfly_hash`. `Crc32`, `Crc32c`, `Crc64Nvme`, and `Adler32` expose
`init()`, `update(bytes)`, `final()`, and `hash(bytes)`. Updates accept arbitrary
alignment and empty slices; `final()` leaves the state usable for more updates.
The checksums preserve the standard-library values and existing on-disk/wire
formats. They are not cryptographic hashes.

| API | Accelerated implementation | Portable implementation |
| --- | --- | --- |
| `Crc32` (IEEE) | ARM64 CRC; x86-64 PCLMUL folding for buffers >=64 bytes | Slicing-by-eight |
| `Crc32c` (Castagnoli) | ARM64 CRC32C; x86-64 SSE4.2 CRC instructions | Slicing-by-eight |
| `Crc64Nvme` | — | Slicing-by-eight |
| `Adler32` | Zig vectors, extracted from the image encoder | Compiler-lowered vectors and scalar tails |

CPU features guaranteed by the compilation target bypass runtime discovery.
Baseline Linux ARM64 builds read HWCAP through libc or Zig's startup auxiliary
vector; macOS ARM64 supports CRC on every supported machine. Hosted x86-64
builds use CPUID. The cache is atomic and allocation-free; optional instructions
are isolated in non-inlined kernels. PCLMUL requires SSE2 but not AVX, SSE4.1,
or SSE4.2. SSE4.2's CRC instruction is used only for Castagnoli, never IEEE.
Other platforms retain target guarantees or portable code. Freestanding builds
only use target-guaranteed features; the C backend uses portable code. Zig 0.16's
non-LLVM x86 backend cannot encode the CRC instructions, so it also uses portable
CRC32/CRC32C kernels; LLVM release builds retain hardware acceleration. A libc-free
Linux library without startup-provided auxv also stays portable.

CRC tables occupy 8 KiB per 32-bit polynomial and 16 KiB for CRC64/NVME.
Compile-time hashing always uses the portable path. CRC64/NVME currently has no
hardware kernel; slicing-by-eight improves on Zig's byte-at-a-time generic CRC.

Production callers include storage, WAL, Raft state, backup/HA formats, Lite,
lake object verification, and both PNG encoders. Image PNG encoding writes the
zlib wrapper around raw deflate so its Adler32 trailer also uses this module;
a compatibility test checks byte-for-byte equality with the standard wrapper.
Wyhash, xxHash, SHA, BLAKE3, and CRCs with other parameters keep their existing
implementations.

## Validation

From the repository root, `make zig-checksums-check` tests and runs the CI
usage guard. It rejects ordinary direct/aliased std checksum calls in production
Zig code, including forward container declarations and explicitly typed
namespace aliases, allowing test blocks and this implementation's reference oracles.
This is a lexical policy check, not a Zig type checker: computed reflection or
cross-file namespace reexports are not resolved. CI runs it and executes native
baseline checksum tests on both x86-64 and Linux ARM64, including libc and
libc-free ARM64 startup paths.

From `zig/`:

```sh
zig build lib-hash-test lib-image-png-test -Doptimize=Debug
zig build lib-hash-test -Dcpu=baseline -Doptimize=ReleaseSafe
zig build lib-hash-test -Doptimize=ReleaseFast -- throughput
```

On Linux x86-64, CI also runs `zig test lib/hash/src/mod.zig -O Debug
-mcpu=baseline -fno-llvm` to exercise the non-LLVM backend's portable kernels.

Hash and PNG tests also run with `unit-test`. Correctness checks cover known
vectors, portable versus dispatched kernels, unaligned buffers, folding/tail
boundaries, incremental updates, Adler reduction limits, and concurrent CPU
cache initialization. The throughput benchmark compares all four algorithms
against std at 64 B, 4 KiB, and 1 MiB, checking identical result sums.

Local Apple M4 / Zig 0.16.0 ReleaseFast results (median of three samples,
32 MiB hashed per sample, 1 MiB buffers):

| Checksum | std time | Shared time | Kernel speedup |
| --- | --- | --- | --- |
| CRC32 | 65.2 ms | 3.22 ms | 20.2× |
| CRC32C | 64.0 ms | 3.23 ms | 19.8× |
| CRC64/NVME | 64.7 ms | 14.1 ms | 4.6× |
| Adler32 | 9.12 ms | 2.73 ms | 3.3× |

These measure checksum throughput, not end-to-end storage or query performance.
The x86-64 baseline kernels were also executed under Rosetta for correctness;
use native x86 hardware for representative x86 performance measurements.

Cross-target compile checks (not runtime validation):

```sh
zig test lib/hash/src/mod.zig -O ReleaseSafe -target x86_64-linux-musl -mcpu=baseline -fno-emit-bin
zig test lib/hash/src/mod.zig -O ReleaseSafe -target aarch64-linux-musl -mcpu=baseline -fno-emit-bin
zig test lib/hash/src/mod.zig -O ReleaseSafe -target aarch64-linux-musl -mcpu=baseline -lc -fno-emit-bin
zig test lib/hash/src/mod.zig -O ReleaseSafe -target wasm32-wasi -fno-emit-bin
```

The x86 IEEE folding algorithm and constants are adapted from Chromium zlib's
[`crc32_simd.c`](https://chromium.googlesource.com/chromium/src/+/main/third_party/zlib/crc32_simd.c). Its BSD notice is retained in `LICENSE.chromium` and the root
`THIRD_PARTY_NOTICES.md` shipped with releases.
