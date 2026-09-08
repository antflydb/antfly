# Hash primitives

Import `antfly_hash` and use `Crc32.init/update/final` or `Crc32.hash`.
This is IEEE CRC32 (reflected polynomial `0xedb88320`), compatible with
`std.hash.Crc32`; it is not CRC32C or a cryptographic integrity primitive.

ARM64 targets with the `crc` feature use IEEE CRC instructions. Generic ARM64,
AMD64 (including SSE4.2 targets), and other targets use allocation-free
slicing-by-eight. The table is 8 KiB; arbitrary byte alignment and incremental
updates are supported. Target feature selection is compile-time, so portable
builds never assume optional ARM instructions exist at runtime.

From `zig/`, run `zig build lib-hash-test -Doptimize=Debug`. The tests also run
with `unit-test`. To explicitly exercise the generic ARM64 path on an ARM64
host, run `zig test lib/hash/src/mod.zig -O Debug -mcpu=generic`.

Linux compile checks (not runtime validation):

```sh
zig test lib/hash/src/mod.zig -O Debug -target x86_64-linux-musl -fno-emit-bin
zig test lib/hash/src/mod.zig -O Debug -target aarch64-linux-musl -mcpu=generic+crc -fno-emit-bin
```

The ReleaseFast-only throughput test measures the checksum kernel, not storage
or query performance. Use native hardware when comparing architecture speeds.
