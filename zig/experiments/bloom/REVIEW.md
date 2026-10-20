# Bit-mask fast path review

Review scope: the production changes in `zig/lib/bloom/src/mod.zig` relative to
`70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a`. The blocked filters and changed hash
derivations are separate experiments and are not part of this production patch.

**Result: no blocking correctness or persisted-format findings.** The change is
a reasonable standalone optimization candidate. The experiments do not establish
a consistent storage-level speedup from this change alone.

## Correctness and compatibility

The existing implementation probes
`(h1 +% (i *% h2)) % bit_count`, after replacing a zero `h2` with the existing
constant. For a positive power of two `bit_count = 2^p`, modulo is exactly
`value & (bit_count - 1)`. Repeated wrapping addition produces the same u64
sequence as wrapping multiplication plus addition: at iteration `i`, the new
`h` is `(h1 + i*h2) mod 2^64`. This also preserves low bits after overflow.

Both insertion and lookup apply the same transformation. The zero-stride
replacement is unchanged; even strides remain valid and preserve their existing
mapping. A zero bit count or zero probe count returns before the mask subtraction,
so empty filters cannot underflow the mask. The probe counter still handles the
full u8 range, including a hash count of 255.

Non-power-of-two filters retain the original modulo path, which matters because
the existing decoder accepts them. Allocation, sizing, ownership, clone behavior,
filter magic/version, hash count, bit count, and serialization are unchanged.
The expected result is identical bitmaps, membership answers, and serialized
bytes; no migration or rebuilding is needed for this patch.

## Validation

The production equivalence test covers powers of two and other bit counts,
multiple hash counts, zero/even/maximum strides, and wrapping arithmetic.

The additional `review_test.zig` compares against a verbatim snapshot of the
original implementation, rather than another version of the optimized formula:

- 16 capacities: 0, 1, 2, 4, 7, 8, 16, 31, 63, 64, 127, 128, 511, 512, 1024,
  and 65536 bits.
- Seven hash counts: 0, 1, 4, 7, 10, 16, and 255.
- 32 insertions for each of 112 configurations, checking exact bitmap equality
  after every insertion, with explicit wrap/zero/even-stride inputs and
  deterministic randomized inputs.
- All inserted memberships plus 256 additional randomized queries per case,
  empty-filter behavior, byte-for-byte encoding equality, original-to-new
  decoding, and new-to-original decoding.

The production tests and differential review tests passed with Zig 0.17.0 in
both Debug and ReleaseSafe. Formatting and `git diff --check` also passed.

```sh
/path/to/zig-0.17.0/zig test zig/lib/bloom/src/mod.zig -O Debug
/path/to/zig-0.17.0/zig test zig/lib/bloom/src/mod.zig -O ReleaseSafe
/path/to/zig-0.17.0/zig test -O Debug --dep bloom \
  -Mroot=zig/experiments/bloom/review_test.zig \
  -Mbloom=zig/lib/bloom/src/mod.zig
/path/to/zig-0.17.0/zig test -O ReleaseSafe --dep bloom \
  -Mroot=zig/experiments/bloom/review_test.zig \
  -Mbloom=zig/lib/bloom/src/mod.zig
```

## Performance limits

For a million-key filter including 64-byte key hashing, median absent lookup
time fell from 20.475 to 17.634 ns (1.16x throughput, 13.9% less time). At eight
million keys it fell from 36.797 to 32.850 ns (1.12x throughput, 10.7% less time).
Memory and observed false-positive rates are identical to the original filter.
The standalone harness measures the actual modified production module.

The storage results are mixed and noisy; no consistent missing-read or hit-read
improvement was demonstrated for the mask patch. Extra branching on filter
shape and the percentage of read cost spent probing filters can affect the
end-to-end result. Measurements were on a busy macOS ARM64 host, not isolated
x86/Linux production hardware. This review supports correctness and a local
component speedup; it does not support an overall database throughput claim.
