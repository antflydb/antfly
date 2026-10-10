# Indexed typed-value chunks

Typed compaction can skip a fully deleted chunk and copy an intact compressed
chunk without constructing its values. This requires document bounds outside
compression and a document-ID base that can change independently of the payload.

New writers use the indexed streaming variant. Existing front-directory sections
and streaming variants remain readable. Compaction migrates old sections by
bounded decoding; it does not require an eager rewrite of existing `.aflite`
files. Older binaries cannot read newly written indexed sections, so rollback
requires retaining an older artifact or using a reader that supports this variant.

## Layout

All integers are little endian. The low five bits of the type byte identify the
physical value type. Flags `0x80`, `0x40`, and `0x20` indicate streaming layout,
an exact decoded-size summary, and indexed navigation respectively. Indexed
navigation requires both other flags.

```
[type | 0xe0: u8][chunk count: u32]
[compressed chunk payloads ...]
[32-byte descriptors ...]
[largest decoded chunk: u64][directory start: u64]
```

Each descriptor contains:

| Offset | Value |
| --- | --- |
| 0 | Compressed chunk end offset, u64 |
| 8 | First document ID, u32 |
| 12 | Last document ID, u32 |
| 16 | External document-ID base, u32 |
| 20 | Stored row count, u32 |
| 24 | Decoded byte count, u64 |

The compressed payload retains the existing count, ID array, and typed-value
encoding, but IDs are relative to the external base. Decoders normalize them
before exposing existing point, bulk, and sequential reader interfaces.
Descriptors are protected by the containing segment's integrity checks. Readers
also validate extents, document ordering, count/range consistency, decoded-size
bounds and the footer summary. Decoding verifies the payload count, byte count,
and normalized first/last IDs against its descriptor.

## Merge behavior and bounds

Append compaction compares deletion ranks at each chunk boundary. A fully deleted
interval needs no payload decode. An intact same-type interval has a constant ID
shift, so compaction copies compressed bytes through a fixed 64 KiB buffer and
shifts the descriptor's bounds and base. Partial deletions, type promotion, or an unrepresentable shifted external base
use the existing bounded decode path. Copy eligibility is checked before writing;
a valid interval can still decode and remap when its base would become negative.

Sorted compaction prepares source-coordinate proofs once per merge plan. Each
maximal consecutive run of at least three coordinates has a 20-byte descriptor
(output start, input, first source ID, and count). A descriptor replaces at least
24 bytes of file coordinates. File plans create a private descriptor run only
when a qualifying span exists; readers consume at most 64 descriptors per batch.
Memory plans retain coalesced descriptors under their allocator budget. Fully
fragmented streams retain no span descriptors and use their original coordinates.

Fields synthesize output references from proved spans, avoiding repeated
coordinate verification. Singleton/pair gaps retain batches of up to 64 original
coordinates; one- or two-row chunks need only a bounded direct proof. A complete
source interval can copy even when chunks appear in a different order. A partial
output prefix flushes before a copied chunk, and decoded batches stop at the next
eligible copy start. Selected-input counts and monotonicity are collected during
the same preparation pass. Inputs with no selected documents do not participate
in physical-type inference. Plans and fields without live typed sections skip
preparation entirely.

Uncached indexed point reads use the directory to select a single chunk before
decoding, for both heap and range sources. Requests outside all chunk bounds need
no decode; missing IDs within a sparse chunk still validate that chunk. Existing
point-cache behavior and legacy decoder paths remain available.

Navigation grows from 8 to 32 bytes per chunk. New writes retain the 64 KiB raw
chunk target, allowing an oversized individual value as the established exception.
Admission includes both input and output directories and peak decoded scratch.
Copying removes codec work and value allocations, but still performs compressed
source reads, output writes, and integrity checks; it is not zero total cost.

Sorted monotonic readers use `Cursor.nextAtOrAfter` to binary-search indexed
chunk bounds before decoding. The cursor never rewinds: within a decoded chunk
it seeks row IDs without constructing skipped values, and across chunks it jumps
directly to the first remaining candidate. Historical columns keep a forward
scan. An intact copied prefix therefore does not force later survivors to decode
the intervening deleted chunks.

Compressed copying consumes bounded borrowed source spans through `View.visitRange`.
Contiguous sources avoid staging; native artifact leases expose the same immutable
range visitor used by their authentication provider. The page facade verifies
cold contiguous pages before exposing their slices and borrows verified native
ranges directly. Cold range providers and providers without a visitor retain
bounded buffered reads and CRC checks. Visitors are synchronous, spans are capped
at 64 KiB, and callers discard private output if any provider or sink operation
fails. No borrowed slice escapes a callback.
