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
shifts the descriptor's bounds and base. Partial deletions or type promotion use
the existing bounded decode path.

Sorted compaction checks that the output coordinates cover a complete source
interval in source order before copying. Coordinate checks use at most 64 records
at a time. A partial output prefix flushes before a copied chunk; decoded batches
stop at the next chunk boundary so they do not consume every potential copy start.
Selected-input counts and source monotonicity are computed once per merge plan,
then shared across fields. Inputs with no selected documents do not participate
in physical-type inference.

Navigation grows from 8 to 32 bytes per chunk. New writes retain the 64 KiB raw
chunk target, allowing an oversized individual value as the established exception.
Admission includes both input and output directories and peak decoded scratch.
Copying removes codec work and value allocations, but still performs compressed
source reads, output writes, and integrity checks; it is not zero total cost.
