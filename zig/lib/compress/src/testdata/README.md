# Zstandard literal fixtures

Generated with PyArrow 25's default Zstandard codec from the first eight 768d
float32 vectors in the VectorDBBench Cohere 1M training corpus. The source file
SHA-256 is `de6b84eb08fc5203a56eef619970a14f5cae08d0a0ae0bcd23b8058d7045f381`.

`cohere_literals.zst` compresses those 24,576 little-endian coordinate bytes.
`cohere_multiblock.zst` compresses twelve copies, with bit `i % 8` flipped every
19 bytes, starting at `i % 19` in copy `i`. Its 294,912-byte output exercises
multiple compressed blocks and retained entropy state. Expected uncompressed
SHA-256 values are pinned in the differential decoder test.

Tests compare complete output with the Zig standard decoder, exercise truncated
frames, and separately exhaust every prefix of balanced and skewed Huffman
tables, including the eleven-bit limit and incomplete-tree rejection.
