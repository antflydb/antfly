# BJ: Native family descriptors and one Metal source encoder

Baseline: `0884ea7a14`. This batch removes **19,929 authored production bytes**
after charging the descriptors and source encoder. Authored lines increase by
219 as escaped strings become readable, formatted MSL. This is a reduction in
maintained source representation and repeated ownership metadata; device
arithmetic and native instruction counts are unchanged.

## Consolidation

The registry now describes 89 native families and expands their 175 existing
regions once. Backend paths, Objective-C indentation and numbered region keys
come from the common owner. Typed source namespaces export contiguous
`source_0`, `source_1`, etc. Empty and gapped families fail compilation.
Custom CUDA header paths retain the same descriptor path and ordering.

Metal owners can keep readable MSL and encode it at one boundary through
`native_program.metal` or `metalAt`. Native host ABI macro splices stay with
their existing owners. The encoder computes the exact output size and fills one
buffer, replacing repeated string concatenation and duplicated escaping.
It preserves quoted characters, backslashes and newline termination behavior.
No runtime interpreter, dispatch branch, shader wrapper or kernel launch is added.

The broader token-template experiment found only about 2.6 KB of additional
near-clone savings in this slice and was not adopted. Existing arithmetic is
already shared heavily; these results do not establish a further 20% kernel-body
reduction.

## Qualification

- All 175 region paths, keys and ordering are identical. Only one generated
  payload changes physical C-string line boundaries; its emitted shader is
  byte-identical to the freshly frozen original.
- All 205 tracked CUDA/native inputs checked under `src/ops/cuda`,
  `src/graph/templates` and `src/ops/gpu` are byte-identical. This batch does not
  require another CUDA artifact rebuild or replace the prior native receipts.
- Local Metal: complete shader identity, 52 independent-reference correctness
  cases, 34 native resource checks and all 12 AA/AB/BA timing gates pass.
- Tile lowering and source-boundary tests, both generator freshness checks and
  the prototype CPU Whisper CLI build pass.
- Compile-failure probes reject both empty and gapped source namespaces.
- Independent enabled-backend cache identity tests pass for Metal and CUDA.
- Model-explorer's 11 tests pass; all 12 generated files remain fresh.

Evidence and source receipts are retained under
`.benchmark-assets/1166/bj-campaign/`. No whole-model throughput improvement or
additional CUDA timing result is claimed for this source-only consolidation.
