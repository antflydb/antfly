# Embedding scripts

Shared embedding utilities live at this level:

- `benchmark_embedding_endpoint.py` benchmarks any OpenAI-compatible embedding
  endpoint and can compare it with a reference endpoint.
- `bench_sparse_embedding.py` benchmarks sparse embedding batches through the
  Antfly CLI.

Checkpoint-specific tooling is grouped by model:

- `bge-m3/bench_pytorch.py` provides the PyTorch CPU/MPS reference, direct
  benchmark, and parity harness for `BAAI/bge-m3`.
- `nomic-embed-text-v1.5/bench_pytorch_mps.py` provides the matched PyTorch MPS
  direct and HTTP reference for `nomic-ai/nomic-embed-text-v1.5`.
- `nomic-embed-text-v1.5/validate_metal_competitiveness.py` validates retained
  Nomic Metal evidence and performance gates.

Run checkpoint-specific harnesses from `zig/pkg/inference`, or pass explicit
absolute paths for model, fixture, binary, and evidence inputs.
