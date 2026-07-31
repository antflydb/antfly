#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
  echo "usage: $0 <tokenizer.json> <corpus.txt> [repeat] [exact|hash]" >&2
  exit 2
fi

resolve_path() {
  local input=$1
  local directory
  directory=$(cd -- "$(dirname -- "${input}")" && pwd)
  printf '%s/%s\n' "${directory}" "$(basename -- "${input}")"
}

tokenizer_json=$(resolve_path "$1")
corpus=$(resolve_path "$2")
repeat=${3:-1}
validation=${4:-exact}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
zig_dir=$(cd -- "${script_dir}/.." && pwd)

run_benchmark() {
  (
    cd "${zig_dir}"
    zig build -Doptimize=ReleaseFast bench-tokenizer -- \
      "${tokenizer_json}" "${corpus}" --repeat "${repeat}" \
      --validation "${validation}" --mmap-corpus --prefault-corpus "$@"
  )
}

echo "experiment=stage_diagnostics"
run_benchmark \
  --warmup 1 --iterations 1 --threads 1 --internal-threads 1 \
  --diagnostics --diagnostic-iterations 1

echo "experiment=cache_profile"
run_benchmark \
  --warmup 1 --iterations 1 --threads 1 --internal-threads 1 \
  --profile-bpe

for cache_mb in 0 2 8 32 64 128 256; do
  echo "experiment=cache_capacity cache_max_mb=${cache_mb}"
  run_benchmark \
    --warmup 2 --iterations 5 --threads 1 --internal-threads 16 \
    --cache-max-mb "${cache_mb}"
done

for bulk_slots in 4096 16384; do
  echo "experiment=bulk_cache bulk_slots_per_shard=${bulk_slots}"
  run_benchmark \
    --warmup 2 --iterations 5 --threads 1 --internal-threads 16 \
    --cache-max-mb 128 --cache-bulk-slots "${bulk_slots}"
done

for tasks in 1 2 4 8 12 16; do
  echo "experiment=internal_tasks internal_threads=${tasks}"
  run_benchmark \
    --warmup 2 --iterations 5 --threads 1 --internal-threads "${tasks}"
done

for chunks_per_task in 1 2 4 8; do
  echo "experiment=chunk_geometry chunks_per_task=${chunks_per_task} max_chunks=128"
  run_benchmark \
    --warmup 2 --iterations 5 --threads 1 --internal-threads 16 \
    --chunks-per-task "${chunks_per_task}" --max-chunks 128
done
