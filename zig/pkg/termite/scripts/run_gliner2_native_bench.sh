#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERMITE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$TERMITE_DIR"

backend="native"
if [[ $# -gt 0 ]]; then
  backend="$1"
  shift
fi

run_one() {
  local one_backend="$1"
  shift
  case "$one_backend" in
    native)
      zig build -Dmlx=false -Donnx=false -Dmetal=true bench-gliner2-native -- --backend native "$@"
      ;;
    metal)
      TERMITE_METAL_UPLOAD_FLOAT32_INPUTS="${TERMITE_METAL_UPLOAD_FLOAT32_INPUTS:-1}" \
        zig build -Dmlx=false -Donnx=false -Dmetal=true bench-gliner2-native -- --backend metal "$@"
      ;;
    *)
      echo "usage: $0 {native|metal|matrix-native|matrix-metal} [bench args...]" >&2
      exit 2
      ;;
  esac
}

run_isolated_matrix() {
  local one_backend="$1"
  shift
  local batches=(1 2 4 8 16)
  local seq_lens=(128 256 512)
  local labels=(8 32)
  local printed_header=0
  for seq_len in "${seq_lens[@]}"; do
    for num_labels in "${labels[@]}"; do
      for batch in "${batches[@]}"; do
        local output
        output="$(run_one "$one_backend" --quant none --seq-len "$seq_len" --num-labels "$num_labels" --batch "$batch" "$@" --format csv 2>&1)"
        if [[ "$printed_header" == 0 ]]; then
          printf '%s\n' "$output"
          printed_header=1
        else
          printf '%s\n' "$output" | sed '1d'
        fi
      done
    done
  done
}

case "$backend" in
  native|metal)
    run_one "$backend" "$@"
    ;;
  matrix-native)
    run_isolated_matrix native "$@"
    ;;
  matrix-metal)
    run_isolated_matrix metal "$@"
    ;;
  *)
    echo "usage: $0 {native|metal|matrix-native|matrix-metal} [bench args...]" >&2
    exit 2
    ;;
esac
