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
      echo "usage: $0 {native|metal|matrix-native|matrix-metal|large-native|large-metal|compare-large} [bench args...]" >&2
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

run_large_matrix() {
  local one_backend="$1"
  shift
  local batches=(4 8 16)
  local seq_lens=(256 512)
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

csv_col() {
  local row="$1"
  local index="$2"
  awk -F, -v idx="$index" '{print $idx}' <<<"$row"
}

run_compare_large() {
  local batches=(4 8 16)
  local seq_lens=(256 512)
  local labels=(8 32)
  printf 'batch,seq_len,num_labels,native_ms,metal_ms,speedup,metal_resident,metal_mps_active,metal_mpsgraph,metal_packed_qkv,metal_packed_qkv_fallbacks,metal_relative_qk_pair,metal_relative_qk_pair_fallbacks,metal_command_plan_reused,metal_frame_gpu_ms,metal_attention_gemm,metal_attention_gemm_fallbacks,metal_attention_legacy\n'
  for seq_len in "${seq_lens[@]}"; do
    for num_labels in "${labels[@]}"; do
      for batch in "${batches[@]}"; do
        local native_output metal_output native_row metal_row native_ms metal_ms speedup
        native_output="$(run_one native --quant none --seq-len "$seq_len" --num-labels "$num_labels" --batch "$batch" "$@" --format csv 2>&1)"
        metal_output="$(run_one metal --quant none --seq-len "$seq_len" --num-labels "$num_labels" --batch "$batch" "$@" --format csv 2>&1)"
        native_row="$(printf '%s\n' "$native_output" | tail -n 1)"
        metal_row="$(printf '%s\n' "$metal_output" | tail -n 1)"
        native_ms="$(csv_col "$native_row" 11)"
        metal_ms="$(csv_col "$metal_row" 11)"
        speedup="$(awk -v n="$native_ms" -v m="$metal_ms" 'BEGIN { if (m > 0) printf "%.3f", n / m; else printf "0.000" }')"
        printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
          "$batch" "$seq_len" "$num_labels" "$native_ms" "$metal_ms" "$speedup" \
          "$(csv_col "$metal_row" 16)" "$(csv_col "$metal_row" 20)" "$(csv_col "$metal_row" 21)" \
          "$(csv_col "$metal_row" 25)" "$(csv_col "$metal_row" 26)" "$(csv_col "$metal_row" 27)" \
          "$(csv_col "$metal_row" 28)" "$(csv_col "$metal_row" 29)" "$(csv_col "$metal_row" 30)" \
          "$(csv_col "$metal_row" 40)" "$(csv_col "$metal_row" 41)" "$(csv_col "$metal_row" 42)"
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
  large-native)
    run_large_matrix native "$@"
    ;;
  large-metal)
    run_large_matrix metal "$@"
    ;;
  compare-large)
    run_compare_large "$@"
    ;;
  *)
    echo "usage: $0 {native|metal|matrix-native|matrix-metal|large-native|large-metal|compare-large} [bench args...]" >&2
    exit 2
    ;;
esac
