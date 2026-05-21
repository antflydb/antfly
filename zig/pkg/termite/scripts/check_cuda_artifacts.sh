#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
termite_dir="$(cd "${script_dir}/.." && pwd)"
cd "${termite_dir}"

cu="src/ops/cuda/artifacts/termite_cuda_kernels.cu"
ptx_files=(
  "src/ops/cuda/artifacts/termite_cuda_kernels.ptx"
  "src/ops/cuda/artifacts/termite_cuda_kernels_sm61.ptx"
)
kernels_zig="src/ops/cuda/kernels.zig"

required_count="$(
  awk -F'"' '/cuModuleGetFunction/ { print $2 }' "${kernels_zig}" |
    sort -u |
    wc -l |
    tr -d '[:space:]'
)"
if [[ "${required_count}" == "0" ]]; then
  echo "failed to discover CUDA runtime loader symbols from ${kernels_zig}" >&2
  exit 1
fi

awk -F'"' '/cuModuleGetFunction/ { print $2 }' "${kernels_zig}" | sort -u | while IFS= read -r symbol; do
  if ! grep -q "void ${symbol}(" "${cu}"; then
    echo "missing CUDA source symbol: ${symbol}" >&2
    exit 1
  fi
  for ptx in "${ptx_files[@]}"; do
    if ! grep -q "\\.visible \\.entry ${symbol}" "${ptx}"; then
      echo "missing PTX entry symbol in ${ptx}: ${symbol}" >&2
      exit 1
    fi
  done
done

for ptx in "${ptx_files[@]}"; do
  duplicate_labels="$(
    awk '/^[A-Za-z_.$][A-Za-z0-9_.$]*:/{gsub(":$", "", $1); print $1}' "${ptx}" |
      sort |
      uniq -d
  )"
  if [[ -n "${duplicate_labels}" ]]; then
    echo "duplicate PTX labels in ${ptx}:" >&2
    echo "${duplicate_labels}" >&2
    exit 1
  fi

  duplicate_shared="$(
    awk '/^[[:space:]]*\.shared /{print $0}' "${ptx}" |
      sort |
      uniq -d
  )"
  if [[ -n "${duplicate_shared}" ]]; then
    echo "duplicate PTX shared declarations in ${ptx}:" >&2
    echo "${duplicate_shared}" >&2
    exit 1
  fi

  duplicate_entries="$(
    awk '/^\.visible \.entry /{print $3}' "${ptx}" |
      sort |
      uniq -d
  )"
  if [[ -n "${duplicate_entries}" ]]; then
    echo "duplicate PTX entries in ${ptx}:" >&2
    echo "${duplicate_entries}" >&2
    exit 1
  fi

  ptx_target="$(
    awk '/^\.target /{gsub(",", "", $2); print $2; exit}' "${ptx}"
  )"
  if [[ -z "${ptx_target}" ]]; then
    echo "missing PTX target directive in ${ptx}" >&2
    exit 1
  fi

  if command -v ptxas >/dev/null 2>&1; then
    cubin="$(mktemp "${TMPDIR:-/tmp}/termite-cuda-kernels.XXXXXX.cubin")"
    trap 'rm -f "${cubin}"' EXIT
    ptxas_log="$(mktemp "${TMPDIR:-/tmp}/termite-cuda-ptxas.XXXXXX.log")"
    if ptxas -arch="${ptx_target}" "${ptx}" -o "${cubin}" 2>"${ptxas_log}"; then
      rm -f "${cubin}" "${ptxas_log}"
      trap - EXIT
    elif grep -q "Value '${ptx_target}' is not defined" "${ptxas_log}"; then
      echo "ptxas does not support ${ptx_target}; relying on runtime JIT smoke for ${ptx}" >&2
      rm -f "${cubin}" "${ptxas_log}"
      trap - EXIT
    else
      cat "${ptxas_log}" >&2
      rm -f "${cubin}" "${ptxas_log}"
      trap - EXIT
      exit 1
    fi
  elif [[ "${TERMITE_CUDA_REQUIRE_PTXAS:-0}" == "1" ]]; then
    echo "ptxas is required by TERMITE_CUDA_REQUIRE_PTXAS=1 but was not found" >&2
    exit 1
  else
    echo "ptxas not found; skipping PTX assembly check for ${ptx}"
  fi
done

echo "CUDA artifact static checks passed"
