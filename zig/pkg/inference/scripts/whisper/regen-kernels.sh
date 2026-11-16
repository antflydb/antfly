#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail
mode="${1:---check}"
case "$mode" in --check|--write) ;; *) echo "usage: $0 [--check|--write]" >&2; exit 2 ;; esac
root="$(git rev-parse --show-toplevel)"
source_path="$root/zig/pkg/inference/src/ops/cuda/whisper_kernels.cu"
artifact_base="$root/zig/pkg/inference/src/ops/cuda/whisper_kernels"
nvcc="${NVCC:-/usr/local/cuda-13.2/bin/nvcc}"
cuobjdump="${CUOBJDUMP:-$(dirname "$nvcc")/cuobjdump}"
case "$("$nvcc" --version)" in *"release 13.2"*) ;; *) echo 'Whisper native artifacts require CUDA 13.2.' >&2; exit 1 ;; esac
mkdir -p "$root/.zig-cache"
staged="$(mktemp "$root/.zig-cache/whisper-artifact.XXXXXX")"
trap 'rm -f "$staged" "$staged.ptx" "$staged.fatbin" "$staged.cubin" "$staged.json"' EXIT
archs=(75 80 89 90 100 110 120)
flags=()
for arch in "${archs[@]}"; do
  flags+=("-gencode=arch=compute_$arch,code=sm_$arch")
done
"$nvcc" --fatbin -O3 "${flags[@]}" -gencode=arch=compute_75,code=compute_75 "$source_path" -o "$staged.fatbin"
"$nvcc" --cubin -O3 -arch=sm_89 "$source_path" -o "$staged.cubin"
"$nvcc" --ptx -O3 -arch=compute_75 "$source_path" -o "$staged.ptx"
dump="$("$cuobjdump" --list-elf "$staged.fatbin")"
for arch in "${archs[@]}"; do
  if ! [[ "$dump" == *"sm_$arch."* || "$dump" == *"sm_$arch "* ]]; then
    echo "Whisper fatbin is missing sm_$arch" >&2; exit 1
  fi
done
python3 - "$source_path" "$staged" "$nvcc" <<'PY'
import hashlib,json,re,subprocess,sys
from pathlib import Path
source,stage,nvcc=sys.argv[1:]
digest=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
manifest={"source_sha256":digest(source),"architectures":[75,80,89,90,100,110,120],
          "artifact_sha256":{suffix:digest(stage+"."+suffix) for suffix in ["ptx","fatbin","cubin"]},
          "compiler":"nvcc "+re.search(r"release [0-9.]+, V([0-9.]+)", subprocess.check_output([nvcc,"--version"],text=True)).group(1),
          "portable_compiler":"CUDA 13.2; compute_75; PTX 9.2",
          "purpose":"Whisper CUDA kernels; architecture coverage is not performance qualification"}
Path(stage+".json").write_text(json.dumps(manifest,indent=2)+"\n")
PY
for pair in "ptx:ptx" "fatbin:fatbin" "cubin:_sm89.cubin" "json:json"; do
  extension="${pair%%:*}"; suffix="${pair#*:}"
  target="$artifact_base.$suffix"
  if [ "$extension" = cubin ]; then target="${artifact_base}_sm89.cubin"; fi
  if [ "$mode" = --write ]; then
    if ! cmp -s "$staged.$extension" "$target"; then
      chmod 644 "$staged.$extension"
      mv "$staged.$extension" "$target"
    fi
  else
    cmp "$staged.$extension" "$target"
  fi
done
