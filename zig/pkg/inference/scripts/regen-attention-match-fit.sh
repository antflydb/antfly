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
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
root=$(cd -- "$script_dir/../../../.." && pwd)
nvcc=${NVCC:-/usr/local/cuda/bin/nvcc}
source_path="$root/zig/pkg/inference/src/ops/cuda/attention_match_fit_kernels.cuh"
output_path="$root/zig/pkg/inference/src/ops/cuda/artifacts/attention_match_fit.fatbin"
cache_path="$root/zig/.zig-cache/attention-match-fit-artifacts"
if [[ ${1:-} == --check ]]; then
  python3 - "$source_path" "$output_path" <<'PY_CHECK'
import hashlib, json, sys
from pathlib import Path
source, output = map(Path, sys.argv[1:])
manifest = json.loads(output.with_suffix('.json').read_text())
for path, key in ((source, 'source_sha256'), (output, 'artifact_sha256')):
    if hashlib.sha256(path.read_bytes()).hexdigest() != manifest[key]:
        raise SystemExit(f'stale native KV fitting artifact: {path}')
print('Native KV fitting source and artifact hashes match.')
PY_CHECK
  exit 0
fi
if [[ $# != 0 ]]; then
  printf 'usage: %s [--check]\n' "$0" >&2
  exit 2
fi
mkdir -p "$cache_path"
flags=(-O3 -std=c++17 -x cu -include cuda_runtime.h --fatbin)
for arch in 75 80 89 90 100 110 120; do
  flags+=("-gencode=arch=compute_$arch,code=sm_$arch")
done
"$nvcc" "${flags[@]}" "$source_path" -o "$cache_path/attention_match_fit.fatbin"
mv "$cache_path/attention_match_fit.fatbin" "$output_path"
python3 - "$source_path" "$output_path" "$nvcc" "${flags[@]}" <<'PY'
import hashlib, json, subprocess, sys
from pathlib import Path
source, output, compiler = map(Path, sys.argv[1:4])
report = {
    'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
    'artifact_sha256': hashlib.sha256(output.read_bytes()).hexdigest(),
    'compiler': subprocess.check_output([str(compiler), '--version'], text=True).strip(),
    'flags': sys.argv[4:],
    'architectures': [75, 80, 89, 90, 100, 110, 120],
    'purpose': 'native KV fitting; compilation is not serving qualification',
}
output.with_suffix('.json').write_text(json.dumps(report, indent=2) + '\n')
PY
