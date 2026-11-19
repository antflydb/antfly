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

"""Retain explicit JAX-compatible compiler options beside a native artifact."""

import argparse
import json
from pathlib import Path
from jax._src.compiler import get_compile_options

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--manifest", type=Path, required=True)
p.add_argument("--cuda-data-dir", type=Path, required=True)
a = p.parse_args()
options = get_compile_options(1, 1, backend=None)
options.executable_build_options.debug_options.xla_gpu_cuda_data_dir = str(
    a.cuda_data_dir.resolve()
)
(a.manifest.parent / "compile-options.pb").write_bytes(options.SerializeAsString())
manifest = json.loads(a.manifest.read_text())
manifest["compile_options"] = "compile-options.pb"
a.manifest.write_text(json.dumps(manifest, indent=2) + "\n")
