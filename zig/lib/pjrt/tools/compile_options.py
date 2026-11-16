# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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
