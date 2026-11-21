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

"""Pinned offline compiler and resource contracts from the native catalog."""
import hashlib
import json
from pathlib import Path
import re

CATALOG = Path(__file__).resolve().parents[1] / "generated/standalone_programs.json"
LIBDEVICE_SHA256 = "91334d6e12748f6cb5bbf0a1cd965a56bcd93dc4f496d2e5c5f8c6e523094356"


def profiles(family):
    rows = [p for p in json.loads(CATALOG.read_text()) if p["family"].startswith(family)]
    if not rows:
        raise ValueError("Unknown native program family: " + family)
    return rows


def compile_program(fn, signature, constants, aligned, spec, options=None):
    import triton
    from triton.compiler import ASTSource
    from triton.backends.compiler import GPUTarget

    if triton.__version__ != "3.7.1":
        raise RuntimeError("Regeneration requires Triton 3.7.1")
    triton.knobs.compilation.disable_line_info = True
    opts = dict(options or {})
    opts.update(num_warps=spec["threads"] // 32, ptx_version=90)
    compiled = triton.compile(
        ASTSource(fn, signature, constexprs=constants,
                  attrs={(i,): [["tt.divisibility", 16]] for i in aligned}),
        target=GPUTarget("cuda", spec["target"], 32), options=opts)
    meta = compiled.metadata
    if (meta.global_scratch_size != 0 or meta.profile_scratch_size != 0
            or meta.num_ctas != 1 or meta.shared != spec["shared"]
            or meta.num_warps * 32 != spec["threads"]):
        raise RuntimeError("Native program resource contract changed: " + spec["file"])
    data = compiled.asm["ptx"].encode()
    if not re.search(rb"\.visible\s+\.entry\s+" + re.escape(spec["entry"].encode()) + rb"\s*\(", data):
        raise RuntimeError("Native program export contract changed: " + spec["file"])
    return data


def emit(output, spec, data):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    (output / spec["file"]).write_bytes(data)
    return dict(file=spec["file"], sha256=hashlib.sha256(data).hexdigest(),
                shared=spec["shared"], threads=spec["threads"])


def write_manifest(output, records, **metadata):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    value = dict(kernels=records, **metadata) if metadata else records
    (output / "manifest.json").write_text(json.dumps(value, indent=2) + "\n")
