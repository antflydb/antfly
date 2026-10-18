#!/usr/bin/env python3
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

"""Regenerate the portable NV12 kernel with NVRTC 11.8 / PTX 7.8.

No CUDA device required. Newer toolkit PTX can exceed deployment driver support;
use the pinned ABI library, keeping runtime video free of compiler dependencies.
"""

import ctypes as c
import hashlib
from pathlib import Path

root = Path(__file__).resolve().parents[2]
source = root / "src/ops/cuda/video_preprocess.cu"
out = source.with_suffix(".ptx")
lib = c.CDLL("libnvrtc.so.11.2")
program = c.c_void_p()


def check(status):
    if status:
        size = c.c_size_t()
        lib.nvrtcGetProgramLogSize(program, c.byref(size))
        log = c.create_string_buffer(size.value)
        lib.nvrtcGetProgramLog(program, log)
        raise RuntimeError(f"NVRTC {status}: {log.value.decode()}")


major, minor = c.c_int(), c.c_int()
check(lib.nvrtcVersion(c.byref(major), c.byref(minor)))
assert (major.value, minor.value) == (11, 8), "use NVRTC 11.8 for portable PTX"
check(
    lib.nvrtcCreateProgram(
        c.byref(program), source.read_bytes(), b"video_preprocess.cu", 0, None, None
    )
)
try:
    options = (c.c_char_p * 1)(b"--gpu-architecture=compute_75")
    check(lib.nvrtcCompileProgram(program, len(options), options))
    size = c.c_size_t()
    check(lib.nvrtcGetPTXSize(program, c.byref(size)))
    ptx = c.create_string_buffer(size.value)
    check(lib.nvrtcGetPTX(program, ptx))
    out.write_bytes(
        b"// source-sha256: "
        + hashlib.sha256(source.read_bytes()).hexdigest().encode()
        + b"\n"
        + ptx.value.rstrip(b"\n")
        + b"\n"
    )
finally:
    lib.nvrtcDestroyProgram(c.byref(program))
