#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Regenerate GLiDE attention PTX with NVRTC 12.8 (SM80+ portable image)."""

import argparse
import ctypes as c
import hashlib
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument(
    "--kernel",
    choices=["glide_attention", "glide_delta", "glide_activation"],
    default="glide_attention",
)
p.add_argument("--nvrtc", default="libnvrtc.so.12")
p.add_argument(
    "--include",
    action="append",
    required=True,
    help="CUDA 12.8 include directory (repeat for crt headers)",
)
a = p.parse_args()
root = Path(__file__).resolve().parents[2] / "src/ops/cuda"
source = root / f"prototypes/{a.kernel}.cu"
lib = c.CDLL(a.nvrtc)
program = c.c_void_p()


def check(code):
    if code:
        size = c.c_size_t()
        lib.nvrtcGetProgramLogSize(program, c.byref(size))
        log = c.create_string_buffer(size.value)
        lib.nvrtcGetProgramLog(program, log)
        raise RuntimeError(f"NVRTC {code}: {log.value.decode()}")


major, minor = c.c_int(), c.c_int()
check(lib.nvrtcVersion(c.byref(major), c.byref(minor)))
assert (major.value, minor.value) == (12, 8), "use NVRTC 12.8"
check(
    lib.nvrtcCreateProgram(
        c.byref(program), source.read_bytes(), f"{a.kernel}.cu".encode(), 0, None, None
    )
)
try:
    flags = [b"--gpu-architecture=compute_80", b"--std=c++17"] + [
        f"--include-path={path}".encode() for path in a.include
    ]
    options = (c.c_char_p * len(flags))(*flags)
    check(lib.nvrtcCompileProgram(program, len(flags), options))
    size = c.c_size_t()
    check(lib.nvrtcGetPTXSize(program, c.byref(size)))
    out = c.create_string_buffer(size.value)
    check(lib.nvrtcGetPTX(program, out))
    (root / f"{a.kernel}.ptx").write_bytes(
        b"// source-sha256: "
        + hashlib.sha256(source.read_bytes()).hexdigest().encode()
        + b"\n"
        + out.value.rstrip()
        + b"\n"
    )
finally:
    lib.nvrtcDestroyProgram(c.byref(program))
