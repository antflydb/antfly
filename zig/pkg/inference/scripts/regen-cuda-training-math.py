#!/usr/bin/env python3
"""Regenerate the isolated CUDA 12.8 training math profile (no GPU needed)."""
import argparse
import ctypes as c
import hashlib
import json
import os
from pathlib import Path
import tempfile

ARTIFACTS = Path(__file__).resolve().parents[1] / "src/ops/cuda/artifacts"
OPTIONS = (b"--gpu-architecture=compute_75", b"--std=c++17", b"--fmad=true",
           b"--ftz=false", b"--prec-div=true", b"--prec-sqrt=true")
COMPILER = b"Cuda compilation tools, release 12.8, V12.8.93"


def compile_ptx(library, source):
    nvrtc = c.CDLL(str(library))

    def check(code):
        if code:
            raise RuntimeError(f"NVRTC error {code}")

    major, minor = c.c_int(), c.c_int()
    check(nvrtc.nvrtcVersion(c.byref(major), c.byref(minor)))
    if (major.value, minor.value) != (12, 8):
        raise RuntimeError("training math requires NVRTC 12.8")
    program = c.c_void_p()
    check(nvrtc.nvrtcCreateProgram(c.byref(program), source, b"gliner25_training_math.cu", 0, None, None))
    try:
        options = (c.c_char_p * len(OPTIONS))(*OPTIONS)
        code = nvrtc.nvrtcCompileProgram(program, len(options), options)
        size = c.c_size_t()
        check(nvrtc.nvrtcGetProgramLogSize(program, c.byref(size)))
        log = c.create_string_buffer(size.value)
        check(nvrtc.nvrtcGetProgramLog(program, log))
        if code:
            raise RuntimeError(f"NVRTC compilation failed ({code}): {log.value.decode()}")
        check(nvrtc.nvrtcGetPTXSize(program, c.byref(size)))
        output = c.create_string_buffer(size.value)
        check(nvrtc.nvrtcGetPTX(program, output))
        ptx = output.raw.rstrip(b"\0")
        if COMPILER not in ptx or b".version 8.7" not in ptx or b".target sm_75" not in ptx:
            raise RuntimeError("expected NVRTC 12.8.93, PTX 8.7, compute_75")
        return ptx
    finally:
        check(nvrtc.nvrtcDestroyProgram(c.byref(program)))


def source_bytes():
    source = (ARTIFACTS / "gliner25_training_math.cu").read_bytes()
    marker = b'#include "../kernels/gliner25_softmax.cuh"'
    if source.count(marker) != 1:
        raise RuntimeError("expected exactly one shared softmax include")
    return source.replace(marker, (ARTIFACTS.parent / "kernels/gliner25_softmax.cuh").read_bytes())


def artifacts(source, ptx):
    metadata = {
        "profile": "gliner25_training_norm_cuda128_v15",
        "compiler": COMPILER.decode(),
        "options": [option.decode() for option in OPTIONS],
        "source_sha256": hashlib.sha256(source).hexdigest(),
        "ptx_sha256": hashlib.sha256(ptx).hexdigest(),
    }
    return {"gliner25_training_math.ptx": ptx,
            "gliner25_training_math.json": (json.dumps(metadata, indent=2, sort_keys=True) + "\n").encode()}


def publish(directory, outputs, write):
    # Compile and validate everything before touching any artifact. Each file
    # is replaced atomically; --check detects a partial/interrupted pair.
    for name, content in outputs.items():
        destination = directory / name
        if not write:
            if not destination.exists() or destination.read_bytes() != content:
                raise RuntimeError(f"stale CUDA training math artifact: {destination}")
            continue
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=directory, prefix=".training-math-", delete=False) as f:
                temporary = Path(f.name)
                f.write(content)
            temporary.chmod(0o644)
            temporary.replace(destination)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--check", action="store_true")
    parser.add_argument("--nvrtc", default=os.environ.get("ANTFLY_CUDA128_NVRTC", "libnvrtc.so.12"))
    args = parser.parse_args()
    source = source_bytes()
    outputs = artifacts(source, compile_ptx(args.nvrtc, source))
    publish(ARTIFACTS, outputs, args.write)
    print("CUDA 12.8 training math artifacts " + ("written" if args.write else "match"))


if __name__ == "__main__":
    main()
