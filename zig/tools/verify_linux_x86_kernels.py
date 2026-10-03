#!/usr/bin/env python3
"""Build and test baseline Linux kernels plus the isolated AVX2 object.

No model downloads or full inference build required. Optional QEMU coverage
executes the baseline binary with AVX disabled and checks forced-AVX rejection.
"""

import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


def run(argv, *, env=None, success=True):
    result = subprocess.run(
        [str(x) for x in argv],
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=600,
    )
    if success and result.returncode:
        raise RuntimeError(f"{argv}:\n{result.stdout}")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", default="zig")
    parser.add_argument("--require-qemu", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    source = root / "lib/linalg/src"
    qemu = shutil.which("qemu-x86_64")
    if args.require_qemu and not qemu:
        parser.error("--require-qemu needs qemu-x86_64")
    with tempfile.TemporaryDirectory(prefix="antfly-x86-kernels-") as tmp:
        out = Path(tmp)
        cache = ["--cache-dir", out / "cache", "--global-cache-dir", out / "global"]
        for abi in ("gnu", "musl"):
            target = f"x86_64-linux-{abi}"
            obj = out / f"avx2-{abi}.o"
            assembly = out / f"avx2-{abi}.s"
            run(
                [
                    args.zig,
                    "build-obj",
                    source / "x86_avx2.zig",
                    "-target",
                    target,
                    "-mcpu=baseline+avx+avx2+fma+f16c",
                    "-O",
                    "ReleaseFast",
                    "-fPIC",
                    f"-femit-bin={obj}",
                    f"-femit-asm={assembly}",
                    *cache,
                ]
            )
            if not re.search(r"\bvfmadd\w*ps\b", assembly.read_text()):
                raise RuntimeError("accelerated object lacks packed FMA")
            portable_asm = out / f"portable-{abi}.s"
            run(
                [
                    args.zig,
                    "build-obj",
                    source / "x86_avx2.zig",
                    "-target",
                    target,
                    "-mcpu=baseline",
                    "-O",
                    "ReleaseFast",
                    "-fPIC",
                    f"-femit-bin={out / ('portable-' + abi + '.o')}",
                    f"-femit-asm={portable_asm}",
                    *cache,
                ]
            )
            if re.search(r"\bcall\w*\s+[^\n]*\bfmaf?\b", portable_asm.read_text()):
                raise RuntimeError("portable GEMM contains a software FMA call")
            # The AVX2 symbols belong to the supplied object, not libc. Keep
            # this link-and-run check libc-free to catch accidental library tags.
            no_libc = out / f"test-{abi}-no-libc"
            run(
                [
                    args.zig,
                    "test",
                    source / "mod.zig",
                    obj,
                    "-target",
                    target,
                    "-mcpu=baseline",
                    "-O",
                    "ReleaseSafe",
                    "--test-no-exec",
                    f"-femit-bin={no_libc}",
                    *cache,
                ]
            )
            result = run([no_libc])
            print(f"{abi}/no-libc: {result.stdout.strip().splitlines()[-1]}")
            if qemu:
                result = run([qemu, "-cpu", "qemu64", no_libc])
                print(f"{abi}/no-libc/no-AVX: {result.stdout.strip().splitlines()[-1]}")
            binary = out / f"test-{abi}"
            run(
                [
                    args.zig,
                    "test",
                    source / "mod.zig",
                    obj,
                    "-lc",
                    "-target",
                    target,
                    "-mcpu=baseline",
                    "-O",
                    "ReleaseSafe",
                    "--test-no-exec",
                    f"-femit-bin={binary}",
                    *cache,
                ]
            )
            for kernel in ("auto", "portable"):
                env = dict(
                    os.environ,
                    ANTFLY_INFERENCE_X86_KERNEL=kernel,
                    ANTFLY_INFERENCE_CPU_THREADS="2",
                )
                result = run([binary], env=env)
                print(f"{abi}/{kernel}: {result.stdout.strip().splitlines()[-1]}")
            if qemu and abi == "musl":
                env = dict(os.environ, ANTFLY_INFERENCE_X86_KERNEL="auto")
                result = run([qemu, "-cpu", "qemu64", binary], env=env)
                print(f"musl/no-AVX: {result.stdout.strip().splitlines()[-1]}")
                env["ANTFLY_INFERENCE_X86_KERNEL"] = "avx2"
                result = run([qemu, "-cpu", "qemu64", binary], env=env, success=False)
                if (
                    result.returncode == 0
                    or "UnsupportedX86Kernel" not in result.stdout
                ):
                    raise RuntimeError(
                        "forcing AVX2 on an unsupported CPU did not reject safely"
                    )
        if not qemu:
            print("QEMU unavailable: no-AVX execution coverage skipped")


if __name__ == "__main__":
    main()
