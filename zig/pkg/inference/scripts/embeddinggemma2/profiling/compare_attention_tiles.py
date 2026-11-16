# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import argparse
import ctypes as C, json
from pathlib import Path
import numpy as np, ml_dtypes

parser = argparse.ArgumentParser(
    description="Alternate original and tile-64 BF16 attention kernels; CUDA event timings and vector parity"
)
parser.add_argument("--cubin", type=Path, required=True)
parser.add_argument("--out", type=Path, required=True)
parser.add_argument("--lengths", type=int, nargs="+", default=[512, 2048, 8192])
parser.add_argument("--repeats", type=int, default=12)
options = parser.parse_args()
if options.repeats < 2 or any(n < 64 or n > 8192 or n % 64 for n in options.lengths):
    parser.error("Use 64..8192 rows divisible by 64 and at least two repetitions")
lib = C.CDLL("libcuda.so.1")
P = C.c_void_p
U = C.c_uint64


# Explicit ABI declarations keep 64-bit addresses intact.
def fn(name, args):
    f = getattr(lib, name)
    f.argtypes = args
    f.restype = C.c_int
    return f


def ok(rc):
    if rc:
        raise RuntimeError("CUDA " + str(rc))


init = fn("cuInit", [C.c_uint])
retain = fn("cuDevicePrimaryCtxRetain", [C.POINTER(P), C.c_int])
current = fn("cuCtxSetCurrent", [P])
load = fn("cuModuleLoad", [C.POINTER(P), C.c_char_p])
get = fn("cuModuleGetFunction", [C.POINTER(P), P, C.c_char_p])
alloc = fn("cuMemAlloc_v2", [C.POINTER(U), C.c_size_t])
free = fn("cuMemFree_v2", [U])
h2d = fn("cuMemcpyHtoD_v2", [U, P, C.c_size_t])
d2h = fn("cuMemcpyDtoH_v2", [P, U, C.c_size_t])
launch = fn(
    "cuLaunchKernel",
    [
        P,
        C.c_uint,
        C.c_uint,
        C.c_uint,
        C.c_uint,
        C.c_uint,
        C.c_uint,
        C.c_uint,
        P,
        C.POINTER(P),
        P,
    ],
)
event = fn("cuEventCreate", [C.POINTER(P), C.c_uint])
record = fn("cuEventRecord", [P, P])
sync = fn("cuEventSynchronize", [P])
elapsed = fn("cuEventElapsedTime", [C.POINTER(C.c_float), P, P])
destroy = fn("cuEventDestroy_v2", [P])
synchronize = fn("cuCtxSynchronize", [])
ok(init(0))
ctx = P()
ok(retain(C.byref(ctx), 0))
ok(current(ctx))
mod = P()
ok(load(C.byref(mod), str(options.cubin.resolve()).encode()))
kernels = {}
for name in [
    "attention64_256",
    "attention64_512",
    "termite_embedding_gemma2_attention_local_flash_bf16",
]:
    f = P()
    ok(get(C.byref(f), mod, name.encode()))
    kernels[name] = f
rng = np.random.default_rng(33)
results = []
for n in options.lengths:
    for dim, kh, radius in [(256, 2, 512), (512, 1, 0)]:
        for mask_holes in [False, True] if n == 512 else [False]:
            q = (rng.standard_normal((n, 4, dim)) * dim ** (-0.25)).astype(
                ml_dtypes.bfloat16
            )
            k = (rng.standard_normal((n, kh, dim)) * dim ** (-0.25)).astype(
                ml_dtypes.bfloat16
            )
            v = rng.standard_normal((n, kh, dim)).astype(ml_dtypes.bfloat16)
            mask = np.ones(n, dtype=np.int64)
            if mask_holes:
                mask[::7] = 0
            ptr = []
            for arr in [np.zeros_like(q), q, k, v, mask]:
                p = U()
                ok(alloc(C.byref(p), arr.nbytes))
                ok(h2d(p, P(arr.ctypes.data), arr.nbytes))
                ptr.append(p)
            vals = ptr + [C.c_uint(x) for x in [1, n, 4, kh, dim, radius]]
            args = (P * len(vals))(*[C.cast(C.byref(x), P) for x in vals])
            vectors = {}
            times = {}
            prepared = {}
            for candidate in [False, True]:
                name = (
                    f"attention64_{dim}"
                    if candidate
                    else "termite_embedding_gemma2_attention_local_flash_bf16"
                )
                tile = (32 if dim == 256 else 64) if candidate else 64
                shared = (
                    64 * (dim + 8) * 2 + tile * 72 * 2 + 7 * tile * 4
                    if candidate
                    else 16 * dim * 2 + 4 * 64 * 16 * 4 + 64 * 16 * 2 + 3 * 64 * 4
                )
                if shared > 48 * 1024:
                    ok(
                        fn("cuFuncSetAttribute", [P, C.c_int, C.c_int])(
                            kernels[name], 8, shared
                        )
                    )
                prepared[candidate] = (kernels[name], tile, shared)
                times[candidate] = []

            def run(candidate):
                function, tile, shared = prepared[candidate]
                ok(
                    launch(
                        function, 4, n // tile, 1, 512, 1, 1, shared, None, args, None
                    )
                )

            for _ in range(3):
                run(False)
                run(True)
            ok(synchronize())
            a = P()
            b = P()
            ok(event(C.byref(a), 0))
            ok(event(C.byref(b), 0))
            for repeat in range(options.repeats):
                for candidate in [False, True] if repeat % 2 == 0 else [True, False]:
                    ok(record(a, None))
                    run(candidate)
                    ok(record(b, None))
                    ok(sync(b))
                    ms = C.c_float()
                    ok(elapsed(C.byref(ms), a, b))
                    times[candidate].append(ms.value)
                    if repeat == options.repeats - 1:
                        out = np.empty_like(q)
                        ok(d2h(P(out.ctypes.data), ptr[0], out.nbytes))
                        vectors[candidate] = out.astype(np.float32)
            ok(destroy(a))
            ok(destroy(b))
            times = {key: float(np.median(value)) for key, value in times.items()}
            a = vectors[False].reshape(-1).astype(np.float64)
            b = vectors[True].reshape(-1).astype(np.float64)
            cos = float(np.dot(a, b) / np.sqrt(np.dot(a, a) * np.dot(b, b)))
            maxabs = float(np.max(np.abs(a - b)))
            assert np.isfinite(b).all()
            assert cos > 0.9999, (n, dim, cos, maxabs)
            row = {
                "sequence": n,
                "dim": dim,
                "radius": radius,
                "mask_holes": mask_holes,
                "old_ms": times[False],
                "new_ms": times[True],
                "speedup": times[False] / times[True],
                "cosine": cos,
                "max_abs": maxabs,
            }
            results.append(row)
            print(json.dumps(row), flush=True)
            for p in ptr:
                ok(free(p))
options.out.write_text(json.dumps(results, indent=2))
