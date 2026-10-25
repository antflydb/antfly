#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Reproduce GLiNER25 strict dot references with pinned PyTorch and F64.

Run in a torch==2.9.1+cu128, NumPy environment on the target GPU. The independent
ctypes cuBLAS invocation uses the retained physical operand layouts directly;
compare each library version with --library. Output includes every forward and
seeded-gradient array, F64 error, hashes and the actual device/library identity.
No application binary or captured application output enters this oracle.
"""
from pathlib import Path
import ctypes as C
import json, hashlib, os, numpy as np, torch
import argparse
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--library', action='append', help='cuBLAS shared library to compare; repeat for multiple versions')
args = parser.parse_args()
if torch.__version__ != '2.9.1+cu128':
    raise RuntimeError('reference requires pinned torch 2.9.1+cu128')
p = args.output
p.mkdir(parents=True, exist_ok=True)
torch.backends.cuda.matmul.allow_tf32 = False
torch.set_float32_matmul_precision('highest')
meta = dict(torch=torch.__version__, cuda=torch.version.cuda, device=torch.cuda.get_device_name(), capability=torch.cuda.get_device_capability(), tf32=torch.backends.cuda.matmul.allow_tf32)

def values(n, m, mod, offset, denom):
    return ((np.arange(n, dtype=np.int64) * m % mod - offset).astype(np.float32) / np.float32(denom)).copy()
for (label, cases) in [('k32', [(2, 7, 32, 11)]), ('head', [(2, 7, 128, 11), (2, 7, 128, 10), (2, 11, 128, 59), (1, 3, 64, 22), (2, 192, 128, 7)])]:
    h = hashlib.sha256()
    rows = []
    for (bi, (b, m, k, n)) in enumerate(cases):
        for lt in [False, True]:
            for rt in [False, True]:
                xn = values(b * m * k, 17, 97, 48, 29).reshape(b, k if lt else m, m if lt else k)
                wn = values(b * k * n, 13, 101, 50, 31).reshape(b, n if rt else k, k if rt else n)
                sn = values(b * m * n, 19, 89, 44, 37).reshape(b, m, n)
                x = torch.tensor(xn, device='cuda', requires_grad=True)
                w = torch.tensor(wn, device='cuda', requires_grad=True)
                seed = torch.tensor(sn, device='cuda')
                y = torch.bmm(x.transpose(1, 2) if lt else x, w.transpose(1, 2) if rt else w)
                y.backward(seed)
                xd = torch.tensor(xn, dtype=torch.float64, device='cpu', requires_grad=True)
                wd = torch.tensor(wn, dtype=torch.float64, device='cpu', requires_grad=True)
                yd = torch.bmm(xd.transpose(1, 2) if lt else xd, wd.transpose(1, 2) if rt else wd)
                yd.backward(torch.tensor(sn, dtype=torch.float64))
                name = f'{label}-{bi}-{int(lt)}-{int(rt)}'
                errors = []
                for (op, z, ref) in zip(['y', 'dx', 'dw'], [y, x.grad, w.grad], [yd, xd.grad, wd.grad]):
                    data = z.detach().cpu().contiguous().numpy()
                    raw = data.tobytes()
                    h.update(raw)
                    (p / f'{name}-{op}.f32').write_bytes(raw)
                    r = ref.detach().numpy()
                    errors.append(dict(op=op, max_abs=float(np.max(np.abs(data.astype(np.float64) - r))), count=data.size))
                rows.append(dict(case=[b, m, k, n], lt=lt, rt=rt, errors=errors))
    meta[label] = dict(sha256=h.hexdigest(), cases=rows)
(p / 'matmul.json').write_text(json.dumps(meta, indent=2) + '\n')
vp = C.c_void_p
ci = C.c_int
ll = C.c_longlong
fp = C.POINTER(C.c_float)
paths = args.library or [str(Path(torch.__file__).parent.parent / 'nvidia/cublas/lib/libcublas.so.12')]
rows = []
for path in paths:
    lib = C.CDLL(path)
    handle = vp()
    assert lib.cublasCreate_v2(C.byref(handle)) == 0
    assert lib.cublasSetMathMode(handle, 0) == 0
    version = ci()
    assert lib.cublasGetVersion_v2(handle, C.byref(version)) == 0
    assert lib.cublasSetStream_v2(handle, vp(torch.cuda.current_stream().cuda_stream)) == 0
    f = lib.cublasSgemmStridedBatched
    f.argtypes = [vp, ci, ci, ci, ci, ci, fp, vp, ci, ll, vp, ci, ll, fp, vp, ci, ll, ci]
    g = lib.cublasSgemm_v2
    g.argtypes = [vp, ci, ci, ci, ci, ci, fp, vp, ci, vp, ci, fp, vp, ci]

    def bmm(x, w, xt=False, wt=False, gemm_one=False):
        B = x.shape[0]
        M = x.shape[2] if xt else x.shape[1]
        K = x.shape[1] if xt else x.shape[2]
        N = w.shape[1] if wt else w.shape[2]
        out = torch.empty((B, M, N), device='cuda')
        alpha = C.c_float(1)
        beta = C.c_float(0)
        if B == 1 and gemm_one:
            r = g(handle, wt, xt, N, M, K, C.byref(alpha), vp(w.data_ptr()), K if wt else N, vp(x.data_ptr()), M if xt else K, C.byref(beta), vp(out.data_ptr()), N)
        else:
            r = f(handle, wt, xt, N, M, K, C.byref(alpha), vp(w.data_ptr()), K if wt else N, N * K, vp(x.data_ptr()), M if xt else K, M * K, C.byref(beta), vp(out.data_ptr()), N, M * N, B)
        assert r == 0, r
        return out
    for one in [False, True]:
        for direct_dw in [False, True]:
            h = hashlib.sha256()
            errors = []
            for (i, (B, M, K, N)) in enumerate([(2, 7, 128, 11), (2, 7, 128, 10), (2, 11, 128, 59), (1, 3, 64, 22), (2, 192, 128, 7)]):
                for lt in [False, True]:
                    for rt in [False, True]:
                        x = torch.tensor(values(B * M * K, 17, 97, 48, 29).reshape(B, K if lt else M, M if lt else K), device='cuda')
                        w = torch.tensor(values(B * K * N, 13, 101, 50, 31).reshape(B, N if rt else K, K if rt else N), device='cuda')
                        seed = torch.tensor(values(B * M * N, 19, 89, 44, 37).reshape(B, M, N), device='cuda')
                        y = bmm(x, w, lt, rt, one)
                        dx = bmm(seed, w, False, not rt, one)
                        dx = dx.transpose(1, 2).contiguous() if lt else dx
                        if rt and direct_dw:
                            dw = bmm(seed, x, True, lt, one)
                        else:
                            dw = bmm(x, seed, not lt, False, one)
                            dw = dw.transpose(1, 2).contiguous() if rt else dw
                        for (op, z) in zip(['y', 'dx', 'dw'], [y, dx, dw]):
                            data = z.cpu().numpy()
                            h.update(data.tobytes())
                            ref = np.fromfile(p / f'head-{i}-{int(lt)}-{int(rt)}-{op}.f32', np.float32).reshape(data.shape)
                            if not np.array_equal(data, ref):
                                errors.append(dict(case=[B, M, K, N], lt=lt, rt=rt, op=op, max_abs=float(np.max(np.abs(data - ref)))))
            rows.append(dict(library=path, library_sha256=hashlib.sha256(Path(path).read_bytes()).hexdigest(), version=version.value, gemm_one=one, direct_dw=direct_dw, sha256=h.hexdigest(), different=errors))
    assert lib.cublasDestroy_v2(handle) == 0
(p / 'cublas.json').write_text(json.dumps(rows, indent=2) + '\n')
print(json.dumps([{k: v for (k, v) in row.items() if k != 'different'} | dict(different=len(row['different'])) for row in rows], indent=2), flush=True)
