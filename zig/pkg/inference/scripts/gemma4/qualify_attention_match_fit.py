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

"""Independent FP64 oracle checks for the diagnostic CUDA compaction fitter.

Requires NumPy/SciPy for qualification only; neither is a runtime dependency.
This exercises constrained fitting, not model serving or release latency.
"""

import argparse
import ctypes as C, hashlib, json, sys, time
from pathlib import Path
import numpy as np
from scipy.optimize import nnls

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--library", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
library = args.library.resolve()
lib = C.CDLL(str(library))
D = C.POINTER(C.c_double)
U = C.POINTER(C.c_size_t)
Callback = C.CFUNCTYPE(C.c_int, C.c_void_p, D, C.c_size_t, C.c_size_t, D, D)
failures = []
calls = 0


@Callback
def fallback(context, ap, q, m, wp, rp):
    global calls
    try:
        a = np.ctypeslib.as_array(ap, shape=(q * m,)).reshape(q, m)
        w, _ = nnls(a, np.ones(q), maxiter=10000)
        np.ctypeslib.as_array(wp, shape=(m,))[:] = w
        np.ctypeslib.as_array(rp, shape=(q,))[:] = 1 - a @ w
        calls += 1
        return 0
    except Exception as e:
        failures.append(repr(e))
        return 1


lib.am_select.argtypes = [
    C.c_size_t,
    C.c_size_t,
    C.c_size_t,
    D,
    D,
    U,
    D,
    C.c_double,
    C.c_void_p,
    Callback,
]
lib.am_select.restype = C.c_int
cases = []
for seed in range(8):
    rng = np.random.default_rng(seed)
    q, n, m, d = 65, 96, 32, 8
    logits = rng.normal(size=(q, d)) @ rng.normal(size=(d, n)) / np.sqrt(d)
    p = np.exp(logits - logits.max(axis=1, keepdims=True))
    p /= p.sum(axis=1, keepdims=True)
    cases.append((f"random-{seed}", p, m))
cases.extend(
    [
        ("duplicates", np.full((8, 12), 1 / 12), 6),
        ("underdetermined", np.tile(np.eye(4), (1, 4)) / 4, 8),
        ("zero_columns", np.column_stack((np.full((8, 4), 0.25), np.zeros((8, 8)))), 6),
    ]
)

start = time.monotonic()
results = []
for name, p, m in cases:
    p = np.ascontiguousarray(p, dtype=np.float64)
    q, n = p.shape
    norms = np.sum(p * p, axis=0)
    indices = np.zeros(m, dtype=np.uintp)
    weights = np.zeros(m, dtype=np.float64)
    status = lib.am_select(
        q,
        n,
        m,
        p.ctypes.data_as(D),
        norms.ctypes.data_as(D),
        indices.ctypes.data_as(U),
        weights.ctypes.data_as(D),
        1e-8,
        None,
        fallback,
    )
    assert status == 0, (name, status, failures)
    assert (
        len(set(indices)) == m
        and np.all(indices < n)
        and np.isfinite(weights).all()
        and np.all(weights >= 0)
    )
    a = p[:, indices]
    residual = 1 - a @ weights
    grad = a.T @ residual
    norm = np.linalg.norm(a, axis=0)
    violation = np.max(
        np.where(weights > 0, np.abs(grad), np.maximum(0, grad))
        / np.maximum(norm, 1e-300)
    )
    oracle, _ = nnls(a, np.ones(q), maxiter=10000)
    gap = float(np.linalg.norm(residual) - np.linalg.norm(1 - a @ oracle))
    assert violation <= 1.01e-8 * np.sqrt(q), (name, violation)
    assert abs(gap) < 1e-7, (name, gap)
    row = {
        "case": name,
        "kkt_violation": float(violation),
        "objective_gap": gap,
        "zero_weights": int(np.count_nonzero(weights == 0)),
    }
    results.append(row)
    print(json.dumps(row), flush=True)
# Cancellation is checked during selection, not just at the API boundary.
Check = C.CFUNCTYPE(C.c_int, C.c_void_p)
checks = 0


@Check
def cancel(context):
    global checks
    checks += 1
    return 77 if checks >= 4 else 0


lib.am_select_v2.argtypes = lib.am_select.argtypes + [C.c_size_t, Check]
lib.am_select_v2.restype = C.c_int
p = np.ascontiguousarray(cases[0][1], dtype=np.float64)
q, n = p.shape
m = cases[0][2]
norms = np.sum(p * p, axis=0)
indices = np.zeros(m, dtype=np.uintp)
weights = np.zeros(m, dtype=np.float64)
status = lib.am_select_v2(
    q,
    n,
    m,
    p.ctypes.data_as(D),
    norms.ctypes.data_as(D),
    indices.ctypes.data_as(U),
    weights.ctypes.data_as(D),
    1e-8,
    None,
    fallback,
    4096,
    cancel,
)
assert status == 77 and checks == 4, (status, checks)


@Check
def proceed(context):
    return 0


status = lib.am_select_v2(
    q,
    n,
    m,
    p.ctypes.data_as(D),
    norms.ctypes.data_as(D),
    indices.ctypes.data_as(U),
    weights.ctypes.data_as(D),
    1e-8,
    None,
    fallback,
    1,
    proceed,
)
assert status == 8, status
status = lib.am_select_v2(
    q,
    n,
    m,
    p.ctypes.data_as(D),
    norms.ctypes.data_as(D),
    indices.ctypes.data_as(U),
    weights.ctypes.data_as(D),
    1e-8,
    None,
    fallback,
    0,
    proceed,
)
assert status == 1, status
report = {
    "passed": True,
    "scope": "numerical GPU selection and NNLS checks only",
    "library": str(library),
    "library_sha256": hashlib.file_digest(library.open("rb"), "sha256").hexdigest(),
    "cases": results,
    "fallback_calls": calls,
    "cancellation_checks": checks,
    "work_limit_checked": True,
    "elapsed_s": time.monotonic() - start,
    "serving_qualified": False,
}
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report), flush=True)
