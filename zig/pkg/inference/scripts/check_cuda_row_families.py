#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Compare frozen original and candidate native row kernels on CUDA hardware.

No Python GPU package: uses the CUDA driver. Baseline/candidate directories
contain kernels.cubin and lighton.ptx built with identical toolchains/flags.
JSONL retains all paired samples. --bench measures direct stream events and
32-call graph replay (AA/AB/BA), with median <=1.05 and p95 <=1.10 declared here.
Shared-GPU controls must pass before interpreting candidate tail measurements.
"""

import argparse
import array
import ctypes as C
import json
import math
import struct
from pathlib import Path


def emit(**kw):
    print(json.dumps(kw), flush=True)


class Device:
    def __init__(self):
        self.lib = C.CDLL("libcuda.so.1")
        self.call("cuInit", C.c_uint(0))
        self.context = C.c_void_p()
        self.call("cuDevicePrimaryCtxRetain", C.byref(self.context), C.c_int(0))
        self.call("cuCtxSetCurrent", self.context)
        self.allocations = []
        self.stream = C.c_void_p()
        self.call("cuStreamCreate", C.byref(self.stream), C.c_uint(0))

    def call(self, name, *args):
        code = getattr(self.lib, name)(*args)
        if code:
            raise RuntimeError(f"{name}: CUDA error {code}")

    def module(self, path):
        m = C.c_void_p()
        self.call("cuModuleLoad", C.byref(m), C.c_char_p(str(path).encode()))
        return m

    def alloc(self, data):
        ptr = C.c_uint64()
        self.call("cuMemAlloc_v2", C.byref(ptr), C.c_size_t(len(data)))
        self.allocations.append(ptr)
        self.upload(ptr, data)
        return ptr

    def upload(self, ptr, data):
        self.call("cuMemcpyHtoD_v2", ptr, C.c_char_p(data), C.c_size_t(len(data)))

    def download(self, ptr, size):
        host = C.create_string_buffer(size)
        self.call("cuMemcpyDtoH_v2", host, ptr, C.c_size_t(size))
        return host.raw

    def function(self, module, name):
        f = C.c_void_p()
        self.call("cuModuleGetFunction", C.byref(f), module, C.c_char_p(name.encode()))
        return f

    def launch(self, fn, args, grid, threads=256, shared_bytes=0):
        gx, gy = (grid, 1) if isinstance(grid, int) else grid
        argv = (C.c_void_p * len(args))(*(C.addressof(a) for a in args))
        self.call(
            "cuLaunchKernel",
            fn,
            C.c_uint(gx),
            C.c_uint(gy),
            C.c_uint(1),
            C.c_uint(threads),
            C.c_uint(1),
            C.c_uint(1),
            C.c_uint(shared_bytes),
            self.stream,
            argv,
            None,
        )

    def synchronize(self):
        self.call("cuStreamSynchronize", self.stream)

    def event(self):
        e = C.c_void_p()
        self.call("cuEventCreate", C.byref(e), C.c_uint(0))
        return e

    def duration(self, run, start, end):
        self.call("cuEventRecord", start, self.stream)
        run()
        self.call("cuEventRecord", end, self.stream)
        self.call("cuEventSynchronize", end)
        ms = C.c_float()
        self.call("cuEventElapsedTime", C.byref(ms), start, end)
        return ms.value * 1000 / 32

    def graph(self, run):
        self.call("cuStreamBeginCapture", self.stream, C.c_uint(0))
        run()
        graph = C.c_void_p()
        self.call("cuStreamEndCapture", self.stream, C.byref(graph))
        executable = C.c_void_p()
        self.call(
            "cuGraphInstantiateWithFlags", C.byref(executable), graph, C.c_uint64(0)
        )
        return graph, executable

    def free_case(self):
        for ptr in self.allocations:
            self.call("cuMemFree_v2", ptr)
        self.allocations.clear()


def f32(x):
    return C.c_float(x).value


def bf16(x):
    (bits,) = struct.unpack("<I", struct.pack("<f", x))
    return ((bits + 0x7FFF + ((bits >> 16) & 1)) >> 16) & 65535


def from_bf16(x):
    return struct.unpack("<f", struct.pack("<I", x << 16))[0]


def rounded(x):
    return from_bf16(bf16(x))


def packed(values, dtype):
    return array.array(
        "H" if dtype == "bf16" else "f",
        (bf16(x) for x in values) if dtype == "bf16" else values,
    ).tobytes()


def values(n, salt=0):
    return [f32((((i * 37 + salt * 71) % 521) - 260) / 83) for i in range(n)]


def benchmark(d, fn, args, grid, name, threads=256, shared_bytes=0):
    start, end = d.event(), d.event()
    runs = [
        lambda f=f: [d.launch(f, args, grid, threads, shared_bytes) for _ in range(32)]
        for f in fn
    ]
    graphs = [d.graph(run) for run in runs]
    for metric in ("stream", "graph"):
        dispatch = (
            runs
            if metric == "stream"
            else [lambda g=g: d.call("cuGraphLaunch", g[1], d.stream) for g in graphs]
        )
        for order, left, right in [("AA", 0, 0), ("AB", 0, 1), ("BA", 1, 0)]:
            for _ in range(4):
                for side in (left, right):
                    d.duration(dispatch[side], start, end)
            samples = [[], []]
            for i in range(31):
                for j, side in enumerate((left, right)):
                    samples[j].append(d.duration(dispatch[side], start, end))
                emit(
                    kind="sample",
                    case=name,
                    metric=metric,
                    order=order,
                    i=i,
                    left_us=samples[0][-1],
                    right_us=samples[1][-1],
                )
            a, b = map(sorted, samples)
            if order == "BA":
                a, b = b, a
            ratios = {"median_ratio": b[15] / a[15], "p95_ratio": b[29] / a[29]}
            emit(
                kind="timing",
                case=name,
                metric=metric,
                order=order,
                passed=ratios["median_ratio"] <= 1.05 and ratios["p95_ratio"] <= 1.10,
                **ratios,
            )
    for graph, exe in graphs:
        d.call("cuGraphExecDestroy", exe)
        d.call("cuGraphDestroy", graph)
    for e in (start, end):
        d.call("cuEventDestroy_v2", e)


def qualify_case(d, modules, width, rows, kind, run_bench):
    n = rows * width
    dtype = (
        "bf16"
        if kind.startswith("bf16") or kind in ("gelu", "packed_gelu", "vision_layer")
        else "f32"
    )
    inp_dtype = "f32" if kind == "vision_layer" else dtype
    if kind in ("gelu", "packed_gelu"):
        inp_dtype = "bf16"
    stride = width + 13 if kind == "bf16_scale_rms" else width
    x = values(rows * stride)
    w = [f32(0.75 + z / 16) for z in values(width, 1)]
    y = values(n, 2)
    beta = [f32(z / 13) for z in values(width, 3)]
    if inp_dtype == "bf16":
        x, w, y = [list(map(rounded, v)) for v in (x, w, y)]
    input_data = packed(x, inp_dtype)
    input_ptr = d.alloc(input_data)
    weight_ptr = d.alloc(packed(w, inp_dtype))
    other_ptr = d.alloc(packed(y, inp_dtype))
    beta_ptr = d.alloc(packed(beta, "f32"))
    scale_ptr = d.alloc(packed([0.75], inp_dtype))
    size = n * (2 if dtype == "bf16" else 4)
    outputs = [d.alloc(bytes(size)) for _ in range(3)]
    u, f = C.c_uint, C.c_float
    shape = [u(rows), u(width), f(1e-6)]
    eps = 1e-6
    refs = []
    extra = []
    mutable = None
    module_index = 0
    grid = rows
    if kind.startswith("rms"):
        suffix = {
            "rms": "",
            "rms_bare": "_bare",
            "rms_add": "_add",
            "rms_scale": "_add_mul_scalar",
            "rms_output_scale": "_add_output_scale",
        }[kind]
        name = "termite_rms_norm" + suffix + "_f32"
        args = [outputs[0], input_ptr]
        if kind != "rms_bare":
            args.append(weight_ptr)
        if kind in ("rms_add", "rms_scale", "rms_output_scale"):
            args.append(other_ptr)
        if kind in ("rms_scale", "rms_output_scale"):
            args.append(scale_ptr)
        args += shape
        ref = []
        for r in range(rows):
            v = x[r * width : (r + 1) * width]
            inv = 1 / math.sqrt(sum(t * t for t in v) / width + eps)
            for c, t in enumerate(v):
                z = t * inv * (1 if kind == "rms_bare" else w[c])
                if kind == "rms_scale":
                    z *= 0.75
                if kind in ("rms_add", "rms_scale", "rms_output_scale"):
                    z += y[r * width + c]
                if kind == "rms_output_scale":
                    z *= 0.75
                ref.append(z)
        refs = [ref]
    elif kind in ("layer", "add_layer", "vision_layer"):
        name = (
            "lighton_vision_norm_bf16"
            if kind == "vision_layer"
            else "termite_" + ("add_" if kind == "add_layer" else "") + "layer_norm_f32"
        )
        module_index = int(kind == "vision_layer")
        args = (
            [outputs[0], input_ptr]
            + ([other_ptr] if kind == "add_layer" else [])
            + [weight_ptr, beta_ptr]
            + shape
        )
        ref = []
        for r in range(rows):
            v = [
                f32(x[r * width + c] + y[r * width + c])
                if kind == "add_layer"
                else x[r * width + c]
                for c in range(width)
            ]
            mean = sum(v) / width
            var = max(sum(t * t for t in v) / width - mean * mean, 0)
            ref += [
                (t - mean) / math.sqrt(var + eps) * w[c] + beta[c]
                for c, t in enumerate(v)
            ]
        refs = [ref]
    elif kind in ("bf16_rms", "bf16_bare", "bf16_scale_rms"):
        name = "termite_embedding_gemma2_" + (
            "scale_rms_bf16" if kind == "bf16_scale_rms" else "rms_norm_bf16"
        )
        args = [
            outputs[0],
            input_ptr,
            C.c_uint64(0) if kind == "bf16_bare" else weight_ptr,
            u(rows),
            u(width),
        ]
        args += [f(0.75), f(eps), u(stride)] if kind == "bf16_scale_rms" else [f(eps)]
        ref = []
        for r in range(rows):
            v = x[r * stride : r * stride + width]
            if kind == "bf16_scale_rms":
                v = [rounded(t * 0.75) for t in v]
            inv = 1 / math.sqrt(sum(t * t for t in v) / width + eps)
            ref += [
                t * inv * (1 if kind == "bf16_bare" else w[c]) for c, t in enumerate(v)
            ]
        refs = [ref]
    elif kind in ("bf16_post", "bf16_post_next"):
        name = "termite_embedding_gemma2_norm_residual_bf16"
        mutable = (outputs[0], packed(y, "bf16"))
        args = [
            outputs[0],
            outputs[1],
            input_ptr,
            weight_ptr,
            weight_ptr if kind.endswith("next") else C.c_uint64(0),
            scale_ptr,
        ] + shape
        ref = []
        nxt = []
        for r in range(rows):
            v = x[r * width : (r + 1) * width]
            inv = 1 / math.sqrt(sum(t * t for t in v) / width + eps)
            z = [
                rounded(rounded(y[r * width + c] + rounded(t * inv * w[c])) * 0.75)
                for c, t in enumerate(v)
            ]
            ref += z
            inv = 1 / math.sqrt(sum(t * t for t in z) / width + eps)
            nxt += [t * inv * w[c] for c, t in enumerate(z)]
        refs = [ref] + ([nxt] if kind.endswith("next") else [])
    elif kind == "triple":
        name = "termite_gemma4_a4b_rms_norm_triple_f32"
        args = outputs + [input_ptr, weight_ptr, weight_ptr, weight_ptr] + shape
        ref = []
        for r in range(rows):
            v = x[r * width : (r + 1) * width]
            inv = 1 / math.sqrt(sum(t * t for t in v) / width + eps)
            ref += [t * inv * w[c] for c, t in enumerate(v)]
        refs = [ref] * 3
    elif kind == "parallel":
        name = "termite_gemma4_a4b_parallel_ffn_post_residual_f32"
        args = [
            outputs[0],
            input_ptr,
            weight_ptr,
            other_ptr,
            weight_ptr,
            weight_ptr,
            other_ptr,
        ] + shape
        ref = []
        for r in range(rows):
            v = x[r * width : (r + 1) * width]
            z = y[r * width : (r + 1) * width]
            iv = 1 / math.sqrt(sum(t * t for t in v) / width + eps)
            iz = 1 / math.sqrt(sum(t * t for t in z) / width + eps)
            combined = [f32(v[c] * iv * w[c] + z[c] * iz * w[c]) for c in range(width)]
            inv = 1 / math.sqrt(sum(t * t for t in combined) / width + eps)
            ref += [t * inv * w[c] + z[c] for c, t in enumerate(combined)]
        refs = [ref]
    else:
        name = "termite_embedding_gemma2_" + (
            "packed_gelu_bf16" if kind == "packed_gelu" else "gelu_mul_bf16"
        )
        if kind == "packed_gelu":
            data = [
                t
                for r in range(rows)
                for plane in (
                    x[r * width : (r + 1) * width],
                    y[r * width : (r + 1) * width],
                )
                for t in plane
            ]
            extra = [d.alloc(packed(data, "bf16"))]
            args = [outputs[0], extra[0], u(rows), u(width)]
        else:
            args = [outputs[0], input_ptr, other_ptr, u(n)]
        grid = (n + 255) // 256
        refs = [
            [
                rounded(
                    0.5
                    * t
                    * (1 + math.tanh(0.7978845608028654 * (t + 0.044715 * t * t * t)))
                )
                * z
                for t, z in zip(x, y)
            ]
        ]
    functions = [d.function(m[module_index], name) for m in modules]
    results = []
    for fn in functions:
        if mutable:
            d.upload(*mutable)
        d.launch(fn, args, grid)
        d.synchronize()
        results.append([d.download(p, size) for p in outputs[: len(refs)]])
    assert results[0] == results[1], f"{name}/{width}: original bitwise mismatch"
    for result, ref in zip(results[1], refs):
        actual = array.array("H" if dtype == "bf16" else "f")
        actual.frombytes(result)
        if dtype == "bf16":
            actual = list(map(from_bf16, actual))
        tolerance = 0.025 if dtype == "bf16" else 2e-5
        for i, (a, b) in enumerate(zip(actual, ref)):
            assert math.isfinite(a) and abs(a - b) <= tolerance * (1 + abs(b)), (
                name,
                width,
                i,
                a,
                b,
            )
    graphs = [d.graph(lambda fn=fn: d.launch(fn, args, grid)) for fn in functions]
    source = extra[0] if extra else input_ptr
    original = packed(data, "bf16") if extra else input_data
    element_bytes = 2 if inp_dtype == "bf16" else 4
    changed = b"".join(
        original[i : i + element_bytes]
        for i in reversed(range(0, len(original), element_bytes))
    )
    d.upload(source, changed)
    captured = []
    for graph, executable in graphs:
        if mutable:
            d.upload(*mutable)
        d.call("cuGraphLaunch", executable, d.stream)
        d.synchronize()
        captured.append([d.download(p, size) for p in outputs[: len(refs)]])
    assert captured[0] == captured[1], f"{name}/{width}: mutable graph bitwise mismatch"
    for graph, executable in graphs:
        d.call("cuGraphExecDestroy", executable)
        d.call("cuGraphDestroy", graph)
    d.upload(source, original)
    label = f"{kind}/rows{rows}/width{width}"
    emit(
        kind="correctness",
        case=label,
        bitwise=True,
        reference=True,
        mutable_graph_replays=1,
    )
    if run_bench and width == 1536 and mutable is None:
        benchmark(d, functions, args, grid, label)
    d.free_case()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--bench", action="store_true")
    opts = parser.parse_args()
    d = Device()
    modules = [
        [d.module(p / "kernels.cubin"), d.module(p / "lighton.ptx")]
        for p in (opts.baseline, opts.candidate)
    ]
    cases = 0
    for width, rows in [(32, 1), (35, 3), (128, 7), (257, 9), (1536, 19)]:
        for kind in [
            "rms",
            "rms_bare",
            "rms_add",
            "rms_scale",
            "rms_output_scale",
            "layer",
            "add_layer",
            "vision_layer",
            "bf16_rms",
            "bf16_bare",
            "bf16_scale_rms",
            "bf16_post",
            "bf16_post_next",
            "triple",
            "parallel",
            "gelu",
            "packed_gelu",
        ]:
            qualify_case(d, modules, width, rows, kind, opts.bench)
            cases += 1
    for width, rows in ((768, 129), (1024, 129), (768, 2592), (1024, 2592)):
        qualify_case(d, modules, width, rows, "vision_layer", False)
        cases += 1
    emit(kind="complete", cases=cases)


if __name__ == "__main__":
    main()
