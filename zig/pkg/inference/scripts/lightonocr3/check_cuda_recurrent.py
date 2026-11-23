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

"""Qualify the default-off CUDA recurrent candidate with a stdlib CPU oracle.

Compile prototypes/gated_delta_recurrent.cu with nvcc -cubin -arch=sm_121
-O3 --fmad=false. This checks F32 state, short-prefix history, continuation,
reset and output canaries. It is not an end-to-end throughput benchmark.
"""

import argparse
import ctypes as c
import json
import math
from pathlib import Path


def oracle(x, z, beta, a, cw, alog, dt, nw, history, state, seq, kh, vh, initialized):
    channels, width = (2 * kh + vh) * 128, vh * 128
    conv = []
    for t in range(seq):
        for channel in range(channels):
            value = 0.0
            for k in range(4):
                source = t + k - 3
                sample = (
                    x[source * channels + channel]
                    if source >= 0
                    else history[channel * 4 + source + 4]
                    if initialized
                    else 0.0
                )
                value += sample * cw[channel * 4 + k]
            conv.append(value / (1 + math.exp(-value)))
    next_history = []
    for channel in range(channels):
        for k in range(4):
            source = seq + k - 4
            next_history.append(
                x[source * channels + channel]
                if 0 <= source < seq
                else history[channel * 4 + source + 4]
                if initialized and source < 0
                else 0.0
            )
    for t in range(seq):
        for h in range(2 * kh):
            offset = t * channels + h * 128
            inverse = 1 / math.sqrt(
                sum(v * v for v in conv[offset : offset + 128]) + 1e-6
            )
            for d in range(128):
                conv[offset + d] *= inverse * (1 / math.sqrt(128) if h < kh else 1)
    state = list(state) if initialized else [0.0] * (vh * 128 * 128)
    output = [0.0] * (seq * width)
    for h in range(vh):
        key_head = h // (vh // kh)
        for t in range(seq):
            offset = t * channels + key_head * 128
            q = conv[offset : offset + 128]
            k = conv[offset + kh * 128 : offset + kh * 128 + 128]
            p = a[t * vh + h] + dt[h]
            softplus = (
                p if p > 20 else math.exp(p) if p < -20 else math.log1p(math.exp(p))
            )
            decay = math.exp(-math.exp(alog[h]) * softplus)
            b = 1 / (1 + math.exp(-beta[t * vh + h]))
            for column in range(128):
                prediction = 0.0
                for row in range(128):
                    index = (h * 128 + row) * 128 + column
                    state[index] *= decay
                    prediction += state[index] * k[row]
                v = conv[t * channels + 2 * kh * 128 + h * 128 + column]
                delta = (v - prediction) * b
                result = 0.0
                for row in range(128):
                    index = (h * 128 + row) * 128 + column
                    state[index] += k[row] * delta
                    result += state[index] * q[row]
                output[t * width + h * 128 + column] = result
    for row in range(seq * vh):
        offset = row * 128
        inverse = 1 / math.sqrt(
            sum(v * v for v in output[offset : offset + 128]) / 128 + 1e-6
        )
        for d in range(128):
            gate = z[offset + d]
            output[offset + d] *= inverse * nw[d] * gate / (1 + math.exp(-gate))
    return output, next_history, state


class Driver:
    def __init__(self, artifact):
        self.lib = c.CDLL("libcuda.so.1")
        self.allocations = []
        self.context = c.c_void_p()
        self.module = c.c_void_p()
        self.call("cuInit", c.c_uint(0))
        self.call("cuDevicePrimaryCtxRetain", c.byref(self.context), c.c_int(0))
        self.call("cuCtxSetCurrent", self.context)
        self.call(
            "cuModuleLoad",
            c.byref(self.module),
            c.c_char_p(bytes(str(artifact.resolve()), "utf8")),
        )

    def call(self, name, *args):
        result = getattr(self.lib, name)(*args)
        if result:
            raise RuntimeError(f"{name} failed: {result}")

    def buffer(self, values):
        array = (c.c_float * (len(values) + 16))(*values, *([123456.0] * 16))
        pointer = c.c_uint64()
        self.call("cuMemAlloc_v2", c.byref(pointer), c.c_size_t(c.sizeof(array)))
        self.allocations.append(pointer)
        self.call("cuMemcpyHtoD_v2", pointer, array, c.c_size_t(c.sizeof(array)))
        return pointer, len(values)

    def read(self, buffer):
        pointer, length = buffer
        array = (c.c_float * (length + 16))()
        self.call("cuMemcpyDtoH_v2", array, pointer, c.c_size_t(c.sizeof(array)))
        assert list(array)[length:] == [123456.0] * 16, (
            "GPU wrote past the buffer boundary"
        )
        return list(array)[:length]

    def read_bytes(self, buffer):
        pointer, length = buffer
        array = (c.c_float * (length + 16))()
        self.call("cuMemcpyDtoH_v2", array, pointer, c.c_size_t(c.sizeof(array)))
        assert list(array)[length:] == [123456.0] * 16, (
            "GPU wrote past the buffer boundary"
        )
        return bytes(array)[: length * 4]

    def launch(self, name, grid, block, *args):
        function = c.c_void_p()
        self.call(
            "cuModuleGetFunction",
            c.byref(function),
            self.module,
            c.c_char_p(name.encode()),
        )
        arguments = [
            arg[0]
            if isinstance(arg, tuple)
            else c.c_float(arg)
            if isinstance(arg, float)
            else c.c_int(arg)
            for arg in args
        ]
        pointers = (c.c_void_p * len(arguments))(
            *(c.addressof(arg) for arg in arguments)
        )
        self.call(
            "cuLaunchKernel",
            function,
            c.c_uint(grid[0]),
            c.c_uint(grid[1]),
            c.c_uint(1),
            c.c_uint(block),
            c.c_uint(1),
            c.c_uint(1),
            c.c_uint(0),
            c.c_void_p(),
            pointers,
            c.c_void_p(),
        )

    def close(self):
        self.call("cuCtxSynchronize")
        for allocation in self.allocations:
            self.call("cuMemFree_v2", allocation)
        self.call("cuModuleUnload", self.module)
        self.call("cuDevicePrimaryCtxRelease_v2", c.c_int(0))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cached-gates", action="store_true")
    parser.add_argument("--parallel-prefix", action="store_true")
    args = parser.parse_args()
    if args.parallel_prefix and not args.cached_gates:
        parser.error("--parallel-prefix requires --cached-gates")
    gpu = Driver(args.artifact)
    results = []
    try:
        for kh, vh in [(1, 2), (2, 4), (16, 16), (16, 32)]:
            channels, width = (2 * kh + vh) * 128, vh * 128
            history, state = [0.0] * (channels * 4), [0.0] * (vh * 128 * 128)
            device_history, device_state = gpu.buffer(history), gpu.buffer(state)
            cw = [((i % 9) - 4) * 0.04 for i in range(channels * 4)]
            alog, dt = (
                [-0.8 + h * 0.3 for h in range(vh)],
                [0.2 - h * 0.1 for h in range(vh)],
            )
            nw = [0.5 + (i % 7) * 0.1 for i in range(128)]
            dcw, dalog, ddt, dnw = map(gpu.buffer, (cw, alog, dt, nw))
            for seq, initialized in [
                (1, False),
                (1, True),
                (3, False),
                (1, True),
                (6, False),
                (3, True),
                (17, False),
                (1, True),
            ]:
                x = [math.sin(i * 0.013) * 0.7 for i in range(seq * channels)]
                z = [math.cos(i * 0.017) for i in range(seq * width)]
                beta = [(i % 5) * 0.2 - 0.4 for i in range(seq * vh)]
                a = [(i % 7) * 0.1 - 0.3 for i in range(seq * vh)]
                expected, next_history, next_state = oracle(
                    x,
                    z,
                    beta,
                    a,
                    cw,
                    alog,
                    dt,
                    nw,
                    history,
                    state,
                    seq,
                    kh,
                    vh,
                    initialized,
                )
                dx, dz, db, da = map(gpu.buffer, (x, z, beta, a))
                conv, core, output = [
                    gpu.buffer([0.0] * count)
                    for count in (seq * channels, seq * width, seq * width)
                ]
                next_device_history = gpu.buffer([0.0] * len(history))
                conv_args = [
                    dx,
                    dcw,
                    device_history,
                    next_device_history,
                    conv,
                    seq,
                    channels,
                    int(initialized),
                ]
                if args.cached_gates:
                    conv_args += [db, da, dalog, ddt, output, vh]
                gpu.launch(
                    "lighton_prefix_conv_normalize_cached_gates"
                    if args.parallel_prefix
                    else "lighton_conv_cached_gates"
                    if args.cached_gates
                    else "antfly_gdn_causal_conv",
                    ((channels + 127) // 128, seq if args.parallel_prefix else 1),
                    128,
                    *conv_args,
                )
                if not args.parallel_prefix:
                    gpu.launch(
                        "lighton_normalize", (seq * 2 * kh, 1), 128, conv, channels, kh
                    )
                else:
                    # Compare private intermediate arithmetic bit for bit with
                    # the existing two-kernel CUDA path, in addition to the
                    # independent CPU output/history/state oracle below.
                    reference_conv = gpu.buffer([0.0] * (seq * channels))
                    reference_history = gpu.buffer([0.0] * len(history))
                    reference_gates = gpu.buffer([0.0] * (seq * width))
                    gpu.launch(
                        "lighton_conv_cached_gates",
                        (channels // 128, 1),
                        128,
                        dx,
                        dcw,
                        device_history,
                        reference_history,
                        reference_conv,
                        seq,
                        channels,
                        int(initialized),
                        db,
                        da,
                        dalog,
                        ddt,
                        reference_gates,
                        vh,
                    )
                    gpu.launch(
                        "lighton_normalize",
                        (seq * 2 * kh, 1),
                        128,
                        reference_conv,
                        channels,
                        kh,
                    )
                    for candidate, reference in (
                        (conv, reference_conv),
                        (next_device_history, reference_history),
                        (output, reference_gates),
                    ):
                        assert gpu.read_bytes(candidate) == gpu.read_bytes(reference), (
                            "parallel prefix intermediate differs",
                            kh,
                            vh,
                            seq,
                            initialized,
                        )
                recurrent_args = [
                    conv,
                    db,
                    da,
                    dalog,
                    ddt,
                    device_state,
                    core,
                    seq,
                    kh,
                    vh,
                    int(initialized),
                ]
                if args.cached_gates:
                    recurrent_args += [output]
                gpu.launch(
                    "lighton_recurrent_cached_gates"
                    if args.cached_gates
                    else "lighton_recurrent",
                    (4, vh),
                    128,
                    *recurrent_args,
                )
                gpu.launch(
                    "lighton_gated_norm",
                    (seq * vh, 1),
                    128,
                    core,
                    dz,
                    dnw,
                    output,
                    vh,
                    1e-6,
                )
                gpu.call("cuCtxSynchronize")
                errors = {}
                for name, buffer, wanted in [
                    ("output", output, expected),
                    ("history", next_device_history, next_history),
                    ("state", device_state, next_state),
                ]:
                    actual = gpu.read(buffer)
                    error = max(abs(v - w) for v, w in zip(actual, wanted))
                    assert all(math.isfinite(v) for v in actual), name
                    assert error < 2e-5, (name, kh, vh, seq, initialized, error)
                    errors[name] = error
                # All read-only inputs and intermediate outputs retain their guards.
                for buffer in (
                    dx,
                    dz,
                    db,
                    da,
                    dcw,
                    dalog,
                    ddt,
                    dnw,
                    conv,
                    core,
                    device_history,
                ):
                    gpu.read(buffer)
                results.append(
                    dict(
                        key_heads=kh,
                        value_heads=vh,
                        seq=seq,
                        continuation=initialized,
                        max_abs_error=errors,
                    )
                )
                history, state, device_history = (
                    next_history,
                    next_state,
                    next_device_history,
                )
    finally:
        gpu.close()
    args.output.write_text(
        json.dumps(
            dict(
                status="ok",
                cached_gates=args.cached_gates,
                parallel_prefix=args.parallel_prefix,
                cases=results,
            ),
            indent=2,
        )
        + "\n"
    )
    print(json.dumps(dict(status="ok", cases=len(results))))


if __name__ == "__main__":
    main()
