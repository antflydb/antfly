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

"""Create rotary fixtures; optionally compare CUDA images with a CPU oracle.

No NumPy, Torch, or CUDA Python package is required. Forward fixtures initialize
writable BF16 inputs; masked forward values must survive, while masked VJP
values must be zero. The generic kernel-family harness consumes cases.bin for
separate balanced timing. This checker supplies its independent forward oracle.
"""
import argparse
import ctypes as ct
import hashlib
import json
import math
from pathlib import Path
import re
import struct


def pack(values, kind='f'):
    return struct.pack('<' + kind * len(values), *values)


def f32(value):
    return struct.unpack('<f', struct.pack('<f', value))[0]


def bf16(value):
    bits = struct.unpack('<I', struct.pack('<f', value))[0]
    return ((bits + 0x7fff + ((bits >> 16) & 1)) >> 16) & 0xffff


def bfvalue(value):
    return struct.unpack('<f', struct.pack('<I', bf16(value) << 16))[0]


def bfbytes(values):
    return pack([bf16(x) for x in values], 'H')


def fixtures(header):
    tables = {}
    for name, dim in [('sliding', 256), ('full', 512)]:
        match = re.search(r'eg2_training_rope_' + name + r'_bits\[\d+\] = \{(.*?)\};', header, re.S)
        tables[dim] = [struct.unpack('<f', struct.pack('<I', int(x, 16)))[0]
                       for x in re.findall(r'0x([0-9a-f]+)u', match[1])]
    shapes = [(1, 3, 3, 1, 2, 37., 2., 'none'),
              (1, 7, 4, 2, 30, 10000., 1., 'partial'),
              (2, 35, 4, 2, 256, 10000., 1., 'partial'),
              (2, 7, 3, 1, 512, 1000000., 2., 'none'),
              (1, 3, 4, 2, 256, 9999., .75, 'zero'),
              (1, 3, 4, 2, 512, 1000000., 1., 'partial')]
    cases = []
    for training in [False, True]:
        for backward in [False, True]:
            for shape, (batch, seq, heads, kvheads, dim, theta, scale, masking) in enumerate(shapes):
                mask = [int(masking != 'zero' and i % 3 != 1) for i in range(batch * seq)]
                values, expected = [], []
                for nh in [heads, kvheads]:
                    x = [f32((i * 13 % 61 - 30) / 32 + (0.0003 if backward else 0.))
                         for i in range(batch * seq * nh * dim)]
                    if not backward:
                        x = [bfvalue(v) for v in x]
                    y = x.copy() if not backward else [0.] * len(x)
                    for bs in range(batch * seq):
                        valid = masking == 'none' or mask[bs] != 0
                        if not valid:
                            continue
                        pos = bs % seq
                        for head in range(nh):
                            for pair in range(dim // 2):
                                if training:
                                    freq = tables[dim][pair] if (dim, theta) in [(256, 10000.), (512, 1000000.)] else f32(1 / f32(theta ** f32(f32(2. * pair) / dim)))
                                    angle = f32(f32(pos / scale) * freq)
                                else:
                                    angle = f32(f32(pos / scale) * f32(theta ** f32(-f32(2. * pair) / dim)))
                                s, c = bfvalue(math.sin(angle)), bfvalue(math.cos(angle))
                                a = (bs * nh + head) * dim + pair
                                b = a + dim // 2
                                x0, x1 = (bfvalue(x[a]), bfvalue(x[b])) if training else (x[a], x[b])
                                sign = 1 if backward else -1
                                if training:
                                    y[a] = bfvalue(bfvalue(x0 * c) + bfvalue(sign * x1 * s))
                                    y[b] = bfvalue(bfvalue(x1 * c) + bfvalue(-sign * x0 * s))
                                else:
                                    y[a], y[b] = f32(x0 * c + sign * x1 * s), f32(x1 * c - sign * x0 * s)
                                if not backward:
                                    y[a], y[b] = bfvalue(y[a]), bfvalue(y[b])
                    values.append(x)
                    expected.append(y)
                kernel = 'termite_embedding_gemma2_' + ('training_' if training else '') + ('rope_vjp_f32' if backward else 'rope_bf16')
                cases.append(dict(name=f'{kernel}-{shape}', kernel=kernel, backward=backward,
                                  values=values, expected=expected, mask=mask if masking != 'none' else None,
                                  dims=[batch, seq, heads, kvheads, dim], theta=theta, scale=scale,
                                  grid=[batch * seq + 1, (dim // 2 + 127) // 128 + 1, heads + 1]))
    return cases


def binary_manifest(cases):
    raw = bytearray(struct.pack('<II', 0x4b464333, len(cases)))
    def string(s):
        b = s.encode()
        return struct.pack('<I', len(b)) + b
    def arg(role, data, written=0, absolute=0., relative=0.):
        return struct.pack('<IQQdd', role, written, len(data), absolute, relative) + data
    for case in cases:
        backward = case['backward']
        args = []
        if backward:
            for y in case['expected']:
                data = pack(y)
                args.append(arg(4, data + b'\xa5' * 64, len(data), .0001, .01))
            args.extend(arg(1, pack(x)) for x in case['values'])
        else:
            for x in case['values']:
                data = bfbytes(x)
                args.append(arg(2, data + b'\xa5' * 64, len(data)))
        args.append(arg(0, struct.pack('<Q', 0)) if case['mask'] is None else arg(1, pack(case['mask'], 'q')))
        args.extend(arg(0, struct.pack('<I', v)) for v in case['dims'])
        args.extend(arg(0, struct.pack('<f', v)) for v in [case['theta'], case['scale']])
        raw += string(case['name']) + string(case['kernel']) + pack(case['grid'] + [128, 1, 1, 0, len(args)], 'I') + b''.join(args)
    return raw


class Cuda:
    def __init__(self):
        self.lib = ct.CDLL('libcuda.so.1')
        def bind(name, args):
            fn = getattr(self.lib, name)
            fn.argtypes, fn.restype = args, ct.c_int
            def checked(*values):
                status = fn(*values)
                if status:
                    raise RuntimeError(f'{name}: CUDA status {status}')
            return checked
        ptr, u64, uint = ct.c_void_p, ct.c_uint64, ct.c_uint
        self.init = bind('cuInit', [uint])
        self.create = bind('cuCtxCreate_v2', [ct.POINTER(ptr), uint, ct.c_int])
        self.destroy = bind('cuCtxDestroy_v2', [ptr])
        self.load = bind('cuModuleLoad', [ct.POINTER(ptr), ct.c_char_p])
        self.unload = bind('cuModuleUnload', [ptr])
        self.function = bind('cuModuleGetFunction', [ct.POINTER(ptr), ptr, ct.c_char_p])
        self.alloc = bind('cuMemAlloc_v2', [ct.POINTER(u64), ct.c_size_t])
        self.free = bind('cuMemFree_v2', [u64])
        self.upload = bind('cuMemcpyHtoD_v2', [u64, ptr, ct.c_size_t])
        self.download = bind('cuMemcpyDtoH_v2', [ptr, u64, ct.c_size_t])
        self.launch = bind('cuLaunchKernel', [ptr] + [uint] * 7 + [ptr, ct.POINTER(ptr), ptr])
        self.sync = bind('cuCtxSynchronize', [])
        self.init(0)
        self.context = ptr()
        self.create(ct.byref(self.context), 0, 0)

    def run(self, module, case):
        allocations, sizes = [], []
        def buffer(data):
            pointer = ct.c_uint64()
            self.alloc(ct.byref(pointer), len(data))
            allocations.append(pointer)
            self.upload(pointer, ct.cast(ct.create_string_buffer(data), ct.c_void_p), len(data))
            sizes.append(len(data))
            return pointer
        try:
            backward = case['backward']
            outputs = [buffer((b'\xff' * (len(x) * 4) if backward else bfbytes(x)) + b'\xa5' * 64) for x in case['values']]
            args = outputs.copy()
            if backward:
                args.extend(buffer(pack(x)) for x in case['values'])
            args.append(ct.c_uint64(0) if case['mask'] is None else buffer(pack(case['mask'], 'q')))
            args.extend(ct.c_uint(x) for x in case['dims'])
            args.extend(ct.c_float(x) for x in [case['theta'], case['scale']])
            function = ct.c_void_p()
            self.function(ct.byref(function), module, case['kernel'].encode())
            pointers = (ct.c_void_p * len(args))(*(ct.addressof(x) for x in args))
            self.launch(function, *case['grid'], 128, 1, 1, 0, None, pointers, None)
            self.sync()
            result, max_error = [], 0.
            for pointer, x, reference in zip(outputs, case['values'], case['expected']):
                size = len(x) * (4 if backward else 2)
                image = ct.create_string_buffer(size + 64)
                self.download(image, pointer, len(image))
                raw = image.raw
                assert raw[size:] == b'\xa5' * 64, (case['name'], 'guard')
                got = struct.unpack('<' + ('f' if backward else 'H') * len(x), raw[:size])
                if not backward:
                    got = [struct.unpack('<f', struct.pack('<I', v << 16))[0] for v in got]
                for index, (a, b) in enumerate(zip(got, reference)):
                    assert math.isfinite(a) and abs(a - b) <= .0001 + .01 * abs(b), (case['name'], index, a, b)
                    max_error = max(max_error, abs(a - b))
                result.append(raw)
            return result, max_error
        finally:
            for pointer in allocations:
                self.free(pointer)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--header', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--baseline', type=Path)
    parser.add_argument('--candidate', type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    cases = fixtures(args.header.read_text())
    manifest = binary_manifest(cases)
    (args.output / 'cases.bin').write_bytes(manifest)
    pins = dict(header=hashlib.sha256(args.header.read_bytes()).hexdigest(), cases=hashlib.sha256(manifest).hexdigest())
    if (args.baseline is None) != (args.candidate is None):
        parser.error('Provide both CUDA images, or neither to generate fixtures only')
    if args.baseline is not None:
        cuda = Cuda()
        modules, receipt = [], []
        try:
            for path in [args.baseline, args.candidate]:
                module = ct.c_void_p()
                cuda.load(ct.byref(module), str(path.resolve()).encode())
                modules.append(module)
                pins[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
            for case in cases:
                a, error_a = cuda.run(modules[0], case)
                b, error_b = cuda.run(modules[1], case)
                assert a == b, (case['name'], 'baseline/candidate mismatch')
                receipt.append(dict(name=case['name'], exact=True, guards=True, reference=True, max_error=max(error_a, error_b)))
                print(case['name'], 'PASS', flush=True)
            (args.output / 'correctness.json').write_text(json.dumps(receipt, indent=2) + '\n')
        finally:
            for module in modules:
                cuda.unload(module)
            cuda.destroy(cuda.context)
    (args.output / 'inputs.json').write_text(json.dumps(pins, indent=2) + '\n')
    print(len(cases), 'rotary fixtures', pins['cases'])


if __name__ == '__main__':
    main()
