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

"""Generate independent packed-split and paged-KV oracles for kernel_family_metal.

Place baseline-main-safe.metal and candidate-main-safe.metal in the output
folder. Each full source is compiled in both safe and fast production modes.
The generated AA/AB/BA manifests use fixed warmups and balanced timing arms.
"""
import argparse
import json
from pathlib import Path
import struct


def pack(values, kind='f'):
    return struct.pack('<' + kind * len(values), *values)


def generate(directory):
    fixtures = directory / 'metal-fixtures'
    fixtures.mkdir(parents=True, exist_ok=True)
    counter = 0
    cases = []

    def binding(index, raw, expected=None, dtype='f32', constants=False):
        nonlocal counter
        path = fixtures / f'{counter}.bin'
        counter += 1
        path.write_bytes(raw)
        result = dict(index=index, binding='bytes' if constants else 'buffer',
                      file=str(path.relative_to(directory)))
        if expected is not None:
            reference = path.with_suffix('.ref')
            reference.write_bytes(pack(expected))
            result.update(output=True, written_bytes=len(expected) * (2 if dtype == 'f16' else 4),
                          reference_file=str(reference.relative_to(directory)), output_dtype=dtype,
                          absolute_tolerance=0, relative_tolerance=0)
        return result

    def output(index, expected, dtype='f32'):
        initial = pack([12345.] * len(expected), 'e' if dtype == 'f16' else 'f')
        return binding(index, initial + b'\xa5' * 64, expected, dtype)

    def add(name, kernel, bindings, work):
        cases.append(dict(name=name, kernel=kernel, bindings=bindings,
                          grid=[max(1, work + 37), 1, 1], threads=[128, 1, 1],
                          dispatch='threads', scratch=[]))

    for third in [True, False]:
        for bias in [False, True]:
            for rows, first, second, padding in [(1, 0, 3, 2), (3, 7, 5, 0),
                                                  (33, 128, 32, -1), (128, 2048, 512, 0)]:
                if not third:
                    second = first
                total = first + second + (second if third else 0) + padding
                widths = [first, second] + ([second] if third else [])
                data = [(i * 13 % 63 - 31) / 32 for i in range(rows * total)]
                biases = [[(i * 17 % 31 - 15) / 64 for i in range(width)] for width in widths]
                expected = [[12345.] * (rows * width) for width in widths]
                for row in range(rows):
                    start = 0
                    for slot, width in enumerate(widths):
                        for column in range(width):
                            if start + column < total:
                                expected[slot][row * width + column] = (
                                    data[row * total + start + column]
                                    + (biases[slot][column] if bias else 0))
                        start += width
                bindings = [binding(0, pack(data))]
                index = 1
                if bias:
                    for values in biases:
                        bindings.append(binding(index, pack(values or [0])))
                        index += 1
                for values in expected:
                    bindings.append(output(index, values))
                    index += 1
                params = [rows, first, second, total] if third else [rows, first, total, 0]
                bindings.append(binding(index, pack(params, 'I'), constants=True))
                kind = 'qkv' if third else 'pair'
                kernel = f'termite_dense_{kind}_split_' + ('bias_' if bias else '') + 'f32_2d'
                add(f'{kind}-{bias}-{rows}-{first}-{padding}', kernel, bindings, rows * total)

    for dtype in ['f32', 'f16']:
        for total, suffix, values, stride, page in [(1, 1, 1, 1, 1), (19, 11, 7, 11, 4),
                                                    (37, 17, 32, 17, 8), (128, 65, 768, 1024, 16)]:
            blocks = (total + page - 1) // page
            table = [(blocks - 1 - i) * page for i in range(blocks)]
            capacity, key_stride = blocks * page, values + 3
            key = [(i * 13 % 63 - 31) / 32 for i in range(suffix * values)]
            value = [(i * 17 % 61 - 30) / 32 for i in range(suffix * stride)]
            if dtype == 'f16' and total == 19:
                # Normal-range halfway cases exercise ties-to-even conversion,
                # independently of the destination indexing/padding oracle.
                edges = [1 + 2**-11, 1 + 3 * 2**-11, -1 - 2**-11,
                         -1 - 3 * 2**-11, 2 - 2**-11, 2048.5, 2049.5, 65504.]
                key = [edges[i % len(edges)] for i in range(len(key))]
                value = [edges[(i + 3) % len(edges)] for i in range(len(value))]
            expected_key = [12345.] * (capacity * key_stride)
            expected_value = [12345.] * (capacity * stride)
            if dtype == 'f16':
                # Unwritten half padding contains the rounded sentinel too.
                sentinel = struct.unpack('<e', struct.pack('<e', 12345.))[0]
                expected_key = [sentinel] * len(expected_key)
                expected_value = [sentinel] * len(expected_value)
            for row in range(suffix):
                logical = total - suffix + row
                physical = table[logical // page] + logical % page
                for column in range(values):
                    expected_key[physical * key_stride + column] = key[row * values + column]
                for column in range(stride):
                    expected_value[physical * stride + column] = value[row * stride + column]
            if dtype == 'f16':
                expected_key = list(struct.unpack('<' + 'e' * len(expected_key),
                                                  pack(expected_key, 'e')))
                expected_value = list(struct.unpack('<' + 'e' * len(expected_value),
                                                    pack(expected_value, 'e')))
            bindings = [binding(0, pack(key)), binding(1, pack(value)),
                        output(2, expected_key, dtype), output(3, expected_value, dtype),
                        binding(4, pack(table, 'I')),
                        binding(5, pack([total, suffix, values, page, key_stride, stride], 'I'),
                                constants=True)]
            add(f'kv-{dtype}-{total}-{values}-{stride}', f'termite_paged_{dtype}_kv_seed',
                bindings, suffix * max(values, stride))

    profiles = [dict(case, name=case['name'] + '-' + mode, math_mode=mode)
                for mode in ['safe', 'fast'] for case in cases]
    for cohort, images in [('AA', ['baseline', 'baseline']), ('AB', ['baseline', 'candidate']),
                           ('BA', ['candidate', 'baseline'])]:
        manifest = dict(baseline=images[0] + '-main-safe.metal',
                        candidate=images[1] + '-main-safe.metal', language='3.1', pipeline_order='AB',
                        warmup=500, warmup_ms=500, repeats=2000,
                        arm_order='ABBABAABABBAABBA' * 2, cases=profiles)
        (directory / f'metal-{cohort}.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(len(profiles), 'Metal profiles')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    generate(parser.parse_args().output.resolve())


if __name__ == '__main__':
    main()
