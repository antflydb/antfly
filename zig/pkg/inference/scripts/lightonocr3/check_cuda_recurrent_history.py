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

"""Check partition invariance independently of the recurrent indexing oracle."""

import argparse
import json
import math
from pathlib import Path

from check_cuda_recurrent import Driver, oracle


def check_oracle_history():
    kh, vh = 1, 1
    channels = (2 * kh + vh) * 128
    for n in [1, 2, 3]:
        x = [float(i + 1) for i in range(n * channels)]
        _, history, _ = oracle(
            x,
            [0.0] * (n * 128),
            [0.0] * n,
            [0.0] * n,
            [0.0] * (channels * 4),
            [0.0],
            [0.0],
            [1.0] * 128,
            [float("nan")] * (channels * 4),
            [float("nan")] * (128 * 128),
            n,
            kh,
            vh,
            False,
        )
        for channel in range(channels):
            expected = [0.0] * (4 - n) + [x[t * channels + channel] for t in range(n)]
            assert history[channel * 4 : channel * 4 + 4] == expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--oracle-only", action="store_true")
    args = parser.parse_args()
    check_oracle_history()
    if args.oracle_only:
        print("explicit left-padded oracle history passed for lengths 1–3")
        return
    if args.artifact is None or args.output is None:
        parser.error("supply --artifact and --output for GPU qualification")
    from check_cuda_recurrent_batch import Page
    from check_cuda_recurrent_ragged import Ragged, launch_fused_decode

    gpu = Driver(args.artifact)
    records = []
    try:
        # The review's concrete counterexample, before normalization/recurrence.
        w = gpu.buffer([1.0] * (128 * 4))
        zero = gpu.buffer([0.0] * (128 * 4))
        first, last = gpu.buffer([0.0] * 512), gpu.buffer([0.0] * 512)
        x1, x2 = gpu.buffer([1.0] * 128), gpu.buffer([2.0] * 128)
        y1, y2 = gpu.buffer([0.0] * 128), gpu.buffer([0.0] * 128)
        gpu.launch("antfly_gdn_causal_conv", (1, 1), 128, x1, w, zero, first, y1, 1, 128, 0)
        gpu.launch("antfly_gdn_causal_conv", (1, 1), 128, x2, w, first, last, y2, 1, 128, 1)
        actual = gpu.read(y2)
        expected = 3.0 / (1.0 + math.exp(-3.0))
        assert max(abs(v - expected) for v in actual) < 2e-5, {
            "inputs": [1, 2],
            "expected": expected,
            "actual": actual[0],
        }
        for vh in [16, 32]:
            kh, count = 16, 7
            channels, width = (32 + vh) * 128, vh * 128
            x = [
                ((i // channels + 1) * 7 + i % 13 - 6) / 32.0
                for i in range(count * channels)
            ]
            z = [0.5] * (count * width)
            beta, a = [0.1] * (count * vh), [-0.2] * (count * vh)
            weights = [((i % 7) - 3) / 16.0 for i in range(channels * 4)]
            cw, alog, dt, norm = map(
                gpu.buffer, (weights, [-0.8] * vh, [0.1] * vh, [1.0] * 128)
            )

            def execute(parts, mode, reset):
                history = gpu.buffer([17.0] * (channels * 4))
                state = gpu.buffer([19.0] * (vh * 128 * 128))
                readonly = [cw, alog, dt, norm]
                snapshots = [gpu.read_bytes(v) for v in readonly]

                def step(lo, n, initialized):
                    nonlocal history
                    dx, dz, db, da = map(
                        gpu.buffer,
                        (
                            x[lo * channels : (lo + n) * channels],
                            z[lo * width : (lo + n) * width],
                            beta[lo * vh : (lo + n) * vh],
                            a[lo * vh : (lo + n) * vh],
                        ),
                    )
                    before = [gpu.read_bytes(v) for v in (dx, dz, db, da)]
                    nh, conv, core, out = [
                        gpu.buffer([0.0] * size)
                        for size in (channels * 4, n * channels, n * width, n * width)
                    ]
                    if mode == "fused" and n == 1:
                        p = Ragged()
                        for field, value in [
                            ("mixed", dx),
                            ("conv_weight", cw),
                            ("conv", conv),
                            ("z", dz),
                            ("beta", db),
                            ("a", da),
                            ("a_log", alog),
                            ("dt_bias", dt),
                            ("core", core),
                            ("norm_weight", norm),
                            ("out", out),
                        ]:
                            setattr(p.base, field, value[0].value)
                        (
                            p.base.items,
                            p.base.key_heads,
                            p.base.value_heads,
                            p.base.eps,
                        ) = 1, kh, vh, 1e-6
                        p.base.pages[0] = Page(
                            history[0].value, nh[0].value, state[0].value
                        )
                        p.lengths[0], p.initialized[0] = 1, int(initialized)
                        launch_fused_decode(gpu, p)
                    else:
                        parallel = mode in ("parallel", "fused")
                        cached = mode != "serial"
                        argv = [
                            dx,
                            cw,
                            history,
                            nh,
                            conv,
                            n,
                            channels,
                            int(initialized),
                        ]
                        if cached:
                            argv += [db, da, alog, dt, out, vh]
                        gpu.launch(
                            "lighton_prefix_conv_normalize_cached_gates"
                            if parallel
                            else "lighton_conv_cached_gates"
                            if cached
                            else "antfly_gdn_causal_conv",
                            (channels // 128, n if parallel else 1),
                            128,
                            *argv,
                        )
                        if not parallel:
                            gpu.launch(
                                "lighton_normalize",
                                (n * 2 * kh, 1),
                                128,
                                conv,
                                channels,
                                kh,
                            )
                        argv = [
                            conv,
                            db,
                            da,
                            alog,
                            dt,
                            state,
                            core,
                            n,
                            kh,
                            vh,
                            int(initialized),
                        ]
                        if cached:
                            argv += [out]
                        gpu.launch(
                            "lighton_recurrent_cached_gates"
                            if cached
                            else "lighton_recurrent",
                            (4, vh),
                            128,
                            *argv,
                        )
                        gpu.launch(
                            "lighton_gated_norm",
                            (n * vh, 1),
                            128,
                            core,
                            dz,
                            norm,
                            out,
                            vh,
                            1e-6,
                        )
                    gpu.call("cuCtxSynchronize")
                    for v, expected in zip((dx, dz, db, da), before):
                        assert gpu.read_bytes(v) == expected, "read-only input changed"
                    gpu.read(conv)
                    gpu.read(core)
                    history = nh
                    return gpu.read(out)

                if reset:
                    step(0, 5, False)
                result, lo = [], 0
                for n in parts:
                    result.extend(step(lo, n, lo != 0))
                    lo += n
                for v, expected in zip(readonly, snapshots):
                    assert gpu.read_bytes(v) == expected, "read-only weight changed"
                return result, gpu.read(history), gpu.read(state)

            reference = execute([count], "serial", False)
            for initial in [1, 2, 3]:
                for parts in [
                    [initial, count - initial],
                    [initial] + [1] * (count - initial),
                ]:
                    for reset in [False, True]:
                        for mode in ["serial", "cached", "parallel", "fused"]:
                            actual = execute(parts, mode, reset)
                            errors = [
                                max(abs(a - b) for a, b in zip(got, expected))
                                for got, expected in zip(actual, reference)
                            ]
                            assert all(
                                math.isfinite(v) for values in actual for v in values
                            )
                            assert errors[0] < 2e-5 and errors[2] < 2e-5, (
                                vh,
                                parts,
                                reset,
                                mode,
                                errors,
                            )
                            assert actual[1] == reference[1], (
                                vh,
                                parts,
                                reset,
                                mode,
                                "history",
                            )
                            row = dict(
                                value_heads=vh,
                                initial=initial,
                                parts=parts,
                                reset=reset,
                                mode=mode,
                                max_abs_error=errors,
                            )
                            records.append(row)
                            print(json.dumps(row), flush=True)
    finally:
        gpu.close()
    args.output.write_text(
        json.dumps(dict(status="ok", cases=records), indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
