#!/usr/bin/env python3
"""Validate the shipped packed-attention PTX against a dense Torch oracle.

Run with a CUDA-enabled Torch environment. This checks the actual kernel image,
including unequal rows, grouped heads, disjoint ranges, and readout-only queries.
"""

import argparse
import ctypes as C
from pathlib import Path
import torch


class Params(C.Structure):
    _fields_ = [(n, C.c_uint64) for n in ("q", "k", "v", "out", "ranges")] + [
        ("queries", C.c_uint32),
        ("heads", C.c_uint32),
        ("kv_heads", C.c_uint32),
        ("scale", C.c_float),
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kernel", choices=["f32", "f16"], default="f32")
    parser.add_argument("--bench", action="store_true")
    parser.add_argument(
        "--gapped",
        action="store_true",
        help="Exclude interior keys between visibility ranges",
    )
    args_cli = parser.parse_args()
    torch.manual_seed(1649)
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.cuda.init()
    torch.empty(1, device="cuda")  # Establish the driver context before loading PTX.
    driver = C.CDLL("libcuda.so.1")

    def check(code):
        if code:
            raise RuntimeError(f"CUDA driver error {code}")

    module, function = C.c_void_p(), C.c_void_p()
    source = Path(__file__).resolve().parents[2] / "src/ops/cuda/glide_attention.ptx"
    image = C.create_string_buffer(source.read_bytes())
    check(driver.cuModuleLoadDataEx(C.byref(module), image, 0, None, None))
    try:
        check(
            driver.cuModuleGetFunction(
                C.byref(function), module, f"glide_attention_{args_cli.kernel}".encode()
            )
        )
        for lengths in (
            ([106], [5129]) if args_cli.bench else ([1], [33, 1, 257], [65, 129], [513])
        ):
            total = sum(lengths)
            starts = [sum(lengths[:i]) for i in range(len(lengths))]
            for readout in (False, True):
                count = len(lengths) if readout else total
                q = torch.randn(count, 8, 256, device="cuda")
                k, v = [torch.randn(total, 2, 256, device="cuda") for _ in range(2)]
                ranges, expected = [], []
                for start, length in zip(starts, lengths):
                    for _ in range(1 if readout else length):
                        # Cover both adjacent spans and a real excluded gap.
                        cut = start + (
                            max(1, length // 3) if args_cli.gapped else length // 2
                        )
                        gap_end = (
                            start + max(cut - start, 2 * length // 3)
                            if args_cli.gapped
                            else cut
                        )
                        ranges.append([start, cut, cut, cut, gap_end, start + length])
                        expected.append((start, cut, gap_end, start + length))
                metadata = torch.tensor(ranges, dtype=torch.int32, device="cuda")
                output = torch.empty_like(q)
                p = Params(
                    *(t.data_ptr() for t in (q, k, v, output, metadata)),
                    count,
                    8,
                    2,
                    1 / 16,
                )
                args = (C.c_void_p * 1)(C.addressof(p))

                def launch():
                    grid = (
                        ((count * 8 + 3) // 4, 1)
                        if args_cli.kernel == "f32"
                        else ((count + 15) // 16, 8)
                    )
                    check(
                        driver.cuLaunchKernel(
                            function,
                            *grid,
                            1,
                            128,
                            1,
                            1,
                            0,
                            C.c_void_p(torch.cuda.current_stream().cuda_stream),
                            args,
                            None,
                        )
                    )

                launch()
                if args_cli.bench:
                    start, end = (
                        torch.cuda.Event(enable_timing=True),
                        torch.cuda.Event(enable_timing=True),
                    )
                    start.record()
                    for _ in range(10):
                        launch()
                    end.record()
                    end.synchronize()
                    print(
                        f"BENCH {args_cli.kernel} length={total} readout={readout} ms={start.elapsed_time(end) / 10:.3f}",
                        flush=True,
                    )
                    continue
                oracle = []
                for i, (start, cut, gap_end, end) in enumerate(expected):
                    keys = torch.cat((k[start:cut], k[gap_end:end])).repeat_interleave(
                        4, dim=1
                    )
                    values = torch.cat(
                        (v[start:cut], v[gap_end:end])
                    ).repeat_interleave(4, dim=1)
                    weights = torch.softmax(
                        torch.einsum("hd,khd->hk", q[i], keys) / 16, dim=-1
                    )
                    oracle.append(torch.einsum("hk,khd->hd", weights, values))
                want = torch.stack(oracle)
                torch.testing.assert_close(
                    output,
                    want,
                    atol=2e-3 if args_cli.kernel == "f16" else 3e-6,
                    rtol=2e-3 if args_cli.kernel == "f16" else 3e-5,
                )
                print(
                    f"PASS lengths={lengths} readout={readout} gapped={args_cli.gapped} max_error={(output - want).abs().max().item():.3g}"
                )
    finally:
        torch.cuda.synchronize()
        check(driver.cuModuleUnload(module))


if __name__ == "__main__":
    main()
