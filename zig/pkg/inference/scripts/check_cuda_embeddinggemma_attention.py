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

"""Qualify portable EmbeddingGemma BF16 attention against CPU FP64 SDPA.

Requires CUDA PyTorch. Optional baseline checks bitwise compatibility and
CUDA-event latency; those measurements describe kernels, not model throughput.
"""

import argparse
import hashlib
import statistics
import ctypes as c
import json
from pathlib import Path
import torch


class Kernel:
    def __init__(self, artifact):
        self.d = c.CDLL("libcuda.so.1")
        self.m = c.c_void_p()
        self.f = c.c_void_p()
        self.tail = c.c_void_p()
        self.call("cuModuleLoad", c.byref(self.m), c.c_char_p(bytes(artifact)))
        self.call(
            "cuModuleGetFunction",
            c.byref(self.f),
            self.m,
            c.c_char_p(b"termite_embedding_gemma2_attention_local_flash_bf16"),
        )

        self.call(
            "cuModuleGetFunction",
            c.byref(self.tail),
            self.m,
            c.c_char_p(b"termite_embedding_gemma2_attention_bf16"),
        )

    def call(self, name, *args):
        r = getattr(self.d, name)(*args)
        if r:
            raise RuntimeError((name, r))

    def run(self, out, q, k, v, mask, radius):
        b, s, h, d = q.shape
        kh = k.shape[2]
        vals = [c.c_uint64(t.data_ptr()) for t in (out, q, k, v, mask)] + [
            c.c_uint(x) for x in (b, s, h, kh, d, radius)
        ]
        args = (c.c_void_p * len(vals))(*(c.addressof(x) for x in vals))
        self.call(
            "cuLaunchKernel",
            self.f,
            c.c_uint(h),
            c.c_uint(s // 64),
            c.c_uint(b),
            c.c_uint(512),
            c.c_uint(1),
            c.c_uint(1),
            c.c_uint(16 * d * 2 + 4 * 64 * 16 * 4 + 64 * 16 * 2 + 3 * 64 * 4),
            c.c_void_p(torch.cuda.current_stream().cuda_stream),
            args,
            c.c_void_p(),
        )

        # Match the production dispatch: partial query tiles use the scalar
        # kernel so WMMA never reads beyond the physical query allocation.
        if s % 64:
            vals.append(c.c_uint(s - s % 64))
            args = (c.c_void_p * len(vals))(*(c.addressof(x) for x in vals))
            self.call(
                "cuLaunchKernel",
                self.tail,
                c.c_uint(s % 64),
                c.c_uint(h),
                c.c_uint(b),
                c.c_uint(32),
                c.c_uint(1),
                c.c_uint(1),
                c.c_uint(0),
                c.c_void_p(torch.cuda.current_stream().cuda_stream),
                args,
                c.c_void_p(),
            )


def oracle(q, k, v, mask, radius):
    q = q.cpu().double().transpose(1, 2)
    k = (
        k.cpu()
        .double()
        .transpose(1, 2)
        .repeat_interleave(q.shape[1] // k.shape[2], dim=1)
    )
    v = (
        v.cpu()
        .double()
        .transpose(1, 2)
        .repeat_interleave(q.shape[1] // v.shape[2], dim=1)
    )
    scores = q @ k.transpose(-1, -2)
    seq = q.shape[2]
    visible = mask.cpu().bool()[:, None, :, None] & mask.cpu().bool()[:, None, None, :]
    if radius:
        visible = (
            visible
            & (
                (torch.arange(seq)[:, None] - torch.arange(seq)[None, :]).abs()
                <= radius
            )[None, None]
        )
    scores = scores.masked_fill(~visible, float("-inf"))
    # Independent full softmax oracle: online kernel rounds probabilities to BF16.
    p = torch.nan_to_num(scores.softmax(-1))
    return (p @ v).transpose(1, 2).float()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--benchmark", action="store_true")
    args = parser.parse_args()
    if args.benchmark and args.baseline is None:
        parser.error("--benchmark requires --baseline")
    torch.manual_seed(421)
    torch.cuda.init()
    torch.zeros(1, device="cuda")
    artifacts = ([args.baseline] if args.baseline else []) + [args.artifact]
    kernels = [Kernel(x) for x in artifacts]
    cases = []
    for b, s, d, kh, radius, kind in [
        (1, 64, 256, 2, 512, "all"),
        (1, 65, 256, 2, 512, "holes"),
        (2, 129, 512, 1, 0, "prefix"),
        (2, 128, 256, 2, 512, "holes"),
        (1, 512, 512, 1, 0, "prefix"),
        (1, 576, 256, 2, 512, "holes"),
        (1, 128, 512, 1, 0, "zero"),
        (1, 2048, 256, 2, 512, "all"),
    ]:
        q = (torch.randn(b, s, 4, d) * 0.12).bfloat16().cuda()
        k = (torch.randn(b, s, kh, d) * 0.12).bfloat16().cuda()
        v = torch.randn(b, s, kh, d).bfloat16().cuda()
        mask = torch.ones(b, s, dtype=torch.int64, device="cuda")
        if kind == "holes":
            mask[:, ::7] = 0
        if kind == "prefix":
            mask[:, s * 3 // 4 :] = 0
        if kind == "zero":
            mask.zero_()
        originals = [x.clone() for x in (q, k, v, mask)]
        n = q.numel()
        guards = [
            torch.full((n + 32,), 17, dtype=torch.bfloat16, device="cuda")
            for _ in kernels
        ]
        outs = [x[16:-16].view_as(q) for x in guards]
        for kernel, out in zip(kernels, outs):
            kernel.run(out, q, k, v, mask, radius)
        torch.cuda.synchronize()
        expected = oracle(q, k, v, mask, radius)
        for out, guard in zip(outs, guards):
            assert torch.isfinite(out).all()
            assert (guard[:16] == 17).all() and (guard[-16:] == 17).all()
            torch.testing.assert_close(
                out.cpu().float(), expected, atol=0.003, rtol=0.03
            )
        for x, original in zip((q, k, v, mask), originals):
            assert torch.equal(x, original), "input modified"
        if args.baseline:
            assert torch.equal(outs[0], outs[-1]), float(
                (outs[0].float() - outs[-1].float()).abs().max()
            )
        cases.append(
            dict(
                batch=b,
                seq=s,
                dim=d,
                mask=kind,
                baseline_bitwise=True if args.baseline else None,
                output_guards=True,
                readonly_inputs=True,
                max_abs_oracle_error=float(
                    (outs[-1].cpu().float() - expected).abs().max()
                ),
            )
        )
    timings = []
    for b, s, d, kh, radius in (
        [
            (1, 512, 256, 2, 512),
            (4, 512, 256, 2, 512),
            (1, 2048, 256, 2, 512),
            (1, 512, 512, 1, 0),
            (4, 512, 512, 1, 0),
        ]
        if args.benchmark and args.baseline
        else []
    ):
        q = (torch.randn(b, s, 4, d) * 0.12).bfloat16().cuda()
        k = (torch.randn(b, s, kh, d) * 0.12).bfloat16().cuda()
        v = torch.randn_like(k)
        mask = torch.ones(b, s, dtype=torch.int64, device="cuda")
        out = torch.empty_like(q)
        rounds = [[], []]
        for round_index in range(7):
            for i in (
                range(len(kernels))
                if round_index % 2 == 0
                else reversed(range(len(kernels)))
            ):
                kernel = kernels[i]
                for _ in range(3):
                    kernel.run(out, q, k, v, mask, radius)
                start, end = (
                    torch.cuda.Event(enable_timing=True),
                    torch.cuda.Event(enable_timing=True),
                )
                start.record()
                for _ in range(100):
                    kernel.run(out, q, k, v, mask, radius)
                end.record()
                end.synchronize()
                rounds[i].append(start.elapsed_time(end) / 100)
        base, new = map(statistics.median, rounds)
        timings.append(
            dict(
                batch=b,
                seq=s,
                dim=d,
                baseline_ms=base,
                portable_ms=new,
                speedup=base / new,
                rounds=rounds,
            )
        )
    receipt = dict(
        passed=True,
        artifact_sha256=hashlib.sha256(args.artifact.read_bytes()).hexdigest(),
        baseline_sha256=hashlib.sha256(args.baseline.read_bytes()).hexdigest()
        if args.baseline
        else None,
        gpu=torch.cuda.get_device_name(),
        capability=torch.cuda.get_device_capability(),
        cases=cases,
        timings=timings,
    )
    args.output.write_text(json.dumps(receipt, indent=2))
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
