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
import math
import struct
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
        self.merge = c.c_void_p()
        self.call("cuModuleLoad", c.byref(self.m), c.c_char_p(bytes(artifact)))
        self.call("cuModuleGetFunction", c.byref(self.f), self.m,
                  c.c_char_p(b"termite_embedding_gemma2_attention_local_flash_bf16"))
        result = self.d.cuModuleGetFunction(
            c.byref(self.merge), self.m, c.c_char_p(b"antfly_attention_split_merge_bf16")
        )
        if result == 500:  # CUDA_ERROR_NOT_FOUND: original deployment ABI.
            self.merge = None
            self.call("cuModuleGetFunction", c.byref(self.tail), self.m,
                      c.c_char_p(b"termite_embedding_gemma2_attention_bf16"))
        elif result:
            raise RuntimeError(("cuModuleGetFunction", result))

    def call(self, name, *args):
        result = getattr(self.d, name)(*args)
        if result:
            raise RuntimeError((name, result))

        self.dimension256 = None
        if self.merge is not None:
            function = c.c_void_p()
            result = self.d.cuModuleGetFunction(c.byref(function), self.m, c.c_char_p(b"antfly_attention_efficient_bf16_hd256"))
            if result == 0:
                self.dimension256 = function
            elif result != 500:
                raise RuntimeError(("cuModuleGetFunction", result))

    @staticmethod
    def split_count(batch, heads, sequence, multiprocessors):
        # Same pinned Flash source heuristic and F32 rounding as the typed
        # producer. This is device capacity, never a GPU-name/default guess.
        f32 = lambda x: struct.unpack("<f", struct.pack("<f", x))[0]
        if multiprocessors <= 0:
            raise ValueError("missing actual multiprocessor count")
        blocks = (sequence + 63) // 64
        work, sms = batch * heads * blocks, multiprocessors * 2
        if f32(work) >= f32(f32(0.8) * f32(sms)):
            return 1
        efficiencies = []
        for n in range(1, min(128, sms, blocks) + 1):
            if n != 1 and (blocks + n - 1) // n == (blocks + n - 2) // (n - 1):
                continue
            waves = f32(f32(work * n) / f32(sms))
            efficiencies.append((n, f32(waves / math.ceil(waves))))
        maximum = max(value for _, value in efficiencies)
        return next(n for n, value in efficiencies if value >= 0.85 * maximum)

    def prepare(self, out, q, k, v, mask, radius, source_sequence=None, workspace=None):
        """Own arguments/workspace once outside clocks; callers keep inputs immutable."""
        batch, sequence, heads, dim = q.shape
        kv_heads = k.shape[2]
        source = sequence if source_sequence is None else source_sequence
        if (not isinstance(source, int) or isinstance(source, bool)
                or not 0 < source <= sequence <= 8192 or not 0 <= radius <= 8192
                or batch <= 0 or heads != 4 or (kv_heads, dim) not in ((2, 256), (1, 512))
                or k.shape != v.shape or k.shape != (batch, sequence, kv_heads, dim)
                or out.shape != q.shape or mask.shape != (batch, sequence)):
            raise ValueError("invalid attention geometry")
        if any(not x.is_cuda or not x.is_contiguous() for x in (out, q, k, v, mask)):
            raise ValueError("attention requires contiguous CUDA banks")
        if any(x.dtype != torch.bfloat16 for x in (out, q, k, v)) or mask.dtype != torch.int64:
            raise ValueError("invalid attention bank dtype")
        commands, owners = [], [out, q, k, v, mask]

        def command(function, values, grid, block, shared=0):
            arguments = (c.c_void_p * len(values))(*(c.addressof(x) for x in values))
            commands.append((function, values, arguments, grid, block, shared))

        pointers = lambda tensors: [c.c_uint64(x.data_ptr()) for x in tensors]
        integers = lambda values: [c.c_uint(x) for x in values]
        if self.merge is None:
            if source != sequence or workspace is not None:
                raise ValueError("legacy ABI has no separate logical source/workspace contract")
            values = pointers((out, q, k, v, mask)) + integers((batch, sequence, heads, kv_heads, dim, radius))
            if sequence // 64:
                command(self.f, values, (heads, sequence // 64, batch), (512, 1, 1),
                        16 * dim * 2 + 4 * 64 * 16 * 4 + 64 * 16 * 2 + 3 * 64 * 4)
            if sequence % 64:
                command(self.tail, values + integers((sequence - sequence % 64,)),
                        (sequence % 64, heads, batch), (32, 1, 1))
        else:
            sms = torch.cuda.get_device_properties(q.device).multi_processor_count
            if sms <= 0:
                raise ValueError("missing actual multiprocessor count")
            if source < sequence and bool((mask[:, source:] != 0).any()):
                raise ValueError("declared physical padding contains a live token")
            all_valid = bool((mask[:, :source] == 1).all())
            schedule = 0 if dim <= 256 and all_valid and (radius == 0 or source < radius) else 1
            splits = self.split_count(batch, heads, source, sms) if schedule == 0 else 1
            partial_ptr = lse_ptr = 0
            if splits > 1:
                count = splits * batch * heads * sequence
                required = count * (dim + 1)
                if workspace is None:
                    workspace = torch.empty(required, device=q.device, dtype=torch.float32)
                elif (not workspace.is_cuda or not workspace.is_contiguous()
                      or workspace.device != q.device or workspace.dtype != torch.float32
                      or workspace.numel() < required):
                    raise ValueError("invalid prepared split workspace")
                start, end = workspace.data_ptr(), workspace.data_ptr() + required * 4
                if any(start < tensor.data_ptr() + tensor.numel() * tensor.element_size()
                       and tensor.data_ptr() < end for tensor in (out, q, k, v, mask)):
                    raise ValueError("split workspace aliases an attention bank")
                owners.append(workspace)
                partial_ptr = workspace.data_ptr()
                lse_ptr = partial_ptr + count * dim * 4
            values = (pointers((out, q, k, v, mask))
                      + integers((batch, sequence, source, heads, kv_heads, dim, radius, schedule))
                      + [c.c_uint64(partial_ptr), c.c_uint64(lse_ptr), c.c_uint(splits), c.c_uint64(0)])
            key_tile = 64 if schedule == 0 else 128
            shared_prefix = 32 * dim * 2
            if schedule == 1 and self.dimension256 is not None:
                shared_prefix = max(16 * dim * 2, 16 * dim * 4 - 16 * key_tile * 4)
            command(self.dimension256 if dim == 256 and schedule == 1 and self.dimension256 is not None else self.f, values, (heads, (sequence + 15) // 16, batch * splits), (128, 1, 1),
                    shared_prefix + 16 * key_tile * 6 + 6 * 16 * 4)
            if splits > 1:
                values = ([c.c_uint64(out.data_ptr()), c.c_uint64(partial_ptr), c.c_uint64(lse_ptr)]
                          + integers((batch, sequence, heads, dim, splits)))
                command(self.merge, values, (batch * heads * sequence, 1, 1), (32, 1, 1))
        stream = c.c_void_p(torch.cuda.current_stream().cuda_stream)

        def invoke():
            # Both ctypes scalar boxes and the actual Torch banks must outlive
            # every launch; no device allocation or mask classification occurs here.
            _ = owners
            for function, values, arguments, grid, block, shared in commands:
                self.call("cuLaunchKernel", function, *integers(grid), *integers(block),
                          c.c_uint(shared), stream, arguments, c.c_void_p())

        return invoke

    def run(self, out, q, k, v, mask, radius):
        self.prepare(out, q, k, v, mask, radius)()


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
        # Freeze current shape/mask facts and workspace before warmups/events.
        prepared = [kernel.prepare(out, q, k, v, mask, radius) for kernel in kernels]
        rounds = [[], []]
        for round_index in range(7):
            for i in (
                range(len(kernels))
                if round_index % 2 == 0
                else reversed(range(len(kernels)))
            ):
                kernel = kernels[i]
                for _ in range(3):
                    prepared[i]()
                start, end = (
                    torch.cuda.Event(enable_timing=True),
                    torch.cuda.Event(enable_timing=True),
                )
                start.record()
                for _ in range(100):
                    prepared[i]()
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
