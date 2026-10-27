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

"""Check Florence cross attention against F64 under uneven warp scheduling.

Requires CUDA 13.0, a CUDA GPU and PyTorch for qualification only. The delayed
variant changes scheduling without changing arithmetic, inputs or tolerances.
"""

import argparse
import ctypes as c
import hashlib
import json
from pathlib import Path
import subprocess


def main():
    package = Path(__file__).resolve().parents[2]
    repository = package.parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=Path,
        default=package / "src/ops/cuda/prototypes/florence_fused.cu",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=repository / ".cache/florence-cuda-schedule"
    )
    parser.add_argument("--nvcc", default="/usr/local/cuda/bin/nvcc")
    args = parser.parse_args()
    output = args.output_dir.resolve()
    if not output.is_relative_to(repository):
        parser.error("qualification output must be inside the worktree")
    compiler = subprocess.check_output([args.nvcc, "--version"], text=True)
    if "release 13.0" not in compiler:
        parser.error("use CUDA 13.0 for the reproducible PTX image")
    output.mkdir(parents=True, exist_ok=True)
    source = args.source.read_text()
    marker = "    const float maximum = scratch[0];\n"
    if source.count(marker) != 1:
        raise RuntimeError("schedule probe location changed")
    delayed = output / "delayed.cu"
    delayed.write_text(
        source.replace(marker, "    if (warp == 7) __nanosleep(10000);\n" + marker)
    )
    for name, path in [("normal", args.source.resolve()), ("delayed", delayed)]:
        with (output / (name + "-build.txt")).open("w") as log:
            subprocess.run(
                [
                    args.nvcc,
                    "-ptx",
                    "-arch=compute_80",
                    "-O3",
                    str(path),
                    "-o",
                    str(output / (name + ".ptx")),
                ],
                check=True,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
    import torch
    from check_cuda_recurrent import Driver

    torch.set_num_threads(2)
    torch.cuda.init()
    torch.backends.cuda.matmul.allow_tf32 = False
    records = []
    for name in ["normal", "delayed"]:
        gpu = Driver(output / (name + ".ptx"))
        try:
            for batch, sequence in [
                (1, 1),
                (1, 17),
                (3, 586),
                (16, 587),
                (64, 586),
                (3, 1024),
            ]:

                def values(count, mul, mod, shift, denominator):
                    return (
                        (torch.arange(count, device="cuda", dtype=torch.int64) * mul)
                        % mod
                        - shift
                    ).float() / denominator

                q = values(batch * 768, 13, 67, 33, 19).reshape(batch, 12, 64)
                k = values(batch * sequence * 768, 7, 79, 39, 23).reshape(
                    batch, sequence, 12, 64
                )
                v = values(k.numel(), 17, 71, 35, 17).reshape_as(k)
                snapshots = [x.clone() for x in (q, k, v)]
                scores = (
                    q.double()[:, :, None, :] * k.double().permute(0, 2, 1, 3)
                ).sum(-1) * 0.125
                probability = torch.softmax(scores, dim=-1)
                expected = (
                    (probability[:, :, :, None] * v.double().permute(0, 2, 1, 3))
                    .sum(2)
                    .float()
                )
                slab = torch.full((q.numel() + 16,), 123456.0, device="cuda")
                actual = slab[:-16].reshape_as(q)
                torch.cuda.synchronize()
                worst = 0.0
                for _ in range(12):
                    pointers = [
                        (c.c_uint64(x.data_ptr()), 0) for x in (actual, q, k, v)
                    ]
                    gpu.launch(
                        "florence_cross_q1_f32",
                        (batch * 12, 1),
                        256,
                        *pointers,
                        batch,
                        sequence,
                    )
                    torch.cuda.synchronize()
                    assert torch.isfinite(actual).all()
                    assert torch.equal(
                        slab[-16:], torch.full_like(slab[-16:], 123456.0)
                    ), "output guard overwritten"
                    worst = max(worst, float((actual - expected).abs().max()))
                assert all(torch.equal(x, y) for x, y in zip((q, k, v), snapshots)), (
                    "input changed"
                )
                row = {
                    "variant": name,
                    "batch": batch,
                    "sequence": sequence,
                    "repeats": 12,
                    "max_abs": worst,
                    "original_atol": 2e-5,
                    "passed": worst <= 2e-5,
                    "readonly": True,
                    "guards": True,
                }
                records.append(row)
                print(json.dumps(row), flush=True)
                del q, k, v, snapshots, scores, probability, expected, slab, actual
        finally:
            gpu.close()
    report = {
        "scope": "Independent F64 oracle and scheduling perturbation. Both variants retain the original 2e-5 threshold. No throughput claim.",
        "compiler": compiler,
        "source_sha256": hashlib.sha256(args.source.read_bytes()).hexdigest(),
        "artifacts": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in output.iterdir()
            if p.suffix in {".cu", ".ptx"}
        },
        "records": records,
    }
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    if not all(row["passed"] for row in records):
        raise SystemExit(
            "Florence schedule qualification failed the original F64 tolerance"
        )
    print("Florence normal and delayed-warp kernels passed the F64 oracle", flush=True)


if __name__ == "__main__":
    main()
