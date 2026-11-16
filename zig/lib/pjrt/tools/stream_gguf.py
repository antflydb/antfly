# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Stream dense parameter bytes in manifest order, using bounded dequant chunks."""

import json, sys, mmap
from pathlib import Path
import numpy as np, ml_dtypes
from gguf import GGUFReader, dequantize, GGML_QUANT_SIZES

spec = json.loads(Path(sys.argv[1]).read_text())
reader = GGUFReader(sys.argv[2])
tensors = {t.name: t for t in reader.tensors}
out = sys.stdout.buffer
for i, p in enumerate(spec["parameters"]):
    dtype = ml_dtypes.bfloat16 if p["dtype"] == "bfloat16" else np.float32
    if "data" in p:
        out.write(np.asarray(p["data"], dtype=dtype).tobytes())
        continue
    t = tensors[p["source"]]
    assert int(np.prod(t.shape)) == int(np.prod(p["shape"]))
    block, bs = GGML_QUANT_SIZES[t.tensor_type]
    flat = t.data.reshape(-1)
    if block == 1:
        for start in range(0, len(flat), 1 << 22):
            out.write(
                dequantize(flat[start : start + (1 << 22)], t.tensor_type)
                .reshape(-1)
                .astype(dtype)
                .tobytes()
            )
    else:
        data = flat.view(np.uint8)
        step = ((1 << 22) // block) * bs
        for start in range(0, len(data), step):
            out.write(
                dequantize(data[start : start + step], t.tensor_type)
                .reshape(-1)
                .astype(dtype)
                .tobytes()
            )
    out.flush()
    try:
        reader.data._mmap.madvise(mmap.MADV_DONTNEED)
    except AttributeError:
        pass
    if i % 50 == 0:
        print("streamed", i, p["name"], file=sys.stderr, flush=True)
