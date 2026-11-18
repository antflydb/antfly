# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Bounded GGUF dequantization in manifest order; no Python GPU execution."""

import mmap

import ml_dtypes
import numpy as np
from gguf import GGML_QUANT_SIZES, GGUFReader, dequantize


def write_all(stream, payload):
    view = memoryview(payload).cast("B")
    while len(view):
        n = stream.write(view)
        if not n:
            raise RuntimeError("Worker closed parameter stream")
        view = view[n:]


def parameters(stream, manifest, gguf, mmproj):
    readers = {"gguf": GGUFReader(str(gguf)), "mmproj": GGUFReader(str(mmproj))}
    tensors = {
        kind: {t.name: t for t in reader.tensors} for kind, reader in readers.items()
    }
    for spec in manifest["parameters"]:
        dtype = ml_dtypes.bfloat16 if spec["dtype"] == "bfloat16" else np.float32
        if "data" in spec:
            write_all(stream, np.asarray(spec["data"], dtype=dtype).tobytes())
            continue
        tensor = tensors[spec["file"]][spec["source"]]
        if int(np.prod(tensor.shape, dtype=np.int64)) != int(
            np.prod(spec["shape"], dtype=np.int64)
        ):
            raise ValueError(("source shape changed", spec["name"]))
        if spec.get("transform") == "patch_nchw_to_nhwc":
            values = (
                dequantize(tensor.data, tensor.tensor_type)
                .reshape(spec["source_shape"])
                .transpose(0, 2, 3, 1)
                .reshape(spec["shape"])
            )
            write_all(stream, np.asarray(values, dtype=dtype).tobytes())
        elif spec.get("transform") is not None:
            raise ValueError(("unknown transform", spec["transform"]))
        else:
            block, size = GGML_QUANT_SIZES[tensor.tensor_type]
            data = tensor.data.reshape(-1)
            step = 1 << 22
            if block != 1:
                data = data.view(np.uint8)
                step = (step // block) * size
            for start in range(0, len(data), step):
                values = (
                    dequantize(data[start : start + step], tensor.tensor_type)
                    .reshape(-1)
                    .astype(dtype)
                )
                write_all(stream, values.tobytes())
        try:
            readers[spec["file"]].data._mmap.madvise(mmap.MADV_DONTNEED)
        except AttributeError:
            pass
    stream.flush()
