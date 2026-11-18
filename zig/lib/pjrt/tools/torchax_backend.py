# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""CUDA reference adapter used for numerical oracles, never native timings."""

import numpy as np
import torch
import ml_dtypes
import jax
import torchax


class Backend:
    def __init__(self):
        if jax.devices()[0].platform != "gpu":
            raise RuntimeError("Reference requires the CUDA JAX plugin")
        self.env = torchax.default_env()
        self.jax, self.ax, self.device = jax, torchax, "jax"

    def context(self):
        return self.env

    def upload(self, tensor):
        # Preserve BF16 bits without an implicit host FP32 expansion.
        array = (
            tensor.view(torch.uint16).numpy().view(ml_dtypes.bfloat16)
            if tensor.dtype == torch.bfloat16
            else tensor.numpy()
        )
        device = jax.device_put(array, jax.devices()[0])
        device.block_until_ready()
        return self.env.j2t_iso(device)

    def sync(self, tensor):
        self.env.t2j_iso(tensor).block_until_ready()

    def cpu(self, tensor):
        self.sync(tensor)
        return torch.from_numpy(np.asarray(self.env.t2j_iso(tensor)).astype("float32"))
