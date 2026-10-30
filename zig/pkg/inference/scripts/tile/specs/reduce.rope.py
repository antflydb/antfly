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

"""Metal elementwise RoPE (reduce.rope): rows = chunks, dim = head_dim.

variant picks rope_dim and the pair layout: 0 copies (rope_dim 0), 1/2 the
whole head half-split / consecutive, 3/4 half the head, 5/6 an odd rope_dim.
"""

from kernel_specs import RAND

# Inputs with the odd NaN / inf.
VAL = "q[i] = (rng() % 41 == 0) ? NAN : (rng() % 43 == 0) ? INFINITY : std::uniform_real_distribution<float>(-3.0f, 3.0f)(rng)"
POS = "q[i] = rng() % 4096u"
ROPE_DIM = "(uint)(variant == 0 ? 0 : variant <= 2 ? dim : variant <= 4 ? (dim / 2) & ~1L : (dim > 1 ? dim - 1 : 1))"
PARAMS = {"total_chunks": "(uint)rows", "head_dim": "(uint)dim", "rope_dim": ROPE_DIM, "theta": "(variant & 1) ? 10000.0f : 1000000.0f", "freq_scale": "(variant == 3 ? 0.25f : 1.0f)", "consecutive_pairs": "(uint)(variant == 2 || variant == 4 || variant == 6)"}
N = "rows * dim"
LAUNCH = {"groups": "(rows * dim + threads - 1) / threads", "threads": "threads"}

spec = {
    "grid": {"rows": [1, 3, 5], "dim": [1, 2, 3, 6, 7, 64, 100], "threads": [32, 64], "variant": [0, 1, 2, 3, 4, 5, 6]},
    "kernels": {
        "termite_apply_rope": {"params": PARAMS, "buffers": {"input": (N, VAL), "positions": ("rows", POS), "output": (N, None)}, "launch": LAUNCH},
        "termite_apply_rope_pair": {"params": PARAMS, "buffers": {"first_input": (N, VAL), "second_input": (N, RAND), "positions": ("rows", POS), "first_output": (N, None), "second_output": (N, None)}, "launch": LAUNCH},
    },
}
