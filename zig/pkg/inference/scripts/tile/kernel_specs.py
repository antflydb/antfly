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

"""Launch specs for check_kernels.py: per family, a sweep `grid` (always
rows, dim, threads, variant) and per kernel its buffers, params and launch.

Expressions are C++ over the grid variables. Buffers map a parameter name to
(element count, fill statement for element i of `q`); outputs only need the
count. `params` sets fields of a Metal `constant` struct, `scalars` sets
scalar arguments (CUDA) or `constant T&` scalars (Metal). `when` filters the
sweep. `launch` gives `groups` (x or (x, y)) and `threads`.
"""

RAND = "q[i] = std::uniform_real_distribution<float>(-3.0f, 3.0f)(rng)"
RAND_POS = "q[i] = std::uniform_real_distribution<float>(0.0f, 3.0f)(rng)"
HALF = "q[i] = (half)std::uniform_real_distribution<float>(-3.0f, 3.0f)(rng)"
# Suppress lists: 9 ids in [-2, dim + 2), including negatives and duplicates.
IDS = "q[i] = (int)(rng() % (unsigned)(dim + 4)) - 2"
EPS = "(variant == 2 ? 1e-6f : 1e-5f)"
SCALE = "(variant == 0 ? 1.0f : 0.75f)"

GRID = {"rows": [1, 3], "dim": [1, 4, 6, 7, 64, 100, 384, 1000, 1024], "threads": [32, 64, 128, 256], "variant": [0, 1, 2]}
SERIAL = "threads == 32"  # serial kernels ignore the block size; run them once


def _rows(n="rows"):
    return f"{n} * dim"


def _metal_norm(params, buffers, groups, *, threads="threads", when="true", scalars=None):
    return {"params": params, "buffers": buffers, "launch": {"groups": groups, "threads": threads}, "when": when, **({"scalars": scalars} if scalars else {})}


_one = {"input": ("dim", RAND), "weight": ("dim", RAND), "output": ("dim", None)}
_one_res = {**_one, "residual": ("dim", RAND)}
_many = {"input": (_rows(), RAND), "weight": ("dim", RAND), "output": (_rows(), None)}
_many_res = {**_many, "residual": (_rows(), RAND)}
_sumsq_one = {"sumsq": ("(dim + 7) / 8", RAND_POS)}
_sumsq_many = {"sumsq": ("rows * ((dim + 7) / 8)", RAND_POS)}
_layer = {"hidden_size": "dim", "eps": EPS}
_layer_scale = {**_layer, "scale": SCALE}
_row = {"rows": "rows", "hidden_size": "dim", "eps": EPS}
_row_scale = {**_row, "scale": SCALE}
_simd4 = "dim % 4 == 0 || dim == 6"


def _cuda_norm(buffers):
    return {"backend": "cuda", "buffers": buffers, "scalars": {"rows": "rows", "dim": "dim", "eps": EPS}, "launch": {"groups": "rows + 1", "threads": "threads"}}


_c = {"dst": (_rows(), None), "input": (_rows(), RAND), "weight": ("dim", RAND)}
_c_res = {**_c, "residual": (_rows(), RAND)}
_c_scalar = {**_c_res, "scalar": ("1", "q[i] = 0.8125f")}

rms_norm = {
    "grid": GRID,
    "kernels": {
        "termite_apply_rms_norm_1x": _metal_norm(_layer, _one, "1", threads="3", when=SERIAL),
        "termite_apply_rms_norm_1x_reduce": _metal_norm(_layer, _one, "1"),
        "termite_compute_rms_inv_scale_1x": _metal_norm(_layer, {"input": ("dim", RAND), "inv_scale": ("1", None)}, "1"),
        "termite_apply_rms_norm_rows": _metal_norm(_row, _many, "1", threads="rows + 1", when=SERIAL),
        "termite_apply_rms_norm_rows_reduce": _metal_norm(_row, _many, "rows + 1"),
        "termite_apply_rms_norm_rows_simd4_reduce": _metal_norm(_row, _many, "rows + 1", when=_simd4),
        "antfly_norm_clamp": _metal_norm({"rows": "rows", "dim": "dim", "flags": "(uint)variant", "eps": EPS, "lo": "-0.5f", "hi": "0.75f"}, _many, "rows + 1", when=_simd4),
        "termite_apply_rms_norm_rows_triple_simd4_reduce": _metal_norm(_row, {"input": (_rows(), RAND), **{f"{w}_weight": ("dim", RAND) for w in ("first", "second", "third")}, **{f"{w}_output": (_rows(), None) for w in ("first", "second", "third")}}, "rows + 1", when=_simd4),
        "termite_apply_rms_norm_add_1x": _metal_norm(_layer, _one_res, "1"),
        "termite_apply_rms_norm_add_rows_canonical": _metal_norm(_layer, _many_res, "rows"),
        "termite_apply_rms_norm_add_1x_sumsq": _metal_norm(_layer, {**_one_res, **_sumsq_one}, "1"),
        "termite_apply_rms_norm_add_rows_sumsq": _metal_norm(_layer, {**_many_res, **_sumsq_many}, "rows"),
        "termite_apply_rms_norm_add_scale_1x_sumsq": _metal_norm(_layer_scale, {**_one_res, **_sumsq_one}, "1"),
        "termite_apply_rms_norm_add_1x_f16_input": _metal_norm(_layer, {**_one_res, "input": ("dim", HALF)}, "1"),
        "termite_apply_rms_norm_add_scale_1x": _metal_norm(_layer_scale, _one_res, "1"),
        "termite_apply_rms_norm_add_scale_rows": _metal_norm(_row_scale, _many_res, "rows + 1"),
        "termite_apply_rms_norm_add_scale_simd4": _metal_norm(_row_scale, _many_res, "rows + 1", when=_simd4),
        "termite_rms_norm_f32": _cuda_norm(_c),
        "termite_rms_norm_f32_bf16": _cuda_norm({**_c, "dst_bf16": (_rows(), None)}),
        "termite_rms_norm_add_f32": _cuda_norm(_c_res),
        "termite_rms_norm_add_f32_bf16": _cuda_norm({**_c_res, "dst_bf16": (_rows(), None)}),
        "termite_rms_norm_add_mul_scalar_f32": _cuda_norm(_c_scalar),
        "termite_rms_norm_add_output_scale_f32": _cuda_norm(_c_scalar),
        "termite_rms_norm_bare_f32": _cuda_norm({"dst": (_rows(), None), "input": (_rows(), RAND)}),
        "termite_gemma4_a4b_rms_norm_triple_f32": _cuda_norm({"input": (_rows(), RAND), **{w: (_rows(), None) for w in ("first", "second", "third")}, **{f"{w}_weight": ("dim", RAND) for w in ("first", "second", "third")}}),
    },
}

_ln = {"gamma": ("dim", RAND), "beta": ("dim", RAND)}
_ln_in = {"input": (_rows(), RAND), **_ln, "output": (_rows(), None)}
_ln_add = {"a": (_rows(), RAND), "b": (_rows(), RAND), **_ln, "output": (_rows(), None)}
_ln_sum = {**_ln_add, "sum_out": (_rows(), None)}
_ln_bias = {"projected": (_rows(), RAND), "bias": ("dim", RAND), "residual": (_rows(), RAND), **_ln, "output": (_rows(), None)}
# Serial "_1x" kernels index row = gid unchecked: launch exactly `rows` threads.
_ln_serial = dict(threads="rows", when=SERIAL)

layer_norm = {
    "grid": GRID,
    "kernels": {
        "termite_apply_layer_norm_1x": _metal_norm(_layer, _ln_in, "1", **_ln_serial),
        "termite_apply_layer_norm_rows": _metal_norm(_layer, _ln_in, "rows"),
        "termite_apply_add_layer_norm_1x": _metal_norm(_layer, _ln_add, "1", **_ln_serial),
        "termite_apply_add_layer_norm_sum_1x": _metal_norm(_layer, _ln_sum, "1", **_ln_serial),
        "termite_apply_add_layer_norm_rows": _metal_norm(_layer, _ln_add, "rows"),
        "termite_apply_add_layer_norm_sum_rows": _metal_norm(_layer, _ln_sum, "rows"),
        "termite_apply_bias_add_layer_norm_1x": _metal_norm(_layer, _ln_bias, "1", **_ln_serial),
        "termite_apply_bias_add_layer_norm_rows": _metal_norm(_layer, _ln_bias, "rows"),
        # One SIMD-group per row.
        "termite_gliner_boundary_ffn_bias_add_layer_norm": _metal_norm(_layer, _ln_bias, "rows", threads="32", when=SERIAL),
        "termite_layer_norm_f32": _cuda_norm({"dst": (_rows(), None), "input": (_rows(), RAND), **_ln}),
        "termite_add_layer_norm_f32": _cuda_norm({"dst": (_rows(), None), "a": (_rows(), RAND), "b": (_rows(), RAND), **_ln}),
    },
}

# Argmax: small integer values so ties are common, plus NaN and -inf.
VAL = "q[i] = (rng() % 23 == 0) ? NAN : (rng() % 19 == 0) ? -INFINITY : (float)((int)(rng() % 9) - 4)"
# Partial ids: some 0xffffffff ("no candidate"), the rest small with repeats.
PIDS = "q[i] = (rng() % 5 == 0) ? 0xffffffffu : (unsigned)(rng() % 64)"
_parts = "((dim + 1023) / 1024)"
_sup = {"out_dim": "(uint)dim", "suppress_count": "(uint)(variant * 4)"}
_sup_ids = ("9", IDS)
_pairs = lambda n: {"partial_values": (n, VAL), "partial_ids": (n, PIDS)}
_c_pairs = lambda n: {"partial_values": (n, VAL), "partial_indices": (n, PIDS)}
_c_rows = {"dst": ("rows", None), "input": ("(rows + 1) * dim", VAL)}
_c_window = {"row_start": "(variant == 1 ? 1 : 0)", "row_count": "rows", "dim": "dim"}

argmax = {
    "grid": {"rows": [1, 3], "dim": [0, 1, 7, 100, 1000, 1024, 1025, 2500], "threads": [32, 64, 128, 256], "variant": [0, 1, 2]},
    "kernels": {
        "termite_argmax_logits_1x": _metal_norm({}, {"logits": ("dim", VAL), "output": ("1", None)}, "1", threads="2", when=SERIAL, scalars={"out_dim": "dim"}),
        "termite_argmax_logits_partials": _metal_norm({}, {"logits": ("dim", VAL), "partial_values": (_parts, None), "partial_ids": (_parts, None)}, _parts, scalars={"out_dim": "dim"}),
        "termite_argmax_logits_suppress_partials": _metal_norm(_sup, {"logits": ("dim", VAL), "suppress_ids": _sup_ids, "partial_values": (_parts, None), "partial_ids": (_parts, None)}, _parts),
        "termite_lighton_batch_argmax_partials": _metal_norm(_sup, {"logits": ("rows * dim", VAL), "suppress_ids": _sup_ids, "partial_values": (f"rows * {_parts}", None), "partial_ids": (f"rows * {_parts}", None)}, (_parts, "rows")),
        "termite_argmax_logits_reduce": _metal_norm({}, {**_pairs("dim"), "output": ("1", None)}, "1", threads="2", when=SERIAL, scalars={"partial_count": "dim"}),
        "termite_lighton_batch_argmax_reduce": _metal_norm({}, {**_pairs("rows * dim"), "output": ("rows", None)}, "1", threads="rows", when=SERIAL, scalars={"partial_count": "dim"}),
        "termite_argmax_logits_reduce_tg": _metal_norm({}, {**_pairs("dim"), "output": ("1", None)}, "1", scalars={"partial_count": "dim"}),
        "termite_argmax_last_row_f32": {"backend": "cuda", "buffers": {"dst": ("1", None), "input": ("rows * dim", VAL)}, "scalars": {"rows": "rows", "dim": "dim"}, "launch": {"groups": "1", "threads": "threads"}},
        "termite_argmax_last_row_suppress_f32": {"backend": "cuda", "buffers": {"dst": ("1", None), "input": ("rows * dim", VAL), "suppress_token_ids": _sup_ids}, "scalars": {"rows": "rows", "dim": "dim", "suppress_count": "variant * 4"}, "launch": {"groups": "1", "threads": "threads"}},
        "termite_argmax_rows_f32": {"backend": "cuda", "buffers": _c_rows, "scalars": _c_window, "launch": {"groups": "rows + 1", "threads": "threads"}},
        "termite_argmax_rows_suppress_f32": {"backend": "cuda", "buffers": {**_c_rows, "suppress_token_ids": _sup_ids}, "scalars": {**_c_window, "suppress_count": "variant * 4"}, "launch": {"groups": "rows + 1", "threads": "threads"}},
        "termite_argmax_reduce_rows_pairs_f32": {"backend": "cuda", "buffers": {"dst": ("rows", None), **_c_pairs("rows * dim")}, "scalars": {"rows": "rows", "col_tiles": "dim"}, "launch": {"groups": "rows + 1", "threads": "threads"}},
        "termite_argmax_reduce_rows_pairs_f32_w16": {"backend": "cuda", "buffers": {"dst": ("rows", None), **_c_pairs("rows * dim")}, "scalars": {"rows": "rows", "col_tiles": "dim"}, "launch": {"groups": "rows + 1", "threads": "threads"}},
        "termite_argmax_reduce_pairs_f32": {"backend": "cuda", "buffers": {"dst": ("1", None), **_c_pairs("dim")}, "scalars": {"count": "dim"}, "launch": {"groups": "1", "threads": "threads"}},
    },
}
