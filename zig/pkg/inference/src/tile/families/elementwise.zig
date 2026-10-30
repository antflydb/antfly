// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Generated kernel regions of the `elementwise` families: one thread per
//! element maps, activation gradients, AdamW, LoRA, layout shuffles and
//! template wrappers. Every kernel renders to its previous hand-written text.

const std = @import("std");
const r = @import("../region.zig");
const e = @import("../elementwise.zig");

const Region = r.Region;
const fmt = std.fmt.comptimePrint;
const metal = e.metal;
const map = e.metalMap;

// ---- Metal: GELU backward and gated activation (main library)

const act_params = "termite_metal_apply_activation_params &p";
const rows_dim = "uint total = p.rows * p.dim; if (gid >= total) return;\n";
const rows_dim_nz = "uint total = p.rows * p.dim; if (gid >= total || p.dim == 0u) return;\n";

/// `out[gid] = upstream * gelu'(x)` for x = `input[gid]`; kind 1 is the erf
/// GELU, anything else the tanh approximation (saturating beyond |inner| 10).
fn geluBackward(comptime input: []const u8, comptime out: []const u8, comptime upstream: []const u8) []const u8 {
    return fmt(
        \\float x = {0s}[gid];
        \\if (!isfinite(x)) {{ {1s}[gid] = 0.0f; return; }}
        \\float x2 = x * x;
        \\float derivative = 0.0f;
        \\if (p.activation_kind == 1u) {{
        \\    float cdf = 0.5f * (1.0f + termite_erf_approx(x * 0.7071067811865476f));
        \\    float pdf = exp(-0.5f * x2) * 0.3989422804014327f;
        \\    derivative = cdf + x * pdf;
        \\}} else {{
        \\    float inner = 0.7978845608028654f * (x + 0.044715f * x * x2);
        \\    if (inner > 10.0f) {{ {1s}[gid] = {2s}; return; }}
        \\    if (inner < -10.0f) {{ {1s}[gid] = 0.0f; return; }}
        \\    float t = tanh(inner);
        \\    float sech2 = 1.0f - t * t;
        \\    derivative = 0.5f * (1.0f + t) + 0.5f * x * sech2 * 0.7978845608028654f * (1.0f + 0.134145f * x2);
        \\}}
        \\{1s}[gid] = isfinite(derivative) ? {2s} * derivative : 0.0f;
        \\
    , .{ input, out, upstream });
}

/// FFN backward through a rank-1 product: first = first_lhs[row] *
/// first_rhs[col], `upstream` = first + the second branch, then the GELU
/// gradient of `gelu_input` scaled by upstream.
fn rank1(comptime name: []const u8, comptime buffers: []const u8, comptime upstream: []const u8, comptime stores: []const u8) []const u8 {
    return map(name, "const float *first_lhs, const float *first_rhs, " ++ buffers ++ ", " ++ act_params, rows_dim_nz ++
        \\uint row = gid / p.dim; uint col = gid - row * p.dim;
        \\float first = first_lhs[row] * first_rhs[col];
        \\
    ++ upstream ++ "first_output[gid] = first;\n" ++ stores ++ geluBackward("gelu_input", "gelu_output", "upstream"));
}

/// act(gate) * up, with an exact zero when the activation is zero.
fn activationMultiply(comptime name: []const u8, comptime out: []const u8) []const u8 {
    const value = "y == 0.0f ? 0.0f : y * up[gid]";
    return map(name, "const float *gate, const float *up, " ++ out ++ " *output, " ++ act_params, rows_dim ++
        "float y = termite_apply_activation_scalar(gate[gid], p.activation_kind);\n" ++
        "output[gid] = " ++ (if (std.mem.eql(u8, out, "float")) value else out ++ "(" ++ value ++ ")") ++ ";\n");
}

const gelu_backward = e.join(&.{
    map("termite_apply_gelu_backward_1x", "const float *input, const float *upstream_grad, float *output, " ++ act_params, rows_dim ++ geluBackward("input", "output", "upstream_grad[gid]")),
    rank1("termite_ffn_gelu_backward_rank1_branches", "const float *second_lhs, const float *second_rhs, const float *gelu_input, float *first_output, float *second_output, float *upstream_output, float *gelu_output",
        \\float second = second_lhs[row] * second_rhs[col];
        \\float upstream = first + second;
        \\
    ,
        \\second_output[gid] = second;
        \\upstream_output[gid] = upstream;
        \\
    ),
    rank1("termite_ffn_gelu_backward_rank1_output", "const float *second_lhs, const float *second_rhs, const float *gelu_input, float *first_output, float *gelu_output", "float upstream = first + second_lhs[row] * second_rhs[col];\n", ""),
    rank1("termite_ffn_gelu_backward_rank1_first_output", "const float *second_output, const float *gelu_input, float *first_output, float *gelu_output", "float upstream = first + second_output[gid];\n", ""),
    rank1("termite_ffn_gelu_backward_rank1_first_branch", "const float *second_output, const float *gelu_input, float *first_output, float *upstream_output, float *gelu_output", "float upstream = first + second_output[gid];\n", "upstream_output[gid] = upstream;\n"),
    activationMultiply("termite_apply_activation_multiply_1x", "float"),
    activationMultiply("termite_apply_activation_multiply_f16_output", "half"),
});

// ---- Metal: binary maps and the training updates (main library)

const add_params = "termite_metal_apply_add_params &p";

/// `lhs op rhs`; flag 1 broadcasts lhs[0], flag 2 rhs[0], flag 4 rhs[col].
fn broadcastBinary(comptime name: []const u8, comptime op: []const u8) []const u8 {
    return map(name, "const float *lhs, const float *rhs, float *output, " ++ add_params, rows_dim ++
        \\uint col = p.dim == 0u ? gid : gid % p.dim;
        \\float l = lhs[(p.flags & 1u) != 0u ? 0u : gid]; float r = rhs[(p.flags & 2u) != 0u ? 0u : ((p.flags & 4u) != 0u ? col : gid)];
        \\
    ++ "output[gid] = l " ++ op ++ " r;\n");
}

/// `output[gid] = value` over rows * dim.
fn apply(comptime name: []const u8, comptime buffers: []const u8, comptime value: []const u8) []const u8 {
    return map(name, buffers, rows_dim ++ "output[gid] = " ++ value ++ ";\n");
}

/// AdamW on one element; `into` writes the weight and moments to new
/// buffers so the inputs stay intact until the caller publishes, and raises
/// flag[0] if any new value is not finite (every writer stores 1, so the
/// race is benign). The gradient is zeroed either way.
fn adamw(comptime into: bool) []const u8 {
    const m_out = if (into) "m_out" else "m";
    const v_out = if (into) "v_out" else "v";
    return map(
        if (into) "termite_training_adamw_into_f32" else "termite_training_adamw_f32",
        if (into)
            "const float *weight, float *grad, const float *m, const float *v, float *weight_out, float *m_out, float *v_out, termite_metal_training_adamw_params &p, float *flag"
        else
            "float *weight, float *grad, float *m, float *v, termite_metal_training_adamw_params &p",
        fmt(
            \\if (gid >= p.elem_count) return;
            \\float g = grad[gid] * p.grad_scale;
            \\float mv = p.beta1 * m[gid] + (1.0f - p.beta1) * g;
            \\float vv = p.beta2 * v[gid] + (1.0f - p.beta2) * g * g;
            \\{s}[gid] = mv;
            \\{s}[gid] = vv;
            \\float mh = mv / p.bias_correction1;
            \\float vh = vv / p.bias_correction2;
            \\
        , .{ m_out, v_out }) ++ (if (into)
            \\float w = weight[gid];
            \\float update = mh / (sqrt(vh) + p.eps) + p.weight_decay * w;
            \\float w_new = w - p.lr * update;
            \\weight_out[gid] = w_new;
            \\grad[gid] = 0.0f;
            \\if (!isfinite(w_new) || !isfinite(mv) || !isfinite(vv)) flag[0] = 1.0f;
            \\
        else
            \\float update = mh / (sqrt(vh) + p.eps) + p.weight_decay * weight[gid];
            \\weight[gid] -= p.lr * update;
            \\grad[gid] = 0.0f;
            \\
        ),
    );
}

const apply_kernels = e.join(&.{
    broadcastBinary("termite_apply_add_1x", "+"),
    apply("termite_apply_add_scale_1x", "const float *lhs, const float *rhs, float *output, " ++ add_params ++ ", float &scale", "(lhs[gid] + rhs[gid]) * scale"),
    apply("termite_apply_scaled_add_scale_1x", "const float *lhs, const float *rhs, float *output, " ++ add_params ++ ", float2 &scales", "(lhs[gid] * scales.x + rhs[gid]) * scales.y"),
    apply("termite_apply_multiply_add_1x", "const float *lhs, const float *rhs, const float *addend, float *output, " ++ add_params, "lhs[gid] * rhs[gid] + addend[gid]"),
    apply("termite_apply_multiply_add2_1x", "const float *lhs0, const float *rhs0, const float *lhs1, const float *rhs1, float *output, " ++ add_params, "lhs0[gid] * rhs0[gid] + lhs1[gid] * rhs1[gid]"),
    map("termite_training_accumulate_f32", "float *accum, const float *grad, termite_metal_training_accumulate_params &p",
        \\if (gid >= p.elem_count) return;
        \\float g = grad[gid] * p.scale;
        \\accum[gid] = (p.first != 0u) ? g : (accum[gid] + g);
        \\
    ),
    adamw(false),
    // Out of place: (previous + gradient / divisor) * renormalize, each term
    // only when its flag is set, in the same operation order as separate
    // divide, add and multiply passes.
    map("termite_training_accumulate_into_f32", "const float *previous, const float *grad, float *output, termite_metal_training_accumulate_into_params &p",
        \\if (gid >= p.elem_count) return;
        \\float value = (p.flags & 1u) != 0u ? previous[gid] : 0.0f;
        \\if ((p.flags & 2u) != 0u) value = value + grad[gid] / p.divisor;
        \\if ((p.flags & 4u) != 0u) value = value * p.renormalize;
        \\output[gid] = value;
        \\
    ),
    adamw(true),
    broadcastBinary("termite_apply_multiply_1x", "*"),
    map("termite_apply_scale_1x", "const float *input, float *output, float &scale", "output[gid] = input[gid] * scale;\n"),
});

// ---- Metal: LoRA forward and gradients (main library)

const lora_params = "termite_metal_lora_linear_f32_params &p";

/// One thread per element of an `outer` x `inner` result (indices `i`, `j`),
/// skipped when the reduction length `k` is 0; `acc` starts at zero.
fn loraMap(comptime name: []const u8, comptime buffers: []const u8, comptime outer: [2][]const u8, comptime inner: [2][]const u8, comptime k: []const u8, comptime body: []const u8) []const u8 {
    return map(name, buffers ++ ", " ++ lora_params, fmt("uint total = {0s} * {1s}; if (gid >= total || {2s} == 0u) return; uint {3s} = gid / {1s}; uint {4s} = gid - {3s} * {1s}; float acc = 0.0f;\n", .{ outer[1], inner[1], k, outer[0], inner[0] }) ++ body);
}

/// A gradient summed over rows: `dst[gid] = sum(a * b) * scale`.
fn loraRows(comptime name: []const u8, comptime buffers: []const u8, comptime outer: [2][]const u8, comptime inner: [2][]const u8, comptime ab: []const u8, comptime store: []const u8) []const u8 {
    return loraMap(name, buffers, outer, inner, "p.rows", "for (uint row = 0u; row < p.rows; ++row) acc += " ++ ab ++ ";\n" ++ store ++ "\n");
}

const lora_row = .{ "row", "p.rows" };
const lora_rk = .{ "rk", "p.rank" };
const lora_out = .{ "out", "p.out_dim" };

const simd_args = "threadgroup float *partials [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint group [[threadgroup_position_in_grid]]";

/// Eight simdgroups accumulate `acc += output_grad[row, out] * <b>` over out.
fn simdOverOut(comptime b: []const u8) []const u8 {
    return "for (uint out = uint(sgitg) * 32u + uint(lane); out < p.out_dim; out += 256u) acc += output_grad[row * p.out_dim + out] * " ++ b ++ ";\n";
}

/// Sums `acc` over the threadgroup and stores `dst = total * scale`.
fn simdStore(comptime dst: []const u8) []const u8 {
    return
    \\if (sgitg == 0u) partials[lane] = 0.0f; acc = simd_sum(acc); threadgroup_barrier(mem_flags::mem_threadgroup);
    \\if (lane == 0u) partials[sgitg] = acc; threadgroup_barrier(mem_flags::mem_threadgroup);
    \\
    ++ "float total_acc = simd_sum(partials[lane]); if (lane == 0u && sgitg == 0u) " ++ dst ++ " = total_acc * p.scale;\n";
}

const lora_kernels = e.join(&.{
    loraMap("termite_lora_after_a_f32", "const float *input, const float *lora_a, float *after_a", lora_row, lora_rk, "p.in_dim",
        \\uint in_base = row * p.in_dim; uint a_base = rk * p.in_dim;
        \\for (uint i = 0u; i < p.in_dim; ++i) acc += input[in_base + i] * lora_a[a_base + i];
        \\after_a[gid] = acc;
        \\
    ),
    loraMap("termite_lora_finish_f32", "const float *base, const float *after_a, const float *lora_b, float *after_b, float *output", lora_row, lora_out, "p.rank",
        \\uint a_base = row * p.rank; uint b_base = out * p.rank;
        \\for (uint rk = 0u; rk < p.rank; ++rk) acc += after_a[a_base + rk] * lora_b[b_base + rk];
        \\after_b[gid] = acc; output[gid] = base[gid] + acc * p.scale;
        \\
    ),
    loraRows("termite_lora_grad_b_f32", "const float *output_grad, const float *after_a, float *grad_b", lora_out, lora_rk, "output_grad[row * p.out_dim + out] * after_a[row * p.rank + rk]", "grad_b[gid] = acc * p.scale;"),
    loraRows("termite_lora_grad_b_transposed_f32", "const float *output_grad, const float *after_a, float *grad_b_t", lora_rk, lora_out, "after_a[row * p.rank + rk] * output_grad[row * p.out_dim + out]", "grad_b_t[gid] = acc * p.scale;"),
    metal("termite_lora_grad_after_a_f32", "const float *output_grad, const float *lora_b, float *grad_after_a, " ++ lora_params, simd_args, "uint total = p.rows * p.rank; if (group >= total || p.out_dim == 0u) return; uint row = group / p.rank; uint rk = group - row * p.rank; float acc = 0.0f;\n" ++
        simdOverOut("lora_b[out * p.rank + rk]") ++ simdStore("grad_after_a[group]")),
    loraRows("termite_lora_grad_a_f32", "const float *grad_after_a, const float *input, float *grad_a", lora_rk, .{ "in_col", "p.in_dim" }, "grad_after_a[row * p.rank + rk] * input[row * p.in_dim + in_col]", "grad_a[gid] = acc;"),
    // Rank 1: groups [0, out_dim) each reduce one grad_b entry on one
    // thread; the remaining rows groups reduce grad_after_a.
    metal("termite_lora_rank1_grad_b_after_a_f32", "const float *output_grad, const float *after_a, const float *lora_b, float *grad_b, float *grad_after_a, " ++ lora_params, simd_args,
        \\if (p.rank != 1u || p.rows == 0u || p.out_dim == 0u) return; uint total_grad_b = p.out_dim; uint total = total_grad_b + p.rows; if (group >= total) return;
        \\if (group < total_grad_b) { if (lane == 0u && sgitg == 0u) { uint out = group; float acc = 0.0f; for (uint row = 0u; row < p.rows; ++row) acc += output_grad[row * p.out_dim + out] * after_a[row]; grad_b[out] = acc * p.scale; } return; }
        \\
    ++ "uint row = group - total_grad_b; float acc = 0.0f; " ++ simdOverOut("lora_b[out]") ++ simdStore("grad_after_a[row]")),
});

// ---- Metal: LightOn tensor-op template instances (own libraries)

/// `kernel void <name>(<sig>){<call><<targs>>(<call args>);}` per entry.
fn wrappers(comptime sig: []const u8, comptime call: []const u8, comptime args: []const u8, comptime list: []const [2][]const u8) []const u8 {
    var s: []const u8 = "";
    for (list) |w| s = s ++ "kernel void " ++ w[0] ++ "(" ++ sig ++ "){" ++ call ++ "<" ++ w[1] ++ ">(" ++ args ++ ");}\n";
    return s;
}

const lighton_indent = "        ";
const lighton_sig = "device float*gb[[buffer(3)]],device float*ub[[buffer(4)]],device {s}*y[[buffer(5)]],constant Params&p[[buffer(6)]],uint2 group[[threadgroup_position_in_grid]]";

const lightonocr3_gate_up = wrappers("device float*x[[buffer(0)]],device bfloat*gw[[buffer(1)]],device bfloat*uw[[buffer(2)]]," ++ fmt(lighton_sig, .{"float"}), "termite_lightonocr3_gate_up_small_body", "x,gw,uw,gb,ub,y,p,group", &.{
    .{ "termite_lightonocr3_gate_up_small16", "16" },
    .{ "termite_lightonocr3_gate_up_small32", "32" },
    .{ "termite_lightonocr3_gate_up_wide16", "16,16" },
    .{ "termite_lightonocr3_gate_up_wide32", "16,32,2" },
    .{ "termite_lightonocr3_gate_up_wide64", "32,64,4" },
});

const lighton_prefill_gate = wrappers("device bfloat*x[[buffer(0)]],device bfloat*g[[buffer(1)]],device bfloat*u[[buffer(2)]]," ++ fmt(lighton_sig, .{"ushort"}), "termite_lighton_prefill_gate", "x,g,u,gb,ub,y,p,group", &.{
    .{ "termite_lighton_prefill_gate0", "32,64,4" },
    .{ "termite_lighton_prefill_gate1", "64,32,4" },
});

pub const regions = [_]Region{
    r.metal("elementwise.gelu_backward", r.metalLines(r.metal_indent, gelu_backward)),
    r.metal("elementwise.apply", r.metalLines(r.metal_indent, apply_kernels)),
    r.metal("elementwise.lora", r.metalLines(r.metal_indent, lora_kernels)),
    r.region(r.metal_kernels_m, lighton_indent, "elementwise.lightonocr3_gate_up", r.metalLines(lighton_indent, lightonocr3_gate_up)),
    r.region(r.metal_kernels_m, lighton_indent, "elementwise.lighton_prefill_gate", r.metalLines(lighton_indent, lighton_prefill_gate)),
};
