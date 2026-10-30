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

//! Metal device sampling passes over 1024-logit blocks: the nucleus
//! (top-p) pipeline (max, probabilities, mass, radix histograms of the
//! probability bits, Gumbel pick), the plain Gumbel-max sampler, and the
//! top-k samplers (insertion top-k, top-p, min-p, one xorshift draw).
//!
//! Kernels are built from shared statements (block bounds, softcap and
//! penalties, the Gumbel hash, threadgroup trees). Most are written one
//! statement list per source line (`line`); the Gumbel sampler puts one
//! statement per line (`Layout.lines`).

const std = @import("std");

const Layout = enum { line, lines };

fn join(comptime layout: Layout, comptime indent: []const u8, comptime stmts: []const []const u8) []const u8 {
    var s: []const u8 = "";
    for (stmts, 0..) |st, i| s = s ++ (if (i == 0) "" else if (layout == .lines) "\n" ++ indent else " ") ++ st;
    return s;
}

/// `head { body }`, the body one statement per line in `.lines`.
fn block(comptime layout: Layout, comptime indent: []const u8, comptime head: []const u8, comptime body: []const []const u8) []const u8 {
    const inner = indent ++ "    ";
    return head ++ " {" ++ (if (layout == .lines) "\n" ++ inner else " ") ++ join(layout, inner, body) ++ (if (layout == .lines) "\n" ++ indent else " ") ++ "}";
}

/// Kernel text: signature, then each body line indented by four spaces.
fn kernel(comptime name: []const u8, comptime args: []const u8, comptime lines: []const []const u8) []const u8 {
    var s: []const u8 = "kernel void " ++ name ++ "(" ++ args ++ ") {\n";
    for (lines) |l| s = s ++ "    " ++ l ++ "\n";
    return s ++ "}\n";
}

const barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);";

/// Threadgroup tree: `stores`, then halve the active threads with `update`.
fn tree(comptime layout: Layout, comptime indent: []const u8, comptime stores: []const []const u8, comptime update: []const []const u8) []const u8 {
    const inner = indent ++ "    ";
    const step = if (update.len == 1) "if (uint(tid) < stride) " ++ update[0] else block(layout, inner, "if (uint(tid) < stride)", update);
    return join(layout, indent, stores ++ .{ barrier, block(layout, indent, "for (uint stride = uint(tg_size) >> 1u; stride > 0u; stride >>= 1u)", &.{ step, barrier }) });
}

const sum_tree = tree(.line, "", &.{"shared[tid] = subtotal;"}, &.{"shared[tid] += shared[uint(tid) + stride];"});

/// Max of finite values in `shared[0..tg_size)` and, when the monotonic
/// softcap is deferred, of the special (non-deferred) values after them.
const hi = "shared[uint(tg_size) + uint(tid)]";
const defer_max = "if (p.defer_monotonic_max != 0u) ";
const max_tree = tree(.line, "", &.{ "shared[tid] = best;", defer_max ++ hi ++ " = special;" }, &.{
    "shared[tid] = max(shared[tid], shared[uint(tid) + stride]);",
    defer_max ++ hi ++ " = max(" ++ hi ++ ", shared[uint(tg_size) + uint(tid) + stride]);",
});

/// (value, index) argmax; ties go to the lower index.
fn argmaxTree(comptime layout: Layout, comptime indent: []const u8) []const u8 {
    return tree(layout, indent, &.{ "sh_vals[tid] = best;", "sh_ids[tid] = best_idx;" }, &.{
        "float other = sh_vals[uint(tid) + stride];",
        "uint other_idx = sh_ids[uint(tid) + stride];",
        "if (other > sh_vals[tid] || (other == sh_vals[tid] && other_idx < sh_ids[tid])) { sh_vals[tid] = other; sh_ids[tid] = other_idx; }",
    });
}

const block_size = "const uint block_size = 1024u;";
const bounds = [_][]const u8{ "uint start = block * block_size;", "uint end = min(start + block_size, p.out_dim);" };
const chunk = [_][]const u8{block_size} ++ bounds;
const chunk_loop = "for (uint i = start + uint(tid); i < end; i += uint(tg_size))";

const raw = "float raw = logits[i];";
const softcap = "if (p.final_logit_softcap > 0.0f) raw = p.final_logit_softcap * tanh(raw / p.final_logit_softcap);";
fn penalties(comptime v: []const u8) []const u8 {
    return "float " ++ v ++ " = termite_apply_sampling_penalties(raw, i, penalties, p.penalty_count, p);";
}
const scaled = "float scaled = adjusted / p.temperature;";

/// Per-(seed, token) uniform in (0, 1) for the Gumbel noise.
const hash = [_][]const u8{
    "uint h = (i ^ p.seed) * 0x9E3779B9u; h ^= h >> 16; h *= 0x85EBCA6Bu; h ^= h >> 13; h *= 0xC2B2AE35u; h ^= h >> 16;",
    "float u = (float(h >> 8) + 0.5f) * (1.0f / 16777216.0f);",
};
const keep_best = "if (score > best || (score == best && i < best_idx)) { best = score; best_idx = i; }";

/// Radix pass over the probability bits: 4 bits per pass, high bits first.
const radix = [_][]const u8{ "uint shift = 28u - state->pass * 4u;", "uint prefix_mask = state->pass == 0u ? 0u : (0xffffffffu << (32u - state->pass * 4u));" };
const prob_key = [_][]const u8{ "float prob = probs[i];", "if (!(prob > 0.0f)) continue;", "uint key = as_type<uint>(prob);" };

const params = "constant termite_metal_sample_logits_params &p";
const penalty_buffer = "device const termite_metal_penalty_entry *penalties";
const state_in = "device const termite_metal_nucleus_state *state [[buffer(3)]]";
const tid_args = "ushort tid [[thread_index_in_threadgroup]], ushort tg_size [[threads_per_threadgroup]]";
const block_arg = "uint block [[threadgroup_position_in_grid]]";
const shared_args = "threadgroup float *shared [[threadgroup(0)]], " ++ tid_args;
const pair_args = "threadgroup float *sh_vals [[threadgroup(0)]], threadgroup uint *sh_ids [[threadgroup(1)]], " ++ tid_args ++ ", " ++ block_arg;
const hist_buffers = "device const float *logits [[buffer(0)]], device const float *probs [[buffer(1)]], device float *histograms [[buffer(2)]], " ++ state_in ++ ", " ++ params ++ " [[buffer(4)]], " ++ penalty_buffer ++ " [[buffer(5)]]";

const max_partials = kernel("termite_sample_nucleus_max_partials", "device const float *logits [[buffer(0)]], device float *partials [[buffer(1)]], " ++ params ++ " [[buffer(2)]], " ++ penalty_buffer ++ " [[buffer(3)]], " ++ shared_args ++ ", " ++ block_arg, &.{
    join(.line, "", &([_][]const u8{ block_size, "const uint block_count = (p.out_dim + block_size - 1u) / block_size;" } ++ bounds ++ .{ "float best = -INFINITY;", "float special = -INFINITY;" })),
    block(.line, "", chunk_loop, &.{ raw, block(.line, "", "if (p.defer_monotonic_max != 0u && isfinite(raw)) { best = max(best, raw); } else", &.{ softcap, penalties("adjusted"), scaled, "if (isfinite(scaled)) { if (p.defer_monotonic_max != 0u) special = max(special, scaled); else best = max(best, scaled); }" }) }),
    max_tree ++ " if (tid == 0u) { partials[block] = shared[0]; if (p.defer_monotonic_max != 0u) partials[block_count + block] = shared[uint(tg_size)]; }",
});

const max_reduce = kernel("termite_sample_nucleus_max_reduce", "device const float *partials [[buffer(0)]], device termite_metal_nucleus_state *state [[buffer(1)]], constant uint &block_count [[buffer(2)]], " ++ params ++ " [[buffer(3)]], " ++ shared_args, &.{join(.line, "", &.{
    "float best = -INFINITY;",
    "float special = -INFINITY;",
    "for (uint i = uint(tid); i < block_count; i += uint(tg_size)) { best = max(best, partials[i]); if (p.defer_monotonic_max != 0u) special = max(special, partials[block_count + i]); }",
    max_tree,
    "if (tid == 0u) { float reduced = shared[0]; if (p.defer_monotonic_max != 0u) { if (isfinite(reduced)) { float transformed = p.final_logit_softcap * tanh(reduced / p.final_logit_softcap); reduced = transformed / p.temperature; } reduced = max(reduced, shared[uint(tg_size)]); } state->max_logit = reduced; state->block_count = block_count; }",
})});

const probs_partials = kernel("termite_sample_nucleus_probs_partials", "device const float *logits [[buffer(0)]], device float *probs [[buffer(1)]], device float *partials [[buffer(2)]], " ++ state_in ++ ", " ++ params ++ " [[buffer(4)]], " ++ penalty_buffer ++ " [[buffer(5)]], " ++ shared_args ++ ", " ++ block_arg, &.{
    join(.line, "", &(chunk ++ .{"float subtotal = 0.0f;"})),
    block(.line, "", chunk_loop, &.{ raw, softcap, penalties("adjusted"), scaled, "float prob = isfinite(scaled) ? exp(scaled - state->max_logit) : 0.0f;", "probs[i] = prob;", "subtotal += prob;" }),
    sum_tree ++ " if (tid == 0u) partials[block] = shared[0];",
});

const sum_reduce = kernel("termite_sample_nucleus_sum_reduce", "device const float *partials [[buffer(0)]], device termite_metal_nucleus_state *state [[buffer(1)]], constant float &top_p [[buffer(2)]], " ++ shared_args, &.{join(.line, "", &.{
    "float subtotal = 0.0f;",
    "for (uint i = uint(tid); i < state->block_count; i += uint(tg_size)) subtotal += partials[i];",
    sum_tree,
    "if (tid == 0u) { state->total_mass = shared[0]; state->target_mass = top_p * shared[0]; state->mass_above = 0.0f; state->prefix = 0u; state->cutoff_key = 0u; state->pass = 0u; }",
})});

/// One thread per bin, serial over the block.
const histogram_serial = kernel("termite_sample_nucleus_radix_histogram_serial", hist_buffers ++ ", ushort tid [[thread_index_in_threadgroup]], " ++ block_arg, &.{
    join(.line, "", &([_][]const u8{"if (tid >= 16u) return;"} ++ chunk ++ radix ++ .{"float mass = 0.0f;"})),
    block(.line, "", "for (uint i = start; i < end; ++i)", &(prob_key ++ .{"if ((key & prefix_mask) == state->prefix && ((key >> shift) & 15u) == uint(tid)) mass += prob;"})) ++ " histograms[block * 16u + uint(tid)] = mass;",
});

/// Per-thread bins, summed per SIMD-group, then across SIMD-groups.
const histogram = kernel("termite_sample_nucleus_radix_histogram", hist_buffers ++ ", threadgroup float *simd_histograms [[threadgroup(0)]], " ++ tid_args ++ ", ushort lane [[thread_index_in_simdgroup]], ushort simdgroup [[simdgroup_index_in_threadgroup]], " ++ block_arg, &.{
    join(.line, "", &(chunk ++ radix ++ .{ "thread float bins[16];", "for (uint b = 0u; b < 16u; ++b) bins[b] = 0.0f;" })),
    block(.line, "", chunk_loop, &(prob_key ++ .{ "if ((key & prefix_mask) != state->prefix) continue;", "bins[(key >> shift) & 15u] += prob;" })),
    "for (uint b = 0u; b < 16u; ++b) { float mass = simd_sum(bins[b]); if (lane == 0u) simd_histograms[uint(simdgroup) * 16u + b] = mass; } " ++ barrier,
    "if (tid < 16u) { float mass = 0.0f; uint simdgroup_count = (uint(tg_size) + 31u) >> 5u; for (uint group = 0u; group < simdgroup_count; ++group) mass += simd_histograms[group * 16u + uint(tid)]; histograms[block * 16u + uint(tid)] = mass; }",
});

/// Gumbel-max over the kept nucleus: log(prob) + Gumbel noise.
const nucleus_gumbel = kernel("termite_sample_nucleus_gumbel_partials", "device const float *probs [[buffer(0)]], device float *partial_values [[buffer(1)]], device uint *partial_ids [[buffer(2)]], " ++ state_in ++ ", " ++ params ++ " [[buffer(4)]], " ++ pair_args, &.{
    join(.line, "", &(chunk ++ .{ "float best = -INFINITY;", "uint best_idx = 0xffffffffu;" })),
    block(.line, "", chunk_loop, &([_][]const u8{ "float prob = probs[i];", "if (!(prob > 0.0f) || as_type<uint>(prob) < state->cutoff_key) continue;" } ++ hash ++ .{ "float score = log(prob) - log(-log(u));", keep_best })),
    argmaxTree(.line, "") ++ " if (tid == 0u) { bool invalid = !(state->total_mass > 0.0f) || !isfinite(state->total_mass); partial_values[block] = invalid && block == 0u ? NAN : sh_vals[0]; partial_ids[block] = invalid && block == 0u ? 0xffffffffu : sh_ids[0]; }",
});

/// Gumbel-max over all tokens: logit / temperature + Gumbel noise.
const gumbel = kernel("termite_sample_gumbel_partials", "device const float *logits [[buffer(0)]], device float *partial_values [[buffer(1)]], device uint *partial_ids [[buffer(2)]], " ++ params ++ " [[buffer(3)]], " ++ penalty_buffer ++ " [[buffer(4)]], " ++ pair_args, &.{join(.lines, "    ", &(chunk ++ .{
    "float inv_temp = p.temperature > 0.0f ? (1.0f / p.temperature) : 1.0f;",
    "float best = -INFINITY;",
    "uint best_idx = 0u;",
    block(.lines, "    ", chunk_loop, &([_][]const u8{ raw, softcap, penalties("logit") } ++ hash ++ .{ "float score = logit * inv_temp - log(-log(u));", keep_best })),
    argmaxTree(.lines, "    "),
    "if (tid == 0u) { partial_values[block] = sh_vals[0]; partial_ids[block] = sh_ids[0]; }",
}))});

// ---------------------------------------------------------------- top-k

/// Insertion into the descending `top_vals` / `top_idx` lists of length `n`.
fn insertTopK(comptime ind: []const u8, comptime n: []const u8, comptime v: []const u8, comptime id: []const u8) []const u8 {
    return std.fmt.comptimePrint(
        \\{0s}if ({2s} <= top_vals[{1s} - 1u]) continue;
        \\{0s}uint insert_at = {1s} - 1u;
        \\{0s}while (insert_at > 0u && {2s} > top_vals[insert_at - 1u]) {{
        \\{0s}    top_vals[insert_at] = top_vals[insert_at - 1u];
        \\{0s}    top_idx[insert_at] = top_idx[insert_at - 1u];
        \\{0s}    insert_at -= 1u;
        \\{0s}}}
        \\{0s}top_vals[insert_at] = {2s};
        \\{0s}top_idx[insert_at] = {3s};
        \\
    , .{ ind, n, v, id });
}

fn clearTopK(comptime ind: []const u8, comptime n: []const u8) []const u8 {
    return ind ++ "for (uint i = 0u; i < " ++ n ++ "; ++i) { top_vals[i] = -INFINITY; top_idx[i] = 0u; }\n";
}

fn topKArrays(comptime n: []const u8) []const u8 {
    return "    thread float top_vals[" ++ n ++ "];\n    thread uint top_idx[" ++ n ++ "];\n";
}

fn scaledLogit(comptime ind: []const u8) []const u8 {
    return ind ++ "float adjusted = termite_apply_sampling_penalties(logits[i], i, penalties, p.penalty_count, p);\n" ++
        ind ++ "float scaled = (p.temperature > 0.0f) ? (adjusted / p.temperature) : adjusted;\n";
}

/// `probs[i] = exp(top_vals[i] - best_scaled)` over the kept candidates.
fn candidateProbs(comptime ind: []const u8, comptime n: []const u8) []const u8 {
    return std.fmt.comptimePrint(
        \\{0s}for (uint i = 0u; i < {1s}; ++i) {{
        \\{0s}    float prob = exp(top_vals[i] - best_scaled);
        \\{0s}    probs[i] = prob;
        \\{0s}    sum += prob;
        \\{0s}    if (prob > max_prob) max_prob = prob;
        \\{0s}}}
        \\
    , .{ ind, n });
}

/// Top-p over the sorted candidates: zero everything below the first
/// probability where the running mass reaches top_p * sum.
fn topP(comptime ind: []const u8, comptime n: []const u8) []const u8 {
    return std.fmt.comptimePrint(
        \\{0s}if (p.top_p > 0.0f && p.top_p < 1.0f) {{
        \\{0s}    float threshold = p.top_p * sum;
        \\{0s}    float cumulative = 0.0f;
        \\{0s}    float nucleus_cutoff = probs[{1s} - 1u];
        \\{0s}    for (uint i = 0u; i < {1s}; ++i) {{
        \\{0s}        cumulative += probs[i];
        \\{0s}        if (cumulative >= threshold) {{ nucleus_cutoff = probs[i]; break; }}
        \\{0s}    }}
        \\{0s}    sum = 0.0f;
        \\{0s}    for (uint i = 0u; i < {1s}; ++i) {{
        \\{0s}        if (probs[i] < nucleus_cutoff) probs[i] = 0.0f;
        \\{0s}        sum += probs[i];
        \\{0s}    }}
        \\{0s}}}
        \\
    , .{ ind, n });
}

const limit = "uint limit = candidate_count != 0u ? candidate_count : p.out_dim;";

/// Min-p, then one xorshift draw over the remaining mass (`n` candidates,
/// or `limit` when `limited`).
fn minPAndDraw(comptime n: []const u8, comptime limited: bool, comptime pick: []const u8) []const u8 {
    const pre = if (limited) "        " ++ limit ++ "\n" else "";
    return std.fmt.comptimePrint(
        \\    if (p.min_p > 0.0f && p.min_p < 1.0f) {{
        \\        float threshold = max_prob * p.min_p;
        \\        sum = 0.0f;
        \\{1s}        for (uint i = 0u; i < {0s}; ++i) {{
        \\            float prob = probs[i];
        \\            if (prob < threshold) {{ probs[i] = 0.0f; }} else {{ sum += prob; }}
        \\        }}
        \\    }}
        \\    if (sum <= 0.0f) {{ output[0] = best_idx; return; }}
        \\    uint state = p.seed ^ 0x9e3779b9u;
        \\    state ^= state << 13; state ^= state >> 17; state ^= state << 5;
        \\    float target = (float(state) / 4294967296.0f) * sum;
        \\    float cumulative = 0.0f;
        \\{2s}    for (uint i = 0u; i < {0s}; ++i) {{
        \\        cumulative += probs[i];
        \\        if (cumulative >= target) {{ output[0] = {3s}; return; }}
        \\    }}
        \\    output[0] = best_idx;
        \\}}
        \\
    , .{ if (limited) "limit" else n, pre, if (limited) "    " ++ limit ++ "\n" else "", pick });
}

const sample_args = "device const float *logits [[buffer(0)]], device float *probs [[buffer(1)]], device uint *output [[buffer(2)]], " ++ params ++ " [[buffer(3)]], " ++ penalty_buffer ++ " [[buffer(4)]], uint gid [[thread_position_in_grid]]";

/// Single-thread sampler: top-k (or the whole vocabulary under top-p for
/// small vocabularies) by insertion, then min-p and one draw.
const logits_1x = "kernel void termite_sample_logits_1x(" ++ sample_args ++ ") {\n" ++
    \\    if (gid != 0) return;
    \\    if (p.out_dim == 0u) { output[0] = 0u; return; }
    \\    const uint max_top_k = 256u;
    \\
++ topKArrays("256") ++
    \\    uint candidate_count = 0u;
    \\    if (p.top_k > 0u && p.top_k <= max_top_k) {
    \\        candidate_count = min(p.top_k, p.out_dim);
    \\    } else if (p.top_p > 0.0f && p.top_p < 1.0f && p.out_dim <= max_top_k) {
    \\        candidate_count = p.out_dim;
    \\    }
    \\    float cutoff = -INFINITY;
    \\    if (candidate_count != 0u) {
    \\
++ clearTopK("        ", "candidate_count") ++
    "        for (uint i = 0u; i < p.out_dim; ++i) {\n" ++ scaledLogit("            ") ++ insertTopK("            ", "candidate_count", "scaled", "i") ++ "        }\n" ++
    \\        cutoff = top_vals[candidate_count - 1u];
    \\    }
    \\    float best_scaled = -INFINITY;
    \\    uint best_idx = 0u;
    \\    for (uint i = 0u; i < p.out_dim; ++i) {
    \\
++ scaledLogit("        ") ++
    \\        if (candidate_count != 0u && scaled < cutoff) continue;
    \\        if (scaled > best_scaled) { best_scaled = scaled; best_idx = i; }
    \\    }
    \\    if (!(best_scaled > -INFINITY)) { output[0] = 0u; return; }
    \\    if (p.temperature <= 0.0f) { output[0] = best_idx; return; }
    \\    float sum = 0.0f;
    \\    float max_prob = 0.0f;
    \\    if (candidate_count != 0u) {
    \\
++ candidateProbs("        ", "candidate_count") ++ topP("        ", "candidate_count") ++
    \\    } else {
    \\        for (uint i = 0u; i < p.out_dim; ++i) {
    \\            float adjusted = termite_apply_sampling_penalties(logits[i], i, penalties, p.penalty_count, p);
    \\            float scaled = adjusted / p.temperature;
    \\            float prob = exp(scaled - best_scaled);
    \\            probs[i] = prob;
    \\            sum += prob;
    \\            if (prob > max_prob) max_prob = prob;
    \\        }
    \\    }
    \\
++ minPAndDraw("", true, "(candidate_count != 0u ? top_idx[i] : i)");

/// Per-512-logit block top-k (up to 64), one thread per block.
const topk_partials = "kernel void termite_sample_topk_partials(device const float *logits [[buffer(0)]], device float *partial_values [[buffer(1)]], device uint *partial_ids [[buffer(2)]], " ++ params ++ " [[buffer(3)]], " ++ penalty_buffer ++ " [[buffer(4)]], uint block [[thread_position_in_grid]]) {\n" ++
    \\    const uint block_size = 512u;
    \\    const uint max_top_k = 64u;
    \\    uint top_k = min(p.top_k, max_top_k);
    \\    if (top_k == 0u) return;
    \\
++ topKArrays("64") ++ clearTopK("    ", "top_k") ++
    \\    uint start = block * block_size;
    \\    uint end = min(start + block_size, p.out_dim);
    \\    for (uint i = start; i < end; ++i) {
    \\
++ scaledLogit("        ") ++ insertTopK("        ", "top_k", "scaled", "i") ++
    \\    }
    \\    uint base = block * top_k;
    \\    for (uint i = 0u; i < top_k; ++i) {
    \\        partial_values[base + i] = top_vals[i];
    \\        partial_ids[base + i] = top_idx[i];
    \\    }
    \\}
    \\
;

/// Merges the block top-k lists, then samples like termite_sample_logits_1x.
const topk_reduce = "kernel void termite_sample_topk_reduce(device const float *partial_values [[buffer(0)]], device const uint *partial_ids [[buffer(1)]], device float *probs [[buffer(2)]], device uint *output [[buffer(3)]], constant termite_metal_topk_reduce_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {\n" ++
    \\    if (gid != 0u) return;
    \\    if (p.candidate_count == 0u || p.top_k == 0u) { output[0] = 0u; return; }
    \\    const uint max_top_k = 64u;
    \\    uint top_k = min(p.top_k, max_top_k);
    \\
++ topKArrays("64") ++ clearTopK("    ", "top_k") ++
    \\    for (uint i = 0u; i < p.candidate_count; ++i) {
    \\        float v = partial_values[i];
    \\
++ insertTopK("        ", "top_k", "v", "partial_ids[i]") ++
    \\    }
    \\    float best_scaled = top_vals[0];
    \\    uint best_idx = top_idx[0];
    \\    if (!(best_scaled > -INFINITY)) { output[0] = 0u; return; }
    \\    float sum = 0.0f;
    \\    float max_prob = 0.0f;
    \\
++ candidateProbs("    ", "top_k") ++ topP("    ", "top_k") ++ minPAndDraw("top_k", false, "top_idx[i]");

pub fn metalRegion() []const u8 {
    comptime {
        @setEvalBranchQuota(1_000_000);
        return max_partials ++ max_reduce ++ probs_partials ++ sum_reduce ++ histogram_serial ++ histogram ++ nucleus_gumbel ++ gumbel ++ logits_1x ++ topk_partials ++ topk_reduce;
    }
}
