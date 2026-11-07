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

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

pub const source_0 =
    program.metal(
        \\kernel void termite_sample_nucleus_max_partials(device const float *logits [[buffer(0)]], device float *partials [[buffer(1)]], constant termite_metal_sample_logits_params &p [[buffer(2)]], device const termite_metal_penalty_entry *penalties [[buffer(3)]], threadgroup float *shared [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort tg_size [[threads_per_threadgroup]], uint block [[threadgroup_position_in_grid]]) {
        \\    const uint block_size = 1024u; const uint block_count = (p.out_dim + block_size - 1u) / block_size; uint start = block * block_size; uint end = min(start + block_size, p.out_dim); float best = -INFINITY; float special = -INFINITY;
        \\    for (uint i = start + uint(tid); i < end; i += uint(tg_size)) { float raw = logits[i]; if (p.defer_monotonic_max != 0u && isfinite(raw)) { best = max(best, raw); } else { if (p.final_logit_softcap > 0.0f) raw = p.final_logit_softcap * tanh(raw / p.final_logit_softcap); float adjusted = termite_apply_sampling_penalties(raw, i, penalties, p.penalty_count, p); float scaled = adjusted / p.temperature; if (isfinite(scaled)) { if (p.defer_monotonic_max != 0u) special = max(special, scaled); else best = max(best, scaled); } } }
        \\    shared[tid] = best; if (p.defer_monotonic_max != 0u) shared[uint(tg_size) + uint(tid)] = special; threadgroup_barrier(mem_flags::mem_threadgroup); for (uint stride = uint(tg_size) >> 1u; stride > 0u; stride >>= 1u) { if (uint(tid) < stride) { shared[tid] = max(shared[tid], shared[uint(tid) + stride]); if (p.defer_monotonic_max != 0u) shared[uint(tg_size) + uint(tid)] = max(shared[uint(tg_size) + uint(tid)], shared[uint(tg_size) + uint(tid) + stride]); } threadgroup_barrier(mem_flags::mem_threadgroup); } if (tid == 0u) { partials[block] = shared[0]; if (p.defer_monotonic_max != 0u) partials[block_count + block] = shared[uint(tg_size)]; }
        \\}
        \\kernel void termite_sample_nucleus_max_reduce(device const float *partials [[buffer(0)]], device termite_metal_nucleus_state *state [[buffer(1)]], constant uint &block_count [[buffer(2)]], constant termite_metal_sample_logits_params &p [[buffer(3)]], threadgroup float *shared [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort tg_size [[threads_per_threadgroup]]) {
        \\    float best = -INFINITY; float special = -INFINITY; for (uint i = uint(tid); i < block_count; i += uint(tg_size)) { best = max(best, partials[i]); if (p.defer_monotonic_max != 0u) special = max(special, partials[block_count + i]); } shared[tid] = best; if (p.defer_monotonic_max != 0u) shared[uint(tg_size) + uint(tid)] = special; threadgroup_barrier(mem_flags::mem_threadgroup); for (uint stride = uint(tg_size) >> 1u; stride > 0u; stride >>= 1u) { if (uint(tid) < stride) { shared[tid] = max(shared[tid], shared[uint(tid) + stride]); if (p.defer_monotonic_max != 0u) shared[uint(tg_size) + uint(tid)] = max(shared[uint(tg_size) + uint(tid)], shared[uint(tg_size) + uint(tid) + stride]); } threadgroup_barrier(mem_flags::mem_threadgroup); } if (tid == 0u) { float reduced = shared[0]; if (p.defer_monotonic_max != 0u) { if (isfinite(reduced)) { float transformed = p.final_logit_softcap * tanh(reduced / p.final_logit_softcap); reduced = transformed / p.temperature; } reduced = max(reduced, shared[uint(tg_size)]); } state->max_logit = reduced; state->block_count = block_count; }
        \\}
        \\kernel void termite_sample_nucleus_probs_partials(device const float *logits [[buffer(0)]], device float *probs [[buffer(1)]], device float *partials [[buffer(2)]], device const termite_metal_nucleus_state *state [[buffer(3)]], constant termite_metal_sample_logits_params &p [[buffer(4)]], device const termite_metal_penalty_entry *penalties [[buffer(5)]], threadgroup float *shared [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort tg_size [[threads_per_threadgroup]], uint block [[threadgroup_position_in_grid]]) {
        \\    const uint block_size = 1024u; uint start = block * block_size; uint end = min(start + block_size, p.out_dim); float subtotal = 0.0f;
        \\    for (uint i = start + uint(tid); i < end; i += uint(tg_size)) { float raw = logits[i]; if (p.final_logit_softcap > 0.0f) raw = p.final_logit_softcap * tanh(raw / p.final_logit_softcap); float adjusted = termite_apply_sampling_penalties(raw, i, penalties, p.penalty_count, p); float scaled = adjusted / p.temperature; float prob = isfinite(scaled) ? exp(scaled - state->max_logit) : 0.0f; probs[i] = prob; subtotal += prob; }
        \\    shared[tid] = subtotal; threadgroup_barrier(mem_flags::mem_threadgroup); for (uint stride = uint(tg_size) >> 1u; stride > 0u; stride >>= 1u) { if (uint(tid) < stride) shared[tid] += shared[uint(tid) + stride]; threadgroup_barrier(mem_flags::mem_threadgroup); } if (tid == 0u) partials[block] = shared[0];
        \\}
        \\kernel void termite_sample_nucleus_sum_reduce(device const float *partials [[buffer(0)]], device termite_metal_nucleus_state *state [[buffer(1)]], constant float &top_p [[buffer(2)]], threadgroup float *shared [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort tg_size [[threads_per_threadgroup]]) {
        \\    float subtotal = 0.0f; for (uint i = uint(tid); i < state->block_count; i += uint(tg_size)) subtotal += partials[i]; shared[tid] = subtotal; threadgroup_barrier(mem_flags::mem_threadgroup); for (uint stride = uint(tg_size) >> 1u; stride > 0u; stride >>= 1u) { if (uint(tid) < stride) shared[tid] += shared[uint(tid) + stride]; threadgroup_barrier(mem_flags::mem_threadgroup); } if (tid == 0u) { state->total_mass = shared[0]; state->target_mass = top_p * shared[0]; state->mass_above = 0.0f; state->prefix = 0u; state->cutoff_key = 0u; state->pass = 0u; }
        \\}
        \\kernel void termite_sample_nucleus_radix_histogram_serial(device const float *logits [[buffer(0)]], device const float *probs [[buffer(1)]], device float *histograms [[buffer(2)]], device const termite_metal_nucleus_state *state [[buffer(3)]], constant termite_metal_sample_logits_params &p [[buffer(4)]], device const termite_metal_penalty_entry *penalties [[buffer(5)]], ushort tid [[thread_index_in_threadgroup]], uint block [[threadgroup_position_in_grid]]) {
        \\    if (tid >= 16u) return; const uint block_size = 1024u; uint start = block * block_size; uint end = min(start + block_size, p.out_dim); uint shift = 28u - state->pass * 4u; uint prefix_mask = state->pass == 0u ? 0u : (0xffffffffu << (32u - state->pass * 4u)); float mass = 0.0f;
        \\    for (uint i = start; i < end; ++i) { float prob = probs[i]; if (!(prob > 0.0f)) continue; uint key = as_type<uint>(prob); if ((key & prefix_mask) == state->prefix && ((key >> shift) & 15u) == uint(tid)) mass += prob; } histograms[block * 16u + uint(tid)] = mass;
        \\}
        \\kernel void termite_sample_nucleus_radix_histogram(device const float *logits [[buffer(0)]], device const float *probs [[buffer(1)]], device float *histograms [[buffer(2)]], device const termite_metal_nucleus_state *state [[buffer(3)]], constant termite_metal_sample_logits_params &p [[buffer(4)]], device const termite_metal_penalty_entry *penalties [[buffer(5)]], threadgroup float *simd_histograms [[threadgroup(0)]], ushort tid [[thread_index_in_threadgroup]], ushort tg_size [[threads_per_threadgroup]], ushort lane [[thread_index_in_simdgroup]], ushort simdgroup [[simdgroup_index_in_threadgroup]], uint block [[threadgroup_position_in_grid]]) {
        \\    const uint block_size = 1024u; uint start = block * block_size; uint end = min(start + block_size, p.out_dim); uint shift = 28u - state->pass * 4u; uint prefix_mask = state->pass == 0u ? 0u : (0xffffffffu << (32u - state->pass * 4u)); thread float bins[16]; for (uint b = 0u; b < 16u; ++b) bins[b] = 0.0f;
        \\    for (uint i = start + uint(tid); i < end; i += uint(tg_size)) { float prob = probs[i]; if (!(prob > 0.0f)) continue; uint key = as_type<uint>(prob); if ((key & prefix_mask) != state->prefix) continue; bins[(key >> shift) & 15u] += prob; }
        \\    for (uint b = 0u; b < 16u; ++b) { float mass = simd_sum(bins[b]); if (lane == 0u) simd_histograms[uint(simdgroup) * 16u + b] = mass; } threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    if (tid < 16u) { float mass = 0.0f; uint simdgroup_count = (uint(tg_size) + 31u) >> 5u; for (uint group = 0u; group < simdgroup_count; ++group) mass += simd_histograms[group * 16u + uint(tid)]; histograms[block * 16u + uint(tid)] = mass; }
        \\}
        \\
    );

pub const source_1 =
    program.metal(
        \\kernel void termite_sample_nucleus_gumbel_partials(device const float *probs [[buffer(0)]], device float *partial_values [[buffer(1)]], device uint *partial_ids [[buffer(2)]], device const termite_metal_nucleus_state *state [[buffer(3)]], constant termite_metal_sample_logits_params &p [[buffer(4)]], threadgroup float *sh_vals [[threadgroup(0)]], threadgroup uint *sh_ids [[threadgroup(1)]], ushort tid [[thread_index_in_threadgroup]], ushort tg_size [[threads_per_threadgroup]], uint block [[threadgroup_position_in_grid]]) {
        \\    const uint block_size = 1024u; uint start = block * block_size; uint end = min(start + block_size, p.out_dim); float best = -INFINITY; uint best_idx = 0xffffffffu;
        \\    for (uint i = start + uint(tid); i < end; i += uint(tg_size)) { float prob = probs[i]; if (!(prob > 0.0f) || as_type<uint>(prob) < state->cutoff_key) continue; uint h = (i ^ p.seed) * 0x9E3779B9u; h ^= h >> 16; h *= 0x85EBCA6Bu; h ^= h >> 13; h *= 0xC2B2AE35u; h ^= h >> 16; float u = (float(h >> 8) + 0.5f) * (1.0f / 16777216.0f); float score = log(prob) - log(-log(u)); if (score > best || (score == best && i < best_idx)) { best = score; best_idx = i; } }
        \\    sh_vals[tid] = best; sh_ids[tid] = best_idx; threadgroup_barrier(mem_flags::mem_threadgroup); termite_argmax_partial_tree(sh_vals, sh_ids, tid, tg_size); if (tid == 0u) { bool invalid = !(state->total_mass > 0.0f) || !isfinite(state->total_mass); partial_values[block] = invalid && block == 0u ? NAN : sh_vals[0]; partial_ids[block] = invalid && block == 0u ? 0xffffffffu : sh_ids[0]; }
        \\}
        \\
    );

pub const source_2 =
    program.metal(
        \\kernel void termite_sample_gumbel_partials(device const float *logits [[buffer(0)]], device float *partial_values [[buffer(1)]], device uint *partial_ids [[buffer(2)]], constant termite_metal_sample_logits_params &p [[buffer(3)]], device const termite_metal_penalty_entry *penalties [[buffer(4)]], threadgroup float *sh_vals [[threadgroup(0)]], threadgroup uint *sh_ids [[threadgroup(1)]], ushort tid [[thread_index_in_threadgroup]], ushort tg_size [[threads_per_threadgroup]], uint block [[threadgroup_position_in_grid]]) {
        \\    const uint block_size = 1024u;
        \\    uint start = block * block_size;
        \\    uint end = min(start + block_size, p.out_dim);
        \\    float inv_temp = p.temperature > 0.0f ? (1.0f / p.temperature) : 1.0f;
        \\    float best = -INFINITY;
        \\    uint best_idx = 0u;
        \\    for (uint i = start + uint(tid); i < end; i += uint(tg_size)) {
        \\        float raw = logits[i];
        \\        if (p.final_logit_softcap > 0.0f) raw = p.final_logit_softcap * tanh(raw / p.final_logit_softcap);
        \\        float logit = termite_apply_sampling_penalties(raw, i, penalties, p.penalty_count, p);
        \\        uint h = (i ^ p.seed) * 0x9E3779B9u; h ^= h >> 16; h *= 0x85EBCA6Bu; h ^= h >> 13; h *= 0xC2B2AE35u; h ^= h >> 16;
        \\        float u = (float(h >> 8) + 0.5f) * (1.0f / 16777216.0f);
        \\        float score = logit * inv_temp - log(-log(u));
        \\        if (score > best || (score == best && i < best_idx)) { best = score; best_idx = i; }
        \\    }
        \\    sh_vals[tid] = best;
        \\    sh_ids[tid] = best_idx;
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    termite_argmax_partial_tree(sh_vals, sh_ids, tid, tg_size);
        \\    if (tid == 0u) { partial_values[block] = sh_vals[0]; partial_ids[block] = sh_ids[0]; }
        \\}
        \\kernel void termite_sample_logits_1x(device const float *logits [[buffer(0)]], device float *probs [[buffer(1)]], device uint *output [[buffer(2)]], constant termite_metal_sample_logits_params &p [[buffer(3)]], device const termite_metal_penalty_entry *penalties [[buffer(4)]], uint gid [[thread_position_in_grid]]) {
        \\    if (gid != 0) return;
        \\    if (p.out_dim == 0u) { output[0] = 0u; return; }
        \\    const uint max_top_k = 256u;
        \\    thread float top_vals[256];
        \\    thread uint top_idx[256];
        \\    uint candidate_count = 0u;
        \\    if (p.top_k > 0u && p.top_k <= max_top_k) {
        \\        candidate_count = min(p.top_k, p.out_dim);
        \\    } else if (p.top_p > 0.0f && p.top_p < 1.0f && p.out_dim <= max_top_k) {
        \\        candidate_count = p.out_dim;
        \\    }
        \\    float cutoff = -INFINITY;
        \\    if (candidate_count != 0u) {
        \\        for (uint i = 0u; i < candidate_count; ++i) { top_vals[i] = -INFINITY; top_idx[i] = 0u; }
        \\        for (uint i = 0u; i < p.out_dim; ++i) {
        \\            float adjusted = termite_apply_sampling_penalties(logits[i], i, penalties, p.penalty_count, p);
        \\            float scaled = (p.temperature > 0.0f) ? (adjusted / p.temperature) : adjusted;
        \\            if (scaled <= top_vals[candidate_count - 1u]) continue;
        \\            uint insert_at = candidate_count - 1u;
        \\            while (insert_at > 0u && scaled > top_vals[insert_at - 1u]) {
        \\                top_vals[insert_at] = top_vals[insert_at - 1u];
        \\                top_idx[insert_at] = top_idx[insert_at - 1u];
        \\                insert_at -= 1u;
        \\            }
        \\            top_vals[insert_at] = scaled;
        \\            top_idx[insert_at] = i;
        \\        }
        \\        cutoff = top_vals[candidate_count - 1u];
        \\    }
        \\    float best_scaled = -INFINITY;
        \\    uint best_idx = 0u;
        \\    for (uint i = 0u; i < p.out_dim; ++i) {
        \\        float adjusted = termite_apply_sampling_penalties(logits[i], i, penalties, p.penalty_count, p);
        \\        float scaled = (p.temperature > 0.0f) ? (adjusted / p.temperature) : adjusted;
        \\        if (candidate_count != 0u && scaled < cutoff) continue;
        \\        if (scaled > best_scaled) { best_scaled = scaled; best_idx = i; }
        \\    }
        \\    if (!(best_scaled > -INFINITY)) { output[0] = 0u; return; }
        \\    if (p.temperature <= 0.0f) { output[0] = best_idx; return; }
        \\    float sum = 0.0f;
        \\    float max_prob = 0.0f;
        \\    if (candidate_count != 0u) {
        \\        for (uint i = 0u; i < candidate_count; ++i) {
        \\            float prob = exp(top_vals[i] - best_scaled);
        \\            probs[i] = prob;
        \\            sum += prob;
        \\            if (prob > max_prob) max_prob = prob;
        \\        }
        \\        if (p.top_p > 0.0f && p.top_p < 1.0f) {
        \\            float threshold = p.top_p * sum;
        \\            float cumulative = 0.0f;
        \\            float nucleus_cutoff = probs[candidate_count - 1u];
        \\            for (uint i = 0u; i < candidate_count; ++i) {
        \\                cumulative += probs[i];
        \\                if (cumulative >= threshold) { nucleus_cutoff = probs[i]; break; }
        \\            }
        \\            sum = 0.0f;
        \\            for (uint i = 0u; i < candidate_count; ++i) {
        \\                if (probs[i] < nucleus_cutoff) probs[i] = 0.0f;
        \\                sum += probs[i];
        \\            }
        \\        }
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
        \\    if (p.min_p > 0.0f && p.min_p < 1.0f) {
        \\        float threshold = max_prob * p.min_p;
        \\        sum = 0.0f;
        \\        uint limit = candidate_count != 0u ? candidate_count : p.out_dim;
        \\        for (uint i = 0u; i < limit; ++i) {
        \\            float prob = probs[i];
        \\            if (prob < threshold) { probs[i] = 0.0f; } else { sum += prob; }
        \\        }
        \\    }
        \\    if (sum <= 0.0f) { output[0] = best_idx; return; }
        \\    uint state = p.seed ^ 0x9e3779b9u;
        \\    state ^= state << 13; state ^= state >> 17; state ^= state << 5;
        \\    float target = (float(state) / 4294967296.0f) * sum;
        \\    float cumulative = 0.0f;
        \\    uint limit = candidate_count != 0u ? candidate_count : p.out_dim;
        \\    for (uint i = 0u; i < limit; ++i) {
        \\        cumulative += probs[i];
        \\        if (cumulative >= target) { output[0] = (candidate_count != 0u ? top_idx[i] : i); return; }
        \\    }
        \\    output[0] = best_idx;
        \\}
        \\kernel void termite_sample_topk_partials(device const float *logits [[buffer(0)]], device float *partial_values [[buffer(1)]], device uint *partial_ids [[buffer(2)]], constant termite_metal_sample_logits_params &p [[buffer(3)]], device const termite_metal_penalty_entry *penalties [[buffer(4)]], uint block [[thread_position_in_grid]]) {
        \\    const uint block_size = 512u;
        \\    const uint max_top_k = 64u;
        \\    uint top_k = min(p.top_k, max_top_k);
        \\    if (top_k == 0u) return;
        \\    thread float top_vals[64];
        \\    thread uint top_idx[64];
        \\    for (uint i = 0u; i < top_k; ++i) { top_vals[i] = -INFINITY; top_idx[i] = 0u; }
        \\    uint start = block * block_size;
        \\    uint end = min(start + block_size, p.out_dim);
        \\    for (uint i = start; i < end; ++i) {
        \\        float adjusted = termite_apply_sampling_penalties(logits[i], i, penalties, p.penalty_count, p);
        \\        float scaled = (p.temperature > 0.0f) ? (adjusted / p.temperature) : adjusted;
        \\        if (scaled <= top_vals[top_k - 1u]) continue;
        \\        uint insert_at = top_k - 1u;
        \\        while (insert_at > 0u && scaled > top_vals[insert_at - 1u]) {
        \\            top_vals[insert_at] = top_vals[insert_at - 1u];
        \\            top_idx[insert_at] = top_idx[insert_at - 1u];
        \\            insert_at -= 1u;
        \\        }
        \\        top_vals[insert_at] = scaled;
        \\        top_idx[insert_at] = i;
        \\    }
        \\    uint base = block * top_k;
        \\    for (uint i = 0u; i < top_k; ++i) {
        \\        partial_values[base + i] = top_vals[i];
        \\        partial_ids[base + i] = top_idx[i];
        \\    }
        \\}
        \\kernel void termite_sample_topk_reduce(device const float *partial_values [[buffer(0)]], device const uint *partial_ids [[buffer(1)]], device float *probs [[buffer(2)]], device uint *output [[buffer(3)]], constant termite_metal_topk_reduce_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {
        \\    if (gid != 0u) return;
        \\    if (p.candidate_count == 0u || p.top_k == 0u) { output[0] = 0u; return; }
        \\    const uint max_top_k = 64u;
        \\    uint top_k = min(p.top_k, max_top_k);
        \\    thread float top_vals[64];
        \\    thread uint top_idx[64];
        \\    for (uint i = 0u; i < top_k; ++i) { top_vals[i] = -INFINITY; top_idx[i] = 0u; }
        \\    for (uint i = 0u; i < p.candidate_count; ++i) {
        \\        float v = partial_values[i];
        \\        if (v <= top_vals[top_k - 1u]) continue;
        \\        uint insert_at = top_k - 1u;
        \\        while (insert_at > 0u && v > top_vals[insert_at - 1u]) {
        \\            top_vals[insert_at] = top_vals[insert_at - 1u];
        \\            top_idx[insert_at] = top_idx[insert_at - 1u];
        \\            insert_at -= 1u;
        \\        }
        \\        top_vals[insert_at] = v;
        \\        top_idx[insert_at] = partial_ids[i];
        \\    }
        \\    float best_scaled = top_vals[0];
        \\    uint best_idx = top_idx[0];
        \\    if (!(best_scaled > -INFINITY)) { output[0] = 0u; return; }
        \\    float sum = 0.0f;
        \\    float max_prob = 0.0f;
        \\    for (uint i = 0u; i < top_k; ++i) {
        \\        float prob = exp(top_vals[i] - best_scaled);
        \\        probs[i] = prob;
        \\        sum += prob;
        \\        if (prob > max_prob) max_prob = prob;
        \\    }
        \\    if (p.top_p > 0.0f && p.top_p < 1.0f) {
        \\        float threshold = p.top_p * sum;
        \\        float cumulative = 0.0f;
        \\        float nucleus_cutoff = probs[top_k - 1u];
        \\        for (uint i = 0u; i < top_k; ++i) {
        \\            cumulative += probs[i];
        \\            if (cumulative >= threshold) { nucleus_cutoff = probs[i]; break; }
        \\        }
        \\        sum = 0.0f;
        \\        for (uint i = 0u; i < top_k; ++i) {
        \\            if (probs[i] < nucleus_cutoff) probs[i] = 0.0f;
        \\            sum += probs[i];
        \\        }
        \\    }
        \\    if (p.min_p > 0.0f && p.min_p < 1.0f) {
        \\        float threshold = max_prob * p.min_p;
        \\        sum = 0.0f;
        \\        for (uint i = 0u; i < top_k; ++i) {
        \\            float prob = probs[i];
        \\            if (prob < threshold) { probs[i] = 0.0f; } else { sum += prob; }
        \\        }
        \\    }
        \\    if (sum <= 0.0f) { output[0] = best_idx; return; }
        \\    uint state = p.seed ^ 0x9e3779b9u;
        \\    state ^= state << 13; state ^= state >> 17; state ^= state << 5;
        \\    float target = (float(state) / 4294967296.0f) * sum;
        \\    float cumulative = 0.0f;
        \\    for (uint i = 0u; i < top_k; ++i) {
        \\        cumulative += probs[i];
        \\        if (cumulative >= target) { output[0] = top_idx[i]; return; }
        \\    }
        \\    output[0] = best_idx;
        \\}
        \\
    );
