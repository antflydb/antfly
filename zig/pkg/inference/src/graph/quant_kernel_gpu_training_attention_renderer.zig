// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One deterministic training attention replay algorithm for Metal and CUDA.
//! The backend retains entry ABI, scratch allocation, execution width and FP
//! compiler flags. Address spaces, atomics and barriers lower before compilation.
const std = @import("std");
const gpu = @import("quant_kernel_gpu_operator_renderer.zig");
const Backend = gpu.Backend;
const Phase = enum { helpers, validate_f32, validate_control, zero, forward, rows, dq, dkdv, relative };
pub const metal_owners = Owners(.metal).values;
pub const cuda_owners = Owners(.cuda).values;
fn Owners(comptime backend: Backend) type {
    return struct {
        pub const values = .{ Owner(backend, .helpers), Owner(backend, .validate_f32), Owner(backend, .validate_control), Owner(backend, .zero), Owner(backend, .forward), Owner(backend, .rows), Owner(backend, .dq), Owner(backend, .dkdv), Owner(backend, .relative) };
    };
}
fn Owner(comptime backend: Backend, comptime phase: Phase) type {
    return struct {
        pub const begin = "    // quant-kernel-codegen:begin shared GPU training attention " ++ @tagName(phase);
        pub const end = "    // quant-kernel-codegen:end shared GPU training attention " ++ @tagName(phase);
        pub fn render(allocator: std.mem.Allocator) ![]u8 {
            const source = switch (phase) {
                .helpers => helpers,
                .validate_f32 => validate_f32,
                .validate_control => validate_control,
                .zero => zero,
                .forward => forward,
                .rows => rows,
                .dq => dq,
                .dkdv => dkdv,
                .relative => relative,
            };
            if (backend == .metal) return gpu.quoteMetalIndented(allocator, source, 4);
            return gpu.lowerNative(allocator, source, &identifiers, &phrases);
        }
    };
}
const identifiers = [_]gpu.Spelling{
    .{ .metal = "inline", .cuda = "__device__ __forceinline__" },
    .{ .metal = "device", .cuda = "" },
    .{ .metal = "constant", .cuda = "const" },
    .{ .metal = "ulong", .cuda = "dt_u64" },
    .{ .metal = "atomic_uint", .cuda = "unsigned int" },
    .{ .metal = "threadgroup", .cuda = "" },
    .{ .metal = "exp", .cuda = "expf" },
};
const phrases = [_]gpu.Spelling{
    .{ .metal = "atomic_fetch_or_explicit(status, code, memory_order_relaxed);", .cuda = "atomicOr(status, code);" },
    .{ .metal = "threadgroup_barrier(mem_flags::mem_threadgroup);", .cuda = "__syncthreads();" },
};

test "training attention lowers synchronization without changing replay arithmetic" {
    const cuda = try Owner(.cuda, .forward).render(std.testing.allocator);
    defer std.testing.allocator.free(cuda);
    const metal = try Owner(.metal, .forward).render(std.testing.allocator);
    defer std.testing.allocator.free(metal);
    try std.testing.expect(std.mem.indexOf(u8, cuda, "__syncthreads();") != null);
    try std.testing.expect(std.mem.indexOf(u8, cuda, "expf(state[0] - maximum)") != null);
    try std.testing.expect(std.mem.indexOf(u8, cuda, "threadgroup") == null);
    try std.testing.expect(std.mem.indexOf(u8, metal, "threadgroup_barrier(mem_flags::mem_threadgroup);") != null);
    inline for (metal_owners ++ cuda_owners) |O| {
        const source = try O.render(std.testing.allocator);
        defer std.testing.allocator.free(source);
        try std.testing.expect(source.len != 0);
    }
}

const helpers =
    \\inline void dt_bad(device atomic_uint* status, uint code) {
    \\    atomic_fetch_or_explicit(status, code, memory_order_relaxed);
    \\}
    \\inline void dt_finite(float value, device atomic_uint* status) {
    \\    if (!isfinite(value)) dt_bad(status, 1u);
    \\}
    \\inline ulong dt_mix(ulong x) {
    \\    x += 0x9e3779b97f4a7c15ul;
    \\    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ul;
    \\    x = (x ^ (x >> 27)) * 0x94d049bb133111ebul;
    \\    return x ^ (x >> 31);
    \\}
    \\inline float dt_keep(constant DTParams& p, uint b, uint h, uint q, uint k) {
    \\    const ulong index = ((ulong(b) * p.heads + h) * p.sequence + q) * p.sequence + k;
    \\    const uint bits = uint(dt_mix(p.dropout_stream ^ dt_mix(index)) >> 32);
    \\    return bits < p.dropout_threshold ? 0.0f : p.dropout_scale;
    \\}
    \\inline uint dt_data(constant DTParams& p, uint b, uint h, uint token) {
    \\    return ((b * p.sequence + token) * p.heads + h) * p.dimension;
    \\}
    \\inline uint dt_total(constant DTParams& p) {
    \\    return p.batch * p.sequence * p.heads * p.dimension;
    \\}
    \\inline uint dt_row(constant DTParams& p, uint b, uint h, uint q) {
    \\    return (b * p.heads + h) * p.sequence + q;
    \\}
    \\inline uint dt_bucket(device const int* control, constant DTParams& p, uint q, uint k) {
    \\    return uint(control[6u + p.batch * p.sequence + q + p.sequence - 1u - k]);
    \\}
    \\inline bool dt_valid(device const int* control, constant DTParams& p, uint b, uint q, uint k) {
    \\    return control[6u + b * p.sequence + q] != 0 && control[6u + b * p.sequence + k] != 0;
    \\}
    \\inline float dt_score(device const float* qkv, device const float* relative,
    \\                     device const int* control, constant DTParams& p,
    \\                     uint b, uint h, uint q, uint k, uint bucket,
    \\                     device atomic_uint* status) {
    \\    // masked_fill uses finite -maxF32 for invalid queries as well as keys.
    \\    if (!dt_valid(control, p, b, q, k)) return -0x1.fffffep127f;
    \\    const uint total = dt_total(p);
    \\    const uint qo = dt_data(p, b, h, q);
    \\    const uint ko = total + dt_data(p, b, h, k);
    \\    const uint ro = (bucket * p.heads + h) * p.dimension;
    \\    const uint kr = p.relative_rows * p.heads * p.dimension + ro;
    \\    float cc = 0.0f, cp = 0.0f, pc = 0.0f;
    \\    for (uint d = 0; d < p.dimension; ++d) {
    \\        const float qv = qkv[qo + d];
    \\        const float kv = qkv[ko + d];
    \\        cc += qv * (kv / p.attention_scale);
    \\        cp += qv * relative[kr + d];
    \\        pc += kv * relative[ro + d];
    \\    }
    \\    const float score = cc + (cp / p.attention_scale + pc / p.attention_scale);
    \\    dt_finite(score, status);
    \\    return score;
    \\}
    \\inline float dt_dp(device const float* qkv, device const float* dout,
    \\                   constant DTParams& p, uint b, uint h, uint q, uint k,
    \\                   device atomic_uint* status) {
    \\    const uint vo = 2u * dt_total(p) + dt_data(p, b, h, k);
    \\    const uint go = dt_data(p, b, h, q);
    \\    float result = 0.0f;
    \\    for (uint d = 0; d < p.dimension; ++d) result += dout[go + d] * qkv[vo + d];
    \\    dt_finite(result, status);
    \\    return result;
    \\}
    \\struct DTAdjoint { float ds, probability; };
    \\inline DTAdjoint dt_adjoint(device const float* qkv, device const float* relative,
    \\                           device const int* control, device const float* dout,
    \\                           device const float* rows, constant DTParams& p,
    \\                           uint b, uint h, uint q, uint k, uint bucket,
    \\                           device atomic_uint* status) {
    \\    const uint row = 3u * dt_row(p, b, h, q);
    \\    const float score = dt_score(qkv, relative, control, p, b, h, q, k, bucket, status);
    \\    const float probability = exp(score - rows[row]) / rows[row + 1u];
    \\    const float keep = dt_keep(p, b, h, q, k);
    \\    const float dp = dt_dp(qkv, dout, p, b, h, q, k, status);
    \\    const float ds = dt_valid(control, p, b, q, k) ? probability * (keep * dp - rows[row + 2u]) : 0.0f;
    \\    dt_finite(ds, status);
    \\    dt_finite(probability, status);
    \\    return { ds, probability * keep };
    \\}
    \\
;

const validate_f32 =
    \\    if (gid < p.count) dt_finite(input[p.begin + gid], status);
    \\
;

const validate_control =
    \\    if (gid >= p.count) return;
    \\    const uint i = p.begin + gid;
    \\    const int value = input[i];
    \\    if (i < 6u) return;
    \\    if (i < 6u + p.batch * p.sequence) {
    \\        if (value != 0 && value != 1) dt_bad(status, 2u);
    \\    } else if (value < 0 || uint(value) >= p.relative_rows) dt_bad(status, 2u);
    \\
;

const zero =
    \\    if (gid < p.count) output[p.begin + gid] = 0.0f;
    \\
;

const forward =
    \\    const uint row = p.begin + group;
    \\    const uint q = row % p.sequence, h = (row / p.sequence) % p.heads, b = row / (p.sequence * p.heads);
    \\    threadgroup float* score = tile;
    \\    threadgroup float* weight = tile + 64;
    \\    threadgroup float* state = tile + 192;
    \\    if (tid == 0) { state[0] = -INFINITY; state[1] = 0.0f; }
    \\    float value = 0.0f;
    \\    threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    for (uint start = 0; start < p.sequence; start += 64u) {
    \\        const uint count = min(64u, p.sequence - start);
    \\        if (tid < count) {
    \\            const uint k = start + tid;
    \\            score[tid] = dt_score(qkv, relative, control, p, b, h, q, k, dt_bucket(control, p, q, k), status);
    \\        }
    \\        threadgroup_barrier(mem_flags::mem_threadgroup);
    \\        if (tid == 0) {
    \\            float maximum = state[0];
    \\            for (uint i = 0; i < count; ++i) maximum = max(maximum, score[i]);
    \\            const float factor = state[1] == 0.0f ? 0.0f : exp(state[0] - maximum);
    \\            state[2] = factor;
    \\            state[0] = maximum;
    \\            float sum = 0.0f;
    \\            for (uint i = 0; i < count; ++i) {
    \\                const float numerator = exp(score[i] - maximum);
    \\                sum += numerator;
    \\                weight[i] = numerator * dt_keep(p, b, h, q, start + i);
    \\            }
    \\            state[1] = state[1] * factor + sum;
    \\        }
    \\        threadgroup_barrier(mem_flags::mem_threadgroup);
    \\        if (tid < p.dimension) {
    \\            value *= state[2];
    \\            for (uint i = 0; i < count; ++i)
    \\                value += weight[i] * qkv[2u * dt_total(p) + dt_data(p, b, h, start + i) + tid];
    \\        }
    \\        threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    }
    \\    if (tid < p.dimension) {
    \\        value /= state[1];
    \\        dt_finite(value, status);
    \\        output[dt_data(p, b, h, q) + tid] = value;
    \\    }
    \\
;

const rows =
    \\    const uint row = p.begin + group;
    \\    const uint q = row % p.sequence, h = (row / p.sequence) % p.heads, b = row / (p.sequence * p.heads);
    \\    threadgroup float* score = tile;
    \\    threadgroup float* dp = tile + 64;
    \\    threadgroup float* state = tile + 192;
    \\    if (tid == 0) { state[0] = -INFINITY; state[1] = 0.0f; state[2] = 0.0f; }
    \\    threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    for (uint start = 0; start < p.sequence; start += 64u) {
    \\        const uint count = min(64u, p.sequence - start);
    \\        if (tid < count) {
    \\            const uint k = start + tid;
    \\            score[tid] = dt_score(qkv, relative, control, p, b, h, q, k, dt_bucket(control, p, q, k), status);
    \\            dp[tid] = dt_dp(qkv, dout, p, b, h, q, k, status);
    \\        }
    \\        threadgroup_barrier(mem_flags::mem_threadgroup);
    \\        if (tid == 0) {
    \\            float maximum = state[0];
    \\            for (uint i = 0; i < count; ++i) maximum = max(maximum, score[i]);
    \\            const float factor = state[1] == 0.0f ? 0.0f : exp(state[0] - maximum);
    \\            state[2] *= factor;
    \\            float sum = 0.0f;
    \\            for (uint i = 0; i < count; ++i) {
    \\                const float numerator = exp(score[i] - maximum);
    \\                sum += numerator;
    \\                state[2] += (numerator * dt_keep(p, b, h, q, start + i)) * dp[i];
    \\            }
    \\            state[0] = maximum;
    \\            state[1] = state[1] * factor + sum;
    \\        }
    \\        threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    }
    \\    if (tid == 0) {
    \\        const float delta = state[2] / state[1];
    \\        dt_finite(state[0], status); dt_finite(state[1], status); dt_finite(delta, status);
    \\        rows[3u * row] = state[0]; rows[3u * row + 1u] = state[1]; rows[3u * row + 2u] = delta;
    \\    }
    \\
;

const dq =
    \\    const uint row = p.begin + group;
    \\    const uint q = row % p.sequence, h = (row / p.sequence) % p.heads, b = row / (p.sequence * p.heads);
    \\    threadgroup float* ds = tile;
    \\    threadgroup uint* buckets = reinterpret_cast<threadgroup uint*>(tile + 128);
    \\    float cc = 0.0f, rel = 0.0f;
    \\    for (uint start = 0; start < p.sequence; start += 64u) {
    \\        const uint count = min(64u, p.sequence - start);
    \\        if (tid < count) {
    \\            const uint k = start + tid, bucket = dt_bucket(control, p, q, k);
    \\            ds[tid] = dt_adjoint(qkv, relative, control, dout, rows, p, b, h, q, k, bucket, status).ds;
    \\            buckets[tid] = bucket;
    \\        }
    \\        threadgroup_barrier(mem_flags::mem_threadgroup);
    \\        if (tid < p.dimension) for (uint i = 0; i < count; ++i) {
    \\            const uint ko = dt_total(p) + dt_data(p, b, h, start + i) + tid;
    \\            const uint kr = ((p.relative_rows + buckets[i]) * p.heads + h) * p.dimension + tid;
    \\            cc += ds[i] * (qkv[ko] / p.attention_scale);
    \\            rel += (ds[i] / p.attention_scale) * relative[kr];
    \\        }
    \\        threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    }
    \\    if (tid < p.dimension) {
    \\        const float value = cc + rel;
    \\        dt_finite(value, status); output[dt_data(p, b, h, q) + tid] = value;
    \\    }
    \\
;

const dkdv =
    \\    const uint row = p.begin + group;
    \\    const uint k = row % p.sequence, h = (row / p.sequence) % p.heads, b = row / (p.sequence * p.heads);
    \\    threadgroup float* ds = tile;
    \\    threadgroup float* probability = tile + 64;
    \\    threadgroup uint* buckets = reinterpret_cast<threadgroup uint*>(tile + 128);
    \\    float cc = 0.0f, rel = 0.0f, value = 0.0f;
    \\    for (uint start = 0; start < p.sequence; start += 64u) {
    \\        const uint count = min(64u, p.sequence - start);
    \\        if (tid < count) {
    \\            const uint q = start + tid, bucket = dt_bucket(control, p, q, k);
    \\            const DTAdjoint adjoint = dt_adjoint(qkv, relative, control, dout, rows, p, b, h, q, k, bucket, status);
    \\            ds[tid] = adjoint.ds; probability[tid] = adjoint.probability; buckets[tid] = bucket;
    \\        }
    \\        threadgroup_barrier(mem_flags::mem_threadgroup);
    \\        if (tid < p.dimension) for (uint i = 0; i < count; ++i) {
    \\            const uint qo = dt_data(p, b, h, start + i) + tid;
    \\            const uint qr = (buckets[i] * p.heads + h) * p.dimension + tid;
    \\            cc += ds[i] * qkv[qo];
    \\            rel += (ds[i] / p.attention_scale) * relative[qr];
    \\            value += probability[i] * dout[qo];
    \\        }
    \\        threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    }
    \\    if (tid < p.dimension) {
    \\        const uint index = dt_data(p, b, h, k) + tid;
    \\        const float dk = cc / p.attention_scale + rel;
    \\        dt_finite(dk, status); dt_finite(value, status);
    \\        output[dt_total(p) + index] = dk;
    \\        output[2u * dt_total(p) + index] = value;
    \\    }
    \\
;

const relative =
    \\    const uint g = p.group_begin + group;
    \\    const uint bucket = uint(group_rows[g]), h = p.head_index, b = p.batch_index;
    \\    const uint offset = 3u * dt_total(p) + (bucket * p.heads + h) * p.dimension;
    \\    const uint relsize = p.relative_rows * p.heads * p.dimension;
    \\    const uint first = uint(offsets[g]), last = uint(offsets[g + 1u]);
    \\    const uint size = last - first;
    \\    // The rare split path only has one query. Descending offset ordinals give
    \\    // ascending key positions; every output has one writer in every wave.
    \\    const uint begin = min(p.order_begin, size);
    \\    const uint end = min(size, begin + p.order_count);
    \\    threadgroup float* ds = tile;
    \\    threadgroup int* keys = reinterpret_cast<threadgroup int*>(tile + 128);
    \\    float qr = tid < p.dimension ? output[offset + tid] : 0.0f;
    \\    float kr = tid < p.dimension ? output[offset + relsize + tid] : 0.0f;
    \\    for (uint q = p.begin; q < p.begin + p.count; ++q) {
    \\        for (uint start = begin; start < end; start += 64u) {
    \\            const uint count = min(64u, end - start);
    \\            if (tid < count) {
    \\                const int diagonal = order[last - 1u - (start + tid)] - int(p.sequence - 1u);
    \\                const int key = int(q) - diagonal;
    \\                keys[tid] = key;
    \\                ds[tid] = key >= 0 && uint(key) < p.sequence ?
    \\                    dt_adjoint(qkv, relative, control, dout, rows, p, b, h, q, uint(key), bucket, status).ds / p.attention_scale : 0.0f;
    \\            }
    \\            threadgroup_barrier(mem_flags::mem_threadgroup);
    \\            if (tid < p.dimension) for (uint i = 0; i < count; ++i) {
    \\                const int key = keys[i];
    \\                if (key >= 0 && uint(key) < p.sequence) {
    \\                    qr += ds[i] * qkv[dt_total(p) + dt_data(p, b, h, uint(key)) + tid];
    \\                    kr += ds[i] * qkv[dt_data(p, b, h, q) + tid];
    \\                }
    \\            }
    \\            threadgroup_barrier(mem_flags::mem_threadgroup);
    \\        }
    \\    }
    \\    if (tid < p.dimension) {
    \\        dt_finite(qr, status); dt_finite(kr, status);
    \\        output[offset + tid] = qr; output[offset + relsize + tid] = kr;
    \\    }
    \\
;
