// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const bert_arch = @import("../architectures/bert.zig");
const ops = @import("../ops/ops.zig");
const web_runtime = @import("runtime_state.zig");
const linalg = @import("termite_linalg");

pub fn rerank(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    input_ids: []const i64,
    attention_mask: []const i64,
    batch: u32,
    seq_len: u32,
    num_labels: u32,
    out_scores_ptr: [*]f32,
) !u32 {
    const config = switch (model.config) {
        .bert => |cfg| cfg,
        .clap, .clip, .deberta, .florence, .gpt, .t5, .whisper => return error.UnsupportedModelType,
    };

    const hidden_size = config.hidden_size;
    const total = batch * seq_len;
    const labels = if (num_labels > 0) num_labels else 1;

    const token_type_ids = try allocator.alloc(i64, total);
    defer allocator.free(token_type_ids);
    for (0..batch) |b| {
        var seg: i64 = 0;
        for (0..seq_len) |s| {
            const idx = b * seq_len + s;
            token_type_ids[idx] = seg;
            if (input_ids[idx] == 102 and seg == 0) {
                seg = 1;
            }
        }
    }

    var cb = model.compute.computeBackend();
    const hidden = try bert_arch.forward(
        &cb,
        allocator,
        config,
        input_ids,
        attention_mask,
        token_type_ids,
        batch,
        seq_len,
    );
    defer allocator.free(hidden);

    const cls_states = try allocator.alloc(f32, batch * hidden_size);
    defer allocator.free(cls_states);
    for (0..batch) |b| {
        @memcpy(cls_states[b * hidden_size ..][0..hidden_size], hidden[b * seq_len * hidden_size ..][0..hidden_size]);
    }

    const logits = try allocator.alloc(f32, batch * labels);
    defer allocator.free(logits);
    try applyClassifierHead(allocator, &cb, cls_states, logits, batch, hidden_size, labels);

    for (0..batch) |b| {
        if (labels == 1) {
            out_scores_ptr[b] = sigmoid(logits[b]);
        } else {
            var max_val: f32 = logits[b * labels];
            for (1..labels) |l| {
                if (logits[b * labels + l] > max_val) max_val = logits[b * labels + l];
            }
            var sum: f32 = 0;
            for (0..labels) |l| {
                sum += @exp(logits[b * labels + l] - max_val);
            }
            const score_idx: usize = if (labels >= 2) 1 else 0;
            out_scores_ptr[b] = @exp(logits[b * labels + score_idx] - max_val) / sum;
        }
    }

    return batch;
}

// If the backend was constructed with an Io, dispatch matmul through it so
// work composes with the caller's runtime thread pool.  Otherwise fall back
// to the *Sync escape hatch (process-wide futex pool).  Cancellation passes
// through naturally; on cancel, the partially-written output is the caller's
// responsibility to discard.
inline fn cbSgemmTransB(
    cb: *const ops.ComputeBackend,
    m: usize,
    n: usize,
    k: usize,
    alpha: f32,
    a: []const f32,
    b: []const f32,
    beta: f32,
    c: []f32,
) error{Canceled}!void {
    if (cb.getIo()) |io| {
        return linalg.sgemmTransB(io, m, n, k, alpha, a, b, beta, c);
    }
    linalg.sgemmTransBSync(m, n, k, alpha, a, b, beta, c);
}

fn applyClassifierHead(
    allocator: std.mem.Allocator,
    cb: *const ops.ComputeBackend,
    cls_states: []const f32,
    logits: []f32,
    batch: u32,
    hidden_size: u32,
    labels: u32,
) !void {
    if (cb.getWeight("classifier.weight")) |cls_w_ct| {
        defer cb.free(cls_w_ct);
        const cls_w = try cb.toFloat32(cls_w_ct, allocator);
        defer allocator.free(cls_w);

        try cbSgemmTransB(cb, batch, labels, hidden_size, 1.0, cls_states, cls_w, 0.0, logits);

        if (cb.getWeight("classifier.bias")) |cls_b_ct| {
            defer cb.free(cls_b_ct);
            const cls_b = try cb.toFloat32(cls_b_ct, allocator);
            defer allocator.free(cls_b);
            for (0..batch) |b| {
                for (0..labels) |l| {
                    logits[b * labels + l] += cls_b[l];
                }
            }
        } else |_| {}
        return;
    } else |_| {}

    if (cb.getWeight("pooler.dense.weight")) |pool_w_ct| {
        defer cb.free(pool_w_ct);
        const pool_w = try cb.toFloat32(pool_w_ct, allocator);
        defer allocator.free(pool_w);

        const pooled = try allocator.alloc(f32, batch * hidden_size);
        defer allocator.free(pooled);
        try cbSgemmTransB(cb, batch, hidden_size, hidden_size, 1.0, cls_states, pool_w, 0.0, pooled);

        if (cb.getWeight("pooler.dense.bias")) |pool_b_ct| {
            defer cb.free(pool_b_ct);
            const pool_b = try cb.toFloat32(pool_b_ct, allocator);
            defer allocator.free(pool_b);
            for (0..batch) |b| {
                for (0..hidden_size) |j| {
                    pooled[b * hidden_size + j] = std.math.tanh(pooled[b * hidden_size + j] + pool_b[j]);
                }
            }
        } else |_| {
            for (0..batch) |b| {
                for (0..hidden_size) |j| {
                    pooled[b * hidden_size + j] = std.math.tanh(pooled[b * hidden_size + j]);
                }
            }
        }

        for (0..batch) |b| {
            for (0..labels) |l| {
                logits[b * labels + l] = pooled[b * hidden_size + l];
            }
        }
        return;
    } else |_| {}

    for (0..batch) |b| {
        for (0..labels) |l| {
            logits[b * labels + l] = cls_states[b * hidden_size + l];
        }
    }
}

fn sigmoid(x: f32) f32 {
    return 1.0 / (1.0 + @exp(-x));
}
