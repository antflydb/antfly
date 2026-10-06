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

pub const RouteSelection = struct {
    indices: []u32,
    weights: []f32,

    pub fn deinit(self: RouteSelection, allocator: std.mem.Allocator) void {
        allocator.free(self.indices);
        allocator.free(self.weights);
    }
};

pub fn sqrtSoftplus(x: f32) f32 {
    const softplus = if (x > 20.0) x else if (x < -20.0) @exp(x) else @log(1.0 + @exp(x));
    return @sqrt(@max(softplus, 0.0));
}

pub fn sigmoid(x: f32) f32 {
    return 1.0 / (1.0 + @exp(-x));
}

pub fn scoreValue(scoring_func: anytype, logit: f32) f32 {
    return switch (scoring_func) {
        .sqrtsoftplus => sqrtSoftplus(logit),
        .sigmoid => sigmoid(logit),
        .softmax => @exp(logit),
        else => logit,
    };
}

pub fn selectTopKRoutes(
    allocator: std.mem.Allocator,
    logits: []const f32,
    correction_bias: ?[]const f32,
    top_k: usize,
    scoring_func: anytype,
    normalize: bool,
    scale: f32,
) !RouteSelection {
    const k = @min(top_k, logits.len);
    var selected = try allocator.alloc(u32, k);
    errdefer allocator.free(selected);
    var weights = try allocator.alloc(f32, k);
    errdefer allocator.free(weights);
    var used = try allocator.alloc(bool, logits.len);
    defer allocator.free(used);
    @memset(used, false);

    var weight_sum: f32 = 0.0;
    for (0..k) |slot| {
        var best_index: usize = 0;
        var best_score: f32 = -std.math.inf(f32);
        for (logits, 0..) |logit, idx| {
            if (used[idx]) continue;
            const route_score = scoreValue(scoring_func, logit);
            const biased_score = route_score + if (correction_bias) |bias| bias[idx] else 0.0;
            if (biased_score > best_score) {
                best_score = biased_score;
                best_index = idx;
            }
        }
        used[best_index] = true;
        selected[slot] = @intCast(best_index);
        weights[slot] = scoreValue(scoring_func, logits[best_index]);
        weight_sum += weights[slot];
    }

    if (normalize and weight_sum > 0.0) {
        for (weights) |*weight| weight.* = weight.* / weight_sum;
    }
    for (weights) |*weight| weight.* *= scale;

    return .{ .indices = selected, .weights = weights };
}

pub fn unweightedRmsRows(data: []f32, row_dim: usize, eps: f32) void {
    if (row_dim == 0) return;
    const rows = data.len / row_dim;
    for (0..rows) |row| {
        const base = row * row_dim;
        var sum_sq: f32 = 0.0;
        for (data[base..][0..row_dim]) |value| sum_sq += value * value;
        const inv = 1.0 / @sqrt(sum_sq / @as(f32, @floatFromInt(row_dim)) + eps);
        for (data[base..][0..row_dim]) |*value| value.* *= inv;
    }
}

pub const HyperStreamsShape = struct {
    rows: usize,
    hc_mult: usize,
    hidden_dim: usize,

    pub fn flatDim(self: HyperStreamsShape) usize {
        return self.hc_mult * self.hidden_dim;
    }

    pub fn streamIndex(self: HyperStreamsShape, row: usize, stream: usize, col: usize) usize {
        return (row * self.hc_mult + stream) * self.hidden_dim + col;
    }

    pub fn flatIndex(self: HyperStreamsShape, row: usize, col: usize) usize {
        return row * self.flatDim() + col;
    }

    pub fn outputIndex(self: HyperStreamsShape, row: usize, col: usize) usize {
        return row * self.hidden_dim + col;
    }

    pub fn hcFnIndex(self: HyperStreamsShape, stream: usize, col: usize) usize {
        return stream * self.flatDim() + col;
    }

    pub fn validateStreams(self: HyperStreamsShape, streams: []const f32) void {
        std.debug.assert(self.rows > 0);
        std.debug.assert(self.hc_mult > 0);
        std.debug.assert(self.hidden_dim > 0);
        std.debug.assert(streams.len == self.rows * self.flatDim());
    }

    pub fn validateReduced(self: HyperStreamsShape, reduced: []const f32) void {
        std.debug.assert(reduced.len == self.rows * self.hidden_dim);
    }

    pub fn validateWeights(self: HyperStreamsShape, weights: []const f32) void {
        std.debug.assert(weights.len == self.rows * self.hc_mult);
    }

    pub fn validateHyperHeadParams(self: HyperStreamsShape, hc_fn: []const f32, hc_base: []const f32, hc_scale: []const f32) void {
        std.debug.assert(hc_fn.len == self.hc_mult * self.flatDim());
        std.debug.assert(hc_base.len == self.hc_mult);
        std.debug.assert(hc_scale.len >= 1);
    }
};

pub fn hyperHeadWeightsRows(
    weights: []f32,
    hidden_streams: []const f32,
    hc_fn: []const f32,
    hc_base: []const f32,
    hc_scale: []const f32,
    shape: HyperStreamsShape,
    hc_eps: f32,
    rms_norm_eps: f32,
) void {
    shape.validateStreams(hidden_streams);
    shape.validateWeights(weights);
    shape.validateHyperHeadParams(hc_fn, hc_base, hc_scale);

    const flat_dim = shape.flatDim();
    const scale = hc_scale[0];
    for (0..shape.rows) |row| {
        var sum_sq: f32 = 0.0;
        for (0..flat_dim) |col| {
            const value = hidden_streams[shape.flatIndex(row, col)];
            sum_sq += value * value;
        }
        const rsqrt = 1.0 / @sqrt(sum_sq / @as(f32, @floatFromInt(flat_dim)) + rms_norm_eps);

        for (0..shape.hc_mult) |stream| {
            var mix: f32 = 0.0;
            for (0..flat_dim) |col| {
                mix += hidden_streams[shape.flatIndex(row, col)] * hc_fn[shape.hcFnIndex(stream, col)];
            }
            weights[row * shape.hc_mult + stream] = sigmoid(mix * rsqrt * scale + hc_base[stream]) + hc_eps;
        }
    }
}

pub fn sigmoidWeightedStreamReduceRows(
    output: []f32,
    hidden_streams: []const f32,
    stream_logits: []const f32,
    shape: HyperStreamsShape,
    eps: f32,
) void {
    shape.validateStreams(hidden_streams);
    shape.validateReduced(output);
    shape.validateWeights(stream_logits);

    @memset(output, 0.0);
    for (0..shape.rows) |row| {
        for (0..shape.hc_mult) |stream| {
            const weight = sigmoid(stream_logits[row * shape.hc_mult + stream]) + eps;
            for (0..shape.hidden_dim) |col| {
                output[shape.outputIndex(row, col)] += weight * hidden_streams[shape.streamIndex(row, stream, col)];
            }
        }
    }
}

pub fn weightedStreamReduceRows(
    output: []f32,
    hidden_streams: []const f32,
    stream_weights: []const f32,
    shape: HyperStreamsShape,
) void {
    shape.validateStreams(hidden_streams);
    shape.validateReduced(output);
    shape.validateWeights(stream_weights);

    @memset(output, 0.0);
    for (0..shape.rows) |row| {
        for (0..shape.hc_mult) |stream| {
            const weight = stream_weights[row * shape.hc_mult + stream];
            for (0..shape.hidden_dim) |col| {
                output[shape.outputIndex(row, col)] += weight * hidden_streams[shape.streamIndex(row, stream, col)];
            }
        }
    }
}

pub fn sumStreamReduceRows(
    output: []f32,
    hidden_streams: []const f32,
    shape: HyperStreamsShape,
) void {
    shape.validateStreams(hidden_streams);
    shape.validateReduced(output);

    @memset(output, 0.0);
    for (0..shape.rows) |row| {
        for (0..shape.hc_mult) |stream| {
            for (0..shape.hidden_dim) |col| {
                output[shape.outputIndex(row, col)] += hidden_streams[shape.streamIndex(row, stream, col)];
            }
        }
    }
}

pub fn hyperHeadCollapseRows(
    output: []f32,
    scratch_weights: []f32,
    hidden_streams: []const f32,
    hc_fn: []const f32,
    hc_base: []const f32,
    hc_scale: []const f32,
    shape: HyperStreamsShape,
    hc_eps: f32,
    rms_norm_eps: f32,
) void {
    hyperHeadWeightsRows(scratch_weights, hidden_streams, hc_fn, hc_base, hc_scale, shape, hc_eps, rms_norm_eps);
    weightedStreamReduceRows(output, hidden_streams, scratch_weights, shape);
}

pub fn broadcastAddStreamsRows(
    hidden_streams: []f32,
    update: []const f32,
    stream_weights: ?[]const f32,
    shape: HyperStreamsShape,
) void {
    shape.validateStreams(hidden_streams);
    shape.validateReduced(update);
    if (stream_weights) |weights| shape.validateWeights(weights);

    for (0..shape.rows) |row| {
        for (0..shape.hc_mult) |stream| {
            const weight = if (stream_weights) |weights| weights[row * shape.hc_mult + stream] else 1.0;
            for (0..shape.hidden_dim) |col| {
                hidden_streams[shape.streamIndex(row, stream, col)] += weight * update[shape.outputIndex(row, col)];
            }
        }
    }
}

pub fn hyperConnectionUpdateRows(
    hidden_streams: []f32,
    update: []const f32,
    scratch_next_row: []f32,
    scratch_comb: []f32,
    scratch_pre_weights: []f32,
    scratch_post_weights: []f32,
    fn_data: []const f32,
    base_data: []const f32,
    scale_data: []const f32,
    shape: HyperStreamsShape,
    hc_eps: f32,
    rms_norm_eps: f32,
    sinkhorn_iters: usize,
) void {
    shape.validateStreams(hidden_streams);
    shape.validateReduced(update);
    const flat_dim = shape.flatDim();
    const mix_dim = (2 + shape.hc_mult) * shape.hc_mult;
    std.debug.assert(fn_data.len == mix_dim * flat_dim);
    std.debug.assert(base_data.len == mix_dim);
    std.debug.assert(scale_data.len >= 3);
    std.debug.assert(scratch_next_row.len == flat_dim);
    std.debug.assert(scratch_comb.len == shape.hc_mult * shape.hc_mult);
    std.debug.assert(scratch_pre_weights.len == shape.hc_mult);
    std.debug.assert(scratch_post_weights.len == shape.hc_mult);

    for (0..shape.rows) |row| {
        const stream_row = hidden_streams[row * flat_dim ..][0..flat_dim];
        var sum_sq: f32 = 0.0;
        for (stream_row) |value| sum_sq += value * value;
        const rsqrt = 1.0 / @sqrt(sum_sq / @as(f32, @floatFromInt(flat_dim)) + rms_norm_eps);

        for (0..shape.hc_mult) |dst_stream| {
            var pre_logit: f32 = 0.0;
            var post_logit: f32 = 0.0;
            const pre_weight_base = dst_stream * flat_dim;
            const post_weight_base = (shape.hc_mult + dst_stream) * flat_dim;
            for (0..flat_dim) |col| {
                pre_logit += stream_row[col] * fn_data[pre_weight_base + col];
                post_logit += stream_row[col] * fn_data[post_weight_base + col];
            }
            scratch_pre_weights[dst_stream] = sigmoid(pre_logit * rsqrt * scale_data[0] + base_data[dst_stream]) + hc_eps;
            scratch_post_weights[dst_stream] = sigmoid(post_logit * rsqrt * scale_data[1] + base_data[shape.hc_mult + dst_stream]) + hc_eps;

            for (0..shape.hc_mult) |src_stream| {
                var comb_logit: f32 = 0.0;
                const comb_index = dst_stream * shape.hc_mult + src_stream;
                const comb_weight_base = (2 * shape.hc_mult + comb_index) * flat_dim;
                for (0..flat_dim) |col| {
                    comb_logit += stream_row[col] * fn_data[comb_weight_base + col];
                }
                scratch_comb[comb_index] = sigmoid(comb_logit * rsqrt * scale_data[2] + base_data[2 * shape.hc_mult + comb_index]) + hc_eps;
            }
        }

        sinkhornInPlace(scratch_comb, shape.hc_mult, sinkhorn_iters, hc_eps);

        for (0..shape.hc_mult) |dst_stream| {
            for (0..shape.hidden_dim) |col| {
                var mixed: f32 = 0.0;
                for (0..shape.hc_mult) |src_stream| {
                    mixed += scratch_comb[dst_stream * shape.hc_mult + src_stream] * stream_row[src_stream * shape.hidden_dim + col];
                }
                scratch_next_row[dst_stream * shape.hidden_dim + col] =
                    scratch_pre_weights[dst_stream] *
                    (mixed + scratch_post_weights[dst_stream] * update[shape.outputIndex(row, col)]);
            }
        }
        @memcpy(stream_row, scratch_next_row);
    }
}

pub fn applyTrailingRopeRows(
    data: []f32,
    positions: []const u32,
    num_heads: usize,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
) void {
    applyTrailingRopeRowsSigned(data, positions, num_heads, head_dim, rope_dim, theta, 1.0);
}

pub fn applyInverseTrailingRopeRows(
    data: []f32,
    positions: []const u32,
    num_heads: usize,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
) void {
    applyTrailingRopeRowsSigned(data, positions, num_heads, head_dim, rope_dim, theta, -1.0);
}

fn applyTrailingRopeRowsSigned(
    data: []f32,
    positions: []const u32,
    num_heads: usize,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
    direction: f32,
) void {
    if (num_heads == 0 or head_dim == 0 or rope_dim == 0) return;
    std.debug.assert(rope_dim <= head_dim);
    std.debug.assert((rope_dim & 1) == 0);
    std.debug.assert(data.len == positions.len * num_heads * head_dim);
    const head_stride = head_dim;
    const row_stride = num_heads * head_dim;
    const rope_start = head_dim - rope_dim;
    for (positions, 0..) |position, row| {
        const pos_f: f32 = @floatFromInt(position);
        for (0..num_heads) |head| {
            const head_base = row * row_stride + head * head_stride;
            var pair: usize = 0;
            while (pair < rope_dim / 2) : (pair += 1) {
                const freq_exponent = @as(f32, @floatFromInt(pair * 2)) / @as(f32, @floatFromInt(rope_dim));
                const angle = direction * pos_f / std.math.pow(f32, theta, freq_exponent);
                const cos_v = @cos(angle);
                const sin_v = @sin(angle);
                const idx = head_base + rope_start + pair * 2;
                const x0 = data[idx];
                const x1 = data[idx + 1];
                data[idx] = x0 * cos_v - x1 * sin_v;
                data[idx + 1] = x0 * sin_v + x1 * cos_v;
            }
        }
    }
}

pub const AttentionRowsShape = struct {
    query_rows: usize,
    key_rows: usize,
    num_query_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    value_dim: usize,

    pub fn queryIndex(self: AttentionRowsShape, row: usize, head: usize, col: usize) usize {
        return (row * self.num_query_heads + head) * self.head_dim + col;
    }

    pub fn keyIndex(self: AttentionRowsShape, row: usize, head: usize, col: usize) usize {
        return (row * self.num_kv_heads + head) * self.head_dim + col;
    }

    pub fn valueIndex(self: AttentionRowsShape, row: usize, head: usize, col: usize) usize {
        return (row * self.num_kv_heads + head) * self.value_dim + col;
    }

    pub fn outputIndex(self: AttentionRowsShape, row: usize, head: usize, col: usize) usize {
        return (row * self.num_query_heads + head) * self.value_dim + col;
    }

    pub fn kvHeadForQueryHead(self: AttentionRowsShape, query_head: usize) usize {
        std.debug.assert(self.num_query_heads >= self.num_kv_heads);
        std.debug.assert((self.num_query_heads % self.num_kv_heads) == 0);
        return query_head / (self.num_query_heads / self.num_kv_heads);
    }

    pub fn validate(self: AttentionRowsShape, query: []const f32, key: []const f32, value: []const f32, output: []const f32) void {
        std.debug.assert(self.num_query_heads > 0);
        std.debug.assert(self.num_kv_heads > 0);
        std.debug.assert(self.head_dim > 0);
        std.debug.assert(self.value_dim > 0);
        std.debug.assert((self.num_query_heads % self.num_kv_heads) == 0);
        std.debug.assert(query.len == self.query_rows * self.num_query_heads * self.head_dim);
        std.debug.assert(key.len == self.key_rows * self.num_kv_heads * self.head_dim);
        std.debug.assert(value.len == self.key_rows * self.num_kv_heads * self.value_dim);
        std.debug.assert(output.len == self.query_rows * self.num_query_heads * self.value_dim);
    }
};

pub fn softmaxAttentionRows(
    output: []f32,
    query: []const f32,
    key: []const f32,
    value: []const f32,
    sink_logits: ?[]const f32,
    shape: AttentionRowsShape,
    scale: f32,
    causal: bool,
) void {
    shape.validate(query, key, value, output);
    if (sink_logits) |sinks| std.debug.assert(sinks.len == shape.num_query_heads);

    for (0..shape.query_rows) |query_row| {
        const max_key_row = if (causal) @min(query_row + 1, shape.key_rows) else shape.key_rows;
        for (0..shape.num_query_heads) |query_head| {
            const kv_head = shape.kvHeadForQueryHead(query_head);
            var max_logit = if (sink_logits) |sinks| sinks[query_head] else -std.math.inf(f32);
            for (0..max_key_row) |key_row| {
                const logit = attentionDot(query, key, shape, query_row, query_head, key_row, kv_head) * scale;
                max_logit = @max(max_logit, logit);
            }

            var denom: f32 = if (sink_logits) |sinks| @exp(sinks[query_head] - max_logit) else 0.0;
            for (0..max_key_row) |key_row| {
                const logit = attentionDot(query, key, shape, query_row, query_head, key_row, kv_head) * scale;
                denom += @exp(logit - max_logit);
            }

            for (0..shape.value_dim) |col| {
                var sum: f32 = 0.0;
                for (0..max_key_row) |key_row| {
                    const logit = attentionDot(query, key, shape, query_row, query_head, key_row, kv_head) * scale;
                    const weight = @exp(logit - max_logit) / denom;
                    sum += weight * value[shape.valueIndex(key_row, kv_head, col)];
                }
                output[shape.outputIndex(query_row, query_head, col)] = sum;
            }
        }
    }
}

fn attentionDot(query: []const f32, key: []const f32, shape: AttentionRowsShape, query_row: usize, query_head: usize, key_row: usize, kv_head: usize) f32 {
    var dot: f32 = 0.0;
    for (0..shape.head_dim) |col| {
        dot += query[shape.queryIndex(query_row, query_head, col)] * key[shape.keyIndex(key_row, kv_head, col)];
    }
    return dot;
}

pub const ChunkCompressionShape = struct {
    rows: usize,
    chunk_size: usize,
    input_dim: usize,
    compressed_dim: usize,

    pub fn chunkCount(self: ChunkCompressionShape) usize {
        std.debug.assert(self.chunk_size > 0);
        return (self.rows + self.chunk_size - 1) / self.chunk_size;
    }

    pub fn inputIndex(self: ChunkCompressionShape, row: usize, col: usize) usize {
        return row * self.input_dim + col;
    }

    pub fn projectionIndex(self: ChunkCompressionShape, compressed_col: usize, input_col: usize) usize {
        return compressed_col * self.input_dim + input_col;
    }

    pub fn outputIndex(self: ChunkCompressionShape, chunk: usize, col: usize) usize {
        return chunk * self.compressed_dim + col;
    }

    pub fn validate(
        self: ChunkCompressionShape,
        input: []const f32,
        projection: []const f32,
        gate_projection: ?[]const f32,
        position_bias: ?[]const f32,
        output: []const f32,
    ) void {
        std.debug.assert(self.chunk_size > 0);
        std.debug.assert(self.input_dim > 0);
        std.debug.assert(self.compressed_dim > 0);
        std.debug.assert(input.len == self.rows * self.input_dim);
        std.debug.assert(projection.len == self.compressed_dim * self.input_dim);
        if (gate_projection) |gate| std.debug.assert(gate.len == self.input_dim);
        if (position_bias) |bias| std.debug.assert(bias.len == self.chunk_size);
        std.debug.assert(output.len == self.chunkCount() * self.compressed_dim);
    }
};

pub fn gatedChunkCompressRows(
    output: []f32,
    input: []const f32,
    projection: []const f32,
    gate_projection: ?[]const f32,
    position_bias: ?[]const f32,
    shape: ChunkCompressionShape,
    gate_bias: f32,
    normalize_by_gate_sum: bool,
) void {
    shape.validate(input, projection, gate_projection, position_bias, output);
    @memset(output, 0.0);

    const chunks = shape.chunkCount();
    for (0..chunks) |chunk| {
        const row_begin = chunk * shape.chunk_size;
        const row_end = @min(row_begin + shape.chunk_size, shape.rows);
        var gate_sum: f32 = 0.0;

        for (row_begin..row_end) |row| {
            const offset = row - row_begin;
            var gate_logit = gate_bias + if (position_bias) |bias| bias[offset] else 0.0;
            if (gate_projection) |gate| {
                for (0..shape.input_dim) |input_col| {
                    gate_logit += input[shape.inputIndex(row, input_col)] * gate[input_col];
                }
            }
            const gate_weight = sigmoid(gate_logit);
            gate_sum += gate_weight;

            for (0..shape.compressed_dim) |compressed_col| {
                var projected: f32 = 0.0;
                for (0..shape.input_dim) |input_col| {
                    projected += input[shape.inputIndex(row, input_col)] * projection[shape.projectionIndex(compressed_col, input_col)];
                }
                output[shape.outputIndex(chunk, compressed_col)] += gate_weight * projected;
            }
        }

        if (normalize_by_gate_sum and gate_sum > 0.0) {
            for (0..shape.compressed_dim) |compressed_col| {
                output[shape.outputIndex(chunk, compressed_col)] /= gate_sum;
            }
        }
    }
}

pub fn selectTopKIndices(scores: []const f32, indices: []u32) void {
    std.debug.assert(indices.len <= scores.len);
    for (0..indices.len) |slot| {
        var best_index: usize = 0;
        var best_score: f32 = undefined;
        var best_set = false;

        for (scores, 0..) |score, idx| {
            var already_selected = false;
            for (indices[0..slot]) |selected| {
                if (selected == idx) {
                    already_selected = true;
                    break;
                }
            }
            if (already_selected) continue;

            if (!best_set or score > best_score) {
                best_index = idx;
                best_score = score;
                best_set = true;
            }
        }

        std.debug.assert(best_set);
        indices[slot] = @intCast(best_index);
    }
}

pub const WeightedGatherRowsShape = struct {
    rows: usize,
    source_rows: usize,
    top_k: usize,
    row_dim: usize,

    pub fn routeIndex(self: WeightedGatherRowsShape, row: usize, slot: usize) usize {
        return row * self.top_k + slot;
    }

    pub fn sourceIndex(self: WeightedGatherRowsShape, row: usize, col: usize) usize {
        return row * self.row_dim + col;
    }

    pub fn outputIndex(self: WeightedGatherRowsShape, row: usize, col: usize) usize {
        return row * self.row_dim + col;
    }

    pub fn validate(self: WeightedGatherRowsShape, output: []const f32, source: []const f32, indices: []const u32, weights: []const f32) void {
        std.debug.assert(self.row_dim > 0);
        std.debug.assert(self.source_rows > 0);
        std.debug.assert(source.len == self.source_rows * self.row_dim);
        std.debug.assert(indices.len == self.rows * self.top_k);
        std.debug.assert(weights.len == indices.len);
        std.debug.assert(output.len == self.rows * self.row_dim);
    }
};

pub fn weightedGatherRows(
    output: []f32,
    source: []const f32,
    indices: []const u32,
    weights: []const f32,
    shape: WeightedGatherRowsShape,
) void {
    shape.validate(output, source, indices, weights);
    @memset(output, 0.0);
    for (0..shape.rows) |row| {
        for (0..shape.top_k) |slot| {
            const source_row: usize = @intCast(indices[shape.routeIndex(row, slot)]);
            std.debug.assert(source_row < shape.source_rows);
            const weight = weights[shape.routeIndex(row, slot)];
            for (0..shape.row_dim) |col| {
                output[shape.outputIndex(row, col)] += weight * source[shape.sourceIndex(source_row, col)];
            }
        }
    }
}

pub const LatentKeyValueRowsShape = struct {
    rows: usize,
    latent_dim: usize,
    key_dim: usize,
    value_dim: usize,

    pub fn latentIndex(self: LatentKeyValueRowsShape, row: usize, col: usize) usize {
        return row * self.latent_dim + col;
    }

    pub fn keyProjectionIndex(self: LatentKeyValueRowsShape, key_col: usize, latent_col: usize) usize {
        return key_col * self.latent_dim + latent_col;
    }

    pub fn valueProjectionIndex(self: LatentKeyValueRowsShape, value_col: usize, latent_col: usize) usize {
        return value_col * self.latent_dim + latent_col;
    }

    pub fn keyIndex(self: LatentKeyValueRowsShape, row: usize, col: usize) usize {
        return row * self.key_dim + col;
    }

    pub fn valueIndex(self: LatentKeyValueRowsShape, row: usize, col: usize) usize {
        return row * self.value_dim + col;
    }

    pub fn validate(self: LatentKeyValueRowsShape, key: []const f32, value: []const f32, latent: []const f32, key_projection: []const f32, value_projection: []const f32) void {
        std.debug.assert(self.latent_dim > 0);
        std.debug.assert(self.key_dim > 0);
        std.debug.assert(self.value_dim > 0);
        std.debug.assert(latent.len == self.rows * self.latent_dim);
        std.debug.assert(key_projection.len == self.key_dim * self.latent_dim);
        std.debug.assert(value_projection.len == self.value_dim * self.latent_dim);
        std.debug.assert(key.len == self.rows * self.key_dim);
        std.debug.assert(value.len == self.rows * self.value_dim);
    }
};

pub fn projectLatentKeyValueRows(
    key: []f32,
    value: []f32,
    latent: []const f32,
    key_projection: []const f32,
    value_projection: []const f32,
    shape: LatentKeyValueRowsShape,
) void {
    shape.validate(key, value, latent, key_projection, value_projection);
    for (0..shape.rows) |row| {
        for (0..shape.key_dim) |key_col| {
            var sum: f32 = 0.0;
            for (0..shape.latent_dim) |latent_col| {
                sum += latent[shape.latentIndex(row, latent_col)] * key_projection[shape.keyProjectionIndex(key_col, latent_col)];
            }
            key[shape.keyIndex(row, key_col)] = sum;
        }
        for (0..shape.value_dim) |value_col| {
            var sum: f32 = 0.0;
            for (0..shape.latent_dim) |latent_col| {
                sum += latent[shape.latentIndex(row, latent_col)] * value_projection[shape.valueProjectionIndex(value_col, latent_col)];
            }
            value[shape.valueIndex(row, value_col)] = sum;
        }
    }
}

pub fn sinkhornInPlace(matrix: []f32, hc_mult: usize, iterations: usize, eps: f32) void {
    if (hc_mult == 0) return;
    std.debug.assert(matrix.len == hc_mult * hc_mult);
    for (0..iterations) |_| {
        for (0..hc_mult) |row| {
            var sum: f32 = 0.0;
            for (0..hc_mult) |col| sum += matrix[row * hc_mult + col];
            const denom = sum + eps;
            for (0..hc_mult) |col| matrix[row * hc_mult + col] /= denom;
        }
        for (0..hc_mult) |col| {
            var sum: f32 = 0.0;
            for (0..hc_mult) |row| sum += matrix[row * hc_mult + col];
            const denom = sum + eps;
            for (0..hc_mult) |row| matrix[row * hc_mult + col] /= denom;
        }
    }
}

pub fn applyClampedSwiGLU(gate_up: []f32, limit: f32) void {
    const half = gate_up.len / 2;
    std.debug.assert(half * 2 == gate_up.len);
    for (0..half) |i| {
        const gate = @min(gate_up[i], limit);
        const up = std.math.clamp(gate_up[half + i], -limit, limit);
        gate_up[i] = silu(gate) * up;
    }
}

pub const GroupedOutputProjectionShape = struct {
    rows: usize,
    groups: usize,
    group_in_dim: usize,
    out_dim: usize,

    pub fn inputIndex(self: GroupedOutputProjectionShape, row: usize, group: usize, col: usize) usize {
        return (row * self.groups + group) * self.group_in_dim + col;
    }

    pub fn packedWeightIndex(self: GroupedOutputProjectionShape, group: usize, out_col: usize, in_col: usize) usize {
        return (group * self.out_dim + out_col) * self.group_in_dim + in_col;
    }

    pub fn outputIndex(self: GroupedOutputProjectionShape, row: usize, col: usize) usize {
        return row * self.out_dim + col;
    }

    pub fn packedWeightLen(self: GroupedOutputProjectionShape) usize {
        return self.groups * self.out_dim * self.group_in_dim;
    }

    pub fn validate(self: GroupedOutputProjectionShape, input: []const f32, weights: []const f32, output: []const f32) void {
        std.debug.assert(self.groups > 0);
        std.debug.assert(self.group_in_dim > 0);
        std.debug.assert(self.out_dim > 0);
        std.debug.assert(input.len == self.rows * self.groups * self.group_in_dim);
        std.debug.assert(weights.len == self.packedWeightLen());
        std.debug.assert(output.len == self.rows * self.out_dim);
    }
};

pub fn groupedOutputProjectionRows(
    output: []f32,
    input: []const f32,
    weights: []const f32,
    shape: GroupedOutputProjectionShape,
) void {
    shape.validate(input, weights, output);
    @memset(output, 0.0);
    for (0..shape.rows) |row| {
        for (0..shape.groups) |group| {
            for (0..shape.out_dim) |out_col| {
                var sum = output[shape.outputIndex(row, out_col)];
                for (0..shape.group_in_dim) |in_col| {
                    sum += input[shape.inputIndex(row, group, in_col)] * weights[shape.packedWeightIndex(group, out_col, in_col)];
                }
                output[shape.outputIndex(row, out_col)] = sum;
            }
        }
    }
}

pub const PackedMoeExpertShape = struct {
    hidden_dim: usize,
    intermediate_dim: usize,
    num_experts: usize,

    pub fn gateUpWeightIndex(self: PackedMoeExpertShape, expert: usize, gate_or_up: usize, intermediate: usize, hidden: usize) usize {
        return (((expert * 2 + gate_or_up) * self.intermediate_dim + intermediate) * self.hidden_dim) + hidden;
    }

    pub fn downWeightIndex(self: PackedMoeExpertShape, expert: usize, hidden: usize, intermediate: usize) usize {
        return (expert * self.hidden_dim + hidden) * self.intermediate_dim + intermediate;
    }

    pub fn splitGateUpWeightIndex(self: PackedMoeExpertShape, expert: usize, intermediate: usize, hidden: usize) usize {
        return (expert * self.intermediate_dim + intermediate) * self.hidden_dim + hidden;
    }

    pub fn gateUpWeightLen(self: PackedMoeExpertShape) usize {
        return self.num_experts * 2 * self.intermediate_dim * self.hidden_dim;
    }

    pub fn splitGateOrUpWeightLen(self: PackedMoeExpertShape) usize {
        return self.num_experts * self.intermediate_dim * self.hidden_dim;
    }

    pub fn downWeightLen(self: PackedMoeExpertShape) usize {
        return self.num_experts * self.hidden_dim * self.intermediate_dim;
    }

    pub fn validate(self: PackedMoeExpertShape, input: []const f32, gate_up_proj: []const f32, down_proj: []const f32, output: []const f32, scratch_gate_up: []const f32) void {
        std.debug.assert(self.hidden_dim > 0);
        std.debug.assert(self.intermediate_dim > 0);
        std.debug.assert(self.num_experts > 0);
        std.debug.assert(input.len == self.hidden_dim);
        std.debug.assert(gate_up_proj.len == self.gateUpWeightLen());
        std.debug.assert(down_proj.len == self.downWeightLen());
        std.debug.assert(output.len == self.hidden_dim);
        std.debug.assert(scratch_gate_up.len == 2 * self.intermediate_dim);
    }

    pub fn validateSplit(self: PackedMoeExpertShape, input: []const f32, gate_proj: []const f32, up_proj: []const f32, down_proj: []const f32, output: []const f32, scratch_gate_up: []const f32) void {
        std.debug.assert(self.hidden_dim > 0);
        std.debug.assert(self.intermediate_dim > 0);
        std.debug.assert(self.num_experts > 0);
        std.debug.assert(input.len == self.hidden_dim);
        std.debug.assert(gate_proj.len == self.splitGateOrUpWeightLen());
        std.debug.assert(up_proj.len == self.splitGateOrUpWeightLen());
        std.debug.assert(down_proj.len == self.downWeightLen());
        std.debug.assert(output.len == self.hidden_dim);
        std.debug.assert(scratch_gate_up.len == 2 * self.intermediate_dim);
    }
};

pub fn applyPackedMoeExpert(
    output: []f32,
    scratch_gate_up: []f32,
    input: []const f32,
    gate_up_proj: []const f32,
    down_proj: []const f32,
    shape: PackedMoeExpertShape,
    expert: usize,
    route_weight: f32,
    swiglu_limit: f32,
    accumulate: bool,
) void {
    shape.validate(input, gate_up_proj, down_proj, output, scratch_gate_up);
    std.debug.assert(expert < shape.num_experts);
    if (!accumulate) @memset(output, 0.0);
    @memset(scratch_gate_up, 0.0);

    for (0..2) |gate_or_up| {
        for (0..shape.intermediate_dim) |intermediate| {
            var sum: f32 = 0.0;
            for (0..shape.hidden_dim) |hidden| {
                sum += gate_up_proj[shape.gateUpWeightIndex(expert, gate_or_up, intermediate, hidden)] * input[hidden];
            }
            scratch_gate_up[gate_or_up * shape.intermediate_dim + intermediate] = sum;
        }
    }

    applyClampedSwiGLU(scratch_gate_up[0 .. 2 * shape.intermediate_dim], swiglu_limit);
    for (0..shape.hidden_dim) |hidden| {
        var sum: f32 = 0.0;
        for (0..shape.intermediate_dim) |intermediate| {
            sum += down_proj[shape.downWeightIndex(expert, hidden, intermediate)] * scratch_gate_up[intermediate];
        }
        output[hidden] += route_weight * sum;
    }
}

pub fn applyPackedMoeExperts(
    output: []f32,
    scratch_gate_up: []f32,
    input: []const f32,
    gate_up_proj: []const f32,
    down_proj: []const f32,
    shape: PackedMoeExpertShape,
    expert_indices: []const u32,
    route_weights: []const f32,
    swiglu_limit: f32,
) void {
    std.debug.assert(expert_indices.len == route_weights.len);
    @memset(output, 0.0);
    for (expert_indices, route_weights) |expert, route_weight| {
        applyPackedMoeExpert(output, scratch_gate_up, input, gate_up_proj, down_proj, shape, @intCast(expert), route_weight, swiglu_limit, true);
    }
}

pub fn applySplitMoeExpert(
    output: []f32,
    scratch_gate_up: []f32,
    input: []const f32,
    gate_proj: []const f32,
    up_proj: []const f32,
    down_proj: []const f32,
    shape: PackedMoeExpertShape,
    expert: usize,
    route_weight: f32,
    swiglu_limit: f32,
    accumulate: bool,
) void {
    shape.validateSplit(input, gate_proj, up_proj, down_proj, output, scratch_gate_up);
    std.debug.assert(expert < shape.num_experts);
    if (!accumulate) @memset(output, 0.0);
    @memset(scratch_gate_up, 0.0);

    for (0..shape.intermediate_dim) |intermediate| {
        var gate_sum: f32 = 0.0;
        var up_sum: f32 = 0.0;
        for (0..shape.hidden_dim) |hidden| {
            const weight_index = shape.splitGateUpWeightIndex(expert, intermediate, hidden);
            gate_sum += gate_proj[weight_index] * input[hidden];
            up_sum += up_proj[weight_index] * input[hidden];
        }
        scratch_gate_up[intermediate] = gate_sum;
        scratch_gate_up[shape.intermediate_dim + intermediate] = up_sum;
    }

    applyClampedSwiGLU(scratch_gate_up[0 .. 2 * shape.intermediate_dim], swiglu_limit);
    for (0..shape.hidden_dim) |hidden| {
        var sum: f32 = 0.0;
        for (0..shape.intermediate_dim) |intermediate| {
            sum += down_proj[shape.downWeightIndex(expert, hidden, intermediate)] * scratch_gate_up[intermediate];
        }
        output[hidden] += route_weight * sum;
    }
}

pub fn silu(x: f32) f32 {
    return x / (1.0 + @exp(-x));
}

test "sqrtsoftplus route selection normalizes and scales" {
    const allocator = std.testing.allocator;
    const logits = [_]f32{ -2.0, 1.0, 4.0, 0.5 };
    const bias = [_]f32{ 0.0, 10.0, 0.0, 0.0 };
    const routes = try selectTopKRoutes(allocator, &logits, &bias, 2, .sqrtsoftplus, true, 1.5);
    defer routes.deinit(allocator);
    try std.testing.expectEqual(@as(u32, 1), routes.indices[0]);
    try std.testing.expectEqual(@as(u32, 2), routes.indices[1]);
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), routes.weights[0] + routes.weights[1], 1e-5);
}

test "split MoE expert execution matches fused gate/up layout" {
    const shape = PackedMoeExpertShape{
        .hidden_dim = 2,
        .intermediate_dim = 2,
        .num_experts = 2,
    };
    const input = [_]f32{ 0.5, -1.0 };
    const fused = [_]f32{
        0.2,  -0.1, 0.4,  0.3,
        -0.5, 0.7,  0.6,  -0.2,
        0.1,  0.8,  -0.4, 0.5,
        0.9,  -0.3, 0.2,  0.6,
    };
    const gate = [_]f32{
        0.2, -0.1, 0.4,  0.3,
        0.1, 0.8,  -0.4, 0.5,
    };
    const up = [_]f32{
        -0.5, 0.7,  0.6, -0.2,
        0.9,  -0.3, 0.2, 0.6,
    };
    const down = [_]f32{
        0.3,  -0.6, 0.2, 0.4,
        -0.1, 0.5,  0.7, -0.2,
    };
    var fused_out = [_]f32{ 0.0, 0.0 };
    var split_out = [_]f32{ 0.0, 0.0 };
    var fused_scratch = [_]f32{ 0.0, 0.0, 0.0, 0.0 };
    var split_scratch = [_]f32{ 0.0, 0.0, 0.0, 0.0 };

    applyPackedMoeExpert(&fused_out, &fused_scratch, &input, &fused, &down, shape, 1, 0.75, 0.0, false);
    applySplitMoeExpert(&split_out, &split_scratch, &input, &gate, &up, &down, shape, 1, 0.75, 0.0, false);

    try std.testing.expectApproxEqAbs(fused_out[0], split_out[0], 1e-6);
    try std.testing.expectApproxEqAbs(fused_out[1], split_out[1], 1e-6);
}

test "unweighted RMS rows normalizes each row" {
    var data = [_]f32{ 3.0, 4.0, 0.0, 0.0, 1.0, 1.0 };
    unweightedRmsRows(&data, 3, 1e-6);
    const row0_rms = @sqrt((data[0] * data[0] + data[1] * data[1] + data[2] * data[2]) / 3.0);
    const row1_rms = @sqrt((data[3] * data[3] + data[4] * data[4] + data[5] * data[5]) / 3.0);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), row0_rms, 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), row1_rms, 1e-5);
}

test "hyper head weights use flat stream RMS rsqrt" {
    const shape = HyperStreamsShape{ .rows = 2, .hc_mult = 2, .hidden_dim = 2 };
    const streams = [_]f32{
        3.0, 4.0, 0.0, 0.0,
        6.0, 8.0, 0.0, 0.0,
    };
    const hc_fn = [_]f32{
        1.0, 0.0, 0.0, 0.0,
        0.0, 0.0, 1.0, 0.0,
    };
    const hc_base = [_]f32{ 0.0, 0.0 };
    const hc_scale = [_]f32{1.0};
    var weights = @as([4]f32, @splat(0.0));

    hyperHeadWeightsRows(&weights, &streams, &hc_fn, &hc_base, &hc_scale, shape, 1e-6, 0.0);

    try std.testing.expectApproxEqAbs(sigmoid(1.2) + 1e-6, weights[0], 1e-6);
    try std.testing.expectApproxEqAbs(sigmoid(0.0) + 1e-6, weights[1], 1e-6);
    try std.testing.expectApproxEqAbs(weights[0], weights[2], 1e-6);
    try std.testing.expectApproxEqAbs(weights[1], weights[3], 1e-6);
}

test "hyper head collapse reduces repeated hc streams per hidden column" {
    const shape = HyperStreamsShape{ .rows = 1, .hc_mult = 2, .hidden_dim = 3 };
    const streams = [_]f32{
        1.0,  2.0,  3.0,
        10.0, 20.0, 30.0,
    };
    const hc_fn = [_]f32{
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
    };
    const hc_base = [_]f32{ 0.0, @log(@as(f32, 3.0)) };
    const hc_scale = [_]f32{1.0};
    var scratch = @as([2]f32, @splat(0.0));
    var output = @as([3]f32, @splat(0.0));

    hyperHeadCollapseRows(&output, &scratch, &streams, &hc_fn, &hc_base, &hc_scale, shape, 0.0, 1e-6);

    try std.testing.expectApproxEqAbs(@as(f32, 8.0), output[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 16.0), output[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 24.0), output[2], 1e-5);
}

test "sum stream reduce collapses persistent streams without weights" {
    const shape = HyperStreamsShape{ .rows = 2, .hc_mult = 2, .hidden_dim = 2 };
    const streams = [_]f32{
        1.0,  2.0,
        10.0, 20.0,
        3.0,  4.0,
        30.0, 40.0,
    };
    var output = @as([4]f32, @splat(0.0));

    sumStreamReduceRows(&output, &streams, shape);

    try std.testing.expectApproxEqAbs(@as(f32, 11.0), output[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 22.0), output[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 33.0), output[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 44.0), output[3], 1e-6);
}

test "broadcast add expands row updates across weighted hc streams" {
    const shape = HyperStreamsShape{ .rows = 1, .hc_mult = 2, .hidden_dim = 2 };
    var streams = [_]f32{
        1.0,  2.0,
        10.0, 20.0,
    };
    const update = [_]f32{ 3.0, 4.0 };
    const weights = [_]f32{ 0.5, 2.0 };

    broadcastAddStreamsRows(&streams, &update, &weights, shape);

    try std.testing.expectApproxEqAbs(@as(f32, 2.5), streams[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), streams[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 16.0), streams[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 28.0), streams[3], 1e-6);
}

test "hyper connection update persists mixed stream state" {
    const shape = HyperStreamsShape{ .rows = 1, .hc_mult = 2, .hidden_dim = 2 };
    var streams = [_]f32{
        1.0, 2.0,
        9.0, 18.0,
    };
    const update = [_]f32{ 3.0, 4.0 };
    const mix_dim = 8;
    const flat_dim = 4;
    const fn_data = @as([(mix_dim * flat_dim)]f32, @splat(0.0));
    const base_data = @as([mix_dim]f32, @splat(0.0));
    const scale_data = [_]f32{ 1.0, 1.0, 1.0 };
    var scratch_next = @as([flat_dim]f32, @splat(0.0));
    var scratch_comb = @as([4]f32, @splat(0.0));
    var scratch_pre = @as([2]f32, @splat(0.0));
    var scratch_post = @as([2]f32, @splat(0.0));

    hyperConnectionUpdateRows(
        &streams,
        &update,
        &scratch_next,
        &scratch_comb,
        &scratch_pre,
        &scratch_post,
        &fn_data,
        &base_data,
        &scale_data,
        shape,
        0.0,
        1e-6,
        1,
    );

    try std.testing.expectApproxEqAbs(@as(f32, 3.25), streams[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 6.0), streams[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 3.25), streams[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 6.0), streams[3], 1e-6);
}

test "trailing rope rotates only the tail of each head" {
    var data = [_]f32{
        11.0, 12.0, 1.0, 0.0,
        21.0, 22.0, 0.0, 1.0,
    };
    const positions = [_]u32{1};
    applyTrailingRopeRows(&data, &positions, 2, 4, 2, 10000.0);
    try std.testing.expectApproxEqAbs(@as(f32, 11.0), data[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 12.0), data[1], 1e-6);
    try std.testing.expectApproxEqAbs(@cos(@as(f32, 1.0)), data[2], 1e-6);
    try std.testing.expectApproxEqAbs(@sin(@as(f32, 1.0)), data[3], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 21.0), data[4], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 22.0), data[5], 1e-6);
    try std.testing.expectApproxEqAbs(-@sin(@as(f32, 1.0)), data[6], 1e-6);
    try std.testing.expectApproxEqAbs(@cos(@as(f32, 1.0)), data[7], 1e-6);
}

test "inverse trailing rope restores rotated tail" {
    var data = [_]f32{
        1.0, 2.0, 3.0, 4.0,
        5.0, 6.0, 7.0, 8.0,
    };
    const original = data;
    const positions = [_]u32{3};
    applyTrailingRopeRows(&data, &positions, 2, 4, 2, 10000.0);
    applyInverseTrailingRopeRows(&data, &positions, 2, 4, 2, 10000.0);
    for (data, original) |actual, expected| {
        try std.testing.expectApproxEqAbs(expected, actual, 1e-5);
    }
}

test "sink aware softmax attention lets sink absorb probability mass" {
    const shape = AttentionRowsShape{
        .query_rows = 1,
        .key_rows = 2,
        .num_query_heads = 1,
        .num_kv_heads = 1,
        .head_dim = 1,
        .value_dim = 1,
    };
    const query = [_]f32{1.0};
    const key = [_]f32{ 0.0, 0.0 };
    const value = [_]f32{ 2.0, 4.0 };
    const sinks = [_]f32{0.0};
    var output = [_]f32{0.0};

    softmaxAttentionRows(&output, &query, &key, &value, &sinks, shape, 1.0, false);

    try std.testing.expectApproxEqAbs(@as(f32, 2.0), output[0], 1e-6);
}

test "causal grouped attention maps query heads to kv heads" {
    const shape = AttentionRowsShape{
        .query_rows = 2,
        .key_rows = 2,
        .num_query_heads = 2,
        .num_kv_heads = 1,
        .head_dim = 1,
        .value_dim = 1,
    };
    const query = [_]f32{ 1.0, 2.0, 1.0, 2.0 };
    const key = [_]f32{ 1.0, 2.0 };
    const value = [_]f32{ 10.0, 20.0 };
    var output = [_]f32{ 0.0, 0.0, 0.0, 0.0 };

    softmaxAttentionRows(&output, &query, &key, &value, null, shape, 1.0, true);

    try std.testing.expectApproxEqAbs(@as(f32, 10.0), output[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 10.0), output[1], 1e-6);
    try std.testing.expect(output[2] > 17.0);
    try std.testing.expect(output[3] > 18.0);
    try std.testing.expectEqual(@as(usize, 0), shape.kvHeadForQueryHead(1));
}

test "gated chunk compression applies projection position bias and normalization" {
    const shape = ChunkCompressionShape{
        .rows = 3,
        .chunk_size = 2,
        .input_dim = 2,
        .compressed_dim = 2,
    };
    const input = [_]f32{
        0.0, 1.0,
        0.0, 2.0,
        4.0, 8.0,
    };
    const projection = [_]f32{
        1.0, 0.0,
        0.0, 1.0,
    };
    const gate_projection = [_]f32{ 0.0, 0.0 };
    const position_bias = [_]f32{ 0.0, @log(@as(f32, 3.0)) };
    var output = @as([4]f32, @splat(0.0));

    gatedChunkCompressRows(&output, &input, &projection, &gate_projection, &position_bias, shape, 0.0, true);

    try std.testing.expectApproxEqAbs(@as(f32, 0.0), output[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.6), output[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), output[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 8.0), output[3], 1e-6);
}

test "top-k indices feed weighted gather rows" {
    const scores = [_]f32{ 0.1, 2.0, 1.5, 2.0 };
    var indices = @as([3]u32, @splat(0));
    selectTopKIndices(&scores, &indices);

    try std.testing.expectEqual(@as(u32, 1), indices[0]);
    try std.testing.expectEqual(@as(u32, 3), indices[1]);
    try std.testing.expectEqual(@as(u32, 2), indices[2]);

    const shape = WeightedGatherRowsShape{
        .rows = 1,
        .source_rows = 4,
        .top_k = 3,
        .row_dim = 2,
    };
    const source = [_]f32{
        10.0, 1.0,
        20.0, 2.0,
        30.0, 3.0,
        40.0, 4.0,
    };
    const weights = [_]f32{ 0.5, 0.25, 0.25 };
    var output = [_]f32{ 99.0, 99.0 };

    weightedGatherRows(&output, &source, &indices, &weights, shape);

    try std.testing.expectApproxEqAbs(@as(f32, 27.5), output[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 2.75), output[1], 1e-6);
}

test "latent key value projection keeps key and value rows aligned" {
    const shape = LatentKeyValueRowsShape{
        .rows = 2,
        .latent_dim = 2,
        .key_dim = 2,
        .value_dim = 1,
    };
    const latent = [_]f32{
        1.0, 2.0,
        3.0, 4.0,
    };
    const key_projection = [_]f32{
        1.0, 0.0,
        0.0, 10.0,
    };
    const value_projection = [_]f32{ 2.0, 3.0 };
    var key = [_]f32{ 99.0, 99.0, 99.0, 99.0 };
    var value = [_]f32{ 99.0, 99.0 };

    projectLatentKeyValueRows(&key, &value, &latent, &key_projection, &value_projection, shape);

    try std.testing.expectApproxEqAbs(@as(f32, 1.0), key[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 20.0), key[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), key[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 40.0), key[3], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 8.0), value[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 18.0), value[1], 1e-6);
}

test "sinkhorn produces near doubly stochastic matrix" {
    var matrix = [_]f32{ 0.2, 0.8, 0.4, 0.6 };
    sinkhornInPlace(&matrix, 2, 20, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), matrix[0] + matrix[1], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), matrix[2] + matrix[3], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), matrix[0] + matrix[2], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), matrix[1] + matrix[3], 1e-4);
}

test "clamped swiglu writes activated first half" {
    var gate_up = [_]f32{ 20.0, -1.0, 3.0, -20.0 };
    applyClampedSwiGLU(&gate_up, 10.0);
    try std.testing.expectApproxEqAbs(silu(10.0) * 3.0, gate_up[0], 1e-5);
    try std.testing.expectApproxEqAbs(silu(-1.0) * -10.0, gate_up[1], 1e-5);
}

test "grouped output projection sums per group packed weights" {
    const shape = GroupedOutputProjectionShape{
        .rows = 1,
        .groups = 2,
        .group_in_dim = 2,
        .out_dim = 2,
    };
    const input = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    var weights = @as([8]f32, @splat(0.0));
    weights[shape.packedWeightIndex(0, 0, 0)] = 1.0;
    weights[shape.packedWeightIndex(0, 1, 1)] = 1.0;
    weights[shape.packedWeightIndex(1, 0, 0)] = 10.0;
    weights[shape.packedWeightIndex(1, 1, 1)] = 10.0;
    var output = [_]f32{ 99.0, 99.0 };

    groupedOutputProjectionRows(&output, &input, &weights, shape);

    try std.testing.expectApproxEqAbs(@as(f32, 31.0), output[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 42.0), output[1], 1e-6);
}

test "packed moe experts apply gate up and down projections over f32 buffers" {
    const shape = PackedMoeExpertShape{
        .hidden_dim = 2,
        .intermediate_dim = 2,
        .num_experts = 2,
    };
    const input = [_]f32{ 1.0, 2.0 };
    var gate_up = @as([16]f32, @splat(0.0));
    var down = @as([8]f32, @splat(0.0));

    gate_up[shape.gateUpWeightIndex(0, 0, 0, 0)] = 1.0;
    gate_up[shape.gateUpWeightIndex(0, 0, 1, 1)] = 1.0;
    gate_up[shape.gateUpWeightIndex(0, 1, 0, 0)] = 3.0;
    gate_up[shape.gateUpWeightIndex(0, 1, 1, 1)] = 4.0;
    down[shape.downWeightIndex(0, 0, 0)] = 5.0;
    down[shape.downWeightIndex(0, 1, 1)] = 6.0;

    gate_up[shape.gateUpWeightIndex(1, 0, 0, 0)] = 0.5;
    gate_up[shape.gateUpWeightIndex(1, 1, 0, 1)] = 2.0;
    down[shape.downWeightIndex(1, 0, 0)] = 7.0;
    down[shape.downWeightIndex(1, 1, 0)] = 11.0;

    var scratch = @as([4]f32, @splat(0.0));
    var output = [_]f32{ 100.0, 100.0 };
    const experts = [_]u32{ 0, 1 };
    const weights = [_]f32{ 0.25, 0.5 };

    applyPackedMoeExperts(&output, &scratch, &input, &gate_up, &down, shape, &experts, &weights, 100.0);

    const expert0_hidden0 = silu(1.0) * 3.0 * 5.0;
    const expert0_hidden1 = silu(2.0) * 8.0 * 6.0;
    const expert1_activated = silu(0.5) * 4.0;
    try std.testing.expectApproxEqAbs(0.25 * expert0_hidden0 + 0.5 * expert1_activated * 7.0, output[0], 1e-5);
    try std.testing.expectApproxEqAbs(0.25 * expert0_hidden1 + 0.5 * expert1_activated * 11.0, output[1], 1e-5);
}
