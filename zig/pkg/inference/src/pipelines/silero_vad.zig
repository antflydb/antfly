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

//! Silero VAD v5, executed natively.
//!
//! The published ONNX export (`onnx-community/silero-vad`, `onnx/model.onnx`)
//! wraps the network in control flow the in-house ONNX importer does not
//! run: an `If` on the runtime `sr` input, initializers captured inside the
//! branch subgraphs, and an `LSTM` node. The network itself is small and
//! fixed, so this module reads the weights out of the 16 kHz branch and
//! evaluates it directly: 64 samples of context plus a 512-sample chunk are
//! reflect-padded, run through an STFT expressed as a strided convolution,
//! reduced to magnitudes, passed through four Conv1d+ReLU blocks, one LSTM
//! cell, and a 1x1 conv with sigmoid. Output is the speech probability for
//! the chunk (32 ms at 16 kHz). Numerics match onnxruntime to about 1e-3.

const std = @import("std");
const onnx_graph = @import("onnx_graph");
const c_file = @import("../util/c_file.zig");

pub const sample_rate: u32 = 16_000;
pub const chunk_samples: usize = 512;
pub const context_samples: usize = 64;
pub const hidden: usize = 128;

const window_samples: usize = context_samples + chunk_samples; // 576
const pad_samples: usize = 64;
const padded_samples: usize = window_samples + pad_samples; // 640
const stft_filters: usize = 258;
const stft_window: usize = 256;
const stft_hop: usize = 128;
const stft_frames: usize = (padded_samples - stft_window) / stft_hop + 1; // 4
const spectrum_bins: usize = stft_filters / 2; // 129

const Conv = struct {
    weight: []f32, // [out][in][3]
    bias: []f32,
    in_channels: usize,
    out_channels: usize,
    stride: usize,
};

pub const Weights = struct {
    allocator: std.mem.Allocator,
    stft_basis: []f32, // [258][256]
    encoder: [4]Conv,
    lstm_w: []f32, // [512][128], gate rows i,o,f,c
    lstm_r: []f32, // [512][128]
    lstm_b: []f32, // [1024] = Wb(512) ++ Rb(512)
    decoder_weight: []f32, // [128]
    decoder_bias: f32,

    pub fn load(allocator: std.mem.Allocator, onnx_path: []const u8) !Weights {
        const data = try c_file.readFile(allocator, onnx_path);
        defer allocator.free(data);
        return loadFromBytes(allocator, data);
    }

    pub fn loadFromBytes(allocator: std.mem.Allocator, data: []const u8) !Weights {
        var model = try onnx_graph.proto.parseModelProto(allocator, data);
        defer model.deinit(allocator);
        const graph = model.graph orelse return error.InvalidSileroModel;
        const branch = try sixteenKilohertzBranch(&graph);

        var lstm_names: [3][]const u8 = undefined;
        var found_lstm = false;
        for (branch.nodes) |node| {
            if (!std.mem.eql(u8, node.op_type, "LSTM")) continue;
            if (node.inputs.len < 4) return error.InvalidSileroModel;
            lstm_names = .{ node.inputs[1], node.inputs[2], node.inputs[3] };
            found_lstm = true;
            break;
        }
        if (!found_lstm) return error.InvalidSileroModel;

        var weights: Weights = undefined;
        weights.allocator = allocator;
        var loaded: usize = 0;
        errdefer weights.freeLoaded(loaded);

        weights.stft_basis = try tensorBySuffix(allocator, branch, "stft.forward_basis_buffer", stft_filters * stft_window);
        loaded += 1;
        const encoder_specs = [4]struct { in: usize, out: usize, stride: usize }{
            .{ .in = spectrum_bins, .out = 128, .stride = 1 },
            .{ .in = 128, .out = 64, .stride = 2 },
            .{ .in = 64, .out = 64, .stride = 2 },
            .{ .in = 64, .out = 128, .stride = 1 },
        };
        inline for (encoder_specs, 0..) |spec, i| {
            var name_buf: [64]u8 = undefined;
            const weight_name = try std.fmt.bufPrint(&name_buf, "encoder.{d}.reparam_conv.weight", .{i});
            const weight = try tensorBySuffix(allocator, branch, weight_name, spec.out * spec.in * 3);
            errdefer allocator.free(weight);
            var bias_buf: [64]u8 = undefined;
            const bias_name = try std.fmt.bufPrint(&bias_buf, "encoder.{d}.reparam_conv.bias", .{i});
            const bias = try tensorBySuffix(allocator, branch, bias_name, spec.out);
            weights.encoder[i] = .{ .weight = weight, .bias = bias, .in_channels = spec.in, .out_channels = spec.out, .stride = spec.stride };
            loaded += 1;
        }
        weights.lstm_w = try tensorByName(allocator, branch, lstm_names[0], 4 * hidden * hidden);
        loaded += 1;
        weights.lstm_r = try tensorByName(allocator, branch, lstm_names[1], 4 * hidden * hidden);
        loaded += 1;
        weights.lstm_b = try tensorByName(allocator, branch, lstm_names[2], 8 * hidden);
        loaded += 1;
        weights.decoder_weight = try tensorBySuffix(allocator, branch, "decoder.decoder.2.weight", hidden);
        loaded += 1;
        const decoder_bias = try tensorBySuffix(allocator, branch, "decoder.decoder.2.bias", 1);
        defer allocator.free(decoder_bias);
        weights.decoder_bias = decoder_bias[0];
        return weights;
    }

    pub fn deinit(self: *Weights) void {
        self.freeLoaded(std.math.maxInt(usize));
    }

    fn freeLoaded(self: *Weights, loaded: usize) void {
        const allocator = self.allocator;
        if (loaded >= 1) allocator.free(self.stft_basis);
        inline for (0..4) |i| if (loaded >= 2 + i) {
            allocator.free(self.encoder[i].weight);
            allocator.free(self.encoder[i].bias);
        };
        if (loaded >= 6) allocator.free(self.lstm_w);
        if (loaded >= 7) allocator.free(self.lstm_r);
        if (loaded >= 8) allocator.free(self.lstm_b);
        if (loaded >= 9) allocator.free(self.decoder_weight);
    }
};

/// Recurrent state plus the 64-sample context carried between chunks.
pub const State = struct {
    h: [hidden]f32 = [_]f32{0} ** hidden,
    c: [hidden]f32 = [_]f32{0} ** hidden,
    context: [context_samples]f32 = [_]f32{0} ** context_samples,

    pub fn reset(self: *State) void {
        self.* = .{};
    }
};

/// Speech probability for one 512-sample chunk at 16 kHz, advancing `state`.
pub fn probability(weights: *const Weights, state: *State, chunk: *const [chunk_samples]f32) f32 {
    // 1. Context + chunk, reflect-padded on the right (ONNX reflect mode
    //    mirrors without repeating the edge sample).
    var x: [padded_samples]f32 = undefined;
    @memcpy(x[0..context_samples], &state.context);
    @memcpy(x[context_samples..window_samples], chunk);
    for (0..pad_samples) |k| x[window_samples + k] = x[window_samples - 2 - k];

    // 2. STFT as a strided convolution, then magnitudes [129][4].
    var magnitudes: [spectrum_bins][stft_frames]f32 = undefined;
    for (0..stft_frames) |t| {
        const frame = x[t * stft_hop ..][0..stft_window];
        for (0..spectrum_bins) |bin| {
            const re = dot(weights.stft_basis[bin * stft_window ..][0..stft_window], frame);
            const im = dot(weights.stft_basis[(spectrum_bins + bin) * stft_window ..][0..stft_window], frame);
            magnitudes[bin][t] = @sqrt(re * re + im * im);
        }
    }

    // 3. Encoder: Conv1d(k=3, pad=1) + ReLU, four times. Channel-major
    //    buffers; the time axis shrinks 4 -> 4 -> 2 -> 1 -> 1.
    // Largest activation is the 129 x 4 spectrogram; later layers are smaller.
    var buf_a: [spectrum_bins * stft_frames]f32 = undefined;
    var buf_b: [spectrum_bins * stft_frames]f32 = undefined;
    var input: []f32 = buf_a[0 .. spectrum_bins * stft_frames];
    for (0..spectrum_bins) |bin| for (0..stft_frames) |t| {
        input[bin * stft_frames + t] = magnitudes[bin][t];
    };
    var frames: usize = stft_frames;
    var output: []f32 = &buf_b;
    for (weights.encoder) |conv| {
        const out_frames = (frames + 2 - 3) / conv.stride + 1;
        output = if (input.ptr == &buf_a) buf_b[0 .. conv.out_channels * out_frames] else buf_a[0 .. conv.out_channels * out_frames];
        conv1d(conv, input, frames, output, out_frames);
        input = output;
        frames = out_frames;
    }
    std.debug.assert(frames == 1);
    const features = input[0..hidden];

    // 4. LSTM cell (ONNX gate order i, o, f, c; sigmoid/tanh/tanh).
    var gates: [4 * hidden]f32 = undefined;
    for (0..4 * hidden) |g| {
        gates[g] = weights.lstm_b[g] + weights.lstm_b[4 * hidden + g] +
            dot(weights.lstm_w[g * hidden ..][0..hidden], features) +
            dot(weights.lstm_r[g * hidden ..][0..hidden], &state.h);
    }
    var new_h: [hidden]f32 = undefined;
    for (0..hidden) |j| {
        const i_gate = sigmoid(gates[j]);
        const o_gate = sigmoid(gates[hidden + j]);
        const f_gate = sigmoid(gates[2 * hidden + j]);
        const c_gate = std.math.tanh(gates[3 * hidden + j]);
        const c_new = f_gate * state.c[j] + i_gate * c_gate;
        state.c[j] = c_new;
        new_h[j] = o_gate * std.math.tanh(c_new);
    }
    state.h = new_h;

    // 5. Decoder: ReLU, 1x1 conv, sigmoid.
    var logit: f32 = weights.decoder_bias;
    for (0..hidden) |j| logit += weights.decoder_weight[j] * @max(new_h[j], 0);
    @memcpy(&state.context, chunk[chunk_samples - context_samples ..]);
    return sigmoid(logit);
}

fn conv1d(conv: Conv, input: []const f32, in_frames: usize, output: []f32, out_frames: usize) void {
    for (0..conv.out_channels) |o| {
        for (0..out_frames) |t| {
            var acc = conv.bias[o];
            const centre = t * conv.stride; // input index of tap 1 is centre; taps 0..2 map to centre-1..centre+1
            for (0..conv.in_channels) |c| {
                const taps = conv.weight[(o * conv.in_channels + c) * 3 ..][0..3];
                const row = input[c * in_frames ..][0..in_frames];
                if (centre >= 1) acc += taps[0] * row[centre - 1];
                if (centre < in_frames) acc += taps[1] * row[centre];
                if (centre + 1 < in_frames) acc += taps[2] * row[centre + 1];
            }
            output[o * out_frames + t] = @max(acc, 0);
        }
    }
}

inline fn dot(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);
    var acc: f32 = 0;
    for (a, b) |x, y| acc += x * y;
    return acc;
}

inline fn sigmoid(v: f32) f32 {
    return 1.0 / (1.0 + @exp(-v));
}

/// The top-level graph is `If (sr == 16000) then ... else ...`; the branch
/// whose comparison constant is 16000 holds the 16 kHz weights.
fn sixteenKilohertzBranch(graph: *const onnx_graph.proto.GraphProto) !*const onnx_graph.proto.GraphProto {
    var equal_constant: ?i64 = null;
    var equal_output: []const u8 = "";
    for (graph.nodes) |node| {
        if (!std.mem.eql(u8, node.op_type, "Equal") or node.inputs.len != 2) continue;
        equal_output = node.outputs[0];
        for (graph.initializers) |init| if (std.mem.eql(u8, init.name, node.inputs[1])) {
            equal_constant = scalarI64(&init);
        };
        for (graph.nodes) |constant| {
            if (!std.mem.eql(u8, constant.op_type, "Constant") or constant.outputs.len == 0) continue;
            if (!std.mem.eql(u8, constant.outputs[0], node.inputs[1])) continue;
            for (constant.attributes) |attr| if (attr.t) |*tensor| {
                equal_constant = scalarI64(tensor);
            };
        }
    }
    for (graph.nodes) |node| {
        if (!std.mem.eql(u8, node.op_type, "If")) continue;
        const want_then = equal_constant == null or equal_constant.? == sample_rate;
        for (node.attributes) |*attr| {
            if (std.mem.eql(u8, attr.name, if (want_then) "then_branch" else "else_branch")) {
                if (attr.g) |*branch| return branch;
            }
        }
    }
    return error.InvalidSileroModel;
}

fn scalarI64(tensor: *const onnx_graph.proto.TensorProto) ?i64 {
    if (tensor.raw_data.len == 8) return std.mem.readInt(i64, tensor.raw_data[0..8], .little);
    if (tensor.raw_data.len == 4) return std.mem.readInt(i32, tensor.raw_data[0..4], .little);
    if (tensor.int64_data.len > 0) {
        // Packed varint; a single value fits in ten bytes.
        var value: u64 = 0;
        var shift: u6 = 0;
        for (tensor.int64_data) |byte| {
            value |= @as(u64, byte & 0x7f) << shift;
            if (byte & 0x80 == 0) break;
            if (shift >= 57) return null;
            shift += 7;
        }
        return @bitCast(value);
    }
    return null;
}

fn tensorBySuffix(allocator: std.mem.Allocator, graph: *const onnx_graph.proto.GraphProto, suffix: []const u8, expected_len: usize) ![]f32 {
    for (graph.initializers) |*init| {
        if (std.mem.endsWith(u8, init.name, suffix)) return tensorFloats(allocator, init, expected_len);
    }
    return error.MissingSileroWeight;
}

fn tensorByName(allocator: std.mem.Allocator, graph: *const onnx_graph.proto.GraphProto, name: []const u8, expected_len: usize) ![]f32 {
    for (graph.initializers) |*init| {
        if (std.mem.eql(u8, init.name, name)) return tensorFloats(allocator, init, expected_len);
    }
    return error.MissingSileroWeight;
}

fn tensorFloats(allocator: std.mem.Allocator, tensor: *const onnx_graph.proto.TensorProto, expected_len: usize) ![]f32 {
    if (tensor.isExternal()) return error.UnsupportedSileroWeightStorage;
    const bytes = if (tensor.raw_data.len > 0) tensor.raw_data else tensor.float_data;
    if (bytes.len != expected_len * 4) return error.InvalidSileroModel;
    const out = try allocator.alloc(f32, expected_len);
    errdefer allocator.free(out);
    for (out, 0..) |*value, i| value.* = @bitCast(std.mem.readInt(u32, bytes[i * 4 ..][0..4], .little));
    return out;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn testModelPath(allocator: std.mem.Allocator) !?[]u8 {
    const home_z = std.c.getenv("HOME") orelse return null;
    const home = std.mem.span(home_z);
    return try std.fs.path.join(allocator, &.{ home, ".antfly", "inference", "models", "onnx-community", "silero-vad", "onnx", "model.onnx" });
}

test "silero vad matches onnxruntime on the spoken test clip and rejects a pure tone" {
    const allocator = std.testing.allocator;
    const model_path = (try testModelPath(allocator)) orelse return error.SkipZigTest;
    defer allocator.free(model_path);
    // The model is pulled with `antfly inference pull onnx-community/silero-vad`;
    // skip when it is not installed on this machine.
    var weights = Weights.load(allocator, model_path) catch return error.SkipZigTest;
    defer weights.deinit();

    // Reference probabilities from onnxruntime 1.x on the first chunks of
    // zig/e2e/inference/testdata/whisper_quality.wav (16 kHz, 64-sample
    // context carried between 512-sample chunks, zero initial state).
    const expected = [_]f32{ 0.9107, 0.9888, 0.9966, 0.9961, 0.9948, 0.9878, 0.9987, 0.9992 };
    const wav_path = "../../e2e/inference/testdata/whisper_quality.wav";
    const wav_bytes = c_file.readFile(allocator, wav_path) catch return error.SkipZigTest;
    defer allocator.free(wav_bytes);
    // 16-bit mono PCM in the RIFF "data" chunk (this file carries a padding
    // chunk before it, so the payload does not start at byte 44).
    const pcm = wavDataChunk(wav_bytes) orelse return error.SkipZigTest;
    var state = State{};
    for (expected, 0..) |want, index| {
        var chunk: [chunk_samples]f32 = undefined;
        for (0..chunk_samples) |i| {
            const sample = std.mem.readInt(i16, pcm[(index * chunk_samples + i) * 2 ..][0..2], .little);
            chunk[i] = @as(f32, @floatFromInt(sample)) / 32768.0;
        }
        const got = probability(&weights, &state, &chunk);
        try std.testing.expectApproxEqAbs(want, got, 3e-3);
    }

    // A 440 Hz tone is loud but not speech: energy VAD passes it, Silero does
    // not. Reference (onnxruntime, fresh state): 0.1014 on the onset chunk,
    // then below 0.03.
    var tone_state = State{};
    var chunk: [chunk_samples]f32 = undefined;
    for (0..16) |c| {
        for (0..chunk_samples) |i| {
            const t = @as(f32, @floatFromInt(c * chunk_samples + i)) / @as(f32, @floatFromInt(sample_rate));
            chunk[i] = 0.2 * @sin(2.0 * std.math.pi * 440.0 * t);
        }
        const got = probability(&weights, &tone_state, &chunk);
        if (c == 0) try std.testing.expectApproxEqAbs(@as(f32, 0.1014), got, 5e-3) else try std.testing.expect(got < 0.03);
    }
}

fn wavDataChunk(bytes: []const u8) ?[]const u8 {
    if (bytes.len < 12 or !std.mem.eql(u8, bytes[0..4], "RIFF")) return null;
    var offset: usize = 12;
    while (offset + 8 <= bytes.len) {
        const size: usize = std.mem.readInt(u32, bytes[offset + 4 ..][0..4], .little);
        if (std.mem.eql(u8, bytes[offset .. offset + 4], "data")) {
            const start = offset + 8;
            return bytes[start..@min(bytes.len, start + size)];
        }
        offset += 8 + size + (size & 1);
    }
    return null;
}

test "silero vad matches onnxruntime on deterministic noise" {
    const allocator = std.testing.allocator;
    const model_path = (try testModelPath(allocator)) orelse return error.SkipZigTest;
    defer allocator.free(model_path);
    var weights = Weights.load(allocator, model_path) catch return error.SkipZigTest;
    defer weights.deinit();
    // LCG noise shared with the reference script; validates the numerics
    // independently of any audio file.
    const expected = [_]f32{ 0.03736, 0.02185, 0.02400, 0.01081, 0.01447, 0.00744 };
    var seed: u64 = 12345;
    var state = State{};
    for (expected) |want| {
        var chunk: [chunk_samples]f32 = undefined;
        for (&chunk) |*sample| {
            seed = (seed * 1103515245 + 12345) % (1 << 31);
            sample.* = @floatCast((@as(f64, @floatFromInt(seed)) / @as(f64, 1 << 31) - 0.5) * 0.5);
        }
        try std.testing.expectApproxEqAbs(want, probability(&weights, &state, &chunk), 2e-3);
    }
}

test "silero state reset clears recurrence and context" {
    var state = State{ .h = [_]f32{1} ** hidden, .c = [_]f32{2} ** hidden, .context = [_]f32{3} ** context_samples };
    state.reset();
    try std.testing.expectEqual(@as(f32, 0), state.h[0]);
    try std.testing.expectEqual(@as(f32, 0), state.c[hidden - 1]);
    try std.testing.expectEqual(@as(f32, 0), state.context[10]);
}
