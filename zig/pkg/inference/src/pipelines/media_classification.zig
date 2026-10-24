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

//! Supervised logits over borrowed decoded media. Vocabulary and preprocessing
//! belong to the model bundle; this path never embeds caller label prompts.
const std = @import("std");
const backends = @import("../backends/backends.zig");
const image = @import("antfly_image");
const audio = @import("inference_audio");
const Profile = @import("../models/media_classifier_profile.zig").Profile;
const outputs = @import("inference_chunker").outputs;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Input = union(enum) { pcm: @import("audio.zig").PcmAudio, raster: image.BorrowedRasterAttachment };

pub fn classify(a: std.mem.Allocator, session: backends.Session, profile: Profile, labels: []const []const u8, input: Input, top_k: usize, control: Control) ![]outputs.Label {
    try profile.validate();
    try control.check();
    if (top_k == 0 or top_k > 128 or labels.len == 0 or labels.len > 65536) return error.InvalidMediaClassifierProfile;
    // The shared session admission accounts for preprocessing and model work.
    const max_elements: usize = switch (input) {
        .pcm => |pcm| blk: {
            if (profile.input != .waveform or pcm.sample_rate == 0 or pcm.samples.len == 0) return error.UnsupportedMediaClassifierInput;
            if (@as(u128, pcm.samples.len) * std.time.ns_per_s > @as(u128, @intCast(profile.max_audio_ns)) * pcm.sample_rate) return error.ChunkUnitTooLarge;
            break :blk @intCast(@divFloor(@as(u128, pcm.samples.len) * profile.sample_rate + pcm.sample_rate - 1, pcm.sample_rate));
        },
        .raster => |raster| blk: {
            if (profile.input != .pixels) return error.UnsupportedMediaClassifierInput;
            try raster.validate();
            break :blk try std.math.mul(usize, try std.math.mul(usize, profile.width, profile.height), 3);
        },
    };
    var permit = try session.admitHostPreprocess(try std.math.mul(usize, max_elements + labels.len, 16));
    defer permit.deinit();
    const prepared = switch (input) {
        .pcm => |pcm| blk: {
            const samples = try audio.copyOrResample(a, pcm.samples, pcm.sample_rate, profile.sample_rate);
            errdefer a.free(samples);
            for (samples) |sample| if (!std.math.isFinite(sample)) return error.InvalidAudioInput;
            if (profile.normalize_waveform) {
                var mean: f64 = 0;
                for (samples) |sample| mean += sample;
                mean /= @as(f64, @floatFromInt(samples.len));
                var variance: f64 = 0;
                for (samples) |sample| variance += (@as(f64, sample) - mean) * (@as(f64, sample) - mean);
                const scale = @sqrt(variance / @as(f64, @floatFromInt(samples.len)) + 1e-7);
                for (samples) |*sample| sample.* = @floatCast((sample.* - mean) / scale);
            }
            break :blk samples;
        },
        .raster => |raster| try image.processing.preprocessDecodedRectWithResample(a, try raster.imageView(), profile.width, profile.height, profile.mean, profile.std_dev, profile.resample),
    };
    defer a.free(prepared);
    return classifyPrepared(a, session, profile, labels, prepared, top_k, control);
}

/// Prepare a displayed frame while the decoder still owns its surface. The
/// caller retains only model-sized pixels, never the codec surface or a session
/// execution lock, until all output models have run.
pub fn prepareImage(a: std.mem.Allocator, profile: Profile, view: image.processing.ImageU8) ![]f32 {
    try profile.validate();
    if (profile.input != .pixels) return error.UnsupportedMediaClassifierInput;
    return image.processing.preprocessDecodedRectWithResample(a, view, profile.width, profile.height, profile.mean, profile.std_dev, profile.resample);
}

pub fn classifyPrepared(a: std.mem.Allocator, session: backends.Session, profile: Profile, labels: []const []const u8, prepared: []const f32, top_k: usize, control: Control) ![]outputs.Label {
    try profile.validate();
    try control.check();
    if (top_k == 0 or top_k > 128 or labels.len == 0 or labels.len > 65536) return error.InvalidMediaClassifierProfile;
    if (prepared.len == 0) return error.InvalidInputShape;
    for (prepared) |value| if (!std.math.isFinite(value)) return error.InvalidInputShape;
    if (profile.input == .pixels and prepared.len != @as(usize, profile.width) * profile.height * 3) return error.InvalidInputShape;
    const inputs = session.inputInfo();
    const expected_name = if (profile.input == .waveform) "input_values" else "pixel_values";
    if (inputs.len != 1 or !std.mem.eql(u8, inputs[0].name, expected_name) or inputs[0].dtype != .f32) return error.UnsupportedMediaClassifier;
    const output_info = session.outputInfo();
    if (output_info.len != 1 or !std.mem.eql(u8, output_info[0].name, "logits") or output_info[0].dtype != .f32) return error.UnsupportedMediaClassifier;
    var permit = try session.admitHostPreprocess(try std.math.mul(usize, prepared.len + labels.len, 16));
    defer permit.deinit();
    var shape: [4]i64 = .{ 1, 3, profile.height, profile.width };
    const selected_shape: []const i64 = if (profile.input == .waveform) blk: {
        shape[1] = @intCast(prepared.len);
        break :blk shape[0..2];
    } else &shape;
    if (inputs[0].shape.len != selected_shape.len) return error.UnsupportedMediaClassifier;
    for (inputs[0].shape, selected_shape) |expected, actual| if (expected > 0 and expected != actual) return error.InvalidInputShape;
    var tensor = try backends.Tensor.initFloat32(a, expected_name, selected_shape, prepared);
    defer tensor.deinit();
    var values = try session.runWithControl(&.{tensor}, a, control);
    defer {
        for (values) |*value| value.deinit();
        a.free(values);
    }
    if (values.len != 1 or values[0].dtype != .f32 or values[0].shape.len != 2 or values[0].shape[0] != 1 or values[0].shape[1] != labels.len) return error.InvalidClassifierOutput;
    return rankLogits(a, values[0].asFloat32(), labels, profile.score_transform, top_k);
}

/// Batch compatible frame tensors; fixed single-frame exports retain the serial path.
pub fn supportsPreparedBatch(session: backends.Session, profile: Profile, count: usize) bool {
    const inputs = session.inputInfo();
    const out = session.outputInfo();
    if (profile.input != .pixels or count == 0 or count > 32 or inputs.len != 1 or out.len != 1 or inputs[0].shape.len != 4 or out[0].shape.len != 2) return false;
    return (inputs[0].shape[0] <= 0 or inputs[0].shape[0] == count) and (out[0].shape[0] <= 0 or out[0].shape[0] == count);
}

/// Reuse one acquired session for both batch exports and fixed batch-one models.
pub fn classifyPreparedFrames(a: std.mem.Allocator, session: backends.Session, profile: Profile, labels: []const []const u8, frames: []const []const f32, top_k: usize, control: Control) ![][]outputs.Label {
    if (frames.len == 0 or frames.len > 32) return error.InvalidInputShape;
    if (supportsPreparedBatch(session, profile, frames.len)) {
        const batched: ?[][]outputs.Label = classifyPreparedBatch(a, session, profile, labels, frames, top_k, control) catch |err| switch (err) {
            error.ResourceTemporarilyUnavailable, error.QueueFull, error.OutOfMemory, error.SharedAdmissionExceeded, error.MemoryBudgetExceeded, error.InferenceAdmissionQueueFull, error.ConcurrencyUnavailable => null,
            else => return err,
        };
        if (batched) |rows| return rows;
    }
    const results = try a.alloc([]outputs.Label, frames.len);
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |row| {
            for (row) |label| a.free(label.label);
            a.free(row);
        }
        a.free(results);
    }
    for (frames, results) |frame, *row| {
        row.* = try classifyPrepared(a, session, profile, labels, frame, top_k, control);
        initialized += 1;
    }
    return results;
}

pub fn classifyPreparedBatch(a: std.mem.Allocator, session: backends.Session, profile: Profile, labels: []const []const u8, frames: []const []const f32, top_k: usize, control: Control) ![][]outputs.Label {
    try profile.validate();
    try control.check();
    if (!supportsPreparedBatch(session, profile, frames.len)) return error.UnsupportedMediaClassifier;
    if (top_k == 0 or top_k > 128 or labels.len == 0 or labels.len > 65536) return error.InvalidMediaClassifierProfile;
    const inputs = session.inputInfo();
    const out = session.outputInfo();
    if (!std.mem.eql(u8, inputs[0].name, "pixel_values") or inputs[0].dtype != .f32 or !std.mem.eql(u8, out[0].name, "logits") or out[0].dtype != .f32) return error.UnsupportedMediaClassifier;
    const elements = try std.math.mul(usize, @as(usize, profile.width) * profile.height, 3);
    const total = try std.math.mul(usize, elements, frames.len);
    var permit = try session.admitHostPreprocess(try std.math.mul(usize, try std.math.add(usize, total, try std.math.mul(usize, labels.len, frames.len)), 16));
    defer permit.deinit();
    const prepared = try a.alloc(f32, total);
    defer a.free(prepared);
    for (frames, 0..) |frame, i| {
        try control.check();
        if (frame.len != elements) return error.InvalidInputShape;
        for (frame) |value| if (!std.math.isFinite(value)) return error.InvalidInputShape;
        @memcpy(prepared[i * elements ..][0..elements], frame);
    }
    const shape = [_]i64{ @intCast(frames.len), 3, profile.height, profile.width };
    for (inputs[0].shape, shape) |expected, actual| if (expected > 0 and expected != actual) return error.InvalidInputShape;
    var tensor = try backends.Tensor.initFloat32(a, "pixel_values", &shape, prepared);
    defer tensor.deinit();
    var values = try session.runWithControl(&.{tensor}, a, control);
    defer {
        for (values) |*value| value.deinit();
        a.free(values);
    }
    if (values.len != 1 or values[0].dtype != .f32 or values[0].shape.len != 2 or values[0].shape[0] != frames.len or values[0].shape[1] != labels.len or values[0].asFloat32().len != frames.len * labels.len) return error.InvalidClassifierOutput;
    const results = try a.alloc([]outputs.Label, frames.len);
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |row| {
            for (row) |label| a.free(label.label);
            a.free(row);
        }
        a.free(results);
    }
    for (results, 0..) |*row, i| {
        try control.check();
        row.* = try rankLogits(a, values[0].asFloat32()[i * labels.len ..][0..labels.len], labels, profile.score_transform, top_k);
        initialized += 1;
    }
    return results;
}

fn rankLogits(a: std.mem.Allocator, logits: []const f32, labels: []const []const u8, transform: @TypeOf(@as(Profile, undefined).score_transform), top_k: usize) ![]outputs.Label {
    if (logits.len == 0 or logits.len != labels.len) return error.InvalidClassifierOutput;
    var maximum: f64 = -std.math.inf(f64);
    for (logits) |value| {
        if (!std.math.isFinite(value)) return error.InvalidClassifierOutput;
        maximum = @max(maximum, value);
    }
    var denominator: f64 = 0;
    if (transform == .softmax) for (logits) |value| {
        denominator += @exp(@as(f64, value) - maximum);
    };
    const ranked = try a.alloc(outputs.Label, logits.len);
    defer a.free(ranked);
    for (ranked, logits, labels) |*result, logit, label| result.* = .{ .label = label, .score = @floatCast(if (transform == .softmax) @exp(@as(f64, logit) - maximum) / denominator else if (logit >= 0) 1 / (1 + @exp(-@as(f64, logit))) else @exp(@as(f64, logit)) / (1 + @exp(@as(f64, logit)))) };
    std.mem.sort(outputs.Label, ranked, {}, struct {
        fn less(_: void, x: outputs.Label, y: outputs.Label) bool {
            return x.score > y.score or (x.score == y.score and std.mem.order(u8, x.label, y.label) == .lt);
        }
    }.less);
    const selected = ranked[0..@min(top_k, ranked.len)];
    const results = try a.dupe(outputs.Label, selected);
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |result| a.free(result.label);
        a.free(results);
    }
    for (results) |*result| {
        result.label = try a.dupe(u8, result.label);
        initialized += 1;
    }
    return results;
}

test "model aware media supervised logits use the model vocabulary and reject nonfinite predictions" {
    const a = std.testing.allocator;
    const result = try rankLogits(a, &.{ 1000, 1001, 1000 }, &.{ "dog", "cat", "bird" }, .softmax, 2);
    defer {
        for (result) |label| a.free(label.label);
        a.free(result);
    }
    try std.testing.expectEqualStrings("cat", result[0].label);
    try std.testing.expectEqualStrings("bird", result[1].label);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5761169), result[0].score, 0.000001);
    try std.testing.expectError(error.InvalidClassifierOutput, rankLogits(a, &.{std.math.nan(f32)}, &.{"bad"}, .sigmoid, 1));
}

test "model aware media supervised classifiers consume borrowed PCM and padded rasters without a codec pass" {
    const Fake = struct {
        waveform: bool = false,
        calls: usize = 0,
        fn inputs(raw: *anyopaque) []const backends.TensorInfo {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (self.waveform) &.{.{ .name = "input_values", .dtype = .f32, .shape = &.{ 1, -1 } }} else &.{.{ .name = "pixel_values", .dtype = .f32, .shape = &.{ 1, 3, 1, 1 } }};
        }
        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{.{ .name = "logits", .dtype = .f32, .shape = &.{ 1, 2 } }};
        }
        fn backend(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
        fn run(raw: *anyopaque, tensors: []const backends.Tensor, a: std.mem.Allocator) anyerror![]backends.Tensor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const values = tensors[0].asFloat32();
            if (self.waveform) {
                try std.testing.expectEqual(@as(usize, 2), values.len);
                try std.testing.expectApproxEqAbs(@as(f32, -1), values[0], 0.0001);
                try std.testing.expectApproxEqAbs(@as(f32, 1), values[1], 0.0001);
            } else try std.testing.expectEqualSlices(f32, &.{ 1, 0, 0 }, values);
            self.calls += 1;
            const result = try a.alloc(backends.Tensor, 1);
            errdefer a.free(result);
            result[0] = try backends.Tensor.initFloat32(a, "logits", &.{ 1, 2 }, &.{ -1, 1 });
            return result;
        }
        fn runWithControl(raw: *anyopaque, tensors: []const backends.Tensor, a: std.mem.Allocator, control: Control) anyerror![]backends.Tensor {
            try control.check();
            return run(raw, tensors, a);
        }
        fn session(self: *@This()) backends.Session {
            return .{ .ptr = self, .vtable = &.{ .run = run, .runWithControl = runWithControl, .inputInfo = inputs, .outputInfo = outputInfo, .backend = backend, .close = close } };
        }
    };
    const a = std.testing.allocator;
    var fake = Fake{};
    const raster = try classify(a, fake.session(), .{ .input = .pixels, .score_transform = .softmax, .width = 1, .height = 1 }, &.{ "negative", "positive" }, .{ .raster = .{ .bytes = &.{ 255, 0, 0, 255, 9, 9, 9, 9 }, .width = 1, .height = 1, .stride_bytes = 8 } }, 1, .{});
    defer {
        for (raster) |label| a.free(label.label);
        a.free(raster);
    }
    try std.testing.expectEqualStrings("positive", raster[0].label);
    // The frame observer borrows the same decoded display image as the video
    // tower; the classifier retains only its own model-sized tensor.
    const frame = @import("antfly_video").preparation.BorrowedFrame{ .rgba = .{ .bytes = &.{ 255, 0, 0, 255 }, .width = 1, .height = 1 } };
    var display = try frame.display(a, .{ .width = 28, .height = 28, .matrix = .bt709 }, .{});
    defer display.deinit(a);
    const profile = Profile{ .input = .pixels, .score_transform = .softmax, .width = 1, .height = 1 };
    const pixels = try prepareImage(a, profile, display.view());
    defer a.free(pixels);
    const framed = try classifyPrepared(a, fake.session(), profile, &.{ "negative", "positive" }, pixels, 1, .{});
    defer {
        for (framed) |label| a.free(label.label);
        a.free(framed);
    }
    try std.testing.expectEqualStrings("positive", framed[0].label);

    fake.waveform = true;
    const pcm = try classify(a, fake.session(), .{ .input = .waveform, .score_transform = .sigmoid }, &.{ "negative", "positive" }, .{ .pcm = .{ .samples = &.{ 0, 2 }, .sample_rate = 16000 } }, 2, .{});
    defer {
        for (pcm) |label| a.free(label.label);
        a.free(pcm);
    }
    try std.testing.expectApproxEqAbs(@as(f32, 0.7310586), pcm[0].score, 0.000001);
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
}

test "model aware media frame classifiers batch compatible tensors with one model call" {
    const Fake = struct {
        calls: usize = 0,
        fixed: bool = false,
        deny_batch: bool = false,
        fn inputs(raw: *anyopaque) []const backends.TensorInfo {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (self.fixed) &.{.{ .name = "pixel_values", .dtype = .f32, .shape = &.{ 1, 3, 1, 1 } }} else &.{.{ .name = "pixel_values", .dtype = .f32, .shape = &.{ -1, 3, 1, 1 } }};
        }
        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{.{ .name = "logits", .dtype = .f32, .shape = &.{ -1, 2 } }};
        }
        fn backend(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
        fn run(raw: *anyopaque, tensors: []const backends.Tensor, a: std.mem.Allocator) anyerror![]backends.Tensor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const serial = tensors[0].shape[0] == 1;
            if (self.deny_batch and !serial) return error.ResourceTemporarilyUnavailable;
            try std.testing.expectEqualSlices(i64, if (serial) &.{ 1, 3, 1, 1 } else &.{ 2, 3, 1, 1 }, tensors[0].shape);
            if (!serial) try std.testing.expectEqualSlices(f32, &.{ 1, 0, 0, 0, 1, 0 }, tensors[0].asFloat32());
            const result = try a.alloc(backends.Tensor, 1);
            errdefer a.free(result);
            result[0] = if (serial) try backends.Tensor.initFloat32(a, "logits", &.{ 1, 2 }, &.{ -1, 1 }) else try backends.Tensor.initFloat32(a, "logits", &.{ 2, 2 }, &.{ -1, 1, 2, -2 });
            return result;
        }
        fn controlled(raw: *anyopaque, tensors: []const backends.Tensor, a: std.mem.Allocator, control: Control) anyerror![]backends.Tensor {
            try control.check();
            return run(raw, tensors, a);
        }
        fn session(self: *@This()) backends.Session {
            return .{ .ptr = self, .vtable = &.{ .run = run, .runWithControl = controlled, .inputInfo = inputs, .outputInfo = outputInfo, .backend = backend, .close = close } };
        }
    };
    const a = std.testing.allocator;
    var fake = Fake{};
    const profile = Profile{ .input = .pixels, .score_transform = .softmax, .width = 1, .height = 1 };
    const labels = try classifyPreparedBatch(a, fake.session(), profile, &.{ "negative", "positive" }, &.{ &.{ 1, 0, 0 }, &.{ 0, 1, 0 } }, 1, .{});
    defer {
        for (labels) |row| {
            for (row) |label| a.free(label.label);
            a.free(row);
        }
        a.free(labels);
    }
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqualStrings("positive", labels[0][0].label);
    try std.testing.expectEqualStrings("negative", labels[1][0].label);
    fake.fixed = true;
    try std.testing.expect(!supportsPreparedBatch(fake.session(), profile, 2));
    try std.testing.expectError(error.UnsupportedMediaClassifier, classifyPreparedBatch(a, fake.session(), profile, &.{"negative"}, &.{ &.{ 1, 0, 0 }, &.{ 0, 1, 0 } }, 1, .{}));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    const serial = try classifyPreparedFrames(a, fake.session(), profile, &.{ "negative", "positive" }, &.{ &.{ 1, 0, 0 }, &.{ 0, 1, 0 } }, 1, .{});
    defer {
        for (serial) |row| {
            for (row) |label| a.free(label.label);
            a.free(row);
        }
        a.free(serial);
    }
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
    try std.testing.expectEqualStrings("positive", serial[0][0].label);
    try std.testing.expectEqualStrings("positive", serial[1][0].label);
    fake.fixed = false;
    fake.deny_batch = true;
    const fallback = try classifyPreparedFrames(a, fake.session(), profile, &.{ "negative", "positive" }, &.{ &.{ 1, 0, 0 }, &.{ 0, 1, 0 } }, 1, .{});
    defer {
        for (fallback) |row| {
            for (row) |label| a.free(label.label);
            a.free(row);
        }
        a.free(fallback);
    }
    try std.testing.expectEqual(@as(usize, 6), fake.calls);
}
