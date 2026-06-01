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
const backends = @import("../backends/backends.zig");
const Tensor = backends.Tensor;
const Session = backends.Session;
const DType = backends.DType;
const document_prep = @import("document_preprocessing.zig");
const document_shared = @import("document_shared.zig");

pub const SequenceResult = struct {
    label: []const u8,
    score: f32,
    logit: f32,
};

pub const TokenPrediction = struct {
    token_index: usize,
    label: []const u8,
    score: f32,
    bbox: [4]i32,
};

pub const EncoderOutputSummary = struct {
    output_count: usize,
    primary_rank: usize,
    primary_shape: []i64,
    cls_width: usize,
    cls_l2_norm: f32,
};

pub fn classifySequencePrepared(
    allocator: std.mem.Allocator,
    session: Session,
    prepared: *const document_prep.PreparedInputs,
    labels: []const []const u8,
) ![]SequenceResult {
    var outputs = try runPrepared(allocator, session, prepared);
    defer freeTensorSlice(allocator, outputs);
    if (outputs.len == 0) return error.NoOutputTensors;

    const logits = try tensorToOwnedF32(allocator, &outputs[0]);
    defer allocator.free(logits);
    if (labels.len == 0) return error.NoLabelsProvided;

    if (outputs[0].shape.len != 2) {
        return error.UnexpectedOutputShape;
    }
    const rows: usize = @intCast(outputs[0].shape[0]);
    const cols: usize = @intCast(outputs[0].shape[1]);
    if (rows != 1 or cols != labels.len) return error.LabelCountMismatch;
    const vector = logits[0..cols];
    const probs = try allocator.alloc(f32, labels.len);
    defer allocator.free(probs);
    document_shared.softmax(vector, probs);

    const results = try allocator.alloc(SequenceResult, labels.len);
    for (0..labels.len) |idx| {
        results[idx] = .{
            .label = labels[idx],
            .score = probs[idx],
            .logit = vector[idx],
        };
    }
    std.mem.sort(SequenceResult, results, {}, struct {
        fn lessThan(_: void, lhs: SequenceResult, rhs: SequenceResult) bool {
            return lhs.score > rhs.score;
        }
    }.lessThan);
    return results;
}

pub fn classifyTokenPrepared(
    allocator: std.mem.Allocator,
    session: Session,
    prepared: *const document_prep.PreparedInputs,
    labels: []const []const u8,
) ![]TokenPrediction {
    var outputs = try runPrepared(allocator, session, prepared);
    defer freeTensorSlice(allocator, outputs);
    if (outputs.len == 0) return error.NoOutputTensors;

    const logits = try tensorToOwnedF32(allocator, &outputs[0]);
    defer allocator.free(logits);
    if (labels.len == 0) return error.NoLabelsProvided;

    const seq_len = prepared.input_ids.len;
    const num_labels = labels.len;
    var rows: usize = 0;
    if (outputs[0].shape.len == 3) {
        const batch: usize = @intCast(outputs[0].shape[0]);
        if (batch != 1) return error.UnexpectedOutputShape;
        rows = @intCast(outputs[0].shape[1]);
        const cols: usize = @intCast(outputs[0].shape[2]);
        if (cols != num_labels) return error.LabelCountMismatch;
    } else if (outputs[0].shape.len == 2) {
        rows = @intCast(outputs[0].shape[0]);
        const cols: usize = @intCast(outputs[0].shape[1]);
        if (cols != num_labels) return error.LabelCountMismatch;
    } else {
        return error.UnexpectedOutputShape;
    }
    if (rows < seq_len) return error.UnexpectedOutputShape;

    var active_count: usize = 0;
    for (prepared.attention_mask, 0..) |mask, idx| {
        if (mask == 0) break;
        if (isZeroBox(prepared.bbox, idx)) continue;
        active_count += 1;
    }
    const predictions = try allocator.alloc(TokenPrediction, active_count);
    var out_idx: usize = 0;
    for (prepared.attention_mask, 0..) |mask, idx| {
        if (mask == 0) break;
        if (isZeroBox(prepared.bbox, idx)) continue;
        const offset = idx * num_labels;
        const probs = try allocator.alloc(f32, num_labels);
        defer allocator.free(probs);
        document_shared.softmax(logits[offset .. offset + num_labels], probs);
        var best_idx: usize = 0;
        var best_score = probs[0];
        for (1..num_labels) |label_idx| {
            if (probs[label_idx] > best_score) {
                best_score = probs[label_idx];
                best_idx = label_idx;
            }
        }
        const base = idx * 4;
        predictions[out_idx] = .{
            .token_index = idx,
            .label = labels[best_idx],
            .score = best_score,
            .bbox = .{
                prepared.bbox[base + 0],
                prepared.bbox[base + 1],
                prepared.bbox[base + 2],
                prepared.bbox[base + 3],
            },
        };
        out_idx += 1;
    }
    return predictions;
}

pub fn runPrepared(
    allocator: std.mem.Allocator,
    session: Session,
    prepared: *const document_prep.PreparedInputs,
) ![]Tensor {
    const inputs = try buildInputs(allocator, session, prepared);
    defer freeTensorSlice(allocator, inputs);
    return session.run(inputs, allocator);
}

pub fn summarizeEncoderPrepared(
    allocator: std.mem.Allocator,
    session: Session,
    prepared: *const document_prep.PreparedInputs,
) !EncoderOutputSummary {
    var outputs = try runPrepared(allocator, session, prepared);
    defer freeTensorSlice(allocator, outputs);
    if (outputs.len == 0) return error.NoOutputTensors;

    const primary = &outputs[0];
    if (primary.shape.len != 3) return error.UnexpectedOutputShape;
    const batch: usize = @intCast(primary.shape[0]);
    const seq_len: usize = @intCast(primary.shape[1]);
    const hidden: usize = @intCast(primary.shape[2]);
    if (batch != 1 or seq_len == 0 or hidden == 0) return error.UnexpectedOutputShape;

    const values = try tensorToOwnedF32(allocator, primary);
    defer allocator.free(values);

    var squared_sum: f64 = 0;
    for (values[0..hidden]) |value| {
        squared_sum += @as(f64, value) * @as(f64, value);
    }

    return .{
        .output_count = outputs.len,
        .primary_rank = primary.shape.len,
        .primary_shape = try allocator.dupe(i64, primary.shape),
        .cls_width = hidden,
        .cls_l2_norm = @floatCast(@sqrt(squared_sum)),
    };
}

fn buildInputs(
    allocator: std.mem.Allocator,
    session: Session,
    prepared: *const document_prep.PreparedInputs,
) ![]Tensor {
    var inputs = std.ArrayListUnmanaged(Tensor).empty;
    errdefer freeTensorSlice(allocator, inputs.items);

    const seq_shape = [_]i64{ 1, @intCast(prepared.input_ids.len) };
    const bbox_shape = [_]i64{ 1, @intCast(prepared.input_ids.len), 4 };
    const pixel_shape = [_]i64{
        1,
        3,
        @intCast(prepared.input_height),
        @intCast(prepared.input_width),
    };

    const input_ids_i64 = try allocator.alloc(i64, prepared.input_ids.len);
    defer allocator.free(input_ids_i64);
    const attention_mask_i64 = try allocator.alloc(i64, prepared.attention_mask.len);
    defer allocator.free(attention_mask_i64);
    const bbox_i64 = try allocator.alloc(i64, prepared.bbox.len);
    defer allocator.free(bbox_i64);
    for (prepared.input_ids, 0..) |value, idx| input_ids_i64[idx] = value;
    for (prepared.attention_mask, 0..) |value, idx| attention_mask_i64[idx] = value;
    for (prepared.bbox, 0..) |value, idx| bbox_i64[idx] = value;

    try inputs.append(allocator, try initIntTensorForDType(
        allocator,
        "input_ids",
        &seq_shape,
        input_ids_i64,
        tensorInputDType(session, "input_ids") orelse .i64,
    ));
    try inputs.append(allocator, try initIntTensorForDType(
        allocator,
        "attention_mask",
        &seq_shape,
        attention_mask_i64,
        tensorInputDType(session, "attention_mask") orelse .i64,
    ));
    try inputs.append(allocator, try initIntTensorForDType(
        allocator,
        "bbox",
        &bbox_shape,
        bbox_i64,
        tensorInputDType(session, "bbox") orelse .i64,
    ));
    if (hasInput(session, "token_type_ids")) {
        const zero_types = try allocator.alloc(i64, prepared.input_ids.len);
        defer allocator.free(zero_types);
        @memset(zero_types, 0);
        try inputs.append(allocator, try initIntTensorForDType(
            allocator,
            "token_type_ids",
            &seq_shape,
            zero_types,
            tensorInputDType(session, "token_type_ids") orelse .i64,
        ));
    }
    try inputs.append(allocator, try initFloatTensorForDType(
        allocator,
        "pixel_values",
        &pixel_shape,
        prepared.pixel_values,
        tensorInputDType(session, "pixel_values") orelse .f32,
    ));
    return inputs.toOwnedSlice(allocator);
}

fn hasInput(session: Session, name: []const u8) bool {
    for (session.inputInfo()) |info| {
        if (std.mem.eql(u8, info.name, name)) return true;
    }
    return false;
}

fn tensorInputDType(session: Session, name: []const u8) ?DType {
    for (session.inputInfo()) |info| {
        if (std.mem.eql(u8, info.name, name)) return info.dtype;
    }
    return null;
}

fn initIntTensorForDType(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    data: []const i64,
    dtype: DType,
) !Tensor {
    return switch (dtype) {
        .i64 => Tensor.initInt64(allocator, name, shape, data),
        .i32 => initInt32Tensor(allocator, name, shape, data),
        else => error.UnsupportedTensorType,
    };
}

fn initInt32Tensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    data: []const i64,
) !Tensor {
    const owned_data = try allocator.alloc(u8, data.len * @sizeOf(i32));
    errdefer allocator.free(owned_data);
    const aligned: []align(@alignOf(i32)) u8 = @alignCast(owned_data);
    const dst = std.mem.bytesAsSlice(i32, aligned);
    for (data, 0..) |value, idx| dst[idx] = @intCast(value);
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = owned_data,
        .dtype = .i32,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn initFloatTensorForDType(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    data: []const f32,
    dtype: DType,
) !Tensor {
    return switch (dtype) {
        .f32 => Tensor.initFloat32(allocator, name, shape, data),
        .f16 => initFloat16Tensor(allocator, name, shape, data),
        .bf16 => initBFloat16Tensor(allocator, name, shape, data),
        else => error.UnsupportedTensorType,
    };
}

fn initFloat16Tensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    data: []const f32,
) !Tensor {
    const owned_data = try allocator.alloc(u8, data.len * 2);
    errdefer allocator.free(owned_data);
    const aligned: []align(@alignOf(u16)) u8 = @alignCast(owned_data);
    const dst = std.mem.bytesAsSlice(u16, aligned);
    for (data, 0..) |value, idx| {
        const half: f16 = @floatCast(value);
        dst[idx] = @bitCast(half);
    }
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = owned_data,
        .dtype = .f16,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn initBFloat16Tensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    data: []const f32,
) !Tensor {
    const owned_data = try allocator.alloc(u8, data.len * 2);
    errdefer allocator.free(owned_data);
    const aligned: []align(@alignOf(u16)) u8 = @alignCast(owned_data);
    const dst = std.mem.bytesAsSlice(u16, aligned);
    for (data, 0..) |value, idx| {
        const bits: u32 = @bitCast(value);
        dst[idx] = @truncate(bits >> 16);
    }
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = owned_data,
        .dtype = .bf16,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn tensorToOwnedF32(allocator: std.mem.Allocator, tensor: *const Tensor) ![]f32 {
    return switch (tensor.dtype) {
        .f32 => allocator.dupe(f32, tensor.asFloat32()),
        .f16 => convertF16ToF32(allocator, tensor.data),
        .bf16 => convertBf16ToF32(allocator, tensor.data),
        else => error.UnsupportedTensorType,
    };
}

fn convertF16ToF32(allocator: std.mem.Allocator, data: []const u8) ![]f32 {
    const count = data.len / 2;
    const out = try allocator.alloc(f32, count);
    const aligned: []align(@alignOf(u16)) const u8 = @alignCast(data);
    const src = std.mem.bytesAsSlice(u16, aligned);
    for (src, 0..) |bits, idx| out[idx] = @floatCast(@as(f16, @bitCast(bits)));
    return out;
}

fn convertBf16ToF32(allocator: std.mem.Allocator, data: []const u8) ![]f32 {
    const count = data.len / 2;
    const out = try allocator.alloc(f32, count);
    const aligned: []align(@alignOf(u16)) const u8 = @alignCast(data);
    const src = std.mem.bytesAsSlice(u16, aligned);
    for (src, 0..) |bits, idx| {
        const wide: u32 = @as(u32, bits) << 16;
        out[idx] = @bitCast(wide);
    }
    return out;
}

fn freeTensorSlice(allocator: std.mem.Allocator, tensors: []Tensor) void {
    for (tensors) |*tensor| tensor.deinit();
    allocator.free(tensors);
}

fn isZeroBox(bbox: []const i32, token_index: usize) bool {
    const base = token_index * 4;
    return bbox[base + 0] == 0 and bbox[base + 1] == 0 and bbox[base + 2] == 0 and bbox[base + 3] == 0;
}

test "sequence classification decodes first output logits" {
    const alloc = std.testing.allocator;
    var fake = FakeSession.initSequence();
    defer fake.deinit(alloc);
    const labels = [_][]const u8{ "invoice", "receipt" };
    var prepared = try fakePrepared(alloc);
    defer prepared.deinit();
    const results = try classifySequencePrepared(alloc, fake.session(), &prepared, &labels);
    defer alloc.free(results);
    try std.testing.expectEqualStrings("receipt", results[0].label);
    try std.testing.expect(results[0].score > results[1].score);
}

test "token classification decodes active token predictions" {
    const alloc = std.testing.allocator;
    var fake = FakeSession.initToken();
    defer fake.deinit(alloc);
    const labels = [_][]const u8{ "O", "B-KEY" };
    var prepared = try fakePrepared(alloc);
    defer prepared.deinit();
    const results = try classifyTokenPrepared(alloc, fake.session(), &prepared, &labels);
    defer alloc.free(results);
    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("B-KEY", results[0].label);
}

const FakeSession = struct {
    input_info: []const backends.TensorInfo,
    output_info: []const backends.TensorInfo,
    output_data: []const f32,
    output_shape: []const i64,
    mode: enum { sequence, token },

    fn initSequence() FakeSession {
        return .{
            .input_info = &.{
                .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, 8 } },
                .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ 1, 8 } },
                .{ .name = "bbox", .dtype = .i64, .shape = &.{ 1, 8, 4 } },
                .{ .name = "pixel_values", .dtype = .f32, .shape = &.{ 1, 3, 224, 224 } },
            },
            .output_info = &.{.{ .name = "logits", .dtype = .f32, .shape = &.{ 1, 2 } }},
            .output_data = &.{ 0.1, 1.5 },
            .output_shape = &.{ 1, 2 },
            .mode = .sequence,
        };
    }

    fn initToken() FakeSession {
        return .{
            .input_info = &.{
                .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, 8 } },
                .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ 1, 8 } },
                .{ .name = "bbox", .dtype = .i64, .shape = &.{ 1, 8, 4 } },
                .{ .name = "pixel_values", .dtype = .f32, .shape = &.{ 1, 3, 224, 224 } },
            },
            .output_info = &.{.{ .name = "logits", .dtype = .f32, .shape = &.{ 1, 8, 2 } }},
            .output_data = &.{
                0.0, 0.0,
                0.1, 2.0,
                1.7, 0.1,
                0.0, 0.0,
                0.0, 0.0,
                0.0, 0.0,
                0.0, 0.0,
                0.0, 0.0,
            },
            .output_shape = &.{ 1, 8, 2 },
            .mode = .token,
        };
    }

    fn session(self: *FakeSession) Session {
        return .{
            .ptr = self,
            .vtable = &.{
                .run = run,
                .inputInfo = inputInfo,
                .outputInfo = outputInfo,
                .backend = backend,
                .close = close,
            },
        };
    }

    fn deinit(self: *FakeSession, allocator: std.mem.Allocator) void {
        _ = self;
        _ = allocator;
    }

    fn run(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror![]Tensor {
        const self: *FakeSession = @ptrCast(@alignCast(ptr));
        try std.testing.expectEqual(@as(usize, 4), inputs.len);
        const out = try allocator.alloc(Tensor, 1);
        out[0] = try Tensor.initFloat32(allocator, "logits", self.output_shape, self.output_data);
        return out;
    }

    fn inputInfo(ptr: *anyopaque) []const backends.TensorInfo {
        const self: *FakeSession = @ptrCast(@alignCast(ptr));
        return self.input_info;
    }

    fn outputInfo(ptr: *anyopaque) []const backends.TensorInfo {
        const self: *FakeSession = @ptrCast(@alignCast(ptr));
        return self.output_info;
    }

    fn backend(_: *anyopaque) backends.BackendType {
        return .native;
    }

    fn close(_: *anyopaque) void {}
};

fn fakePrepared(allocator: std.mem.Allocator) !document_prep.PreparedInputs {
    return .{
        .allocator = allocator,
        .input_ids = try allocator.dupe(i32, &.{ 101, 11, 12, 102, 0, 0, 0, 0 }),
        .attention_mask = try allocator.dupe(i32, &.{ 1, 1, 1, 1, 0, 0, 0, 0 }),
        .bbox = try allocator.dupe(i32, &.{
            0, 0, 0, 0,
            1, 2, 3, 4,
            5, 6, 7, 8,
            0, 0, 0, 0,
            0, 0, 0, 0,
            0, 0, 0, 0,
            0, 0, 0, 0,
            0, 0, 0, 0,
        }),
        .pixel_values = try allocator.alloc(f32, 3 * 224 * 224),
        .source_width = 224,
        .source_height = 224,
        .input_width = 224,
        .input_height = 224,
        .token_count = 2,
        .wordpiece_token_count = 4,
        .special_token_count = 6,
        .cls_token_id = 101,
        .sep_token_id = 102,
        .pad_token_id = 0,
        .max_length = 8,
    };
}
