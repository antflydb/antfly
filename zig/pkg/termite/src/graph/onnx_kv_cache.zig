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

pub const Tensor = backends.Tensor;
pub const TensorInfo = backends.TensorInfo;

const past_prefix = "past_key_values.";
const present_prefix = "present.";

pub const KvCache = struct {
    allocator: std.mem.Allocator,
    tensors: []Tensor = &.{},
    pending_outputs: ?[]Tensor = null,

    pub fn init(allocator: std.mem.Allocator) KvCache {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *KvCache) void {
        for (self.tensors) |*tensor| tensor.deinit();
        if (self.tensors.len > 0) self.allocator.free(self.tensors);
        self.tensors = &.{};
        if (self.pending_outputs) |outputs| {
            freeTensorSlice(self.allocator, outputs);
            self.pending_outputs = null;
        }
    }

    pub fn replace(self: *KvCache, allocator: std.mem.Allocator, output_info: []const TensorInfo, outputs_opt: ?[]Tensor) !void {
        _ = output_info;
        if (self.tensors.len > 0) {
            for (self.tensors) |*tensor| tensor.deinit();
            allocator.free(self.tensors);
            self.tensors = &.{};
        }
        const outputs = outputs_opt orelse return;
        self.pending_outputs = null;

        var count: usize = 0;
        for (outputs) |tensor| {
            if (std.mem.startsWith(u8, tensor.name, "present.")) count += 1;
        }
        const tensors = try allocator.alloc(Tensor, count);
        var idx: usize = 0;
        for (outputs, 0..) |tensor, out_idx| {
            if (!std.mem.startsWith(u8, tensor.name, "present.")) continue;
            tensors[idx] = tensor;
            idx += 1;
            outputs[out_idx].owns_data = false;
            outputs[out_idx].owns_shape = false;
        }
        freeTensorSlice(allocator, outputs);
        self.tensors = tensors;
    }

    pub fn seqLen(self: *const KvCache) usize {
        for (self.tensors) |tensor| {
            if (tensor.shape.len == 4 and std.mem.startsWith(u8, tensor.name, "present.")) {
                return @intCast(tensor.shape[2]);
            }
        }
        return 0;
    }
};

pub fn presentNameForPastInput(allocator: std.mem.Allocator, input_name: []const u8) ![]u8 {
    const suffix = pastInputSuffix(input_name) orelse return error.InvalidPastKeyName;
    return std.fmt.allocPrint(allocator, "{s}{s}", .{ present_prefix, suffix });
}

pub fn pastInputSuffix(input_name: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, input_name, past_prefix)) return null;
    return input_name[past_prefix.len..];
}

pub fn presentOutputSuffix(output_name: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, output_name, present_prefix)) return null;
    return output_name[present_prefix.len..];
}

pub fn presentNameMatchesPastInput(past_input_name: []const u8, present_output_name: []const u8) bool {
    const past_suffix = pastInputSuffix(past_input_name) orelse return false;
    const present_suffix = presentOutputSuffix(present_output_name) orelse return false;
    return std.mem.eql(u8, past_suffix, present_suffix);
}

pub fn hasPastInputs(input_info: []const TensorInfo) bool {
    for (input_info) |info| {
        if (pastInputSuffix(info.name) != null) return true;
    }
    return false;
}

pub fn hasPresentOutputs(output_info: []const TensorInfo) bool {
    for (output_info) |info| {
        if (presentOutputSuffix(info.name) != null) return true;
    }
    return false;
}

pub fn hasPresentOutputForPastInput(output_info: []const TensorInfo, past_input_name: []const u8) bool {
    for (output_info) |info| {
        if (presentNameMatchesPastInput(past_input_name, info.name)) return true;
    }
    return false;
}

pub fn supportsPastPresentIo(input_info: []const TensorInfo, output_info: []const TensorInfo) bool {
    var past_count: usize = 0;
    for (input_info) |info| {
        if (pastInputSuffix(info.name) == null) continue;
        past_count += 1;
        if (!hasPresentOutputForPastInput(output_info, info.name)) return false;
    }
    return past_count > 0;
}

pub fn findTensor(tensors: []const Tensor, present_name: []const u8) ?Tensor {
    for (tensors) |tensor| {
        if (std.mem.eql(u8, tensor.name, present_name)) return tensor;
    }
    return null;
}

pub fn borrowTensor(name: []const u8, source: Tensor) Tensor {
    return .{
        .data = source.data,
        .dtype = source.dtype,
        .shape = source.shape,
        .name = name,
        .allocator = source.allocator,
        .owns_data = false,
        .owns_shape = false,
    };
}

pub fn appendPastInputs(
    allocator: std.mem.Allocator,
    input_info: []const TensorInfo,
    cache: *const KvCache,
    inputs: *std.ArrayListUnmanaged(Tensor),
) !void {
    for (input_info) |info| {
        if (!std.mem.startsWith(u8, info.name, "past_key_values.")) continue;
        const present_name = try presentNameForPastInput(allocator, info.name);
        defer allocator.free(present_name);
        const tensor = findTensor(cache.tensors, present_name) orelse return error.MissingPastKeyValue;
        try inputs.append(allocator, borrowTensor(info.name, tensor));
    }
}

pub fn freeTensorSlice(allocator: std.mem.Allocator, tensors: []Tensor) void {
    for (tensors) |*tensor| tensor.deinit();
    allocator.free(tensors);
}

test "present name mapping matches Gemma decoder cache names" {
    const allocator = std.testing.allocator;
    const mapped = try presentNameForPastInput(allocator, "past_key_values.17.value");
    defer allocator.free(mapped);
    try std.testing.expectEqualStrings("present.17.value", mapped);
}

test "kv cache replace keeps present tensors for subsequent decode" {
    const allocator = std.testing.allocator;

    var outputs = try allocator.alloc(Tensor, 3);
    outputs[0] = try Tensor.initFloat32(allocator, "logits", &.{ 1, 2 }, &.{ 0.0, 1.0 });
    outputs[1] = try Tensor.initFloat32(allocator, "present.0.key", &.{ 1, 1, 1, 2 }, &.{ 1.0, 2.0 });
    outputs[2] = try Tensor.initFloat32(allocator, "present.0.value", &.{ 1, 1, 1, 2 }, &.{ 3.0, 4.0 });

    var cache = KvCache.init(allocator);
    defer cache.deinit();
    try cache.replace(allocator, &.{}, outputs);

    try std.testing.expectEqual(@as(usize, 2), cache.tensors.len);
    try std.testing.expectEqualStrings("present.0.key", cache.tensors[0].name);
    try std.testing.expectEqualStrings("present.0.value", cache.tensors[1].name);
    try std.testing.expectEqual(@as(usize, 1), cache.seqLen());
}

test "append past inputs borrows matching present tensors" {
    const allocator = std.testing.allocator;

    var key = try Tensor.initFloat32(allocator, "present.0.key", &.{ 1, 1, 1, 2 }, &.{ 1.0, 2.0 });
    defer key.deinit();
    var value = try Tensor.initFloat32(allocator, "present.0.value", &.{ 1, 1, 1, 2 }, &.{ 3.0, 4.0 });
    defer value.deinit();

    var cache_tensors = [_]Tensor{ key, value };
    const cache = KvCache{
        .allocator = allocator,
        .tensors = cache_tensors[0..],
    };
    const infos = [_]TensorInfo{
        .{ .name = "past_key_values.0.key", .dtype = .f32, .shape = &.{ 1, 1, 1, 2 } },
        .{ .name = "past_key_values.0.value", .dtype = .f32, .shape = &.{ 1, 1, 1, 2 } },
    };
    var inputs = std.ArrayListUnmanaged(Tensor).empty;
    defer inputs.deinit(allocator);
    defer for (inputs.items) |*tensor| tensor.deinit();

    try appendPastInputs(allocator, &infos, &cache, &inputs);

    try std.testing.expectEqual(@as(usize, 2), inputs.items.len);
    try std.testing.expectEqualStrings("past_key_values.0.key", inputs.items[0].name);
    try std.testing.expect(!inputs.items[0].owns_data);
    try std.testing.expect(!inputs.items[0].owns_shape);
}

test "past/present ABI detection requires matching cache outputs" {
    const inputs = [_]TensorInfo{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, 1 } },
        .{ .name = "past_key_values.0.key", .dtype = .f32, .shape = &.{ 1, 1, 1, 2 } },
        .{ .name = "past_key_values.0.value", .dtype = .f32, .shape = &.{ 1, 1, 1, 2 } },
    };
    const outputs = [_]TensorInfo{
        .{ .name = "logits", .dtype = .f32, .shape = &.{ 1, 1, 8 } },
        .{ .name = "present.0.key", .dtype = .f32, .shape = &.{ 1, 1, 2, 2 } },
        .{ .name = "present.0.value", .dtype = .f32, .shape = &.{ 1, 1, 2, 2 } },
    };
    const incomplete_outputs = [_]TensorInfo{
        .{ .name = "logits", .dtype = .f32, .shape = &.{ 1, 1, 8 } },
        .{ .name = "present.0.key", .dtype = .f32, .shape = &.{ 1, 1, 2, 2 } },
    };

    try std.testing.expect(hasPastInputs(&inputs));
    try std.testing.expect(hasPresentOutputs(&outputs));
    try std.testing.expect(presentNameMatchesPastInput("past_key_values.0.key", "present.0.key"));
    try std.testing.expect(supportsPastPresentIo(&inputs, &outputs));
    try std.testing.expect(!supportsPastPresentIo(&inputs, &incomplete_outputs));
}
