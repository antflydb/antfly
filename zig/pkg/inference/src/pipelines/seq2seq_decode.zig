// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Request-owned incremental execution for the explicit merged seq2seq ABI.
//! Reuses the graph KV owner, never a process-global prompt/context cache.
const std = @import("std");
const backends = @import("../backends/backends.zig");
const kv = @import("../graph/onnx_kv_cache.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Dispatch = @import("../server/tensor_microbatch.zig").Dispatch;

pub fn qualified(session: backends.Session) bool {
    var ids = false;
    var encoder = false;
    var branch = false;
    var past_count: usize = 0;
    for (session.inputInfo()) |info| {
        if (std.mem.eql(u8, info.name, "input_ids")) {
            if (info.dtype != .i64 or info.shape.len != 2 or info.shape[0] > 1 or info.shape[1] > 0) return false;
            ids = true;
        } else if (std.mem.eql(u8, info.name, "encoder_hidden_states")) {
            if (info.dtype != .f32 or info.shape.len != 3 or info.shape[0] > 1) return false;
            encoder = true;
        } else if (std.mem.eql(u8, info.name, "encoder_attention_mask")) {
            if (info.dtype != .i64 or info.shape.len != 2 or info.shape[0] > 1) return false;
        } else if (std.mem.eql(u8, info.name, "use_cache_branch")) {
            if (info.dtype != .bool_ or info.shape.len > 1 or (info.shape.len == 1 and info.shape[0] != 1)) return false;
            branch = true;
        } else if (kv.pastInputSuffix(info.name)) |suffix| {
            if (!validCacheSuffix(suffix) or info.dtype != .f32 or info.shape.len != 4 or info.shape[0] > 1 or info.shape[1] <= 0 or info.shape[2] > 0 or info.shape[3] <= 0) return false;
            if (!kv.hasPresentOutputForPastInput(session.outputInfo(), info.name)) return false;
            past_count += 1;
        } else return false;
    }
    var present_count: usize = 0;
    var logits = false;
    for (session.outputInfo()) |info| {
        if (std.mem.eql(u8, info.name, "logits")) {
            if (info.dtype != .f32 or info.shape.len != 3 or info.shape[0] > 1) return false;
            logits = true;
        } else if (kv.presentOutputSuffix(info.name)) |suffix| {
            if (!validCacheSuffix(suffix) or info.dtype != .f32 or info.shape.len != 4 or info.shape[0] > 1) return false;
            present_count += 1;
        } else return false;
    }
    return ids and encoder and branch and logits and past_count > 0 and past_count == present_count;
}

fn validCacheSuffix(suffix: []const u8) bool {
    const dot = std.mem.indexOfScalar(u8, suffix, '.') orelse return false;
    _ = std.fmt.parseInt(usize, suffix[0..dot], 10) catch return false;
    for ([_][]const u8{ ".decoder.key", ".decoder.value", ".encoder.key", ".encoder.value" }) |ending|
        if (std.mem.eql(u8, suffix[dot..], ending)) return true;
    return false;
}

pub fn cacheBound(session: backends.Session, batch: usize, tokens: usize, context: usize) !usize {
    var total: usize = 0;
    for (session.inputInfo()) |info| {
        const suffix = kv.pastInputSuffix(info.name) orelse continue;
        const time = if (std.mem.indexOf(u8, suffix, ".encoder.") != null) context else tokens;
        const rows = try std.math.mul(usize, batch, time);
        const features = try std.math.mul(usize, @intCast(info.shape[1]), @intCast(info.shape[3]));
        const bytes = try std.math.mul(usize, try std.math.mul(usize, rows, features), info.dtype.byteSize());
        total = try std.math.add(usize, total, try std.math.add(usize, bytes, 4 * @sizeOf(i64)));
    }
    return total;
}

pub const State = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    cache: kv.KvCache,
    /// Encoder buffers and mask are pinned by the enclosing generation request.
    encoder: backends.Tensor,
    mask: []const i64,
    processed: usize = 0,
    vocab_size: usize,

    pub fn init(allocator: std.mem.Allocator, session: backends.Session, encoder: backends.Tensor, mask: []const i64, vocab_size: usize) ?State {
        if (!qualified(session) or vocab_size == 0) return null;
        var planned = session;
        planned.cached_decoder_geometry = .{ .vocab_size = vocab_size };
        return .{ .allocator = allocator, .session = planned, .cache = kv.KvCache.init(allocator), .encoder = encoder.borrowedView("encoder_hidden_states"), .mask = mask, .vocab_size = vocab_size };
    }

    pub fn deinit(self: *State) void {
        self.cache.deinit();
    }

    pub fn stepOutputs(self: *State, prefix: []const i64, dispatch: ?Dispatch, control: ?Control) ![]backends.Tensor {
        const outputs = try self.allocator.alloc(backends.Tensor, 1);
        errdefer self.allocator.free(outputs);
        outputs[0] = try self.step(prefix, dispatch, control);
        return outputs;
    }

    /// Prefix belongs to this request and grows monotonically. Only new tokens
    /// reach the graph after prefill; forced prompt tokens can append together.
    pub fn step(self: *State, prefix: []const i64, dispatch: ?Dispatch, control: ?Control) !backends.Tensor {
        if (prefix.len <= self.processed) return error.InvalidDecoderPosition;
        if (control) |active| try active.check();
        const tokens = prefix[self.processed..];
        const shape = [_]i64{ 1, @intCast(tokens.len) };
        const mask_shape = [_]i64{ 1, @intCast(self.mask.len) };
        const branch_value = [_]u8{if (self.processed == 0) 0 else 1};
        var inputs = std.ArrayListUnmanaged(backends.Tensor).empty;
        defer {
            for (inputs.items) |*input| input.deinit();
            inputs.deinit(self.allocator);
        }
        try inputs.ensureTotalCapacity(self.allocator, self.session.inputInfo().len);
        for (self.session.inputInfo()) |info| {
            const tensor: backends.Tensor = if (std.mem.eql(u8, info.name, "input_ids"))
                .{ .data = @constCast(std.mem.sliceAsBytes(tokens)), .shape = &shape, .dtype = .i64, .name = info.name, .allocator = self.allocator, .owns_data = false, .owns_shape = false }
            else if (std.mem.eql(u8, info.name, "encoder_hidden_states")) self.encoder else if (std.mem.eql(u8, info.name, "encoder_attention_mask"))
                .{ .data = @constCast(std.mem.sliceAsBytes(self.mask)), .shape = &mask_shape, .dtype = .i64, .name = info.name, .allocator = self.allocator, .owns_data = false, .owns_shape = false }
            else if (std.mem.eql(u8, info.name, "use_cache_branch"))
                .{ .data = @constCast(&branch_value), .shape = info.shape, .dtype = .bool_, .name = info.name, .allocator = self.allocator, .owns_data = false, .owns_shape = false }
            else blk: {
                if (self.processed == 0) break :blk try backends.Tensor.initFloat32(self.allocator, info.name, &.{ 1, info.shape[1], 0, info.shape[3] }, &.{});
                const name = try kv.presentNameForPastInput(self.allocator, info.name);
                defer self.allocator.free(name);
                const cached = kv.findTensor(self.cache.tensors, name) orelse return error.MissingPastKeyValue;
                break :blk cached.borrowedView(info.name);
            };
            inputs.appendAssumeCapacity(tensor);
        }
        const outputs = if (dispatch) |d| try d.run(self.allocator, self.session, null, self.session.execution_gate, inputs.items, control) else try self.session.runWithControl(inputs.items, self.allocator, control);
        var owned = true;
        errdefer if (owned) kv.freeTensorSlice(self.allocator, outputs);
        // Validate before committing state; errors never replay the model call.
        if (outputs.len != self.session.outputInfo().len) return error.InvalidDecoderOutput;
        var logits_index: ?usize = null;
        for (outputs, self.session.outputInfo(), 0..) |output, info, index| {
            if (!std.mem.eql(u8, output.name, info.name) or output.dtype != .f32) return error.InvalidDecoderOutput;
            if (std.mem.eql(u8, output.name, "logits")) {
                if (output.shape.len != 3 or output.shape[0] != 1 or output.shape[1] != tokens.len or output.shape[2] != self.vocab_size) return error.InvalidDecoderOutput;
                logits_index = index;
            } else {
                if (output.shape.len != 4) return error.InvalidDecoderOutput;
                const cross = std.mem.indexOf(u8, output.name, ".encoder.") != null;
                if (cross and output.data.len == 0 and self.processed > 0) continue;
                const expected = if (cross) self.mask.len else prefix.len;
                if (output.shape[0] != 1 or output.shape[2] != expected) return error.InvalidDecoderOutput;
                const suffix = kv.presentOutputSuffix(output.name).?;
                for (inputs.items) |input| {
                    const past_suffix = kv.pastInputSuffix(input.name) orelse continue;
                    if (!std.mem.eql(u8, suffix, past_suffix)) continue;
                    if (output.shape[1] != input.shape[1] or output.shape[3] != input.shape[3]) return error.InvalidDecoderOutput;
                    break;
                }
            }
            var elements: usize = 1;
            for (output.shape) |dim| {
                if (dim <= 0) return error.InvalidDecoderOutput;
                elements = try std.math.mul(usize, elements, @intCast(dim));
            }
            if (try std.math.mul(usize, elements, @sizeOf(f32)) != output.data.len) return error.InvalidDecoderOutput;
        }
        const index = logits_index orelse return error.InvalidDecoderOutput;
        var logits = outputs[index];
        outputs[index] = logits.borrowedView(logits.name);
        errdefer logits.deinit();
        try self.cache.replaceSeq2Seq(self.allocator, outputs);
        owned = false;
        self.processed = prefix.len;
        return logits;
    }
};

const TestDecoder = struct {
    calls: usize = 0,
    submitted_tokens: usize = 0,
    cancel_after: ?*std.atomic.Value(bool) = null,
    const input_info = [_]backends.TensorInfo{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
        .{ .name = "encoder_hidden_states", .dtype = .f32, .shape = &.{ -1, -1, 2 } },
        .{ .name = "use_cache_branch", .dtype = .bool_, .shape = &.{1} },
        .{ .name = "past_key_values.0.decoder.key", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
        .{ .name = "past_key_values.0.decoder.value", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
        .{ .name = "past_key_values.0.encoder.key", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
        .{ .name = "past_key_values.0.encoder.value", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
    };
    const output_info = [_]backends.TensorInfo{
        .{ .name = "logits", .dtype = .f32, .shape = &.{ -1, -1, 4 } },
        .{ .name = "present.0.decoder.key", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
        .{ .name = "present.0.decoder.value", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
        .{ .name = "present.0.encoder.key", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
        .{ .name = "present.0.encoder.value", .dtype = .f32, .shape = &.{ -1, 1, -1, 2 } },
    };
    fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
        return &input_info;
    }
    fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
        return &output_info;
    }
    fn backend(_: *anyopaque) backends.BackendType {
        return .onnx;
    }
    fn close(_: *anyopaque) void {}
    fn controlled(raw: *anyopaque, inputs: []const backends.Tensor, allocator: std.mem.Allocator, control: Control) ![]backends.Tensor {
        try control.check();
        return run(raw, inputs, allocator);
    }
    fn run(raw: *anyopaque, inputs: []const backends.Tensor, allocator: std.mem.Allocator) ![]backends.Tensor {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.calls += 1;
        const ids = inputs[0].asInt64();
        self.submitted_tokens += ids.len;
        const cached = inputs[2].data[0] == 1;
        const old = inputs[3].asFloat32();
        const context = inputs[1].asFloat32()[0];
        if (cached) {
            try std.testing.expect(old.len > 0);
            try std.testing.expectEqual(context, inputs[5].asFloat32()[0]);
        } else try std.testing.expectEqual(@as(usize, 0), old.len);
        const time = old.len / 2 + ids.len;
        const values = try allocator.alloc(f32, time * 2);
        defer allocator.free(values);
        @memcpy(values[0..old.len], old);
        for (ids, 0..) |id, i| @memset(values[old.len + i * 2 ..][0..2], @as(f32, @floatFromInt(id)));
        const outputs = try allocator.alloc(backends.Tensor, output_info.len);
        var initialized: usize = 0;
        errdefer {
            for (outputs[0..initialized]) |*output| output.deinit();
            allocator.free(outputs);
        }
        const logits = try allocator.alloc(f32, ids.len * 4);
        defer allocator.free(logits);
        @memset(logits, 0);
        var sum: usize = @intFromFloat(context);
        var i: usize = 0;
        while (i < values.len) : (i += 2) sum += @intFromFloat(values[i]);
        logits[(ids.len - 1) * 4 + sum % 4] = 10;
        for (output_info, 0..) |info, index| {
            outputs[index] = if (index == 0)
                try backends.Tensor.initFloat32(allocator, info.name, &.{ 1, @intCast(ids.len), 4 }, logits)
            else if (index < 3)
                try backends.Tensor.initFloat32(allocator, info.name, &.{ 1, 1, @intCast(time), 2 }, values)
            else if (cached)
                try backends.Tensor.initFloat32(allocator, info.name, &.{ 1, 1, 0, 2 }, &.{})
            else
                try backends.Tensor.initFloat32(allocator, info.name, &.{ 1, 1, 2, 2 }, inputs[1].asFloat32());
            initialized += 1;
        }
        if (self.cancel_after) |flag| flag.store(true, .release);
        return outputs;
    }
    fn session(self: *@This()) backends.Session {
        return .{ .ptr = self, .vtable = &.{ .run = run, .runWithControl = controlled, .inputInfo = inputInfo, .outputInfo = outputInfo, .backend = backend, .close = close } };
    }
};

fn checkIncremental(allocator: std.mem.Allocator) !void {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    defer std.debug.assert(controller.snapshot().hostTotalBytes() == 0);
    var fake = TestDecoder{};
    var session = fake.session();
    session.run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = 8192 }, .static_workspace_bytes = 1, .check_live_memory = false };
    var encoder = try backends.Tensor.initFloat32(allocator, "last_hidden_state", &.{ 1, 2, 2 }, &.{ 1, 1, 1, 1 });
    defer encoder.deinit();
    var other_encoder = try backends.Tensor.initFloat32(allocator, "last_hidden_state", &.{ 1, 2, 2 }, &.{ 2, 2, 2, 2 });
    defer other_encoder.deinit();
    var first = State.init(allocator, session, encoder, &.{ 1, 1 }, 4).?;
    defer first.deinit();
    var second = State.init(allocator, session, other_encoder, &.{ 1, 1 }, 4).?;
    defer second.deinit();
    const prefix = [_]i64{ 0, 2, 3 };
    for (1..4) |length| {
        for ([_]*State{ &first, &second }, 1..) |state, context| {
            var logits = try state.step(prefix[0..length], null, null);
            defer logits.deinit();
            var full_prefix_sum = context;
            for (prefix[0..length]) |id| full_prefix_sum += @intCast(id);
            try std.testing.expectEqual(@as(f32, 10), logits.asFloat32()[full_prefix_sum % 4]);
            try std.testing.expectEqual(@as(i64, 1), logits.shape[1]);
        }
    }
    try std.testing.expectEqual(@as(usize, 6), fake.calls);
    try std.testing.expectEqual(@as(usize, 6), fake.submitted_tokens); // Full-prefix replay would submit 12.
    try std.testing.expectEqual(@as(usize, 3), first.processed);
    try std.testing.expectEqual(@as(usize, 3), second.processed);
    try std.testing.expect(controller.snapshot().host_kv_bytes > 0);
}

test "incremental seq2seq reuses self and cross KV with context isolation and full-prefix parity" {
    try checkIncremental(std.testing.allocator);
}

test "incremental seq2seq cache ownership unwinds allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkIncremental, .{});
}

test "incremental seq2seq requires explicit merged ABI" {
    var fake = TestDecoder{};
    try std.testing.expect(qualified(fake.session()));
    var vtable = fake.session().vtable.*;
    const MissingBranch = struct {
        fn info(_: *anyopaque) []const backends.TensorInfo {
            return TestDecoder.input_info[3..];
        }
    };
    vtable.inputInfo = MissingBranch.info;
    try std.testing.expect(!qualified(.{ .ptr = &fake, .vtable = &vtable }));
}

test "incremental seq2seq admits cache bytes before the backend call" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    var fake = TestDecoder{};
    var session = fake.session();
    session.run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .kv_limit_bytes = 1 }, .static_workspace_bytes = 1, .check_live_memory = false };
    var encoder = try backends.Tensor.initFloat32(std.testing.allocator, "hidden", &.{ 1, 2, 2 }, &.{ 1, 1, 1, 1 });
    defer encoder.deinit();
    var state = State.init(std.testing.allocator, session, encoder, &.{ 1, 1 }, 4).?;
    defer state.deinit();
    try std.testing.expectError(error.ResourceLimitExceeded, state.step(&.{0}, null, null));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "incremental seq2seq cancellation after forwarding does not publish or replay cache" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    var canceled = std.atomic.Value(bool).init(false);
    var fake = TestDecoder{ .cancel_after = &canceled };
    var session = fake.session();
    session.run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{}, .static_workspace_bytes = 1, .check_live_memory = false };
    var encoder = try backends.Tensor.initFloat32(std.testing.allocator, "hidden", &.{ 1, 2, 2 }, &.{ 1, 1, 1, 1 });
    defer encoder.deinit();
    var state = State.init(std.testing.allocator, session, encoder, &.{ 1, 1 }, 4).?;
    defer state.deinit();
    const Probe = struct {
        fn check(raw: ?*anyopaque) bool {
            const flag: *std.atomic.Value(bool) = @ptrCast(@alignCast(raw.?));
            return flag.load(.acquire);
        }
    };
    try std.testing.expectError(error.Cancelled, state.step(&.{0}, null, .{ .cancellation = .{ .ptr = &canceled, .is_cancelled_fn = Probe.check } }));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(usize, 0), state.processed);
    try std.testing.expectEqual(@as(usize, 0), state.cache.tensors.len);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}
