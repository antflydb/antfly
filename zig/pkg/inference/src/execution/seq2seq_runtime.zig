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

//! Request-owned incremental execution for the explicit merged seq2seq ABI.
//! Reuses the graph KV owner, never a process-global prompt/context cache.
const std = @import("std");
const backends = @import("../backends/backends.zig");
const kv = @import("../graph/onnx_kv_cache.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub fn qualified(session: backends.Session) bool {
    return qualifiedSignature(session.inputInfo(), session.outputInfo());
}

/// Shared by metadata-only cold planning and the loaded-session ABI check.
pub fn qualifiedSignature(inputs: []const backends.TensorInfo, outputs: []const backends.TensorInfo) bool {
    var ids = false;
    var encoder = false;
    var branch = false;
    var past_count: usize = 0;
    for (inputs, 0..) |info, index| {
        for (inputs[0..index]) |prior| if (std.mem.eql(u8, prior.name, info.name)) return false;
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
            if (!kv.hasPresentOutputForPastInput(outputs, info.name)) return false;
            past_count += 1;
        } else return false;
    }
    var present_count: usize = 0;
    var logits = false;
    for (outputs, 0..) |info, index| {
        for (outputs[0..index]) |prior| if (std.mem.eql(u8, prior.name, info.name)) return false;
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

fn cacheBound(session: backends.Session, batch: usize, tokens: usize, context: usize) !usize {
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

/// Request admission accounts for both previous and replacement K/V. Move
/// the retained inputs to the KV category without charging the same host
/// payload again as temporary ingestion storage.
pub fn estimateAdmission(session: backends.Session, admission: @import("../backends/session.zig").RunAdmission, base: @import("../backends/session.zig").RunRequest, batch: usize, tokens: usize, context: usize, vocabulary: usize) !@import("../runtime/tier/memory.zig").AdmissionAmounts {
    if (!qualified(session)) return admission.estimateRequest(base, session.outputInfo());
    const cache_bytes = try cacheBound(session, batch, tokens, context);
    const logit_bytes = try std.math.mul(usize, vocabulary, @sizeOf(f32));
    const per_item = try std.math.add(usize, logit_bytes, 3 * @sizeOf(i64));
    var request = base;
    request.input_bytes = try std.math.add(usize, request.input_bytes, cache_bytes);
    request.output_kv_bytes = cache_bytes;
    request.output_bytes = try std.math.add(usize, cache_bytes, try std.math.mul(usize, batch, per_item));
    var peak = try admission.estimateRequest(request, session.outputInfo());
    peak.host_scratch_bytes = std.math.sub(usize, peak.host_scratch_bytes, cache_bytes) catch return error.InvalidSeq2SeqAdmission;
    peak.host_kv_bytes = try std.math.add(usize, peak.host_kv_bytes, cache_bytes);
    return peak;
}

const InputRole = union(enum) {
    tokens,
    encoder,
    mask,
    cache_branch,
    past: struct { output_index: usize, empty_shape: [4]i64, cross: bool },
};

pub const State = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    roles: []InputRole,
    inputs: []backends.Tensor,
    /// Outputs retain the signature order. The logits slot carries no
    /// ownership after publication, and each cross slot owns its latest value.
    cached_outputs: []backends.Tensor = &.{},
    logits_index: usize,
    /// Encoder buffers and mask are pinned by the enclosing generation request.
    encoder: backends.Tensor,
    mask: []const i64,
    processed: usize = 0,
    vocab_size: usize,

    pub fn init(allocator: std.mem.Allocator, session: backends.Session, encoder: backends.Tensor, mask: []const i64, vocab_size: usize) !?State {
        if (!qualified(session) or vocab_size == 0 or vocab_size > std.math.maxInt(i64)) return null;
        const input_info = session.inputInfo();
        const output_info = session.outputInfo();
        const roles = try allocator.alloc(InputRole, input_info.len);
        errdefer allocator.free(roles);
        const inputs = try allocator.alloc(backends.Tensor, input_info.len);
        errdefer allocator.free(inputs);
        for (input_info, roles) |info, *role| {
            role.* = if (kv.pastInputSuffix(info.name)) |suffix| cache: {
                for (output_info, 0..) |output, index| {
                    if (kv.presentNameMatchesPastInput(info.name, output.name)) break :cache .{ .past = .{
                        .output_index = index,
                        .empty_shape = .{ 1, info.shape[1], 0, info.shape[3] },
                        .cross = std.mem.indexOf(u8, suffix, ".encoder.") != null,
                    } };
                }
                unreachable; // The immutable qualified signature has an exact match.
            } else if (std.mem.eql(u8, info.name, "input_ids")) .tokens else if (std.mem.eql(u8, info.name, "encoder_hidden_states")) .encoder else if (std.mem.eql(u8, info.name, "encoder_attention_mask")) .mask else .cache_branch;
        }
        var logits_index: usize = 0;
        for (output_info, 0..) |output, index| if (std.mem.eql(u8, output.name, "logits")) {
            logits_index = index;
            break;
        };
        var planned = session;
        planned.cached_decoder_geometry = .{ .vocab_size = vocab_size };
        planned.broadcast_inputs = &.{"use_cache_branch"};
        return .{
            .allocator = allocator,
            .session = planned,
            .roles = roles,
            .inputs = inputs,
            .logits_index = logits_index,
            .encoder = encoder.borrowedView("encoder_hidden_states"),
            .mask = mask,
            .vocab_size = vocab_size,
        };
    }

    pub fn deinit(self: *State) void {
        if (self.cached_outputs.len != 0) kv.freeTensorSlice(self.allocator, self.cached_outputs);
        self.allocator.free(self.inputs);
        self.allocator.free(self.roles);
    }

    fn borrow(self: *const State, info: backends.TensorInfo, shape: []const i64, data: []const u8) backends.Tensor {
        return .{ .data = @constCast(data), .shape = shape, .dtype = info.dtype, .name = info.name, .allocator = self.allocator, .owns_data = false, .owns_shape = false };
    }

    /// Prefix grows monotonically. The same typed input map is reused across
    /// prefill and incremental calls; no names or empty cache tensors are
    /// allocated at a token boundary.
    pub fn step(self: *State, prefix: []const i64, dispatch: anytype, control: ?Control) !backends.Tensor {
        if (prefix.len <= self.processed) return error.InvalidDecoderPosition;
        if (control) |active| try active.check();
        _ = self.session.compactExclusiveRows(self.allocator, self.cached_outputs, 64 * 1024, control) catch |err| switch (err) {
            error.OutOfMemory, error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable => false,
            else => return err,
        };
        const fresh = self.processed == 0;
        const tokens = prefix[self.processed..];
        const shape = [_]i64{ 1, @intCast(tokens.len) };
        const mask_shape = [_]i64{ 1, @intCast(self.mask.len) };
        const branch = [_]u8{@intFromBool(!fresh)};
        const empty = std.mem.sliceAsBytes(@as([]const f32, &.{}));
        for (self.session.inputInfo(), self.roles, self.inputs) |info, *role, *input| {
            input.* = switch (role.*) {
                .tokens => self.borrow(info, &shape, std.mem.sliceAsBytes(tokens)),
                .encoder => self.encoder,
                .mask => self.borrow(info, &mask_shape, std.mem.sliceAsBytes(self.mask)),
                .cache_branch => self.borrow(info, info.shape, &branch),
                .past => |*past| if (fresh)
                    self.borrow(info, &past.empty_shape, empty)
                else
                    self.cached_outputs[past.output_index].borrowedView(info.name),
            };
        }
        const tensor_dispatch = @import("tensor_dispatch.zig");
        const outputs = try tensor_dispatch.run(self.allocator, self.session, null, self.session.execution_gate, self.inputs, control, dispatch);
        errdefer kv.freeTensorSlice(self.allocator, outputs);
        if (outputs.len != self.session.outputInfo().len) return error.InvalidDecoderOutput;
        for (outputs, self.session.outputInfo()) |output, info| {
            if (!std.mem.eql(u8, output.name, info.name) or output.dtype != .f32) return error.InvalidDecoderOutput;
        }
        const logits = outputs[self.logits_index];
        const logits_shape = [_]i64{ 1, @intCast(tokens.len), @intCast(self.vocab_size) };
        if (!std.mem.eql(i64, logits.shape, &logits_shape)) return error.InvalidDecoderOutput;
        try validateOutputBytes(logits);
        for (self.roles) |role| switch (role) {
            .past => |past| {
                const output = outputs[past.output_index];
                if (output.shape.len != 4) return error.InvalidDecoderOutput;
                if (!fresh and past.cross and output.data.len == 0) {
                    if (!tensor_dispatch.validTensor(output)) return error.InvalidDecoderOutput;
                    continue;
                }
                const expected = [_]i64{ 1, past.empty_shape[1], @intCast(if (past.cross) self.mask.len else prefix.len), past.empty_shape[3] };
                if (!std.mem.eql(i64, output.shape, &expected)) return error.InvalidDecoderOutput;
                try validateOutputBytes(output);
            },
            else => {},
        };
        if (control) |active| try active.check();
        // Every possible error precedes ownership transfer or advancement.
        // An empty cross output explicitly keeps this request's previous K/V.
        if (!fresh) {
            for (self.roles) |role| switch (role) {
                .past => |past| if (past.cross and outputs[past.output_index].data.len == 0) {
                    const old = &self.cached_outputs[past.output_index];
                    outputs[past.output_index].deinit();
                    outputs[past.output_index] = old.*;
                    old.* = old.borrowedView(old.name);
                },
                else => {},
            };
            kv.freeTensorSlice(self.allocator, self.cached_outputs);
        }
        outputs[self.logits_index] = self.borrow(self.session.outputInfo()[self.logits_index], &.{}, &.{});
        self.cached_outputs = outputs;
        self.processed = prefix.len;
        return logits;
    }

    fn validateOutputBytes(output: backends.Tensor) !void {
        var elements: usize = 1;
        for (output.shape) |dim| {
            if (dim <= 0) return error.InvalidDecoderOutput;
            elements = try std.math.mul(usize, elements, @intCast(dim));
        }
        if (try std.math.mul(usize, elements, @sizeOf(f32)) != output.data.len) return error.InvalidDecoderOutput;
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
        return .native;
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
        const rows: usize = @intCast(inputs[0].shape[0]);
        const tokens = ids.len / rows;
        self.submitted_tokens += ids.len;
        const cached = inputs[2].data[0] == 1;
        const old = inputs[3].asFloat32();
        const old_time = old.len / rows / 2;
        const time = old_time + tokens;
        const values = try allocator.alloc(f32, rows * time * 2);
        defer allocator.free(values);
        for (0..rows) |row| {
            const data = values[row * time * 2 ..][0 .. time * 2];
            @memcpy(data[0 .. old_time * 2], old[row * old_time * 2 ..][0 .. old_time * 2]);
            for (ids[row * tokens ..][0..tokens], 0..) |id, i| @memset(data[(old_time + i) * 2 ..][0..2], @as(f32, @floatFromInt(id)));
        }
        const outputs = try allocator.alloc(backends.Tensor, output_info.len);
        var initialized: usize = 0;
        errdefer {
            for (outputs[0..initialized]) |*output| output.deinit();
            allocator.free(outputs);
        }
        const logits = try allocator.alloc(f32, ids.len * 4);
        defer allocator.free(logits);
        @memset(logits, 0);
        for (0..rows) |row| {
            const context = inputs[1].asFloat32()[row * 4];
            if (cached) {
                try std.testing.expect(old_time > 0);
                try std.testing.expectEqual(context, inputs[5].asFloat32()[row * 4]);
            } else try std.testing.expectEqual(@as(usize, 0), old_time);
            var sum: usize = @intFromFloat(context);
            var i: usize = 0;
            while (i < time) : (i += 1) sum += @intFromFloat(values[(row * time + i) * 2]);
            logits[((row + 1) * tokens - 1) * 4 + sum % 4] = 10;
        }
        for (output_info, 0..) |info, index| {
            outputs[index] = if (index == 0)
                try backends.Tensor.initFloat32(allocator, info.name, &.{ @intCast(rows), @intCast(tokens), 4 }, logits)
            else if (index < 3)
                try backends.Tensor.initFloat32(allocator, info.name, &.{ @intCast(rows), 1, @intCast(time), 2 }, values)
            else if (cached)
                try backends.Tensor.initFloat32(allocator, info.name, &.{ 0, 1, 0, 2 }, &.{})
            else
                try backends.Tensor.initFloat32(allocator, info.name, &.{ @intCast(rows), 1, 2, 2 }, inputs[1].asFloat32());
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
    var first = (try State.init(allocator, session, encoder, &.{ 1, 1 }, 4)).?;
    defer first.deinit();
    var second = (try State.init(allocator, session, other_encoder, &.{ 1, 1 }, 4)).?;
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

test "incremental seq2seq fuses prefill and cached steps with broadcast branch and isolated contexts" {
    const Dispatch = @import("../server/tensor_microbatch.zig").Dispatch;
    const micro = @import("../server/executor_microbatch.zig");
    const tensors = @import("../server/tensor_microbatch.zig");
    const memory = @import("../runtime/tier/memory.zig");
    const Harness = struct {
        broker: micro.Broker,
        fn dispatch(raw: *anyopaque, task: micro.Task, alloc: std.mem.Allocator, session: backends.Session, permit: ?*@import("../backends/session.zig").RunPermit, gate: ?*std.atomic.Mutex, inputs: []const backends.Tensor, control: ?Control) ![]backends.Tensor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return tensors.run(&self.broker, alloc, std.testing.io, task, session, permit, gate.?, inputs, control, null, 500_000);
        }
    };
    const Job = struct {
        state: *State,
        prefix: []const i64,
        dispatch: Dispatch,
        result: ?backends.Tensor = null,
        err: ?anyerror = null,
        fn run(self: *@This()) std.Io.Cancelable!void {
            self.result = self.state.step(self.prefix, self.dispatch, null) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
    defer std.debug.assert(scheduler.head == null and scheduler.tail == null);
    var harness = Harness{ .broker = micro.Broker.init(std.testing.allocator) };
    defer harness.broker.deinit();
    var controller = memory.AdmissionController{};
    defer std.debug.assert(controller.snapshot().hostTotalBytes() == 0);
    var fake = TestDecoder{};
    var gate = std.atomic.Mutex.unlocked;
    var session = fake.session();
    session.execution_gate = &gate;
    session.cohort_scheduler = &scheduler;
    session.run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = 64 * 1024 }, .static_workspace_bytes = 1, .check_live_memory = false };
    var encoder = try backends.Tensor.initFloat32(std.testing.allocator, "hidden", &.{ 1, 2, 2 }, &.{ 1, 1, 1, 1 });
    defer encoder.deinit();
    var other = try backends.Tensor.initFloat32(std.testing.allocator, "hidden", &.{ 1, 2, 2 }, &.{ 2, 2, 2, 2 });
    defer other.deinit();
    var first = (try State.init(std.testing.allocator, session, encoder, &.{ 1, 1 }, 4)).?;
    defer first.deinit();
    var second = (try State.init(std.testing.allocator, session, other, &.{ 1, 1 }, 4)).?;
    defer second.deinit();
    const prefix = [_]i64{ 0, 2, 3 };
    for (1..4) |length| {
        var jobs = [_]Job{
            .{ .state = &first, .prefix = prefix[0..length], .dispatch = .{ .ptr = &harness, .task = .rewrite, .run_fn = Harness.dispatch } },
            .{ .state = &second, .prefix = prefix[0..length], .dispatch = .{ .ptr = &harness, .task = .rewrite, .run_fn = Harness.dispatch } },
        };
        defer for (&jobs) |*job| if (job.result) |*value| value.deinit();
        var group = std.Io.Group.init;
        defer group.cancel(std.testing.io);
        for (&jobs) |*job| group.async(std.testing.io, Job.run, .{job});
        try group.await(std.testing.io);
        for (&jobs, 1..) |*job, context| {
            if (job.err) |err| return err;
            var sum = context;
            for (prefix[0..length]) |id| sum += @intCast(id);
            try std.testing.expectEqual(@as(f32, 10), job.result.?.asFloat32()[sum % 4]);
        }
    }
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
    try std.testing.expectEqual(@as(usize, 6), fake.submitted_tokens);
}

test "incremental seq2seq cache ownership unwinds allocation failures" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, checkIncremental, .{});
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

test "incremental seq2seq request admission charges retained and replacement caches separately from host IO" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    var fake = TestDecoder{};
    var session = fake.session();
    session.run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false };
    const amount = try estimateAdmission(session, session.run_admission.?, .{ .batch = 2, .sequence = 3, .input_bytes = 20 }, 2, 3, 5, 4);
    try std.testing.expectEqual(@as(usize, 1152), amount.host_kv_bytes);
    try std.testing.expectEqual(@as(usize, 180), amount.host_scratch_bytes);
}

test "incremental seq2seq admits cache bytes before the backend call" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    var fake = TestDecoder{};
    var session = fake.session();
    session.run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .kv_limit_bytes = 1 }, .static_workspace_bytes = 1, .check_live_memory = false };
    var encoder = try backends.Tensor.initFloat32(std.testing.allocator, "hidden", &.{ 1, 2, 2 }, &.{ 1, 1, 1, 1 });
    defer encoder.deinit();
    var state = (try State.init(std.testing.allocator, session, encoder, &.{ 1, 1 }, 4)).?;
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
    var state = (try State.init(std.testing.allocator, session, encoder, &.{ 1, 1 }, 4)).?;
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
    try std.testing.expectEqual(@as(usize, 0), state.cached_outputs.len);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}
