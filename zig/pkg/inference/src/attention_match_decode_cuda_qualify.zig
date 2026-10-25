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

//! Native ownership/admission qualification. Does not enable serving dispatch.
const std = @import("std");
const Context = @import("ops/cuda/context.zig").CudaContext;
const Buffer = @import("ops/cuda/buffer.zig").DeviceBuffer;
const decode = @import("ops/cuda/attention_match_decode.zig");
const storage = @import("ops/cuda/attention_match_storage.zig");
const checkpoint = @import("runtime/kv/attention_match_checkpoint.zig");

const fixture: checkpoint.State = .{
    .identity = .{ .weights = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4) },
    .logical_length = 4,
    .tokens = &.{ 0, 0, 0, 0 },
    .logits = &.{0},
    .layer_donors = &.{0},
    .donors = &.{.{ .layer = 0, .heads = &.{
        .{ .dim = 1, .compacted_until = 4, .positions = &.{ 0, 2 }, .coverage_ends = &.{ 2, 4 }, .bias = &.{ @log(@as(f32, 2)), @log(@as(f32, 2)) }, .keys = &.{ 0, 0 }, .values = &.{ 2, 4 } },
        .{ .dim = 1, .positions = &.{ 0, 1, 2, 3 }, .coverage_ends = &.{ 1, 2, 3, 4 }, .bias = &.{ 0, 0, 0, 0 }, .keys = &.{ 0, 0, 0, 0 }, .values = &.{ 1, 2, 3, 4 } },
    } }},
};

pub fn main() !void {
    var context = try Context.initDefault();
    defer context.deinit();
    try context.device_allocations.setLimit(128 * 1024 * 1024);
    for ([_]storage.Precision{ .f16, .f32 }) |precision| {
        try run(&context, precision);
        try runLocal(&context, precision);
        try runAppendBoundary(&context, precision);
        for ([_]u32{ 64, 128, 256, 512 }) |dim| {
            for ([_]bool{ false, true }) |all_compressed| try runRotated(&context, precision, dim, all_compressed, 8, false);
        }
        try runRotated(&context, precision, 512, false, 520, false);
        try runRotated(&context, precision, 512, true, 520, false);
        try runRotated(&context, precision, 512, false, 520, true);
        if (context.device_allocations.snapshot().live != 0) return error.DeviceAllocationLeak;
    }
    std.debug.print("PASS native ragged dispatch: both precisions, chunking, masks, cancellation, admission, alias rejection, cleanup; serving_qualified=false\n", .{});
}

fn runAppendBoundary(context: *Context, precision: storage.Precision) !void {
    const backend: storage.Backend = .{ .allocator = std.heap.page_allocator, .context = context, .precision = precision };
    var state = try backend.upload(fixture, 128 * 1024 * 1024, .{});
    defer state.deinit();
    var runner = try decode.Runner.init(context, 128 * 1024 * 1024);
    defer runner.deinit();
    const zeros: [512 * 2]f32 = @splat(0);
    const values: [512 * 2]f32 = @splat(3);
    var keys = try Buffer.alloc(context, @sizeOf(@TypeOf(zeros)));
    defer keys.free(context);
    var vals = try Buffer.alloc(context, @sizeOf(@TypeOf(values)));
    defer vals.free(context);
    try keys.copyFromHost(context, std.mem.sliceAsBytes(&zeros));
    try vals.copyFromHost(context, std.mem.sliceAsBytes(&values));
    // Fill the entire admitted prompt suffix, then append a generated token.
    // Previously the first decode append failed with CompactionAppendLimit.
    while (state.logical_length < fixture.logical_length + 8192)
        try runner.append(&state, 0, keys, vals, 512, state.logical_length, .{});
    try runner.append(&state, 0, keys, vals, 1, state.logical_length, .{});
    if (state.logical_length != 8197) return error.InvalidAppendState;
    var output = try Buffer.alloc(context, 2 * @sizeOf(f32));
    defer output.free(context);
    try runner.attend(.{ .heads = state.heads, .precision = precision, .queries = keys, .output = output, .query_rows = 1, .query_heads = 2, .first_query_position = 8196, .scale = 1 }, .{});
    var actual: [2]f32 = undefined;
    try output.copyToHost(context, std.mem.sliceAsBytes(&actual));
    try context.synchronize();
    if (@abs(actual[0] - 3) > 1e-5 or @abs(actual[1] - (3 - @as(f32, 2) / 8197)) > 1e-5)
        return error.NumericalQualificationFailed;
    // Output may use the remaining logical budget, but never exceed 32K.
    while (state.logical_length < checkpoint.max_context_tokens)
        try runner.append(&state, 0, keys, vals, @intCast(@min(512, checkpoint.max_context_tokens - state.logical_length)), state.logical_length, .{});
    const live = context.device_allocations.snapshot().live;
    if (runner.append(&state, 0, keys, vals, 1, state.logical_length, .{})) |_| return error.ExpectedAppendLimit else |err| if (err != error.CompactionAppendLimit) return err;
    if (state.logical_length != checkpoint.max_context_tokens or context.device_allocations.snapshot().live != live) return error.InvalidAppendState;
    std.debug.print("PASS append boundary: 8192 prompt tokens plus decode through 32768, precision={s}\n", .{@tagName(precision)});
}

fn run(context: *Context, precision: storage.Precision) !void {
    const allocator = std.heap.page_allocator;
    const backend: storage.Backend = .{ .allocator = allocator, .context = context, .precision = precision };
    var state = try backend.upload(fixture, 128 * 1024 * 1024, .{});
    defer state.deinit();
    var queries = try Buffer.alloc(context, 17 * 4 * 4);
    defer queries.free(context);
    const zeros: [17 * 4]f32 = @splat(0);
    try queries.copyFromHost(context, std.mem.sliceAsBytes(&zeros));
    var output = try Buffer.alloc(context, queries.len);
    defer output.free(context);
    var mass = try Buffer.alloc(context, queries.len);
    defer mass.free(context);
    try context.synchronize();
    const initial_live = context.device_allocations.snapshot().live;
    var request: decode.Request = .{ .heads = state.heads, .precision = precision, .queries = queries, .output = output, .log_mass = mass, .query_rows = 17, .query_heads = 4, .first_query_position = 4, .scale = 1 };
    {
        var denied = try decode.Runner.init(context, initial_live + 208);
        defer denied.deinit();
        if (denied.attend(request, .{})) |_| return error.ExpectedAdmissionFailure else |err| if (err != error.CudaMemoryLimitExceeded) return err;
        if (context.device_allocations.snapshot().live != initial_live + 208) return error.UnexpectedAllocation;
    }
    if (context.device_allocations.snapshot().live != initial_live) return error.DeviceAllocationLeak;
    var runner = try decode.Runner.init(context, 128 * 1024 * 1024);
    defer runner.deinit();
    runner.max_scratch_bytes = 1;
    if (runner.attend(request, .{})) |_| return error.ExpectedAdmissionFailure else |err| if (err != error.CompactionWorkspaceLimit) return err;
    runner.max_scratch_bytes = 64 * 1024 * 1024;
    try runner.attend(request, .{});
    var values: [17 * 4]f32 = undefined;
    var masses: [17 * 4]f32 = undefined;
    try output.copyToHost(context, std.mem.sliceAsBytes(&values));
    try mass.copyToHost(context, std.mem.sliceAsBytes(&masses));
    try context.synchronize();
    for (values, masses, 0..) |v, m, i| {
        if (@abs(v - @as(f32, if (i % 4 < 2) 3 else 2.5)) > 1e-6 or @abs(m - @log(@as(f32, 4))) > 1e-6) return error.NumericalQualificationFailed;
    }
    const live = context.device_allocations.snapshot().live;
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 3) return error.CompactionCancelled;
        }
    };
    var cancel: Cancel = .{};
    if (runner.attend(request, .{ .context = &cancel, .check_fn = Cancel.check })) |_| return error.ExpectedCancellation else |err| if (err != error.CompactionCancelled) return err;
    if (cancel.calls != 3 or context.device_allocations.snapshot().live != live) return error.DeviceAllocationLeak;
    // Recovery after cancellation reuses valid context-owned workspace.
    try runner.attend(request, .{});
    request.output = queries;
    if (runner.attend(request, .{})) |_| return error.ExpectedShapeFailure else |err| if (err != error.InvalidAttentionShape) return err;
    request.output = output;
    request.first_query_position = std.math.maxInt(u64);
    if (runner.attend(request, .{})) |_| return error.ExpectedShapeFailure else |err| if (err != error.InvalidAttentionShape) return err;
    request.first_query_position = 0;
    request.query_rows = 1;
    try runner.attend(request, .{});
    try output.copyToHost(context, std.mem.sliceAsBytes(&values));
    try mass.copyToHost(context, std.mem.sliceAsBytes(&masses));
    try context.synchronize();
    // Both fitted rows are hidden by coverage, while the exact head sees row 0.
    if (values[0] != 0 or values[1] != 0 or masses[0] != -std.math.inf(f32) or masses[1] != -std.math.inf(f32) or
        values[2] != 1 or values[3] != 1 or masses[2] != 0 or masses[3] != 0) return error.NumericalQualificationFailed;
    try qualifyAppend(context, &state, &runner, queries, output, mass);
    std.debug.print("PASS precision={s} live_device_bytes={d}\n", .{ @tagName(precision), live });
}

fn qualifyAppend(context: *Context, state: *storage.DeviceState, runner: *decode.Runner, queries: Buffer, output: Buffer, mass: Buffer) !void {
    var keys = try Buffer.alloc(context, 4 * 4);
    defer keys.free(context);
    var vals = try Buffer.alloc(context, 4 * 4);
    defer vals.free(context);
    var k: [4]f32 = @splat(0);
    const v: [4]f32 = .{ 6, 8, 10, 12 };
    try keys.copyFromHost(context, std.mem.sliceAsBytes(&k));
    try vals.copyFromHost(context, std.mem.sliceAsBytes(&v));
    try context.synchronize();
    try runner.append(state, 0, keys, vals, 2, 4, .{});
    if (state.logical_length != 6 or state.heads[0].rows != 4 or state.heads[1].rows != 6) return error.InvalidAppendState;
    const request: decode.Request = .{ .heads = state.heads, .precision = state.precision, .queries = queries, .output = output, .log_mass = mass, .query_rows = 2, .query_heads = 4, .first_query_position = 4, .scale = 1 };
    try runner.attend(request, .{});
    var actual: [8]f32 = undefined;
    try output.copyToHost(context, std.mem.sliceAsBytes(&actual));
    try context.synchronize();
    const expected: [8]f32 = .{ 3.6, 3.6, 3.6, 3.6, 28.0 / 6.0, 28.0 / 6.0, 5, 5 };
    for (actual, expected) |a, e| if (@abs(a - e) > 2e-6) return error.NumericalQualificationFailed;
    k[0] = std.math.nan(f32);
    try keys.copyFromHost(context, std.mem.sliceAsBytes(&k));
    try context.synchronize();
    if (runner.append(state, 0, keys, vals, 2, 6, .{})) |_| return error.ExpectedFiniteFailure else |err| if (err != error.NonFiniteAttention) return err;
    if (state.precision == .f16) {
        k[0] = 70000;
        try keys.copyFromHost(context, std.mem.sliceAsBytes(&k));
        try context.synchronize();
        if (runner.append(state, 0, keys, vals, 2, 6, .{})) |_| return error.ExpectedFiniteFailure else |err| if (err != error.NonFiniteAttention) return err;
    }
    k[0] = 0;
    try keys.copyFromHost(context, std.mem.sliceAsBytes(&k));
    try context.synchronize();
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 4) return error.CompactionCancelled;
        }
    };
    var cancel: Cancel = .{};
    if (runner.append(state, 0, keys, vals, 2, 6, .{ .context = &cancel, .check_fn = Cancel.check })) |_| return error.ExpectedCancellation else |err| if (err != error.CompactionCancelled) return err;
    if (cancel.calls != 4 or state.logical_length != 6 or state.heads[0].rows != 4 or state.heads[1].rows != 6) return error.InvalidAppendState;
    try runner.attend(request, .{});
    try output.copyToHost(context, std.mem.sliceAsBytes(&actual));
    try context.synchronize();
    for (actual, expected) |a, e| if (@abs(a - e) > 2e-6) return error.NumericalQualificationFailed;
    if (runner.append(state, 0, state.heads[0].keys, vals, 2, 6, .{})) |_| return error.ExpectedShapeFailure else |err| if (err != error.InvalidAttentionShape) return err;
    state.logical_length = checkpoint.max_context_tokens;
    if (runner.append(state, 0, keys, vals, 1, state.logical_length, .{})) |_| return error.ExpectedAppendLimit else |err| if (err != error.CompactionAppendLimit) return err;
    state.logical_length = 6;
    try runner.append(state, 0, keys, vals, 2, 6, .{});
    if (state.logical_length != 8) return error.InvalidAppendState;
    var metadata = fixture;
    metadata.logical_length = 8;
    metadata.tokens = &@as([8]u32, @splat(0));
    const before_rollback = try qualifyReadback(state, metadata);
    const RollbackCancel = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 2) return error.CompactionCancelled;
        }
    };
    var rollback_cancel: RollbackCancel = .{};
    if (state.truncate(6, .{ .context = &rollback_cancel, .check_fn = RollbackCancel.check })) |_| return error.ExpectedCancellation else |err| if (err != error.CompactionCancelled) return err;
    if (state.logical_length != 8 or rollback_cancel.calls != 2) return error.InvalidRollback;
    if (state.truncate(5, .{})) |_| return error.ExpectedRollbackFailure else |err| if (err != error.UnsupportedAttentionMatchRollback) return err;
    try state.truncate(6, .{});
    if (state.logical_length != 6 or state.heads[0].rows != 4 or state.heads[1].rows != 6) return error.InvalidRollback;
    try runner.append(state, 0, keys, vals, 2, 6, .{});
    const after_rollback = try qualifyReadback(state, metadata);
    if (!std.mem.eql(u8, &before_rollback, &after_rollback)) return error.InvalidRollback;
    std.debug.print("APPEND precision={s} nonfinite=true cancellation_after_kernel=true unchanged_source=true bounded=true\n", .{@tagName(state.precision)});
}

fn runLocal(context: *Context, precision: storage.Precision) !void {
    const local: checkpoint.State = .{ .identity = fixture.identity, .logical_length = 4, .tokens = fixture.tokens, .logits = fixture.logits, .layer_donors = &.{0}, .donors = &.{.{ .layer = 0, .window = 3, .heads = &.{.{ .dim = 1, .positions = &.{ 1, 2, 3 }, .coverage_ends = &.{ 2, 3, 4 }, .keys = &.{ 0, 0, 0 }, .values = &.{ 1, 2, 3 }, .bias = &.{ 0, 0, 0 } }} }} };
    const backend: storage.Backend = .{ .allocator = std.heap.page_allocator, .context = context, .precision = precision };
    var state = try backend.upload(local, 128 * 1024 * 1024, .{});
    defer state.deinit();
    var runner = try decode.Runner.init(context, 128 * 1024 * 1024);
    defer runner.deinit();
    var queries = try Buffer.alloc(context, 8);
    defer queries.free(context);
    var vals = try Buffer.alloc(context, 8);
    defer vals.free(context);
    var output = try Buffer.alloc(context, 8);
    defer output.free(context);
    const zero: [2]f32 = .{ 0, 0 };
    const initial: [2]f32 = .{ 4, 5 };
    try queries.copyFromHost(context, std.mem.sliceAsBytes(&zero));
    try vals.copyFromHost(context, std.mem.sliceAsBytes(&initial));
    try context.synchronize();
    try runner.append(&state, 0, queries, vals, 2, 4, .{});
    var request: decode.Request = .{ .heads = state.heads, .precision = precision, .queries = queries, .output = output, .query_rows = 2, .query_heads = 1, .first_query_position = 4, .scale = 1 };
    try runner.attend(request, .{});
    var actual: [2]f32 = undefined;
    try output.copyToHost(context, std.mem.sliceAsBytes(&actual));
    try context.synchronize();
    if (@abs(actual[0] - 3) > 1e-6 or @abs(actual[1] - 4) > 1e-6) return error.NumericalQualificationFailed;
    try state.truncate(4, .{});
    try runner.append(&state, 0, queries, vals, 2, 4, .{});
    try runner.attend(request, .{});
    try output.copyToHost(context, std.mem.sliceAsBytes(&actual));
    try context.synchronize();
    if (@abs(actual[0] - 3) > 1e-6 or @abs(actual[1] - 4) > 1e-6) return error.InvalidRollback;
    try state.retainLocalWindow(0);
    if (state.truncate(5, .{})) |_| return error.ExpectedRollbackFailure else |err| if (err != error.UnsupportedAttentionMatchRollback) return err;
    const live = context.device_allocations.snapshot().live;
    request.query_rows = 1;
    for (6..606) |position| {
        const value: f32 = @floatFromInt(position % 16);
        try vals.copyFromHost(context, std.mem.asBytes(&value));
        try context.synchronize();
        try runner.append(&state, 0, queries, vals, 1, position, .{});
        request.first_query_position = position;
        try runner.attend(request, .{});
        try output.copyToHost(context, std.mem.sliceAsBytes(&actual));
        try context.synchronize();
        const expected = @as(f32, @floatFromInt(position % 16 + (position - 1) % 16 + (position - 2) % 16)) / 3;
        if (@abs(actual[0] - expected) > 1e-6) return error.NumericalQualificationFailed;
        if (position == 605) {
            try state.truncate(position, .{});
            try runner.append(&state, 0, queries, vals, 1, position, .{});
            try runner.attend(request, .{});
            try output.copyToHost(context, std.mem.sliceAsBytes(&actual));
            try context.synchronize();
            if (@abs(actual[0] - expected) > 1e-6) return error.InvalidRollback;
        }
        try state.retainLocalWindow(0);
        if (state.heads[0].rows != 3 or state.logical_length != position + 1 or context.device_allocations.snapshot().live != live) return error.InvalidAppendState;
    }
    const before = actual[0];
    var metadata = local;
    metadata.logical_length = 606;
    metadata.tokens = &@as([606]u32, @splat(0));
    const before_digest = try qualifyReadback(&state, metadata);
    try state.grow(&state.heads[0], 1024, 128 * 1024 * 1024, .{});
    if (state.heads[0].start != 0 or state.heads[0].rows != 3) return error.InvalidAppendState;
    try runner.attend(request, .{});
    try output.copyToHost(context, std.mem.sliceAsBytes(&actual));
    try context.synchronize();
    if (actual[0] != before) return error.NumericalQualificationFailed;
    const after_digest = try qualifyReadback(&state, metadata);
    if (!std.mem.eql(u8, &before_digest, &after_digest)) return error.InvalidReadback;
    std.debug.print("LOCAL precision={s} steps=600 wrapped_growth=true stable_allocations=true\n", .{@tagName(precision)});
}

fn qualifyReadback(state: *storage.DeviceState, metadata: checkpoint.State) ![32]u8 {
    const a = std.heap.page_allocator;
    if (state.download(a, metadata, .{}, 1, .{})) |_| return error.ExpectedAdmissionFailure else |err| if (err != error.CompactionWorkspaceLimit) return err;
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 3) return error.CompactionCancelled;
        }
    };
    var cancel: Cancel = .{};
    if (state.download(a, metadata, .{}, 1024 * 1024, .{ .context = &cancel, .check_fn = Cancel.check })) |_| return error.ExpectedCancellation else |err| if (err != error.CompactionCancelled) return err;
    var snapshot = try state.download(a, metadata, .{}, 1024 * 1024, .{});
    defer snapshot.deinit();
    const head = snapshot.state.donors[0].heads[0];
    if (metadata.logical_length == 8) {
        if (!std.mem.eql(u64, head.positions, &.{ 0, 2, 4, 5, 6, 7 }) or
            !std.mem.eql(f32, head.values, &.{ 2, 4, 6, 10, 6, 10 })) return error.InvalidReadback;
    } else if (metadata.logical_length == 606) {
        if (!std.mem.eql(u64, head.positions, &.{ 603, 604, 605 }) or
            !std.mem.eql(f32, head.values, &.{ 11, 12, 13 })) return error.InvalidReadback;
    }
    const bytes = try checkpoint.encode(a, snapshot.state, .{});
    defer a.free(bytes);
    try qualifyClone(state, metadata, bytes);
    var imported = try checkpoint.decode(a, bytes, metadata.identity, .{});
    defer imported.deinit();
    const backend: storage.Backend = .{ .allocator = a, .context = state.context, .precision = state.precision };
    var uploaded = try backend.upload(imported.state, 128 * 1024 * 1024, .{});
    defer uploaded.deinit();
    var again = try uploaded.download(a, metadata, .{}, 1024 * 1024, .{});
    defer again.deinit();
    const bytes_again = try checkpoint.encode(a, again.state, .{});
    defer a.free(bytes_again);
    if (!std.mem.eql(u8, bytes, bytes_again)) return error.InvalidReadback;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return digest;
}
fn qualifyClone(state: *storage.DeviceState, metadata: checkpoint.State, expected: []const u8) !void {
    const a = std.heap.page_allocator;
    const live = state.context.device_allocations.snapshot().live;
    if (state.clone(a, live, .{})) |_| return error.ExpectedAdmissionFailure else |err| if (err != error.CudaMemoryLimitExceeded) return err;
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 3) return error.CompactionCancelled;
        }
    };
    var cancel: Cancel = .{};
    if (state.clone(a, 128 * 1024 * 1024, .{ .context = &cancel, .check_fn = Cancel.check })) |_| return error.ExpectedCancellation else |err| if (err != error.CompactionCancelled) return err;
    if (cancel.calls != 3 or state.context.device_allocations.snapshot().live != live) return error.DeviceAllocationLeak;
    try state.context.device_allocations.setLimit(live + 1);
    defer state.context.device_allocations.setLimit(128 * 1024 * 1024) catch {};
    if (state.clone(a, 128 * 1024 * 1024, .{})) |_| return error.ExpectedAdmissionFailure else |err| if (err != error.CudaMemoryLimitExceeded) return err;
    if (state.context.device_allocations.snapshot().live != live) return error.DeviceAllocationLeak;
    try state.context.device_allocations.setLimit(128 * 1024 * 1024);
    const saved_length = state.heads[0].logical_length;
    state.heads[0].logical_length += 1;
    const invalid = state.clone(a, 128 * 1024 * 1024, .{});
    state.heads[0].logical_length = saved_length;
    if (invalid) |_| return error.ExpectedShapeFailure else |err| if (err != error.InvalidAttentionShape) return err;
    {
        var cloned = try state.clone(a, 128 * 1024 * 1024, .{});
        defer cloned.deinit();
        if (cloned.initial_logical_length != state.initial_logical_length or cloned.logical_length != state.logical_length) return error.InvalidClone;
        for (state.heads, cloned.heads) |source, copy| {
            if (copy.keys.ptr == source.keys.ptr or copy.values.ptr == source.values.ptr or copy.positions.ptr == source.positions.ptr or copy.bias.ptr == source.bias.ptr or copy.coverage_ends.ptr == source.coverage_ends.ptr or copy.start != 0 or copy.capacity != source.rows or copy.rows != source.rows) return error.InvalidClone;
        }
        var snapshot = try cloned.download(a, metadata, .{}, 1024 * 1024, .{});
        defer snapshot.deinit();
        const encoded = try checkpoint.encode(a, snapshot.state, .{});
        defer a.free(encoded);
        if (!std.mem.eql(u8, expected, encoded)) return error.InvalidClone;
        // Mutate the shadow, then prove the live cache remained byte-identical.
        const changed16: [1]f16 = .{999};
        const changed32: [1]f32 = .{999};
        try cloned.heads[0].values.copyFromHost(state.context, if (state.precision == .f16) std.mem.sliceAsBytes(&changed16) else std.mem.sliceAsBytes(&changed32));
        try cloned.synchronize();
        var original = try state.download(a, metadata, .{}, 1024 * 1024, .{});
        defer original.deinit();
        const original_bytes = try checkpoint.encode(a, original.state, .{});
        defer a.free(original_bytes);
        if (!std.mem.eql(u8, expected, original_bytes)) return error.CloneMutatedSource;
    }
    if (state.context.device_allocations.snapshot().live != live) return error.DeviceAllocationLeak;
}

// Exercise the production upload/dispatch/append/clone/download path, comparing
// against attention over the canonical checkpoint's decoded host rows.
fn runRotated(context: *Context, precision: storage.Precision, dim: u32, all_compressed: bool, rows: usize, fragmented: bool) !void {
    const a = std.heap.page_allocator;
    const packed_storage = checkpoint.packed_storage;
    const keys = try a.alloc(f32, rows * dim);
    defer a.free(keys);
    const values = try a.alloc(f32, keys.len);
    defer a.free(values);
    for (keys, values, 0..) |*k, *v, i| {
        k.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 37)) - 18)) / 32;
        v.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 29)) - 14)) / 8;
    }
    const begin: usize = if (all_compressed) 0 else 1;
    const end: usize = if (all_compressed) rows else if (fragmented) rows - 2 else rows / 2;
    const second_first = end + @max(@as(usize, 1), rows / 16);
    const second_end = rows - 2;
    var first = try (packed_storage.Packed{}).append(a, begin, dim, keys[begin * dim .. end * dim], values[begin * dim .. end * dim], .{});
    defer first.deinit(a);
    var compressed = if (all_compressed or fragmented) try first.clone(a) else try first.append(a, second_first, dim, keys[second_first * dim .. second_end * dim], values[second_first * dim .. second_end * dim], .{});
    defer compressed.deinit(a);
    if (fragmented) {
        const selected = try a.alloc(bool, end - begin);
        defer a.free(selected);
        // Retain enough fragmented exact rows to cross the 128K-element
        // conversion slab boundary while still exercising over 100 spans.
        for (selected, 0..) |*keep, i| keep.* = i % 4 == 0;
        const filtered = try compressed.selectNew(a, begin, selected, dim, .{});
        compressed.deinit(a);
        compressed = filtered;
        if (compressed.spans.len < 100) return error.InsufficientFragmentedCoverage;
    }
    for (0..rows) |r| switch (compressed.index(r)) {
        .exact => {},
        .compressed => |i| try compressed.decodeRow(dim, i, keys[r * dim ..][0..dim], values[r * dim ..][0..dim]),
    };
    const positions = try a.alloc(u64, rows);
    defer a.free(positions);
    const ends = try a.alloc(u64, rows);
    defer a.free(ends);
    const biases = try a.alloc(f32, rows);
    defer a.free(biases);
    const tokens = try a.alloc(u32, rows + 1);
    defer a.free(tokens);
    @memset(tokens, 0);
    const boundary = if (all_compressed) rows else second_end;
    for (positions, ends, biases, 0..) |*position, *coverage, *bias, i| {
        position.* = i;
        const quantized = compressed.index(i) == .compressed;
        coverage.* = if (quantized) @min(i + 2, boundary) else i + 1;
        bias.* = if (!quantized) 0 else if (i % 2 == 0) 0.3 else -0.2;
    }
    const head: checkpoint.Head = .{ .dim = dim, .compacted_until = boundary, .positions = positions, .coverage_ends = ends, .keys = keys, .values = values, .bias = biases, .compressed = compressed };
    const host: checkpoint.State = .{ .identity = fixture.identity, .logical_length = rows, .tokens = tokens[0..rows], .logits = &.{0}, .layer_donors = &.{0}, .donors = &.{.{ .heads = &.{head} }} };
    const backend: storage.Backend = .{ .allocator = a, .context = context, .precision = precision };
    const before = context.device_allocations.snapshot().live;
    var state = try backend.upload(host, 128 * 1024 * 1024, .{});
    defer state.deinit();
    if (state.allocated_bytes != try backend.allocationBytes(host) or context.device_allocations.snapshot().live - before != state.allocated_bytes) return error.UnexpectedAllocation;
    var cloned = try state.clone(a, 128 * 1024 * 1024, .{});
    defer cloned.deinit();
    var restored = try cloned.download(a, host, .{}, 16 * 1024 * 1024, .{});
    defer restored.deinit();
    const expected_wire = try checkpoint.encodeWithEncoding(a, host, .{}, .rotated4, .{});
    defer a.free(expected_wire);
    const actual_wire = try checkpoint.encodeWithEncoding(a, restored.state, .{}, .rotated4, .{});
    defer a.free(actual_wire);
    if (!std.mem.eql(u8, expected_wire, actual_wire)) return error.CheckpointChangedOnDevice;
    const queries = try a.alloc(f32, 17 * 2 * dim);
    defer a.free(queries);
    for (queries, 0..) |*q, i| q.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 23)) - 11)) / 16;
    var q = try Buffer.alloc(context, queries.len * 4);
    defer q.free(context);
    var output = try Buffer.alloc(context, q.len);
    defer output.free(context);
    var mass = try Buffer.alloc(context, 17 * 2 * 4);
    defer mass.free(context);
    try q.copyFromHost(context, std.mem.sliceAsBytes(queries));
    try context.synchronize();
    var runner = try decode.Runner.init(context, 128 * 1024 * 1024);
    defer runner.deinit();
    const actual = try a.alloc(f32, queries.len);
    defer a.free(actual);
    var masses: [34]f32 = undefined;
    const first_query = if (rows > 17) rows - 17 else 0;
    for ([_]f32{ 0, 2 }) |softcap| {
        try runner.attend(.{ .heads = state.heads, .precision = precision, .queries = q, .output = output, .log_mass = mass, .query_rows = 17, .query_heads = 2, .first_query_position = first_query, .scale = 0.125, .softcap = softcap }, .{});
        try output.copyToHost(context, std.mem.sliceAsBytes(actual));
        try mass.copyToHost(context, std.mem.sliceAsBytes(&masses));
        try context.synchronize();
        for (0..34) |query| {
            var scores_buffer: [520]f64 = undefined;
            const scores = scores_buffer[0..rows];
            var maximum: f64 = -std.math.inf(f64);
            for (scores, 0..) |*score, r| {
                score.* = -std.math.inf(f64);
                if (head.coverage_ends[r] > first_query + query / 2 + 1) continue;
                var dot: f64 = 0;
                for (0..dim) |d| dot += @as(f64, queries[query * dim + d]) * keys[r * dim + d];
                dot *= 0.125;
                if (softcap != 0) dot = softcap * std.math.tanh(dot / softcap);
                score.* = dot + head.bias[r];
                maximum = @max(maximum, score.*);
            }
            if (maximum == -std.math.inf(f64)) {
                if (masses[query] != -std.math.inf(f32)) return error.NumericalQualificationFailed;
                for (actual[query * dim ..][0..dim]) |value| if (value != 0) return error.NumericalQualificationFailed;
                continue;
            }
            var total: f64 = 0;
            for (scores) |*score| {
                score.* = @exp(score.* - maximum);
                total += score.*;
            }
            if (!std.math.isFinite(masses[query]) or @abs(masses[query] - (maximum + @log(total))) > 2e-5) return error.NumericalQualificationFailed;
            for (0..dim) |d| {
                var expected: f64 = 0;
                for (scores, 0..) |score, r| expected += score * values[r * dim + d];
                if (!std.math.isFinite(actual[query * dim + d]) or @abs(actual[query * dim + d] - expected / total) > 2e-5) return error.NumericalQualificationFailed;
            }
        }
    }
    // Growing transfers compressed ownership only after allocation/copy succeeds.
    const growth = ((rows + 255) / 256 + 1) * 256;
    const old_ptr = state.heads[0].encoded_keys.ptr;
    const live = context.device_allocations.snapshot().live;
    if (state.grow(&state.heads[0], growth, live, .{})) |_| return error.ExpectedAdmissionFailure else |err| if (err != error.CudaMemoryLimitExceeded) return err;
    if (state.heads[0].encoded_keys.ptr != old_ptr or context.device_allocations.snapshot().live != live) return error.DeviceAllocationLeak;
    const replacement_kv_bytes = (growth - state.heads[0].compressed_rows) * dim * @as(usize, if (precision == .f16) 2 else 4);
    for ([_]usize{ 0, replacement_kv_bytes, 2 * replacement_kv_bytes, 2 * replacement_kv_bytes + growth * 4, 2 * replacement_kv_bytes + growth * 12 }) |admitted| {
        try context.device_allocations.setLimit(live + admitted);
        defer context.device_allocations.setLimit(128 * 1024 * 1024) catch unreachable;
        if (state.grow(&state.heads[0], growth, 128 * 1024 * 1024, .{})) |_| return error.ExpectedAdmissionFailure else |err| if (err != error.CudaMemoryLimitExceeded) return err;
        if (context.device_allocations.snapshot().live != live or state.heads[0].encoded_keys.ptr != old_ptr or state.heads[0].capacity != rows) return error.DeviceAllocationLeak;
    }
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 2) return error.CompactionCancelled;
        }
    };
    var cancel: Cancel = .{};
    if (state.grow(&state.heads[0], growth, 128 * 1024 * 1024, .{ .context = &cancel, .check_fn = Cancel.check })) |_| return error.ExpectedCancellation else |err| if (err != error.CompactionCancelled) return err;
    if (context.device_allocations.snapshot().live != live or state.heads[0].encoded_keys.ptr != old_ptr) return error.DeviceAllocationLeak;
    // Append exact token-major rows after packed spans, then export and rollback.
    try runner.append(&state, 0, q, q, 1, rows, .{});
    var appended_host = host;
    appended_host.logical_length = rows + 1;
    appended_host.tokens = tokens;
    var appended = try state.download(a, appended_host, .{}, 16 * 1024 * 1024, .{});
    defer appended.deinit();
    const appended_head = appended.state.donors[0].heads[0];
    if (!std.mem.eql(u8, compressed.keys, appended_head.compressed.keys) or
        !std.mem.eql(f32, queries[0..dim], appended_head.keys[rows * dim ..])) return error.CheckpointChangedOnDevice;
    try state.truncate(rows, .{});
    var rolled_back = try state.download(a, host, .{}, 16 * 1024 * 1024, .{});
    defer rolled_back.deinit();
    const rollback_wire = try checkpoint.encodeWithEncoding(a, rolled_back.state, .{}, .rotated4, .{});
    defer a.free(rollback_wire);
    if (!std.mem.eql(u8, expected_wire, rollback_wire)) return error.CheckpointChangedOnDevice;
    std.debug.print("PASS rotated4 precision={s} dim={d} rows={d} all_compressed={} fragmented={} upload clone masks softcap GQA append grow rollback checkpoint bytes allocation-failures cancellation\n", .{ @tagName(precision), dim, rows, all_compressed, fragmented });
}

test {
    _ = storage;
    _ = checkpoint;
    _ = @import("runtime/kv/attention_match_transaction.zig");
    _ = @import("runtime/kv/attention_match_resume.zig");
}
