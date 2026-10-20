// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
        if (context.device_allocations.snapshot().live != 0) return error.DeviceAllocationLeak;
    }
    std.debug.print("PASS native ragged dispatch: both precisions, chunking, masks, cancellation, admission, alias rejection, cleanup; serving_qualified=false\n", .{});
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
        var denied = try decode.Runner.init(context, initial_live + 128);
        defer denied.deinit();
        if (denied.attend(request, .{})) |_| return error.ExpectedAdmissionFailure else |err| if (err != error.CudaMemoryLimitExceeded) return err;
        if (context.device_allocations.snapshot().live != initial_live + 128) return error.UnexpectedAllocation;
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
    state.logical_length = state.initial_logical_length + 8192;
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
