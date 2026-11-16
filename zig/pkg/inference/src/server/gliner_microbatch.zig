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

//! GLiNER classification budgets and results, and boundary request presentation.
//! Both adapters use the shared executor broker; serving profiles select models.
const std = @import("std");
const Allocator = std.mem.Allocator;
const memory = @import("../runtime/tier/memory.zig");
const span = @import("../extractors/gliner_span_v2_executor.zig");
const OutputOwner = @import("output_owner.zig").OutputOwner;
const micro = @import("executor_microbatch.zig");
const wire = @import("../extractors/extraction_v2.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const boundary = @import("../extractors/gliner_boundary_executor.zig");
const metrics_mod = @import("extraction_metrics.zig");
const observation = metrics_mod.observation;

/// Only failures before entering the learned executor permit subdivision or
/// serial fallback. The same errors after dispatch must never replay work.
pub fn boundaryExecutionError(err: anyerror, brokered: bool, started: bool) anyerror {
    if (brokered and !started) switch (err) {
        error.MemoryBudgetExceeded, error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable => return error.BoundaryMicrobatchCapacity,
        else => {},
    };
    return err;
}

pub fn serializeBoundaryRow(a: Allocator, model: []const u8, source: wire.Item, sample: pipeline.Sample, tokens: usize, limit: usize) ![]u8 {
    var writer = wire.ResponseWriter.init(a, limit, 1);
    defer writer.deinit();
    try writer.begin(model);
    try writer.append(source, sample);
    return writer.finish(tokens);
}

test "gliner microbatch capacity fallback is restricted to pre-forward failures" {
    for ([_]anyerror{ error.MemoryBudgetExceeded, error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable }) |err| {
        try std.testing.expectEqual(error.BoundaryMicrobatchCapacity, boundaryExecutionError(err, true, false));
        try std.testing.expectEqual(err, boundaryExecutionError(err, true, true));
        try std.testing.expectEqual(err, boundaryExecutionError(err, false, false));
    }
    for ([_]anyerror{ error.OutOfMemory, error.Canceled, error.DeadlineExceeded, error.ExtractionOutputLimitExceeded }) |err| {
        try std.testing.expectEqual(err, boundaryExecutionError(err, true, false));
    }
}

test "gliner microbatch a fitting singleton survives a refused cohort run budget" {
    const limits = memory.Limits{ .host_limit_bytes = 200, .backend_limit_bytes = 200, .scratch_limit_bytes = 100 };
    var singleton = memory.RunBudget.init(limits);
    try singleton.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = 40, .scratch_tier = .host });
    try singleton.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .backend, .scratch_bytes = 40, .scratch_tier = .backend });
    var cohort = memory.RunBudget.init(limits);
    try cohort.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = 40, .scratch_tier = .host });
    cohort.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .backend, .scratch_bytes = 80, .scratch_tier = .backend }) catch |err| {
        try std.testing.expectEqual(error.BoundaryMicrobatchCapacity, boundaryExecutionError(err, true, false));
        try std.testing.expectEqual(err, boundaryExecutionError(err, true, true));
        try std.testing.expectEqual(@as(usize, 40), singleton.backend_scratch_bytes);
        return;
    };
    return error.ExpectedCohortCapacityRefusal;
}

test "gliner microbatch bounded row serialization releases every failed allocation" {
    const Probe = struct {
        fn run(a: Allocator) !void {
            var request = try wire.parseJson(a,
                \\{"schema_version":2,"model":"m","schema":{"entities":["person"]},"inputs":[{"id":"small","content":"Ada"}]}
            , .{});
            defer request.deinit();
            const output = try serializeBoundaryRow(a, request.model, request.items[0], .{}, 17, 512);
            defer a.free(output);
            var parsed = try std.json.parseFromSlice(std.json.Value, a, output, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(i64, 17), parsed.value.object.get("usage").?.object.get("prompt_tokens").?.integer);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

pub const BoundaryConsumer = struct {
    model: []const u8,
    slot: *micro.ResultSlot,
    control: micro.ItemControl = .{},
    observer: ?observation.Observer = null,
    failure: ?*wire.FailureContext = null,
    max_output_values: usize,
    max_response_bytes: usize,
};

pub const BoundaryCohort = struct {
    consumers: []const BoundaryConsumer,
    group_key: usize,
    evidence: []const boundary.ItemExecution,
    failure: *wire.FailureContext,
    owner: *OutputOwner,
    output_limit: usize,
    output_bytes: usize = 0,

    pub fn observe(raw: ?*anyopaque, event: observation.Event) void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        // Decoded samples are routed individually by publish, with solver
        // witnesses intact. Physical phases cover all waiting consumers.
        if (event != .phase) return;
        for (self.consumers) |consumer| {
            if (consumer.slot.completed) continue;
            if (consumer.failure) |failure| failure.* = .{ .input_index = if (self.failure.input_index != null) 0 else null, .stage = self.failure.stage };
            if (consumer.observer) |observer| observer.emit(event);
        }
    }

    pub fn checkRow(raw: ?*anyopaque, index: usize) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try self.consumers[index].control.check();
    }

    pub fn fail(raw: *anyopaque, index: usize, err: anyerror) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const consumer = self.consumers[index];
        if (consumer.failure) |failure| failure.* = .{ .input_index = 0, .stage = "decoding" };
        if (consumer.observer) |observer| observer.emit(.{ .phase = .execution });
        consumer.slot.fail(err);
    }

    pub fn publish(raw: *anyopaque, index: usize, source: wire.Item, sample: @import("../pipelines/gliner_boundary_pipeline.zig").Sample, tokens: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const consumer = self.consumers[index];
        const observed = self.evidence[index];
        boundary.observeSample(consumer.observer, sample, tokens, observed.output_values);
        consumer.control.check() catch |err| {
            consumer.slot.fail(err);
            return;
        };
        if (observed.output_values > consumer.max_output_values) {
            consumer.slot.fail(error.ExtractionOutputLimitExceeded);
            return;
        }
        const shared = std.heap.smp_allocator;
        const output = serializeBoundaryRow(shared, consumer.model, source, sample, tokens, consumer.max_response_bytes) catch |err| {
            consumer.slot.fail(err);
            return;
        };
        if (output.len > self.output_limit -| self.output_bytes) {
            shared.free(output);
            consumer.slot.fail(error.MemoryBudgetExceeded);
            return;
        }
        self.output_bytes += output.len;
        _ = self.owner.remaining.fetchAdd(1, .monotonic);
        consumer.slot.physical_group_key = self.group_key + observed.group;
        consumer.slot.setValue(BoundaryReply, .{ .bytes = output, .owner = self.owner }, if (observed.rows > 1) .native_batch else .serial);
    }
};

pub fn classificationBudget(limits: memory.Limits, config: span.EncoderConfig, sequence: usize, rows: usize, workspace_bytes: usize) !memory.RunBudget {
    if (rows == 0 or rows > 8 or sequence == 0 or sequence > 512) return error.InvalidMicrobatchLimits;
    const activations = try std.math.mul(usize, try span.deviceScratchUpperBound(config, sequence), rows);
    // All projections share one workspace; physical rows multiply only activations.
    const device = try std.math.add(usize, activations, workspace_bytes);
    var budget = memory.RunBudget.init(limits);
    try budget.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = 32 * 1024 * 1024, .scratch_tier = .host });
    try budget.reserveEstimate(.{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .backend, .scratch_bytes = device, .scratch_tier = .backend });
    return budget;
}

pub const ClassificationRows = struct { rows: [][]f64, owner: ?*OutputOwner = null };

pub fn cloneClassificationRows(a: std.mem.Allocator, value: ClassificationRows) !ClassificationRows {
    const copy = try a.alloc([]f64, value.rows.len);
    var done: usize = 0;
    errdefer {
        for (copy[0..done]) |row| a.free(row);
        a.free(copy);
    }
    for (value.rows, copy) |row, *dest| {
        dest.* = try a.dupe(f64, row);
        done += 1;
    }
    return .{ .rows = copy };
}

pub fn destroyClassificationRows(a: std.mem.Allocator, value: ClassificationRows) void {
    for (value.rows) |row| a.free(row);
    a.free(value.rows);
    if (value.owner) |owner| owner.release();
}

pub const BoundaryReply = struct { bytes: []u8, owner: *OutputOwner };

pub fn destroyBoundaryReply(a: Allocator, reply: BoundaryReply) void {
    a.free(reply.bytes);
    reply.owner.release();
}

/// A capacity refusal precedes model execution. Rejoin the caller before it
/// uses its already-admitted heap for serial execution. Other failures must
/// propagate: retrying them could replay a failed backend operation.
pub fn boundaryResult(a: Allocator, result: micro.ItemResult(BoundaryReply)) !?[]u8 {
    return switch (result.result) {
        .value => |reply| try a.dupe(u8, reply.bytes),
        .item_error => |failure| if (failure.cause == error.BoundaryMicrobatchCapacity) null else failure.cause,
    };
}

/// Keep schemas and options identical, including per-input overrides. Text and
/// source IDs belong to rows, so neither can prevent independent HTTP batching.
/// A different JSON field order can conservatively create a separate cohort.
/// Legacy upgrades and v2 callers can resolve different presentation policies
/// from the same wire schema, so include that resolved policy as well.
pub fn boundaryKey(a: Allocator, json: []const u8, classification_multi_label_fallback: bool) !?[]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const root = parsed.value.object;
    const inputs = root.get("inputs") orelse return null;
    if (inputs != .array or inputs.array.items.len != 1 or inputs.array.items[0] != .object) return null;
    const input = inputs.array.items[0].object;
    return try std.json.Stringify.valueAlloc(a, .{
        .schema = root.get("schema"),
        .options = root.get("options"),
        .item_schema = input.get("schema"),
        .item_options = input.get("options"),
        .classification_multi_label_fallback = classification_multi_label_fallback,
    }, .{});
}

test "gliner microbatch classification counts every padded row and never widens owner limits" {
    const config = span.EncoderConfig{ .deberta = .{} };
    const one = try span.deviceScratchUpperBound(config, 128);
    const limits = memory.Limits{ .backend_limit_bytes = one * 2, .scratch_limit_bytes = 32 * 1024 * 1024 + one * 2 };
    const budget = try classificationBudget(limits, config, 128, 2, 0);
    try std.testing.expectEqual(one * 2, budget.backend_scratch_bytes);
    try std.testing.expectEqualDeep(limits, budget.limits);
    const workspace = 32 * 1024 * 1024;
    const with_workspace = try classificationBudget(.{}, config, 128, 2, workspace);
    try std.testing.expectEqual(one * 2 + workspace, with_workspace.backend_scratch_bytes);
    try std.testing.expectError(error.MemoryBudgetExceeded, classificationBudget(limits, config, 128, 2, workspace));
    try std.testing.expectError(error.MemoryBudgetExceeded, classificationBudget(limits, config, 128, 3, 0));
    _ = try classificationBudget(limits, config, 128, 1, 0);
    try std.testing.expectError(error.InvalidMicrobatchLimits, classificationBudget(limits, config, 513, 1, 0));
    try std.testing.expectError(error.InvalidMicrobatchLimits, classificationBudget(limits, config, 128, 9, 0));
}

test "gliner microbatch classification output admission outlives independent consumers and clone failures" {
    const a = std.heap.smp_allocator;
    var controller = memory.AdmissionController{};
    const owner = try a.create(OutputOwner);
    owner.* = .{ .lease = try controller.tryAcquire(.gpu, .{}, .{ .host_scratch_bytes = 128 }, false), .remaining = .init(2) };
    const first_rows = try a.alloc([]f64, 1);
    first_rows[0] = try a.dupe(f64, &.{ 1, 2 });
    const second_rows = try a.alloc([]f64, 1);
    second_rows[0] = try a.dupe(f64, &.{ 3, 4, 5 });
    const first = ClassificationRows{ .rows = first_rows, .owner = owner };
    const second = ClassificationRows{ .rows = second_rows, .owner = owner };
    const Probe = struct {
        fn run(alloc: std.mem.Allocator, value: ClassificationRows) !void {
            const copy = try cloneClassificationRows(alloc, value);
            defer destroyClassificationRows(alloc, copy);
            try std.testing.expect(copy.owner == null);
            try std.testing.expectEqualSlices(f64, value.rows[0], copy.rows[0]);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{first});
    destroyClassificationRows(a, first);
    try std.testing.expectEqual(@as(usize, 128), controller.snapshot().host_scratch_bytes);
    destroyClassificationRows(a, second);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}

test "gliner microbatch boundary keys isolate schemas and options while retaining independent row identities" {
    const a = std.testing.allocator;
    const one = (try boundaryKey(a,
        \\{"schema":{"entities":["person"]},"inputs":[{"id":"a","content":"Ada"}]}
    , true)).?;
    defer a.free(one);
    const two = (try boundaryKey(a,
        \\{"schema":{"entities":["person"]},"inputs":[{"id":"b","content":"Bob"}]}
    , true)).?;
    defer a.free(two);
    try std.testing.expectEqualStrings(one, two);
    const other = (try boundaryKey(a,
        \\{"schema":{"entities":["person"]},"inputs":[{"content":"Bob","options":{"threshold":0.9}}]}
    , true)).?;
    defer a.free(other);
    try std.testing.expect(!std.mem.eql(u8, one, other));
    try std.testing.expectEqual(@as(?[]u8, null), try boundaryKey(a, "{\"inputs\":[{},{}]}", true));
}

test "gliner microbatch boundary keys separate legacy and v2 classification fallback" {
    const a = std.testing.allocator;
    const json =
        \\{"schema":{"classifications":[{"name":"intent","labels":["sales","support"],"multi_label":true,"threshold":0.99}]},"inputs":[{"content":"Help"}]}
    ;
    const legacy = (try boundaryKey(a, json, false)).?;
    defer a.free(legacy);
    const v2 = (try boundaryKey(a, json, true)).?;
    defer a.free(v2);
    try std.testing.expect(!std.mem.eql(u8, legacy, v2));
}

test "gliner microbatch boundary results fall back only for pre-forward capacity" {
    const a = std.testing.allocator;
    const Result = micro.ItemResult(BoundaryReply);
    try std.testing.expectEqual(@as(?[]u8, null), try boundaryResult(a, Result{
        .identity = .{},
        .execution = .serial,
        .result = .{ .item_error = .{ .cause = error.BoundaryMicrobatchCapacity } },
    }));
    for ([_]anyerror{ error.OutOfMemory, error.MemoryBudgetExceeded, error.InvalidInputShape, error.Canceled, error.DeadlineExceeded }) |err| {
        try std.testing.expectError(err, boundaryResult(a, Result{
            .identity = .{},
            .execution = .serial,
            .result = .{ .item_error = .{ .cause = err } },
        }));
    }
    const owner = try std.heap.smp_allocator.create(OutputOwner);
    var controller = memory.AdmissionController{};
    owner.* = .{ .lease = try controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 128 }, false), .remaining = .init(1) };
    const reply = BoundaryReply{ .bytes = try a.dupe(u8, "{\"data\":[]}"), .owner = owner };
    defer destroyBoundaryReply(a, reply);
    const copy = (try boundaryResult(a, .{ .identity = .{}, .execution = .native_batch, .result = .{ .value = reply } })).?;
    defer a.free(copy);
    try std.testing.expectEqualStrings(reply.bytes, copy);
    try std.testing.expect(reply.bytes.ptr != copy.ptr);
    try std.testing.expectEqual(@as(usize, 128), controller.snapshot().host_scratch_bytes);
}

test "gliner microbatch boundary output admission outlives execution and every independent consumer" {
    var controller = memory.AdmissionController{};
    const owner = try std.heap.smp_allocator.create(OutputOwner);
    owner.* = .{ .lease = try controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 128 }, false), .remaining = .init(3) };
    const one = BoundaryReply{ .bytes = try std.testing.allocator.dupe(u8, "first"), .owner = owner };
    const two = BoundaryReply{ .bytes = try std.testing.allocator.dupe(u8, "second"), .owner = owner };
    owner.release(); // physical invocation retires before its consumers
    try std.testing.expectEqual(@as(usize, 128), controller.snapshot().host_scratch_bytes);
    destroyBoundaryReply(std.testing.allocator, two);
    try std.testing.expectEqual(@as(usize, 128), controller.snapshot().host_scratch_bytes);
    destroyBoundaryReply(std.testing.allocator, one);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}

test "gliner microbatch isolates row serialization and preserves caller observations" {
    const a = std.testing.allocator;
    var request = try wire.parseJson(a,
        \\{"schema_version":2,"model":"m","schema":{"entities":["person"]},"inputs":[{"id":"large","content":"Ada"},{"id":"small","content":"Bob"}]}
    , .{});
    defer request.deinit();
    const oversized_id: [2048]u8 = @splat('x');
    request.items[0].id = &oversized_id;
    var metrics = [_]metrics_mod.Metrics{ .{}, .{} };
    var traces = [_]metrics_mod.Trace{ metrics[0].begin(.direct), metrics[1].begin(.direct) };
    var failures = [_]wire.FailureContext{ .{}, .{} };
    var replies: [2]BoundaryReply = undefined;
    var slots: [2]micro.ResultSlot = undefined;
    var consumers: [2]BoundaryConsumer = undefined;
    for (&slots, &consumers, 0..) |*slot, *consumer, i| {
        slot.* = .{ .output = &replies[i] };
        consumer.* = .{ .model = request.model, .slot = slot, .max_response_bytes = 512, .max_output_values = 65536, .observer = traces[i].observer(), .failure = &failures[i] };
    }
    var controller = memory.AdmissionController{};
    const owner = try std.heap.smp_allocator.create(OutputOwner);
    owner.* = .{ .lease = try controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 1024 }, false), .remaining = .init(1) };
    defer owner.release();
    const evidence = [_]boundary.ItemExecution{
        .{ .prompt_tokens = 11, .output_values = 0, .rows = 2 },
        .{ .prompt_tokens = 17, .output_values = 0, .rows = 2 },
    };
    var failure = wire.FailureContext{ .input_index = 1, .stage = "encoder" };
    var cohort = BoundaryCohort{ .consumers = &consumers, .group_key = 1, .evidence = &evidence, .failure = &failure, .owner = owner, .output_limit = 1024 };
    BoundaryCohort.observe(&cohort, .{ .phase = .execution });
    const sample = @import("../pipelines/gliner_boundary_pipeline.zig").Sample{
        .classification_solver = .{ .status = .feasible, .visited_nodes = 7, .utility = 0, .exhausted = true },
    };
    BoundaryCohort.publish(&cohort, 0, request.items[0], sample, 11);
    BoundaryCohort.publish(&cohort, 1, request.items[1], sample, 17);
    try std.testing.expectEqual(error.ExtractionOutputLimitExceeded, slots[0].err.?);
    try std.testing.expect(slots[1].completed and slots[1].err == null);
    defer destroyBoundaryReply(std.heap.smp_allocator, replies[1]);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, replies[1].bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("small", parsed.value.object.get("data").?.array.items[0].object.get("id").?.string);
    try std.testing.expectEqual(@as(i64, 17), parsed.value.object.get("usage").?.object.get("prompt_tokens").?.integer);
    failure = .{ .stage = "serializing" };
    BoundaryCohort.observe(&cohort, .{ .phase = .serializing });
    for (failures) |value| {
        try std.testing.expectEqualStrings("encoder", value.stage);
        try std.testing.expectEqual(@as(?usize, 0), value.input_index);
    }
    traces[0].finish(error.ExtractionOutputLimitExceeded);
    traces[1].finish(null);
    try std.testing.expectEqual(@as(u64, 1), metrics[0].decoded_items.impl.count);
    try std.testing.expectEqual(@as(u64, 17), metrics[1].decoded_prompt_tokens.impl.count);
    try std.testing.expectEqual(@as(u64, 1), metrics[1].returned_items.impl.count);
    try std.testing.expectEqual(@as(u64, 7), metrics[1].solver_nodes.get(.classification));
    try std.testing.expectEqual(@as(u64, 1), metrics[1].solver_exhausted.get(.classification));
}
