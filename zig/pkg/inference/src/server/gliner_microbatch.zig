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

pub fn classificationBudget(limits: memory.Limits, config: span.EncoderConfig, sequence: usize, rows: usize) !memory.RunBudget {
    if (rows == 0 or rows > 8 or sequence == 0 or sequence > 512) return error.InvalidMicrobatchLimits;
    const device = try std.math.mul(usize, try span.deviceScratchUpperBound(config, sequence), rows);
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

/// Split only the successful physical result; do not run or replay a backend.
/// Token usage is the unpadded count captured for this exact row at execution.
pub fn boundaryRow(a: Allocator, value: std.json.Value, index: usize, tokens: usize, limit: usize) ![]u8 {
    if (value != .object) return error.InvalidExtractionOutput;
    const model = value.object.get("model") orelse return error.InvalidExtractionOutput;
    const rows = value.object.get("data") orelse return error.InvalidExtractionOutput;
    if (model != .string or rows != .array or index >= rows.array.items.len) return error.InvalidExtractionOutput;
    const json = try std.json.Stringify.valueAlloc(a, .{
        .object = "extraction",
        .model = model.string,
        .schema_version = @as(u32, 2),
        .data = rows.array.items[index .. index + 1],
        .usage = .{ .prompt_tokens = tokens, .completion_tokens = @as(usize, 0), .total_tokens = tokens },
    }, .{});
    if (json.len > limit) {
        a.free(json);
        return error.ExtractionOutputLimitExceeded;
    }
    return json;
}

test "gliner microbatch classification counts every padded row and never widens owner limits" {
    const config = span.EncoderConfig{ .deberta = .{} };
    const one = try span.deviceScratchUpperBound(config, 128);
    const limits = memory.Limits{ .backend_limit_bytes = one * 2, .scratch_limit_bytes = 32 * 1024 * 1024 + one * 2 };
    const budget = try classificationBudget(limits, config, 128, 2);
    try std.testing.expectEqual(one * 2, budget.backend_scratch_bytes);
    try std.testing.expectEqualDeep(limits, budget.limits);
    try std.testing.expectError(error.MemoryBudgetExceeded, classificationBudget(limits, config, 128, 3));
    _ = try classificationBudget(limits, config, 128, 1);
    try std.testing.expectError(error.InvalidMicrobatchLimits, classificationBudget(limits, config, 513, 1));
    try std.testing.expectError(error.InvalidMicrobatchLimits, classificationBudget(limits, config, 128, 9));
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

test "gliner microbatch boundary response retains source IDs and row-local usage and output limits" {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, a,
        \\{"model":"m","data":[{"id":"one","entities":[]},{"id":"two","entities":[{"text":"Bob"}]}],"usage":{"prompt_tokens":90}}
    , .{});
    defer parsed.deinit();
    const output = try boundaryRow(a, parsed.value, 1, 17, 4096);
    defer a.free(output);
    var row = try std.json.parseFromSlice(std.json.Value, a, output, .{});
    defer row.deinit();
    try std.testing.expectEqual(@as(usize, 1), row.value.object.get("data").?.array.items.len);
    try std.testing.expectEqualStrings("two", row.value.object.get("data").?.array.items[0].object.get("id").?.string);
    try std.testing.expectEqual(@as(i64, 17), row.value.object.get("usage").?.object.get("prompt_tokens").?.integer);
    try std.testing.expectError(error.ExtractionOutputLimitExceeded, boundaryRow(a, parsed.value, 1, 17, 1));
    try std.testing.expectError(error.InvalidExtractionOutput, boundaryRow(a, parsed.value, 2, 17, 4096));
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
