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

//! Caller-local presentation for independently owned boundary requests.
//! Neither this adapter nor a batch key grants backend qualification.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const OutputOwner = @import("read_pool_microbatch.zig").OutputOwner;
pub const Reply = struct { bytes: []u8, owner: *OutputOwner };

pub fn destroyReply(a: Allocator, reply: Reply) void {
    a.free(reply.bytes);
    reply.owner.release();
}

/// Keep schemas and options identical, including per-input overrides. Text and
/// source IDs belong to rows, so neither can prevent independent HTTP batching.
/// A different JSON field order can conservatively create a separate cohort.
pub fn boundaryKey(a: Allocator, json: []const u8) !?[]u8 {
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

test "write pool microbatch keys isolate schemas and options while retaining independent row identities" {
    const a = std.testing.allocator;
    const one = (try boundaryKey(a,
        \\{"schema":{"entities":["person"]},"inputs":[{"id":"a","content":"Ada"}]}
    )).?;
    defer a.free(one);
    const two = (try boundaryKey(a,
        \\{"schema":{"entities":["person"]},"inputs":[{"id":"b","content":"Bob"}]}
    )).?;
    defer a.free(two);
    try std.testing.expectEqualStrings(one, two);
    const other = (try boundaryKey(a,
        \\{"schema":{"entities":["person"]},"inputs":[{"content":"Bob","options":{"threshold":0.9}}]}
    )).?;
    defer a.free(other);
    try std.testing.expect(!std.mem.eql(u8, one, other));
    try std.testing.expectEqual(@as(?[]u8, null), try boundaryKey(a, "{\"inputs\":[{},{}]}"));
}

test "write pool microbatch response retains source IDs and row-local usage and output limits" {
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

test "write pool microbatch output admission outlives execution and every independent consumer" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    const owner = try std.heap.smp_allocator.create(OutputOwner);
    owner.* = .{ .lease = try controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 128 }, false), .remaining = .init(3) };
    const one = Reply{ .bytes = try std.testing.allocator.dupe(u8, "first"), .owner = owner };
    const two = Reply{ .bytes = try std.testing.allocator.dupe(u8, "second"), .owner = owner };
    owner.release(); // physical invocation retires before its consumers
    try std.testing.expectEqual(@as(usize, 128), controller.snapshot().host_scratch_bytes);
    destroyReply(std.testing.allocator, two);
    try std.testing.expectEqual(@as(usize, 128), controller.snapshot().host_scratch_bytes);
    destroyReply(std.testing.allocator, one);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}
