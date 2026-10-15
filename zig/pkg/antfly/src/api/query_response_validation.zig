// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const ant_json = @import("antfly-json");
const metadata = @import("antfly_metadata_openapi");

pub fn validate(alloc: std.mem.Allocator, body: []const u8) !void {
    try ant_json.validateTyped(metadata.QueryResponses, @import("antfly_platform").allocator.reclaimingScratch(alloc), body);
}

test "public graph metric borrowed wire response validator preserves generated schema rejection" {
    const a = std.testing.allocator;
    const cases = [_][]const u8{
        "{}",
        "{\"responses\":[{\"took\":0,\"status\":200}]}",
        "{\"responses\":[{\"took\":\"0\",\"status\":200,\"hits\":{\"hits\":[{\"_id\":\"x\",\"_score\":1,\"_source\":{\"a\":[1,2,{\"nested\":true}],\"b\":\"escaped \\u263a\"}}]}}]}",
        "{\"responses\":[{\"took\":0,\"status\":200,\"hits\":{\"hits\":[{\"_id\":[120],\"_score\":1}]}}]}",
        "{\"responses\":null}",
        "{\"responses\":[{\"status\":200}]}",
        "{\"responses\":[{\"took\":0,\"status\":2147483648}]}",
        "{\"responses\":[{\"took\":0,\"status\":200,\"hits\":null}]}",
        "{\"responses\":[{\"took\":0,\"took\":1,\"status\":200}]}",
        "{\"responses\":[{\"took\":0,\"status\":200,\"unknown\":{}}]}",
        "{\"responses\":[{\"took\":0,\"status\":200,\"hits\":{\"hits\":[{\"_id\":\"x\",\"_score\":1,\"_source\":{\"a\":0,\"\\u0061\":1}}]}}]}",
        "{\"responses\":[{\"took\":0,\"status\":200,\"hits\":{\"hits\":[{\"_id\":\"x\"}]}}]}",
        "{\"responses\":[{\"took\":0,\"status\":200,\"hits\":{\"hits\":[{\"_id\":\"x\",\"_score\":1,\"_source\":[]}]}}]}",
        "{\"responses\":[{\"took\":0,\"status\":200}]} trailing",
    };
    for (cases) |body| {
        var expected = std.json.parseFromSlice(metadata.QueryResponses, a, body, .{ .allocate = .alloc_always });
        if (expected) |*parsed| {
            defer parsed.deinit();
            try validate(a, body);
        } else |err| try std.testing.expectError(err, validate(a, body));
    }
    const Fail = struct {
        fn run(alloc: std.mem.Allocator) !void {
            try validate(alloc, cases[2]);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fail.run, .{});
}

test "public graph metric borrowed wire response schema validation bounds numeric array scratch" {
    const a = std.testing.allocator;
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"responses\":[{\"took\":0,\"status\":200,\"hits\":{\"hits\":[{\"_id\":\"doc\",\"_score\":1,\"_source\":{\"_embeddings\":{\"dense\":[");
    for (0..65536) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("0.123456789");
    }
    try input.writer.writeAll("]}}}]}}]}");
    var memory: [32 * 1024]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&memory);
    try validate(bounded.allocator(), input.written());
    try std.testing.expect(bounded.end_index < memory.len);
    var arena_memory: [32 * 1024]u8 = undefined;
    var arena_backing = std.heap.FixedBufferAllocator.init(&arena_memory);
    var response_arena = std.heap.ArenaAllocator.init(arena_backing.allocator());
    defer response_arena.deinit();
    try validate(response_arena.allocator(), input.written());
    var baseline = std.heap.FixedBufferAllocator.init(&memory);
    try std.testing.expectError(error.OutOfMemory, std.json.parseFromSlice(metadata.QueryResponses, baseline.allocator(), input.written(), .{}));
}

test "public graph metric borrowed wire response schema validation matches mutated wire acceptance" {
    const a = std.testing.allocator;
    const body = "{\"responses\":[{\"took\":0,\"status\":200,\"hits\":{\"hits\":[{\"_id\":\"x\",\"_score\":1,\"_source\":{\"a\":[1,2,{\"b\":\"c\"}]}}]}}]}";
    var changed: [body.len]u8 = undefined;
    for (0..body.len) |i| for ("x0{}[]:,\"\\") |byte| {
        @memcpy(&changed, body);
        changed[i] = byte;
        var parsed = std.json.parseFromSlice(metadata.QueryResponses, a, &changed, .{});
        const expected = if (parsed) |*value| accepted: {
            value.deinit();
            break :accepted true;
        } else |_| false;
        const actual = if (validate(a, &changed)) |_| true else |err| rejected: {
            if (err == error.OutOfMemory) return err;
            break :rejected false;
        };
        try std.testing.expectEqual(expected, actual);
    };
}
