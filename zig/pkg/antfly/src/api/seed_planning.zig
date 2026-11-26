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

//! Validation-only artifact seed planning for public batches
//! (`{"validate_only": true}`, #1791). Every insert or transform that carries
//! artifact seeds is planned by its owning shard through a private routed
//! lookup: the owner runs the write's exact producer preparation and reports
//! one outcome per output without committing. Nothing is written.
//!
//! Response: `{"seeds":[{key,name,outcome,existing_origin?,error?}]}` where
//! `outcome` is `accepted | unchanged | conflict | replaced | deleted |
//! missing | rejected`. `rejected` items carry the validation `error` that a
//! write would fail with.
const std = @import("std");
const batch_api = @import("antfly_local_sources").api_batch;
const db_types = @import("antfly_local_sources").storage_db_types;

/// Whether a public batch body asks for validation-only planning.
pub fn requested(body: []const u8) bool {
    if (std.mem.indexOf(u8, body, "validate_only") == null) return false;
    var parsed = std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, body, .{ .parse_numbers = false }) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    const flag = parsed.value.object.get("validate_only") orelse return false;
    return flag == .bool and flag.bool;
}

fn carriesSeeds(value: []const u8) bool {
    return std.mem.indexOf(u8, value, "_artifacts") != null or std.mem.indexOf(u8, value, "_chunks") != null;
}

fn transformSeeds(transform: anytype) bool {
    for (transform.operations) |operation| {
        const path = if (std.mem.startsWith(u8, operation.path, "$.")) operation.path[2..] else operation.path;
        if (std.mem.startsWith(u8, path, "_artifacts.") or std.mem.startsWith(u8, path, "_chunks.")) return true;
    }
    return false;
}

/// One-document plan request in the `{inserts, transforms}` shape that
/// `storage/db/artifact_seed_plan.zig` accepts.
fn insertPlanAlloc(alloc: std.mem.Allocator, key: []const u8, value: []const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const document = try std.json.parseFromSliceLeaky(std.json.Value, a, value, .{ .parse_numbers = false });
    var inserts = std.json.ObjectMap.empty;
    try inserts.put(a, key, document);
    var root = std.json.ObjectMap.empty;
    try root.put(a, "inserts", .{ .object = inserts });
    return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = root }, .{});
}

fn transformPlanAlloc(alloc: std.mem.Allocator, transform: anytype) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var operations = std.json.Array.init(a);
    for (transform.operations) |operation| {
        var op = std.json.ObjectMap.empty;
        try op.put(a, "op", .{ .string = db_types.transformOpText(operation.op) });
        try op.put(a, "path", .{ .string = operation.path });
        if (operation.value_json) |json| try op.put(a, "value", try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{ .parse_numbers = false }));
        try operations.append(.{ .object = op });
    }
    var item = std.json.ObjectMap.empty;
    try item.put(a, "key", .{ .string = transform.key });
    try item.put(a, "operations", .{ .array = operations });
    try item.put(a, "upsert", .{ .bool = transform.upsert });
    var transforms = std.json.Array.init(a);
    try transforms.append(.{ .object = item });
    var root = std.json.ObjectMap.empty;
    try root.put(a, "transforms", .{ .array = transforms });
    return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = root }, .{});
}

fn appendPlanned(a: std.mem.Allocator, out: *std.json.Array, source: anytype, table_name: []const u8, key: []const u8, plan: []const u8) !void {
    var response = (source.lookup(a, table_name, key, .{ .seed_plan_json = plan, .include_all_fields = false }, .read_index) catch |err| {
        // The write would fail with this error; report it per item.
        var rejected = std.json.ObjectMap.empty;
        try rejected.put(a, "key", .{ .string = key });
        try rejected.put(a, "outcome", .{ .string = "rejected" });
        try rejected.put(a, "error", .{ .string = @errorName(err) });
        try out.append(.{ .object = rejected });
        return;
    }) orelse return;
    defer response.deinit(a);
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, response.json, .{ .allocate = .alloc_always });
    const seeds = (if (parsed == .object) parsed.object.get("seeds") else null) orelse return error.InvalidResponse;
    if (seeds != .array) return error.InvalidResponse;
    try out.appendSlice(seeds.array.items);
}

/// Plans every seeded item of a public batch body. Returns the owned JSON
/// response body.
pub fn planTableBatch(alloc: std.mem.Allocator, source: anytype, table_name: []const u8, body: []const u8) ![]u8 {
    var owned = try batch_api.parseBatchRequest(alloc, body);
    defer owned.deinit(alloc);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var seeds = std.json.Array.init(a);
    for (owned.writes) |write| {
        if (!carriesSeeds(write.value)) continue;
        try appendPlanned(a, &seeds, source, table_name, write.key, try insertPlanAlloc(a, write.key, write.value));
    }
    for (owned.transforms) |transform| {
        if (!transformSeeds(transform)) continue;
        try appendPlanned(a, &seeds, source, table_name, transform.key, try transformPlanAlloc(a, transform));
    }
    var root = std.json.ObjectMap.empty;
    try root.put(a, "seeds", .{ .array = seeds });
    return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = root }, .{});
}

test "seed planning plans each seeded item and reports rejections per item" {
    const alloc = std.testing.allocator;
    const Fake = struct {
        calls: usize = 0,
        const Response = struct {
            json: []u8,
            pub fn deinit(self: *@This(), a: std.mem.Allocator) void {
                a.free(self.json);
            }
        };
        pub fn lookup(self: *@This(), a: std.mem.Allocator, table: []const u8, key: []const u8, opts: db_types.LookupOptions, consistency: anytype) !?Response {
            _ = consistency;
            self.calls += 1;
            try std.testing.expectEqualStrings("docs", table);
            try std.testing.expect(std.mem.indexOf(u8, opts.seed_plan_json, key) != null);
            if (std.mem.eql(u8, key, "bad")) return error.UnknownArtifactSeed;
            return .{ .json = try std.fmt.allocPrint(a, "{{\"seeds\":[{{\"key\":\"{s}\",\"name\":\"v\",\"outcome\":\"accepted\"}}]}}", .{key}) };
        }
    };
    var fake = Fake{};
    const body =
        \\{"validate_only":true,"inserts":{"a":{"t":1,"_artifacts":{"v":{"value":[1]}}},"plain":{"t":2},"bad":{"t":3,"_artifacts":{"x":{"value":"y"}}}},
        \\ "transforms":[{"key":"b","expected_version":"2","operations":[{"op":"$set","path":"_artifacts.v","value":{"value":[2]}}]}]}
    ;
    try std.testing.expect(requested(body));
    try std.testing.expect(!requested("{\"inserts\":{}}"));
    const response = try planTableBatch(alloc, &fake, "docs", body);
    defer alloc.free(response);
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
    try std.testing.expect(std.mem.indexOf(u8, response, "\"key\":\"a\",\"name\":\"v\",\"outcome\":\"accepted\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "\"key\":\"b\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "\"outcome\":\"rejected\",\"error\":\"UnknownArtifactSeed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "plain") == null);
}
