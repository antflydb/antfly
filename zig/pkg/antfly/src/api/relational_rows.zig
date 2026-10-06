// Copyright 2026 Antfly, Inc.
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

//! Generated relational wire boundary. Mutations enter the same durable table
//! coordinator as public batches, with exact version and schema preconditions.
const std = @import("std");
const wire = @import("antfly_metadata_openapi").types;
const batch = @import("batch.zig");
const types = @import("../storage/db/types.zig");

pub fn parseMutation(alloc: std.mem.Allocator, body: []const u8) !batch.OwnedBatchRequest {
    var parsed = std.json.parseFromSlice(wire.RelationalRowMutationRequest, alloc, body, .{ .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidBatchRequest,
    };
    defer parsed.deinit();
    return prepareMutation(alloc, parsed.value);
}

/// Shared typed boundary for HTTP and native SQL. Returned batch owns every
/// key, predicate, and encoded row; callers may release the input immediately.
pub fn prepareMutation(alloc: std.mem.Allocator, req: wire.RelationalRowMutationRequest) !batch.OwnedBatchRequest {
    const schema_version = std.math.cast(u32, req.schema_version) orelse return error.InvalidBatchRequest;
    // Zero is the initial active schema epoch, not an absent precondition.
    if (req.mutations.len == 0 or req.mutations.len > 4096) return error.InvalidBatchRequest;
    var result: batch.OwnedBatchRequest = .{};
    errdefer result.deinit(alloc);
    var writes: std.ArrayList(types.BatchWrite) = .empty;
    defer writes.deinit(alloc);
    errdefer for (writes.items) |write| {
        alloc.free(write.key);
        alloc.free(write.value);
        for (write.json_null_fields) |name| alloc.free(name);
        if (write.json_null_fields.len != 0) alloc.free(write.json_null_fields);
    };
    var deletes: std.ArrayList([]const u8) = .empty;
    defer deletes.deinit(alloc);
    errdefer for (deletes.items) |key| alloc.free(key);
    var predicates: std.ArrayList(types.TransactionVersionPredicate) = .empty;
    defer predicates.deinit(alloc);
    errdefer for (predicates.items) |predicate| alloc.free(predicate.key);
    var write_count: usize = 0;
    for (req.mutations) |mutation| if (mutation.row != null) {
        write_count += 1;
    };
    try writes.ensureTotalCapacity(alloc, write_count);
    try deletes.ensureTotalCapacity(alloc, req.mutations.len - write_count);
    try predicates.ensureTotalCapacity(alloc, req.mutations.len);
    var keys: std.StringHashMapUnmanaged(void) = .empty;
    defer keys.deinit(alloc);
    try keys.ensureTotalCapacity(alloc, @intCast(req.mutations.len));
    for (req.mutations) |mutation| {
        if (mutation.key.len == 0 or !std.unicode.utf8ValidateSlice(mutation.key) or mutation.expected_version.len == 0) return error.InvalidBatchRequest;
        for (mutation.expected_version) |byte| if (byte < '0' or byte > '9') return error.InvalidBatchRequest;
        const version = std.fmt.parseInt(u64, mutation.expected_version, 10) catch return error.InvalidBatchRequest;
        if (keys.getOrPutAssumeCapacity(mutation.key).found_existing) return error.InvalidBatchRequest;
        predicates.appendAssumeCapacity(.{ .key = try alloc.dupe(u8, mutation.key), .expected_version = version });
        const key = try alloc.dupe(u8, mutation.key);
        errdefer alloc.free(key);
        const fields: []const []const u8 = mutation.json_null_fields orelse &.{};
        if (mutation.row) |row| {
            const value = try std.json.Stringify.valueAlloc(alloc, row, .{});
            errdefer alloc.free(value);
            var seen: std.StringHashMapUnmanaged(void) = .empty;
            defer seen.deinit(alloc);
            for (fields) |name| {
                const datum = row.map.get(name) orelse return error.InvalidBatchRequest;
                if (datum != .null or (try seen.getOrPut(alloc, name)).found_existing) return error.InvalidBatchRequest;
            }
            writes.appendAssumeCapacity(.{ .key = key, .value = value, .json_null_fields = try types.cloneJsonNullFields(alloc, fields) });
        } else {
            if (fields.len != 0) return error.InvalidBatchRequest;
            deletes.appendAssumeCapacity(key);
        }
    }
    result.writes = try writes.toOwnedSlice(alloc);
    result.deletes = try deletes.toOwnedSlice(alloc);
    result.predicates = try predicates.toOwnedSlice(alloc);
    result.req = .{
        .writes = result.writes,
        .deletes = result.deletes,
        .predicates = result.predicates,
        .relational_schema_version = schema_version,
        .sync_level = if (req.sync_level) |level| switch (level) {
            .propose => .propose,
            .write => .write,
            .full_text => .full_text,
            .enrichments => .enrichments,
            .full_index => .full_index,
        } else .write,
    };
    return result;
}

test "relational mutation boundary preserves exact row and version integers" {
    const alloc = std.testing.allocator;
    var parsed = try parseMutation(alloc,
        \\{"schema_version":4,"mutations":[{"key":"a","expected_version":"18446744073709551615","row":{"id":9007199254740993}},{"key":"b","expected_version":"0"}]}
    );
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(u64, std.math.maxInt(u64)), parsed.req.predicates[0].expected_version);
    try std.testing.expectEqual(@as(?u32, 4), parsed.req.relational_schema_version);
    try std.testing.expectEqualStrings("{\"id\":9007199254740993}", parsed.req.writes[0].value);
    try std.testing.expectEqualStrings("b", parsed.req.deletes[0]);
    try std.testing.expectError(error.InvalidBatchRequest, parseMutation(alloc,
        \\{"schema_version":4,"mutations":[{"key":"a","expected_version":"0"},{"key":"a","expected_version":"0"}]}
    ));
}

test "relational mutation boundary preserves required initial zero schema epoch" {
    const alloc = std.testing.allocator;
    var parsed = try parseMutation(alloc,
        \\{"schema_version":0,"mutations":[{"key":"a","expected_version":"0","row":{"id":1}}]}
    );
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(?u32, 0), parsed.req.relational_schema_version);
    for ([_][]const u8{
        "{\"mutations\":[{\"key\":\"a\",\"expected_version\":\"0\"}]}",
        "{\"schema_version\":null,\"mutations\":[{\"key\":\"a\",\"expected_version\":\"0\"}]}",
        "{\"schema_version\":-1,\"mutations\":[{\"key\":\"a\",\"expected_version\":\"0\"}]}",
        "{\"schema_version\":4294967296,\"mutations\":[{\"key\":\"a\",\"expected_version\":\"0\"}]}",
    }) |invalid| try std.testing.expectError(error.InvalidBatchRequest, parseMutation(alloc, invalid));
}

test "SQL typed mutation boundary owns input and matches JSON preparation" {
    const alloc = std.testing.allocator;
    const text =
        \\{"schema_version":0,"sync_level":"full_index","mutations":[{"key":"a","expected_version":"18446744073709551615","row":{"n":9007199254740993}},{"key":"b","expected_version":"0"}]}
    ;
    var parsed = try std.json.parseFromSlice(wire.RelationalRowMutationRequest, alloc, text, .{ .parse_numbers = false, .allocate = .alloc_always });
    var typed = prepareMutation(alloc, parsed.value) catch |err| {
        parsed.deinit();
        return err;
    };
    parsed.deinit();
    defer typed.deinit(alloc);
    var json = try parseMutation(alloc, text);
    defer json.deinit(alloc);
    try std.testing.expectEqualStrings(json.req.writes[0].key, typed.req.writes[0].key);
    try std.testing.expectEqualStrings(json.req.writes[0].value, typed.req.writes[0].value);
    try std.testing.expectEqualStrings(json.req.deletes[0], typed.req.deletes[0]);
    try std.testing.expectEqual(json.req.predicates[0].expected_version, typed.req.predicates[0].expected_version);
    try std.testing.expectEqual(json.req.sync_level, typed.req.sync_level);
    try std.testing.expectEqual(json.req.relational_schema_version, typed.req.relational_schema_version);
}

test "SQL typed mutation boundary releases partial allocations at every fault" {
    var parsed = try std.json.parseFromSlice(wire.RelationalRowMutationRequest, std.testing.allocator,
        \\{"schema_version":3,"mutations":[{"key":"first","expected_version":"0","row":{"name":"owned","j":null},"json_null_fields":["j"]},{"key":"second","expected_version":"42"},{"key":"third","expected_version":"1","row":{"n":123}}]}
    , .{ .parse_numbers = false });
    defer parsed.deinit();
    const Fault = struct {
        fn run(alloc: std.mem.Allocator, request: wire.RelationalRowMutationRequest) !void {
            var prepared = try prepareMutation(alloc, request);
            defer prepared.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 2), prepared.req.writes.len);
            try std.testing.expectEqual(@as(usize, 1), prepared.req.deletes.len);
            try std.testing.expectEqual(@as(usize, 3), prepared.req.predicates.len);
            try std.testing.expectEqualStrings("j", prepared.req.writes[0].json_null_fields[0]);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fault.run, .{parsed.value});
}

test "SQL typed mutation boundary rejects invalid JSON null provenance" {
    for ([_][]const u8{
        \\{"schema_version":1,"mutations":[{"key":"a","expected_version":"0","json_null_fields":["j"]}]}
        ,
        \\{"schema_version":1,"mutations":[{"key":"a","expected_version":"0","row":{"j":null},"json_null_fields":["j","j"]}]}
        ,
        \\{"schema_version":1,"mutations":[{"key":"a","expected_version":"0","row":{"j":1},"json_null_fields":["j"]}]}
        ,
        \\{"schema_version":1,"mutations":[{"key":"a","expected_version":"0","row":{},"json_null_fields":["j"]}]}
        ,
    }) |body| {
        try std.testing.expectError(error.InvalidBatchRequest, parseMutation(std.testing.allocator, body));
    }
}

test "SQL typed mutation boundary detects duplicate keys at full admission capacity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const mutations = try alloc.alloc(wire.RelationalRowMutation, 4096);
    for (mutations, 0..) |*mutation, i| mutation.* = .{ .key = try std.fmt.allocPrint(alloc, "shared-prefix-{d}", .{i}), .expected_version = "0" };
    var prepared = try prepareMutation(std.testing.allocator, .{ .schema_version = 1, .mutations = mutations });
    defer prepared.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 4096), prepared.req.predicates.len);
    mutations[4095].key = mutations[0].key;
    try std.testing.expectError(error.InvalidBatchRequest, prepareMutation(std.testing.allocator, .{ .schema_version = 1, .mutations = mutations }));
}
