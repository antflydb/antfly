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
    const req = parsed.value;
    const schema_version = std.math.cast(u32, req.schema_version) orelse return error.InvalidBatchRequest;
    if (req.mutations.len == 0 or req.mutations.len > 4096 or schema_version == 0) return error.InvalidBatchRequest;
    var result: batch.OwnedBatchRequest = .{};
    errdefer result.deinit(alloc);
    var writes: std.ArrayList(types.BatchWrite) = .empty;
    defer writes.deinit(alloc);
    errdefer for (writes.items) |write| {
        alloc.free(write.key);
        alloc.free(write.value);
    };
    var deletes: std.ArrayList([]const u8) = .empty;
    defer deletes.deinit(alloc);
    errdefer for (deletes.items) |key| alloc.free(key);
    var predicates: std.ArrayList(types.TransactionVersionPredicate) = .empty;
    defer predicates.deinit(alloc);
    errdefer for (predicates.items) |predicate| alloc.free(predicate.key);
    try writes.ensureTotalCapacity(alloc, req.mutations.len);
    try deletes.ensureTotalCapacity(alloc, req.mutations.len);
    try predicates.ensureTotalCapacity(alloc, req.mutations.len);
    for (req.mutations, 0..) |mutation, i| {
        if (mutation.key.len == 0 or !std.unicode.utf8ValidateSlice(mutation.key) or mutation.expected_version.len == 0) return error.InvalidBatchRequest;
        for (mutation.expected_version) |byte| if (byte < '0' or byte > '9') return error.InvalidBatchRequest;
        const version = std.fmt.parseInt(u64, mutation.expected_version, 10) catch return error.InvalidBatchRequest;
        for (req.mutations[0..i]) |prior| if (std.mem.eql(u8, prior.key, mutation.key)) return error.InvalidBatchRequest;
        predicates.appendAssumeCapacity(.{ .key = try alloc.dupe(u8, mutation.key), .expected_version = version });
        const key = try alloc.dupe(u8, mutation.key);
        errdefer alloc.free(key);
        if (mutation.row) |row| {
            const value = try std.json.Stringify.valueAlloc(alloc, row, .{});
            writes.appendAssumeCapacity(.{ .key = key, .value = value });
        } else deletes.appendAssumeCapacity(key);
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
