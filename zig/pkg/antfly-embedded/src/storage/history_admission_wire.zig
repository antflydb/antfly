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

//! Private read-only completion admission qualification. A positive result
//! proves native preconditions at a fenced snapshot, not reserved capacity or
//! a prepared vote. Catalog/leader authority is established by the caller.
const std = @import("std");
const completion = @import("transaction_completion.zig");
const publication = @import("history_publication_wire.zig");
const A = std.mem.Allocator;
pub const max_bytes: usize = 4096;
pub const Query = struct {
    version: u8 = 1,
    owner: completion.RoutedOwner,
    minimum_applied_index: u64 = 1,
    pub fn validate(self: Query) !void {
        try self.owner.validate();
        if (self.version != 1 or self.minimum_applied_index == 0) return error.InvalidHistoryAdmission;
    }
    pub fn digest(self: Query) completion.Digest {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:history-admission-read:v1");
        sha.update(&.{self.version});
        sha.update(&self.owner.digest());
        var index: [8]u8 = undefined;
        std.mem.writeInt(u64, &index, self.minimum_applied_index, .little);
        sha.update(&index);
        return sha.finalResult();
    }
};
pub const Reply = struct {
    version: u8 = 1,
    request_digest: completion.Digest,
    cut: publication.CompletionPublicationPosition,
    /// Total policy ceiling, never a promise of currently available capacity.
    retention_limit_bytes: u64,
    pub fn init(query: Query, cut: publication.CompletionPublicationPosition, retention_limit_bytes: u64) !Reply {
        const result: Reply = .{ .request_digest = query.digest(), .cut = cut, .retention_limit_bytes = retention_limit_bytes };
        try result.validate(query);
        return result;
    }
    pub fn validate(self: Reply, query: Query) !void {
        try query.validate();
        if (self.version != 1 or !std.mem.eql(u8, &self.request_digest, &query.digest()) or self.cut.term == 0 or self.cut.index < query.minimum_applied_index or self.retention_limit_bytes == 0) return error.HistoryCorrupt;
    }
};
pub fn decodeQuery(a: A, raw: []const u8) !Query {
    if (raw.len == 0 or raw.len > max_bytes) return error.InvalidHistoryAdmission;
    var parsed = std.json.parseFromSlice(Query, a, raw, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidHistoryAdmission;
    defer parsed.deinit();
    try parsed.value.validate();
    return parsed.value;
}
pub fn decodeReply(a: A, raw: []const u8, query: Query) !Reply {
    if (raw.len == 0 or raw.len > max_bytes) return error.HistoryCorrupt;
    var parsed = std.json.parseFromSlice(Reply, a, raw, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.HistoryCorrupt;
    defer parsed.deinit();
    try parsed.value.validate(query);
    return parsed.value;
}

test "history admission wire qualifies original owner and positive actual native cut" {
    const a = std.testing.allocator;
    const query: Query = .{ .owner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 7, .topology_epoch = 3 }, .minimum_applied_index = 8 };
    const reply = try Reply.init(query, .{ .term = 2, .index = 10 }, 256 * 1024 * 1024);
    const request_bytes = try std.json.Stringify.valueAlloc(a, query, .{});
    defer a.free(request_bytes);
    try std.testing.expectEqualDeep(query, try decodeQuery(a, request_bytes));
    const bytes = try std.json.Stringify.valueAlloc(a, reply, .{});
    defer a.free(bytes);
    try std.testing.expectEqualDeep(reply, try decodeReply(a, bytes, query));
    var changed = query;
    changed.owner.root_generation += 1;
    try std.testing.expectError(error.HistoryCorrupt, reply.validate(changed));
    changed = query;
    changed.minimum_applied_index += 1;
    try std.testing.expectError(error.HistoryCorrupt, reply.validate(changed));
    var forged = reply;
    forged.cut.index = query.minimum_applied_index - 1;
    try std.testing.expectError(error.HistoryCorrupt, forged.validate(query));
    forged = reply;
    forged.cut.term = 0;
    try std.testing.expectError(error.HistoryCorrupt, forged.validate(query));
    forged = reply;
    forged.retention_limit_bytes = 0;
    try std.testing.expectError(error.HistoryCorrupt, forged.validate(query));
    const duplicate = try std.fmt.allocPrint(a, "{s},\"version\":1}}", .{bytes[0 .. bytes.len - 1]});
    defer a.free(duplicate);
    try std.testing.expectError(error.HistoryCorrupt, decodeReply(a, duplicate, query));
    const unknown = try std.fmt.allocPrint(a, "{s},\"reserved_capacity\":true}}", .{request_bytes[0 .. request_bytes.len - 1]});
    defer a.free(unknown);
    try std.testing.expectError(error.InvalidHistoryAdmission, decodeQuery(a, unknown));
    const oversized = try a.alloc(u8, max_bytes + 1);
    defer a.free(oversized);
    try std.testing.expectError(error.InvalidHistoryAdmission, decodeQuery(a, oversized));
    try std.testing.expectError(error.HistoryCorrupt, decodeReply(a, oversized, query));
}
