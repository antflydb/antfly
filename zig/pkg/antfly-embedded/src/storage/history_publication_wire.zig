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

//! Bounded, request-bound publication observations. Structural validation is
//! not source authentication; callers establish original-owner and ReadIndex
//! authority before exposing a native observation.
const std = @import("std");
const completion = @import("transaction_completion.zig");
const A = std.mem.Allocator;
pub const CompletionPublicationPosition = struct { term: u64, index: u64 };
pub const CompletionPublication = struct { digest: completion.Digest, retained_bytes: u64, position: CompletionPublicationPosition };
pub const CompletionPublicationQuery = struct {
    version: u8 = 1,
    owner: completion.RoutedOwner,
    transaction_id: [16]u8,
    manifest_digest: completion.Digest,
    commit_version: u64,
    minimum_applied_index: u64,
    pub fn validate(self: CompletionPublicationQuery) !void {
        try self.owner.validate();
        if (self.version != 1 or self.commit_version == 0 or self.minimum_applied_index == 0 or std.mem.allEqual(u8, &self.transaction_id, 0) or std.mem.allEqual(u8, &self.manifest_digest, 0)) return error.InvalidHistoryAdmission;
    }
    pub fn digest(self: CompletionPublicationQuery) completion.Digest {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:history-completion-publication-read:v1");
        sha.update(&.{self.version});
        sha.update(&self.owner.digest());
        sha.update(&self.transaction_id);
        sha.update(&self.manifest_digest);
        for ([_]u64{ self.commit_version, self.minimum_applied_index }) |value| {
            var raw: [8]u8 = undefined;
            std.mem.writeInt(u64, &raw, value, .little);
            sha.update(&raw);
        }
        return sha.finalResult();
    }
};
pub const CompletionPublicationObservation = struct {
    version: u8 = 1,
    request_digest: completion.Digest,
    cut: CompletionPublicationPosition,
    publication: ?CompletionPublication,
    pub fn init(query: CompletionPublicationQuery, cut: CompletionPublicationPosition, publication: ?CompletionPublication) !CompletionPublicationObservation {
        const result: CompletionPublicationObservation = .{ .request_digest = query.digest(), .cut = cut, .publication = publication };
        try result.validate(query);
        return result;
    }
    pub fn validate(self: CompletionPublicationObservation, query: CompletionPublicationQuery) !void {
        try query.validate();
        if (self.version != 1 or !std.mem.eql(u8, &self.request_digest, &query.digest()) or self.cut.term == 0 or self.cut.index < query.minimum_applied_index) return error.HistoryCorrupt;
        if (self.publication) |publication| {
            if (std.mem.allEqual(u8, &publication.digest, 0) or publication.retained_bytes == 0 or publication.retained_bytes > 2 * completion.max_bytes + 128 or publication.position.term == 0 or publication.position.index == 0 or publication.position.index > self.cut.index or publication.position.term > self.cut.term or (publication.position.index == self.cut.index and publication.position.term != self.cut.term)) return error.HistoryCorrupt;
        }
    }
};

pub const publication_read_max_bytes: usize = 4096;
pub fn decodeCompletionPublicationQuery(a: A, raw: []const u8) !CompletionPublicationQuery {
    if (raw.len == 0 or raw.len > publication_read_max_bytes) return error.InvalidHistoryAdmission;
    var parsed = std.json.parseFromSlice(CompletionPublicationQuery, a, raw, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidHistoryAdmission;
    defer parsed.deinit();
    try parsed.value.validate();
    return parsed.value;
}
pub fn decodeCompletionPublicationObservation(a: A, raw: []const u8, query: CompletionPublicationQuery) !CompletionPublicationObservation {
    if (raw.len == 0 or raw.len > publication_read_max_bytes) return error.HistoryCorrupt;
    var parsed = std.json.parseFromSlice(CompletionPublicationObservation, a, raw, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.HistoryCorrupt;
    defer parsed.deinit();
    try parsed.value.validate(query);
    return parsed.value;
}

test "history publication wire binds original owner and actual publication cut" {
    const a = std.testing.allocator;
    const query: CompletionPublicationQuery = .{ .owner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 7, .topology_epoch = 3 }, .transaction_id = @splat(4), .manifest_digest = @splat(5), .commit_version = 6, .minimum_applied_index = 7 };
    const reply = try CompletionPublicationObservation.init(query, .{ .term = 3, .index = 10 }, .{ .digest = @splat(6), .retained_bytes = 1024, .position = .{ .term = 2, .index = 8 } });
    const raw = try std.json.Stringify.valueAlloc(a, reply, .{});
    defer a.free(raw);
    try std.testing.expectEqualDeep(reply, try decodeCompletionPublicationObservation(a, raw, query));
    var other = query;
    other.owner.root_generation += 1;
    try std.testing.expectError(error.HistoryCorrupt, reply.validate(other));
    other = query;
    other.minimum_applied_index += 1;
    try std.testing.expectError(error.HistoryCorrupt, reply.validate(other));
    var forged = reply;
    forged.publication.?.position.index = 11;
    try std.testing.expectError(error.HistoryCorrupt, forged.validate(query));
    forged = reply;
    forged.publication.?.position.term = 4;
    try std.testing.expectError(error.HistoryCorrupt, forged.validate(query));
    forged = reply;
    forged.publication.?.retained_bytes = 0;
    try std.testing.expectError(error.HistoryCorrupt, forged.validate(query));
    const absent = try CompletionPublicationObservation.init(query, reply.cut, null);
    try std.testing.expect(absent.publication == null);
    const query_raw = try std.json.Stringify.valueAlloc(a, query, .{});
    defer a.free(query_raw);
    try std.testing.expectEqualDeep(query, try decodeCompletionPublicationQuery(a, query_raw));
    const duplicated = try std.fmt.allocPrint(a, "{{\"version\":1,{s}", .{query_raw[1..]});
    defer a.free(duplicated);
    try std.testing.expectError(error.InvalidHistoryAdmission, decodeCompletionPublicationQuery(a, duplicated));
    const oversized: [publication_read_max_bytes + 1]u8 = @splat(' ');
    try std.testing.expectError(error.InvalidHistoryAdmission, decodeCompletionPublicationQuery(a, &oversized));
    try std.testing.expectError(error.HistoryCorrupt, decodeCompletionPublicationObservation(a, &oversized, query));
}
