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

const std = @import("std");
const artifacts = @import("store.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

pub fn exercise(store: *artifacts.ArtifactStore) !void {
    const a = std.testing.allocator;
    const scope: artifacts.UploadScope = .{ .domain = @splat(1), .attempt = @splat(2) };
    const other_attempt: artifacts.UploadScope = .{ .domain = scope.domain, .attempt = @splat(3) };
    const other_domain: artifacts.UploadScope = .{ .domain = @splat(4), .attempt = scope.attempt };
    var first = try store.putScoped(scope, "same content", .none);
    defer first.deinit(store.allocator);
    var second = try store.putScoped(other_attempt, "same content", .none);
    defer second.deinit(store.allocator);
    var third = try store.putScoped(other_domain, "same content", .none);
    defer third.deinit(store.allocator);
    var ordinary = try store.put("same content");
    defer ordinary.deinit(store.allocator);
    try std.testing.expectEqualStrings(first.checksum, second.checksum);
    try std.testing.expectEqualStrings(first.checksum, third.checksum);
    try std.testing.expect(!std.mem.eql(u8, first.artifact_id, second.artifact_id));
    try std.testing.expect(!std.mem.eql(u8, first.artifact_id, third.artifact_id));
    for ([_]artifacts.ArtifactMetadata{ first, second, third, ordinary }) |metadata| {
        const body = try store.getAlloc(metadata.artifact_id);
        defer store.allocator.free(body);
        try std.testing.expectEqualStrings("same content", body);
        try store.verifyContentWithCancellationUsingAllocator(a, metadata.artifact_id, metadata.byte_len, metadata.checksum, .none);
        const range = try store.getVerifiedRangeAllocWithCancellationUsingAllocator(a, metadata.artifact_id, metadata.byte_len, metadata.checksum, 5, 7, .none);
        defer a.free(range);
        try std.testing.expectEqualStrings("content", range);
    }
    // More than one remote inventory page, with no candidate manifest at all.
    for (0..260) |i| {
        var buffer: [32]u8 = undefined;
        var metadata = try store.putScoped(scope, try std.fmt.bufPrint(&buffer, "page-{d}", .{i}), .none);
        metadata.deinit(store.allocator);
    }
    const Visitor = struct {
        alloc: std.mem.Allocator,
        domain: [32]u8,
        seen: std.StringHashMapUnmanaged(void) = .empty,
        fn visit(ptr: *anyopaque, found: artifacts.UploadScope, id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualSlices(u8, &self.domain, &found.domain);
            try std.testing.expect(!self.seen.contains(id));
            const owned = try self.alloc.dupe(u8, id);
            errdefer self.alloc.free(owned);
            try self.seen.put(self.alloc, owned, {});
        }
        fn clear(self: *@This()) void {
            var keys = self.seen.keyIterator();
            while (keys.next()) |key| self.alloc.free(key.*);
            self.seen.clearRetainingCapacity();
        }
        fn capability(self: *@This()) artifacts.ScopedUploadVisitor {
            return .{ .ptr = self, .visit = visit };
        }
    };
    var visitor: Visitor = .{ .alloc = a, .domain = scope.domain };
    defer {
        visitor.clear();
        visitor.seen.deinit(a);
    }
    try store.visitScopedUploads(scope.domain, visitor.capability(), .none);
    try std.testing.expectEqual(@as(u32, 262), visitor.seen.count());
    try std.testing.expect(visitor.seen.contains(first.artifact_id));
    try std.testing.expect(visitor.seen.contains(second.artifact_id));
    try store.delete(first.artifact_id);
    visitor.clear();
    try store.visitScopedUploads(scope.domain, visitor.capability(), .none);
    try std.testing.expectEqual(@as(u32, 261), visitor.seen.count());
    // A delayed upload remains enumerable; removing an earlier inventory
    // snapshot can never make this object permanently undiscoverable.
    var late = try store.putScoped(scope, "same content", .none);
    defer late.deinit(store.allocator);
    visitor.clear();
    try store.visitScopedUploads(scope.domain, visitor.capability(), .none);
    try std.testing.expectEqual(@as(u32, 262), visitor.seen.count());
    try std.testing.expect(visitor.seen.contains(late.artifact_id));
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, store.visitScopedUploads(scope.domain, visitor.capability(), CancellationToken.fromAtomic(&canceled)));
    // A publication-local capability scopes plain and cancellable writes too;
    // an explicit root nonce may differ, but authority must not escape.
    var publication = store.*;
    publication.upload_scope = scope;
    var implicit = try publication.put("same content");
    defer implicit.deinit(store.allocator);
    try std.testing.expectEqualStrings(first.artifact_id, implicit.artifact_id);
    var cancellable = try publication.putWithCancellation("same content", .none);
    defer cancellable.deinit(store.allocator);
    try std.testing.expectEqualStrings(first.artifact_id, cancellable.artifact_id);
    try std.testing.expectError(error.InvalidArtifactUploadScope, publication.putScoped(other_attempt, "rejected", .none));
    try std.testing.expectError(error.InvalidArtifactUploadScope, publication.putScoped(other_domain, "rejected", .none));
    var sibling = scope;
    sibling.attempt[15] ^= 1;
    var explicit = try publication.putScoped(sibling, "same content", .none);
    defer explicit.deinit(store.allocator);
    try std.testing.expectEqual(sibling, (try artifacts.uploadScopeFromArtifactId(explicit.artifact_id)).?);
    try std.testing.expect(store.upload_scope == null);
}
