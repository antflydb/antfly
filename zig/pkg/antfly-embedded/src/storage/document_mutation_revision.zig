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

//! Optimistic read fences for a document and its derived physical state.
//! Revisions advance atomically with mutations, independently of TTL clocks.
//! Keep tombstones after deletion to reject delete/reinsert ABA.
const std = @import("std");
const keys = @import("internal_keys.zig");
const prefix = "\x00\x00__metadata__:document_mutation_revision_v1:";
pub const Key = [prefix.len + 32]u8;
const content_prefix = "\x00\x00__metadata__:document_content_authority_v1:";
pub const ContentKey = [content_prefix.len + 32]u8;
pub const Content = struct { revision: u64, bytes: u64, kind: u8, digest: [32]u8 };
fn contentKey(key: Key) ContentKey {
    var result: ContentKey = undefined;
    @memcpy(result[0..content_prefix.len], content_prefix);
    @memcpy(result[content_prefix.len..], key[prefix.len..]);
    return result;
}
fn decodeContent(raw: []const u8) !Content {
    if (raw.len != 49) return error.InvalidDocumentContentAuthority;
    return .{ .revision = std.mem.readInt(u64, raw[0..8], .little), .bytes = std.mem.readInt(u64, raw[8..16], .little), .kind = raw[16], .digest = raw[17..49].* };
}
/// The existing mutation revision is also the compatibility fence. Older
/// writers advance it without refreshing this cache, forcing a primary read.
pub fn loadContent(txn: anytype, physical: []const u8) !?Content {
    if (!keys.isStoredDocumentRowKey(physical)) return null;
    const key = keyForPhysical(physical).?;
    const raw = txn.get(&contentKey(key)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const value = try decodeContent(raw);
    if (value.kind != physical[physical.len - 1] or value.revision != try load(txn, key)) return null;
    return value;
}
fn encodeContent(value: Content) [49]u8 {
    var raw: [49]u8 = undefined;
    std.mem.writeInt(u64, raw[0..8], value.revision, .little);
    std.mem.writeInt(u64, raw[8..16], value.bytes, .little);
    raw[16] = value.kind;
    @memcpy(raw[17..49], &value.digest);
    return raw;
}

fn keyForPrefix(document_prefix: []const u8) Key {
    var result: Key = undefined;
    @memcpy(result[0..prefix.len], prefix);
    std.crypto.hash.sha2.Sha256.hash(document_prefix, result[prefix.len..], .{});
    return result;
}

pub fn keyForPhysical(candidate: []const u8) ?Key {
    if (!keys.isInternalUserKey(candidate)) return null;
    const end = (keys.findComponentTerminator(candidate, 1) orelse return null) + 2;
    return keyForPrefix(candidate[0..end]);
}

pub fn keyForDocumentAlloc(alloc: std.mem.Allocator, document: []const u8) !Key {
    const physical_prefix = try keys.documentExactPrefixAlloc(alloc, document);
    defer alloc.free(physical_prefix);
    return keyForPrefix(physical_prefix);
}

pub fn load(txn: anytype, key: Key) !u64 {
    const raw = txn.get(&key) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    if (raw.len != 8) return error.InvalidDocumentMutationRevision;
    return std.mem.readInt(u64, raw[0..8], .little);
}

/// One revision per affected document per transaction, including artifact-only
/// commits. Capture fixed keys rather than retaining every artifact key.
pub const Capture = struct {
    const Change = union(enum) { unchanged, deleted, written: Content };
    documents: std.AutoHashMapUnmanaged(Key, Change) = .empty,
    staged: bool = false,
    poisoned: bool = false,

    pub fn deinit(self: *Capture, alloc: std.mem.Allocator) void {
        self.documents.deinit(alloc);
    }

    pub fn touch(self: *Capture, alloc: std.mem.Allocator, physical_key: []const u8) !void {
        const key = keyForPhysical(physical_key) orelse return;
        if (self.staged or self.poisoned) return error.RetainedEffectsTransactionFailed;
        errdefer self.poisoned = true;
        const entry = try self.documents.getOrPut(alloc, key);
        if (!entry.found_existing) entry.value_ptr.* = .unchanged;
    }

    /// Hash primary bytes once at the common physical writer boundary. The
    /// fixed map entry coalesces repeated writes and retains no document body.
    pub fn touchValue(self: *Capture, alloc: std.mem.Allocator, physical: []const u8, value: ?[]const u8) !void {
        try self.touch(alloc, physical);
        if (!keys.isStoredDocumentRowKey(physical)) return;
        const entry = self.documents.getPtr(keyForPhysical(physical).?).?;
        if (value) |bytes| {
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
            entry.* = .{ .written = .{ .revision = 0, .bytes = bytes.len, .kind = physical[physical.len - 1], .digest = digest } };
        } else entry.* = .deleted;
    }

    pub fn stage(self: *Capture, txn: anytype) !void {
        if (self.poisoned) return error.RetainedEffectsTransactionFailed;
        if (self.staged) return;
        errdefer self.poisoned = true;
        var iter = self.documents.iterator();
        while (iter.next()) |entry| {
            const key = entry.key_ptr.*;
            const old = try load(txn, key);
            const next = std.math.add(u64, old, 1) catch return error.DocumentMutationRevisionExhausted;
            const cached_key = contentKey(key);
            var cached: ?Content = switch (entry.value_ptr.*) {
                .written => |value| value,
                .deleted => null,
                .unchanged => blk: {
                    const raw = txn.get(&cached_key) catch |err| switch (err) {
                        error.NotFound => break :blk null,
                        else => return err,
                    };
                    const value = try decodeContent(raw);
                    break :blk if (value.revision == old) value else null;
                },
            };
            // Update before publishing the revision; both share the commit.
            // Artifact/TTL-only changes can retain a previously proven digest.
            if (cached) |*value| {
                value.revision = next;
                try txn.put(&cached_key, &encodeContent(value.*));
            } else if (entry.value_ptr.* == .deleted) {
                txn.delete(&cached_key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
            }
            var raw: [8]u8 = undefined;
            std.mem.writeInt(u64, &raw, next, .little);
            try txn.put(&key, &raw);
        }
        self.staged = true;
    }
};

test "document mutation revision capture owns allocation failures and coalesces owners" {
    const Case = struct {
        const Fake = struct {
            writes: usize = 0,
            revision: [8]u8 = .{ 7, 0, 0, 0, 0, 0, 0, 0 },
            pub fn get(self: *@This(), key: []const u8) ![]const u8 {
                if (std.mem.startsWith(u8, key, content_prefix)) return error.NotFound;
                return &self.revision;
            }
            pub fn delete(_: *@This(), _: []const u8) !void {
                return error.NotFound;
            }
            pub fn put(self: *@This(), _: []const u8, raw: []const u8) !void {
                try std.testing.expectEqual(@as(u64, 8), std.mem.readInt(u64, raw[0..8], .little));
                self.writes += 1;
            }
        };
        fn run(alloc: std.mem.Allocator) !void {
            var capture = Capture{};
            defer capture.deinit(alloc);
            var txn = Fake{};
            for ([_][]const u8{ "\x01doc:a\x00\x00\x10", "\x01doc:a\x00\x00\x20asset", "\x01doc:b\x00\x00\x10" }) |physical| {
                capture.touch(alloc, physical) catch |err| {
                    try std.testing.expectError(error.RetainedEffectsTransactionFailed, capture.stage(&txn));
                    try std.testing.expectEqual(@as(usize, 0), txn.writes);
                    return err;
                };
            }
            try capture.stage(&txn);
            try capture.stage(&txn);
            try std.testing.expectEqual(@as(usize, 2), txn.writes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "public graph metric borrowed wire cached content authority fences legacy writes and coalesces primary bytes" {
    const Case = struct {
        const Fake = struct {
            revision: [8]u8 = @splat(0),
            content: ?[49]u8 = null,
            read_bytes: usize = 0,
            pub fn get(self: *@This(), key: []const u8) ![]const u8 {
                if (std.mem.startsWith(u8, key, content_prefix)) {
                    if (self.content == null) return error.NotFound;
                    self.read_bytes += 49;
                    return &self.content.?;
                }
                if (!std.mem.startsWith(u8, key, prefix)) return error.TestUnexpectedResult;
                self.read_bytes += 8;
                return &self.revision;
            }
            pub fn put(self: *@This(), key: []const u8, raw: []const u8) !void {
                if (std.mem.startsWith(u8, key, content_prefix)) self.content = raw[0..49].* else self.revision = raw[0..8].*;
            }
            pub fn delete(self: *@This(), _: []const u8) !void {
                self.content = null;
            }
        };
        fn run(a: std.mem.Allocator) !void {
            const physical = try keys.documentKeyAlloc(a, "doc\x00binary");
            defer a.free(physical);
            var fake = Fake{};
            {
                var capture = Capture{};
                defer capture.deinit(a);
                try capture.touchValue(a, physical, "old");
                try capture.touchValue(a, physical, "last bytes");
                try capture.stage(&fake);
            }
            fake.read_bytes = 0;
            const cached = (try loadContent(&fake, physical)).?;
            try std.testing.expectEqual(@as(u64, 10), cached.bytes);
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash("last bytes", &digest, .{});
            try std.testing.expectEqualSlices(u8, &digest, &cached.digest);
            try std.testing.expectEqual(@as(usize, 57), fake.read_bytes);
            // TTL/artifact-only revisions preserve content without hashing it.
            {
                var capture = Capture{};
                defer capture.deinit(a);
                try capture.touch(a, physical);
                try capture.stage(&fake);
            }
            try std.testing.expectEqual(@as(u64, 2), (try loadContent(&fake, physical)).?.revision);
            // An older writer advances the revision without our cache update.
            std.mem.writeInt(u64, &fake.revision, 3, .little);
            try std.testing.expect((try loadContent(&fake, physical)) == null);
            {
                var capture = Capture{};
                defer capture.deinit(a);
                try capture.touch(a, physical);
                try capture.stage(&fake);
            }
            try std.testing.expect((try loadContent(&fake, physical)) == null);
            {
                var capture = Capture{};
                defer capture.deinit(a);
                try capture.touchValue(a, physical, null);
                try capture.stage(&fake);
            }
            try std.testing.expect(fake.content == null);
            try std.testing.expectEqual(@as(u64, 5), try load(&fake, keyForPhysical(physical).?));
        }
    };
    try Case.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
    std.debug.print("CONTENT_AUTHORITY cached_read_bytes=57 primary_reads=0 query_hash_bytes=0\n", .{});
}
