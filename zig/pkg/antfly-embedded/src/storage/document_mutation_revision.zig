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
    documents: std.AutoHashMapUnmanaged(Key, void) = .empty,
    staged: bool = false,
    poisoned: bool = false,

    pub fn deinit(self: *Capture, alloc: std.mem.Allocator) void {
        self.documents.deinit(alloc);
    }

    pub fn touch(self: *Capture, alloc: std.mem.Allocator, physical_key: []const u8) !void {
        const key = keyForPhysical(physical_key) orelse return;
        if (self.staged or self.poisoned) return error.RetainedEffectsTransactionFailed;
        errdefer self.poisoned = true;
        try self.documents.put(alloc, key, {});
    }

    pub fn stage(self: *Capture, txn: anytype) !void {
        if (self.poisoned) return error.RetainedEffectsTransactionFailed;
        if (self.staged) return;
        errdefer self.poisoned = true;
        var iter = self.documents.keyIterator();
        while (iter.next()) |key| {
            const next = std.math.add(u64, try load(txn, key.*), 1) catch return error.DocumentMutationRevisionExhausted;
            var raw: [8]u8 = undefined;
            std.mem.writeInt(u64, &raw, next, .little);
            try txn.put(key, &raw);
        }
        self.staged = true;
    }
};

test "document mutation revision capture owns allocation failures and coalesces owners" {
    const Case = struct {
        const Fake = struct {
            writes: usize = 0,
            revision: [8]u8 = .{ 7, 0, 0, 0, 0, 0, 0, 0 },
            pub fn get(self: *@This(), _: []const u8) ![]const u8 {
                return &self.revision;
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
