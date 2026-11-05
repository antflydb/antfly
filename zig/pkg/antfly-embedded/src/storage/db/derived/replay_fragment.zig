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

//! Durable progress within a full-text journal record. All effects belonging
//! to one primary document family stay together, preserving overwrite/delete
//! precedence and asset fanout semantics. The scalar applied sequence remains
//! behind this record until every family has durable index output.
const std = @import("std");
const Crc32 = @import("antfly_hash").Crc32;
const types = @import("derived_types.zig");
const keys = @import("../../internal_keys.zig");

pub const prefix = "\x00\x00__metadata__:derived_fragment:";
pub const Position = struct {
    sequence: u64,
    next_item: u64,
    total_items: u64,
    fingerprint: [32]u8,
    applied: bool = false,
};
pub const Identity = struct { generation: u64, config_hash: u64, incarnation: u128 };
pub const Checkpoint = struct { identity: Identity, position: Position };
pub const encoded_size = 101;

pub fn encode(checkpoint: Checkpoint) [encoded_size]u8 {
    var raw: [encoded_size]u8 = undefined;
    @memcpy(raw[0..8], "AFRFRG01");
    std.mem.writeInt(u64, raw[8..16], checkpoint.position.sequence, .little);
    std.mem.writeInt(u64, raw[16..24], checkpoint.position.next_item, .little);
    std.mem.writeInt(u64, raw[24..32], checkpoint.position.total_items, .little);
    @memcpy(raw[32..64], &checkpoint.position.fingerprint);
    std.mem.writeInt(u64, raw[64..72], checkpoint.identity.generation, .little);
    std.mem.writeInt(u64, raw[72..80], checkpoint.identity.config_hash, .little);
    std.mem.writeInt(u128, raw[80..96], checkpoint.identity.incarnation, .little);
    raw[96] = @intFromBool(checkpoint.position.applied);
    std.mem.writeInt(u32, raw[97..101], Crc32.hash(raw[0..97]), .little);
    return raw;
}

pub fn decode(raw: []const u8) !Checkpoint {
    if (raw.len != encoded_size or !std.mem.eql(u8, raw[0..8], "AFRFRG01") or raw[96] > 1 or
        std.mem.readInt(u32, raw[97..101], .little) != Crc32.hash(raw[0..97])) return error.InvalidDerivedApplyState;
    const result: Checkpoint = .{
        .identity = .{ .generation = std.mem.readInt(u64, raw[64..72], .little), .config_hash = std.mem.readInt(u64, raw[72..80], .little), .incarnation = std.mem.readInt(u128, raw[80..96], .little) },
        .position = .{
            .sequence = std.mem.readInt(u64, raw[8..16], .little),
            .next_item = std.mem.readInt(u64, raw[16..24], .little),
            .total_items = std.mem.readInt(u64, raw[24..32], .little),
            .fingerprint = raw[32..64].*,
            .applied = raw[96] == 1,
        },
    };
    if (result.position.sequence == 0 or result.position.next_item > result.position.total_items) return error.InvalidDerivedApplyState;
    return result;
}

const Kind = enum(u8) { document, deleted, overwritten, artifact };
const Item = struct { key: []const u8, owner: []const u8, kind: Kind, index: usize };

fn ownerKey(a: std.mem.Allocator, key: []const u8, kind: Kind) ![]const u8 {
    // Journal document IDs are logical IDs; artifact keys are physical keys.
    // Encode the former before comparing families, including escaped NULs.
    if (kind == .artifact or (kind == .deleted and keys.isInternalUserKey(key) and !keys.isStoredDocumentRowKey(key))) {
        if (keys.findComponentTerminator(key, 1)) |end| return key[0 .. end + 2];
    }
    return try keys.documentExactPrefixAlloc(a, key);
}

pub const Piece = struct {
    batch: types.DerivedBatch,
    next_item: usize,
    families: usize,

    pub fn deinit(self: *Piece, a: std.mem.Allocator) void {
        a.free(self.batch.documents);
        a.free(self.batch.deleted_keys);
        a.free(self.batch.overwritten_doc_keys);
        a.free(self.batch.changed_artifact_keys);
    }
};

pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    batch: types.DerivedBatch,
    items: []Item,
    fingerprint: [32]u8,

    pub fn init(a: std.mem.Allocator, batch: types.DerivedBatch, name: []const u8) !Plan {
        const count = batch.documents.len + batch.deleted_keys.len + batch.overwritten_doc_keys.len + batch.changed_artifact_keys.len;
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const scratch = arena.allocator();
        const items = try scratch.alloc(Item, count);
        var cursor: usize = 0;
        for (batch.documents, 0..) |document, i| {
            items[cursor] = .{ .key = document.key, .owner = try ownerKey(scratch, document.key, .document), .kind = .document, .index = i };
            cursor += 1;
        }
        inline for (.{ .{ Kind.deleted, batch.deleted_keys }, .{ Kind.overwritten, batch.overwritten_doc_keys }, .{ Kind.artifact, batch.changed_artifact_keys } }) |set| {
            for (set[1], 0..) |key, i| {
                items[cursor] = .{ .key = key, .owner = try ownerKey(scratch, key, set[0]), .kind = set[0], .index = i };
                cursor += 1;
            }
        }
        std.mem.sort(Item, items, {}, struct {
            fn less(_: void, lhs: Item, rhs: Item) bool {
                const owner_order = std.mem.order(u8, lhs.owner, rhs.owner);
                if (owner_order != .eq) return owner_order == .lt;
                const key_order = std.mem.order(u8, lhs.key, rhs.key);
                if (key_order != .eq) return key_order == .lt;
                if (lhs.kind != rhs.kind) return @backingInt(lhs.kind) < @backingInt(rhs.kind);
                return lhs.index < rhs.index;
            }
        }.less);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("full-text replay fragment v1");
        hash.update(name);
        var encoded: [8]u8 = undefined;
        std.mem.writeInt(u64, &encoded, batch.sequence, .little);
        hash.update(&encoded);
        for (items) |item| {
            hash.update(&.{@backingInt(item.kind)});
            std.mem.writeInt(u64, &encoded, item.key.len, .little);
            hash.update(&encoded);
            hash.update(item.key);
            if (item.kind == .document) {
                const doc = batch.documents[item.index];
                hash.update(&.{@backingInt(doc.action)});
                if (doc.cleaned_value != null) return error.InvalidDerivedApplyState;
                std.mem.writeInt(u64, &encoded, doc.targets.len, .little);
                hash.update(&encoded);
                for (doc.targets) |target| {
                    hash.update(&.{@backingInt(target.kind)});
                    std.mem.writeInt(u64, &encoded, target.index_name.len, .little);
                    hash.update(&encoded);
                    hash.update(target.index_name);
                }
            }
        }
        var fingerprint: [32]u8 = undefined;
        hash.final(&fingerprint);
        return .{ .arena = arena, .batch = batch, .items = items, .fingerprint = fingerprint };
    }

    pub fn deinit(self: *Plan, a: std.mem.Allocator) void {
        _ = a;
        self.arena.deinit();
    }

    pub fn startPosition(self: *const Plan, position: ?Position) !struct { start: usize, applied: bool } {
        const saved = position orelse return .{ .start = 0, .applied = false };
        if (saved.sequence != self.batch.sequence or saved.total_items != self.items.len or
            !std.mem.eql(u8, &saved.fingerprint, &self.fingerprint)) return error.InvalidDerivedApplyState;
        const start = std.math.cast(usize, saved.next_item) orelse return error.InvalidDerivedApplyState;
        if (start > self.items.len or (start != 0 and start != self.items.len and
            std.mem.eql(u8, self.items[start - 1].owner, self.items[start].owner))) return error.InvalidDerivedApplyState;
        return .{ .start = start, .applied = saved.applied };
    }

    pub fn familyCount(self: *const Plan) usize {
        var count: usize = 0;
        for (self.items, 0..) |item, i| {
            if (i == 0 or !std.mem.eql(u8, self.items[i - 1].owner, item.owner)) count += 1;
        }
        return count;
    }

    pub fn piece(self: *const Plan, a: std.mem.Allocator, start: usize, max_families: usize) !Piece {
        std.debug.assert(max_families != 0 and start <= self.items.len);
        var end = start;
        var families: usize = 0;
        while (end < self.items.len) {
            if (end == start or !std.mem.eql(u8, self.items[end - 1].owner, self.items[end].owner)) {
                if (families == max_families) break;
                families += 1;
            }
            end += 1;
        }
        var counts: [4]usize = @splat(0);
        for (self.items[start..end]) |item| counts[@backingInt(item.kind)] += 1;
        const documents = try a.alloc(types.DerivedDocument, counts[0]);
        errdefer a.free(documents);
        const deleted = try a.alloc([]const u8, counts[1]);
        errdefer a.free(deleted);
        const overwritten = try a.alloc([]const u8, counts[2]);
        errdefer a.free(overwritten);
        const artifacts = try a.alloc([]const u8, counts[3]);
        errdefer a.free(artifacts);
        counts = @splat(0);
        for (self.items[start..end]) |item| {
            const slot = counts[@backingInt(item.kind)];
            counts[@backingInt(item.kind)] += 1;
            switch (item.kind) {
                .document => documents[slot] = self.batch.documents[item.index],
                .deleted => deleted[slot] = item.key,
                .overwritten => overwritten[slot] = item.key,
                .artifact => artifacts[slot] = item.key,
            }
        }
        return .{ .next_item = end, .families = families, .batch = .{ .sequence = self.batch.sequence, .documents = documents, .deleted_keys = deleted, .overwritten_doc_keys = overwritten, .changed_artifact_keys = artifacts } };
    }
};

test "replay fragments keep document deletion overwrite and asset effects together" {
    const a = std.testing.allocator;
    const document = "one\x00escaped";
    const asset = try keys.artifactNamedPrefixAlloc(a, "one\x00escaped", "asset", "text");
    defer a.free(asset);
    var plan = try Plan.init(a, .{ .sequence = 42, .documents = &.{.{ .key = document, .action = .upsert }}, .deleted_keys = &.{ document, "two" }, .overwritten_doc_keys = &.{document}, .changed_artifact_keys = &.{asset} }, "index");
    defer plan.deinit(a);
    var first = try plan.piece(a, 0, 1);
    defer first.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), first.families);
    try std.testing.expectEqual(@as(usize, 2), plan.familyCount());
    try std.testing.expectEqual(@as(usize, 1), first.batch.documents.len);
    try std.testing.expectEqual(@as(usize, 1), first.batch.overwritten_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), first.batch.deleted_keys.len);
    try std.testing.expectEqual(@as(usize, 1), first.batch.changed_artifact_keys.len);
    const position: Position = .{ .sequence = 42, .next_item = first.next_item, .total_items = plan.items.len, .fingerprint = plan.fingerprint, .applied = true };
    try std.testing.expectEqual(first.next_item, (try plan.startPosition(position)).start);
    var invalid = position;
    invalid.next_item = 1;
    try std.testing.expectError(error.InvalidDerivedApplyState, plan.startPosition(invalid));
    var second = try plan.piece(a, first.next_item, 1);
    defer second.deinit(a);
    try std.testing.expectEqual(plan.items.len, second.next_item);
    try std.testing.expectEqualStrings("two", second.batch.deleted_keys[0]);
    const encoded = encode(.{ .identity = .{ .generation = 1, .config_hash = 2, .incarnation = 3 }, .position = position });
    try std.testing.expectEqualDeep(position, (try decode(&encoded)).position);
    var corrupt = encoded;
    corrupt[24] ^= 1;
    try std.testing.expectError(error.InvalidDerivedApplyState, decode(&corrupt));
}
