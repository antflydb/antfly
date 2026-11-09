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

//! Immutable, seekable document-family replay chunks. A fixed-size manifest
//! authenticates bounded chunk reads with Merkle proofs. All family effects
//! stay together; native transactions publish the manifest and every chunk.
const std = @import("std");
const journal = @import("change_journal.zig");
const fragments = @import("replay_fragment.zig");
const types = @import("derived_types.zig");
const keys = @import("../../internal_keys.zig");
const Allocator = std.mem.Allocator;
const Hash = std.crypto.hash.Blake3;
pub const max_chunks = 256;
pub const families_per_chunk = 64;
pub const manifest_size = 67;
const chunk_header_size = 99;
const manifest_magic = "AFRCHK01";
const chunk_magic = "AFRCKD01";
pub const chunk_kind: u8 = 0xfd;

pub fn chunkKey(sequence: u64, ordinal: u32) [14]u8 {
    var key: [14]u8 = undefined;
    key[0] = 2;
    key[1] = chunk_kind;
    std.mem.writeInt(u64, key[2..10], sequence, .big);
    std.mem.writeInt(u32, key[10..14], ordinal, .big);
    return key;
}

pub fn isManifest(raw: []const u8) bool {
    return std.mem.startsWith(u8, raw, manifest_magic);
}

fn digest(raw: []const u8) [32]u8 {
    var hash: [32]u8 = undefined;
    Hash.hash(raw, &hash, .{});
    return hash;
}

fn parent(left: [32]u8, right: [32]u8) [32]u8 {
    var hash = Hash.init(.{});
    hash.update("antfly replay chunk node v1");
    hash.update(&left);
    hash.update(&right);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

fn hintMask(hints: []const journal.TargetHint) u8 {
    var mask: u8 = 0;
    for (hints) |hint| mask |= journal.singleHintMask(hint);
    return mask;
}

pub const Manifest = struct {
    sequence: u64,
    count: u32,
    families: u32,
    hints: u8,
    version: u16,
    root: [32]u8,
    retired_chunks: u32 = 0,

    pub fn decode(raw: []const u8) !Manifest {
        if (raw.len != manifest_size or !isManifest(raw) or
            std.mem.readInt(u32, raw[63..67], .little) != std.hash.Crc32.hash(raw[0..63])) return error.InvalidReplayChunk;
        const value: Manifest = .{
            .sequence = std.mem.readInt(u64, raw[8..16], .little),
            .hints = raw[16],
            .count = std.mem.readInt(u32, raw[17..21], .little),
            .families = std.mem.readInt(u32, raw[21..25], .little),
            .version = std.mem.readInt(u16, raw[25..27], .little),
            .root = raw[27..59].*,
            .retired_chunks = std.mem.readInt(u32, raw[59..63], .little),
        };
        if (value.sequence == 0 or value.count == 0 or value.count > max_chunks or value.retired_chunks > value.count or value.families == 0 or
            value.families > value.count * families_per_chunk or value.families <= (value.count - 1) * families_per_chunk) return error.InvalidReplayChunk;
        return value;
    }

    pub fn encode(self: Manifest) [manifest_size]u8 {
        var raw: [manifest_size]u8 = undefined;
        @memcpy(raw[0..8], manifest_magic);
        std.mem.writeInt(u64, raw[8..16], self.sequence, .little);
        raw[16] = self.hints;
        std.mem.writeInt(u32, raw[17..21], self.count, .little);
        std.mem.writeInt(u32, raw[21..25], self.families, .little);
        std.mem.writeInt(u16, raw[25..27], self.version, .little);
        @memcpy(raw[27..59], &self.root);
        std.mem.writeInt(u32, raw[59..63], self.retired_chunks, .little);
        std.mem.writeInt(u32, raw[63..67], std.hash.Crc32.hash(raw[0..63]), .little);
        return raw;
    }

    pub fn fingerprint(self: Manifest, index_name: []const u8) [32]u8 {
        var hash = Hash.init(.{});
        hash.update("antfly seekable replay position v1");
        var immutable = self;
        immutable.retired_chunks = 0;
        hash.update(&immutable.encode());
        hash.update(index_name);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
};

pub const Prepared = struct {
    allocator: Allocator,
    manifest: Manifest,
    chunks: [][]u8,

    pub fn deinit(self: *Prepared) void {
        for (self.chunks) |chunk| self.allocator.free(chunk);
        self.allocator.free(self.chunks);
        self.* = undefined;
    }

    pub fn stage(self: *const Prepared, txn: anytype) !void {
        for (self.chunks, 0..) |chunk, ordinal| {
            const key = chunkKey(self.manifest.sequence, @intCast(ordinal));
            try txn.put(&key, chunk);
        }
        const key = keys.replayEntryKey(keys.replay_all_kind, self.manifest.sequence);
        try txn.put(&key, &self.manifest.encode());
    }
};

/// Null retains the inline representation for small and exceptionally large
/// atomic records. The largest chunked record can retire in one bounded batch.
pub fn prepare(a: Allocator, record: journal.Record) !?Prepared {
    const documents = try a.alloc(types.DerivedDocument, record.changed_doc_keys.len);
    defer a.free(documents);
    for (record.changed_doc_keys, documents) |key, *document| document.* = .{ .key = key };
    var plan = try fragments.Plan.init(a, .{ .sequence = record.sequence, .documents = documents, .deleted_keys = record.deleted_doc_keys, .overwritten_doc_keys = record.overwritten_doc_keys, .changed_artifact_keys = record.changed_artifact_keys }, "");
    defer plan.deinit(a);
    var family_count: usize = 0;
    for (plan.items, 0..) |item, ordinal| {
        if (ordinal == 0 or !std.mem.eql(u8, plan.items[ordinal - 1].owner, item.owner)) family_count += 1;
    }
    if (family_count <= families_per_chunk or family_count > max_chunks * families_per_chunk) return null;
    const count = std.math.divCeil(usize, family_count, families_per_chunk) catch unreachable;
    const payloads = try a.alloc([]u8, count);
    defer a.free(payloads);
    var initialized: usize = 0;
    defer for (payloads[0..initialized]) |bytes| a.free(bytes);
    var families: [max_chunks]u16 = @splat(0);
    var start: usize = 0;
    for (payloads, 0..) |*payload, ordinal| {
        var piece = try plan.piece(a, start, families_per_chunk);
        defer piece.deinit(a);
        const changed = try a.alloc([]const u8, piece.batch.documents.len);
        defer a.free(changed);
        for (piece.batch.documents, changed) |document, *key| key.* = document.key;
        for (plan.items[start..piece.next_item], start..) |item, item_ordinal| {
            if (item_ordinal == start or !std.mem.eql(u8, plan.items[item_ordinal - 1].owner, item.owner)) families[ordinal] += 1;
        }
        payload.* = try journal.encodeRecord(a, .{ .version = record.version, .sequence = record.sequence, .changed_doc_keys = changed, .deleted_doc_keys = piece.batch.deleted_keys, .overwritten_doc_keys = piece.batch.overwritten_doc_keys, .changed_artifact_keys = piece.batch.changed_artifact_keys, .target_hints = record.target_hints });
        initialized += 1;
        start = piece.next_item;
    }
    var leaves: usize = 1;
    var depth: u8 = 0;
    while (leaves < count) : (depth += 1) leaves *= 2;
    var tree: [max_chunks * 2][32]u8 = @splat(@splat(0));
    for (payloads, 0..) |payload, ordinal| tree[leaves + ordinal] = digest(payload);
    var node = leaves;
    while (node > 1) {
        node -= 1;
        tree[node] = parent(tree[node * 2], tree[node * 2 + 1]);
    }
    const manifest: Manifest = .{ .sequence = record.sequence, .count = @intCast(count), .families = @intCast(family_count), .hints = hintMask(record.target_hints), .version = record.version, .root = tree[1] };
    const chunks = try a.alloc([]u8, count);
    errdefer a.free(chunks);
    var encoded: usize = 0;
    errdefer for (chunks[0..encoded]) |chunk| a.free(chunk);
    for (chunks, payloads, 0..) |*chunk, payload, ordinal| {
        const raw = try a.alloc(u8, chunk_header_size + @as(usize, depth) * 32 + payload.len);
        chunk.* = raw;
        encoded += 1;
        @memcpy(raw[0..8], chunk_magic);
        std.mem.writeInt(u64, raw[8..16], record.sequence, .little);
        std.mem.writeInt(u32, raw[16..20], @intCast(ordinal), .little);
        std.mem.writeInt(u32, raw[20..24], @intCast(count), .little);
        std.mem.writeInt(u16, raw[24..26], families[ordinal], .little);
        raw[26] = depth;
        std.mem.writeInt(u32, raw[27..31], @intCast(payload.len), .little);
        @memcpy(raw[31..63], &manifest.root);
        @memcpy(raw[63..95], &tree[leaves + ordinal]);
        std.mem.writeInt(u32, raw[95..99], std.hash.Crc32.hash(raw[0..95]), .little);
        node = leaves + ordinal;
        for (0..depth) |level| {
            @memcpy(raw[chunk_header_size + level * 32 ..][0..32], &tree[node ^ 1]);
            node /= 2;
        }
        @memcpy(raw[chunk_header_size + @as(usize, depth) * 32 ..], payload);
    }
    return .{ .allocator = a, .manifest = manifest, .chunks = chunks };
}

pub const Chunk = struct { payload: []const u8, families: u16 };
pub fn decodeChunk(manifest: Manifest, ordinal: u32, raw: []const u8) !Chunk {
    if (ordinal >= manifest.count) return error.InvalidReplayChunk;
    if (raw.len < chunk_header_size or !std.mem.eql(u8, raw[0..8], chunk_magic) or
        std.mem.readInt(u32, raw[95..99], .little) != std.hash.Crc32.hash(raw[0..95]) or
        std.mem.readInt(u64, raw[8..16], .little) != manifest.sequence or
        std.mem.readInt(u32, raw[16..20], .little) != ordinal or
        std.mem.readInt(u32, raw[20..24], .little) != manifest.count or
        !std.mem.eql(u8, raw[31..63], &manifest.root)) return error.InvalidReplayChunk;
    var leaves: usize = 1;
    var depth: usize = 0;
    while (leaves < manifest.count) : (depth += 1) leaves *= 2;
    const payload_offset = chunk_header_size + depth * 32;
    const payload_len = std.mem.readInt(u32, raw[27..31], .little);
    const families = std.mem.readInt(u16, raw[24..26], .little);
    const expected_families = if (ordinal + 1 == manifest.count) manifest.families - (manifest.count - 1) * families_per_chunk else families_per_chunk;
    if (ordinal >= manifest.count or raw[26] != depth or families != expected_families or
        raw.len != payload_offset + payload_len) return error.InvalidReplayChunk;
    const payload = raw[payload_offset..];
    var hash = digest(payload);
    if (!std.mem.eql(u8, raw[63..95], &hash)) return error.InvalidReplayChunk;
    var node = leaves + ordinal;
    for (0..depth) |level| {
        const sibling = raw[chunk_header_size + level * 32 ..][0..32].*;
        hash = if (node & 1 == 0) parent(hash, sibling) else parent(sibling, hash);
        node /= 2;
    }
    if (!std.mem.eql(u8, &hash, &manifest.root)) return error.InvalidReplayChunk;
    return .{ .payload = payload, .families = families };
}

/// Legacy/full-record consumers reconstruct once. Bounded fragment consumers
/// use the same collector over only the next authenticated chunks.
pub const Collector = struct {
    arena: std.heap.ArenaAllocator,
    lists: [4]std.ArrayListUnmanaged([]const u8) = @splat(.empty),
    pub fn init(a: Allocator) Collector {
        return .{ .arena = std.heap.ArenaAllocator.init(a) };
    }
    pub fn deinit(self: *Collector) void {
        self.arena.deinit();
    }
    pub fn append(self: *Collector, payload: []const u8) !void {
        const a = self.arena.allocator();
        var decoded = try journal.decodeRecord(a, payload);
        defer decoded.deinit();
        inline for (.{ decoded.record.changed_doc_keys, decoded.record.deleted_doc_keys, decoded.record.overwritten_doc_keys, decoded.record.changed_artifact_keys }, 0..) |list, ordinal| {
            for (list) |key| try self.lists[ordinal].append(a, try a.dupe(u8, key));
        }
    }
    pub fn encode(self: *Collector, a: Allocator, manifest: Manifest) ![]u8 {
        var hint_buffer: [8]journal.TargetHint = undefined;
        var hint_count: usize = 0;
        for (0..8) |ordinal| {
            const hint: journal.TargetHint = @fromBackingInt(@intCast(ordinal));
            if (manifest.hints & journal.singleHintMask(hint) != 0) {
                hint_buffer[hint_count] = hint;
                hint_count += 1;
            }
        }
        const hints = hint_buffer[0..hint_count];
        return journal.encodeRecord(a, self.record(manifest, hints));
    }
    /// Borrow the selected families for one final encoding/projection.
    pub fn record(self: *const Collector, manifest: Manifest, hints: []const journal.TargetHint) journal.Record {
        var result = self.recordAtSequence(manifest.sequence, hints);
        result.version = manifest.version;
        return result;
    }
    /// Normalized, ephemeral derived window; canonical records and versions
    /// have already been independently authenticated/decoded before joining.
    pub fn recordAtSequence(self: *const Collector, sequence: u64, hints: []const journal.TargetHint) journal.Record {
        return .{ .sequence = sequence, .target_hints = hints, .changed_doc_keys = self.lists[0].items, .deleted_doc_keys = self.lists[1].items, .overwritten_doc_keys = self.lists[2].items, .changed_artifact_keys = self.lists[3].items };
    }
};

pub const FamilySlice = struct { payload: []u8, families: usize };

/// Decode at most one fixed-size family chunk, regardless of the record's
/// retained size. Continuation work is bounded by 64 families, including when
/// memory feedback lowers the requested application quantum to eight.
pub fn sliceFamilies(a: Allocator, manifest: Manifest, ordinal: u32, raw: []const u8, skip: usize, limit: usize) !FamilySlice {
    const chunk = try decodeChunk(manifest, ordinal, raw);
    if (skip >= chunk.families or limit == 0) return error.InvalidReplayChunk;
    var decoded = try journal.decodeRecord(a, chunk.payload);
    defer decoded.deinit();
    const record = decoded.record;
    const documents = try a.alloc(types.DerivedDocument, record.changed_doc_keys.len);
    defer a.free(documents);
    for (record.changed_doc_keys, documents) |key, *document| document.* = .{ .key = key };
    var plan = try fragments.Plan.init(a, .{ .sequence = record.sequence, .documents = documents, .deleted_keys = record.deleted_doc_keys, .overwritten_doc_keys = record.overwritten_doc_keys, .changed_artifact_keys = record.changed_artifact_keys }, "");
    defer plan.deinit(a);
    var families: usize = 0;
    var start: usize = 0;
    while (start < plan.items.len) : (start += 1) {
        if (start == 0 or !std.mem.eql(u8, plan.items[start - 1].owner, plan.items[start].owner)) {
            if (families == skip) break;
            families += 1;
        }
    }
    var piece = try plan.piece(a, start, limit);
    defer piece.deinit(a);
    var selected: usize = 0;
    for (plan.items[start..piece.next_item], start..) |item, index| {
        if (index == start or !std.mem.eql(u8, plan.items[index - 1].owner, item.owner)) selected += 1;
    }
    if (selected != @min(limit, @as(usize, chunk.families) - skip)) return error.InvalidReplayChunk;
    const changed = try a.alloc([]const u8, piece.batch.documents.len);
    defer a.free(changed);
    for (piece.batch.documents, changed) |document, *key| key.* = document.key;
    return .{ .families = selected, .payload = try journal.encodeRecord(a, .{ .version = record.version, .sequence = record.sequence, .target_hints = record.target_hints, .changed_doc_keys = changed, .deleted_doc_keys = piece.batch.deleted_keys, .overwritten_doc_keys = piece.batch.overwritten_doc_keys, .changed_artifact_keys = piece.batch.changed_artifact_keys }) };
}

/// Called only after the admission owner has checked every durable consumer
/// and repair/producer retention fence. The cursor is no credit: this one
/// transaction physically deletes bytes and publishes their retained frontier.
pub fn retirePrefix(txn: anytype, manifest: Manifest, completed_families: u64) !bool {
    if (completed_families > manifest.families) return error.InvalidDerivedApplyState;
    const retired: u32 = if (completed_families == manifest.families) manifest.count else @intCast(completed_families / families_per_chunk);
    if (retired <= manifest.retired_chunks) return false;
    for (manifest.retired_chunks..retired) |ordinal| {
        const key = chunkKey(manifest.sequence, @intCast(ordinal));
        try txn.delete(&key);
    }
    var replacement = manifest;
    replacement.retired_chunks = retired;
    const canonical = keys.replayEntryKey(keys.replay_all_kind, manifest.sequence);
    try txn.put(&canonical, &replacement.encode());
    return true;
}

pub fn resolveAll(a: Allocator, txn: anytype, raw: []const u8) ![]u8 {
    const manifest = try Manifest.decode(raw);
    if (manifest.retired_chunks != 0) return error.ReplayHistoryUnavailable;
    var collector = Collector.init(a);
    defer collector.deinit();
    for (0..manifest.count) |ordinal| {
        const key = chunkKey(manifest.sequence, @intCast(ordinal));
        const chunk = try decodeChunk(manifest, @intCast(ordinal), try txn.get(&key));
        try collector.append(chunk.payload);
    }
    return collector.encode(a, manifest);
}

test "seekable replay chunks authenticate bounded family slices and reject mixed generations" {
    const a = std.testing.allocator;
    var key_buffer: [130][16]u8 = undefined;
    var doc_keys: [130][]const u8 = undefined;
    for (&doc_keys, 0..) |*key, ordinal| key.* = try std.fmt.bufPrint(&key_buffer[ordinal], "doc-{d:0>4}", .{ordinal});
    doc_keys[0] = "a";
    doc_keys[1] = "你好";
    doc_keys[2] = "one\x00two";
    doc_keys[3] = "κόσμος";
    var prepared = (try prepare(a, .{ .sequence = 71, .changed_doc_keys = &doc_keys, .deleted_doc_keys = &.{"a"}, .overwritten_doc_keys = &.{"a"}, .target_hints = &.{ .full_text, .algebraic } })).?;
    defer prepared.deinit();
    try std.testing.expectEqual(@as(u32, 3), prepared.manifest.count);
    try std.testing.expectEqualDeep(prepared.manifest, try Manifest.decode(&prepared.manifest.encode()));
    var saw_family = false;
    for (prepared.chunks, 0..) |raw, ordinal| {
        const chunk = try decodeChunk(prepared.manifest, @intCast(ordinal), raw);
        var decoded = try journal.decodeRecord(a, chunk.payload);
        defer decoded.deinit();
        try std.testing.expect(decoded.record.changed_doc_keys.len <= families_per_chunk);
        if (decoded.record.deleted_doc_keys.len != 0) {
            saw_family = true;
            try std.testing.expectEqual(@as(usize, 1), decoded.record.deleted_doc_keys.len);
            try std.testing.expectEqual(@as(usize, 1), decoded.record.overwritten_doc_keys.len);
        }
        const slice = try sliceFamilies(a, prepared.manifest, @intCast(ordinal), raw, 0, 8);
        defer a.free(slice.payload);
        try std.testing.expect(slice.families <= 8);
        raw[raw.len - 1] ^= 1;
        try std.testing.expectError(error.InvalidReplayChunk, decodeChunk(prepared.manifest, @intCast(ordinal), raw));
        raw[raw.len - 1] ^= 1;
        try std.testing.expectError(error.InvalidReplayChunk, decodeChunk(prepared.manifest, @intCast((ordinal + 1) % prepared.manifest.count), raw));
    }
    try std.testing.expect(saw_family);
}
