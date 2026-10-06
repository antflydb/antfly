// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Conservative physical-family facts, independent of the live catalog.
//! Mutations never infer absence. Only a complete bounded key walk, whose
//! per-family mutation epoch still matches at commit, may certify absence.
const std = @import("std");
const keys = @import("internal_keys.zig");
pub const key = "\x00\x00__metadata__:artifact_footprint_v1";
pub const scan_key = "\x00\x00__metadata__:artifact_footprint_scan_v1";
pub const Family = enum(u2) { graph, vector, generated, resolution };
pub const all: u8 = 15;
pub fn bit(family: Family) u8 {
    return @as(u8, 1) << @backingInt(family);
}
pub fn isKey(candidate: []const u8) bool {
    return std.mem.eql(u8, candidate, key) or std.mem.eql(u8, candidate, scan_key);
}

pub fn classify(candidate: []const u8) ?Family {
    if (!keys.isInternalUserKey(candidate)) return null;
    const doc_end = (keys.findComponentTerminator(candidate, 1) orelse return null) + 2;
    if (doc_end >= candidate.len) return null;
    // Match family prefixes, not only well-formed complete records. A corrupt
    // graph suffix cannot turn a physical graph owner into an absence proof.
    return switch (candidate[doc_end]) {
        keys.graph_asset_state_kind, keys.graph_edge_contender_kind, keys.graph_global_edge_contender_kind => .graph,
        keys.promoted_keys_state_kind => .resolution,
        keys.extraction_stream_manifest_kind, keys.extraction_generation_row_kind, keys.extraction_generation_head_kind, keys.extraction_generation_state_kind, keys.extraction_generation_clock_kind => .generated,
        keys.extraction_generation_name_kind, keys.extraction_generation_ordinal_kind, keys.extraction_generation_directory_kind => .generated,
        keys.asset_state_kind, keys.document_unit_navigation_summary_kind, keys.document_unit_navigation_block_kind, keys.pdf_page_embedding_stage_kind, keys.document_extraction_unit_spool_kind, keys.shared_pdf_consumer_kind, keys.producer_stream_manifest_kind, keys.producer_generation_row_kind, keys.producer_generation_head_kind, keys.producer_generation_state_kind, keys.producer_generation_clock_kind => .generated,
        keys.artifact_kind => if (keys.componentEquals(candidate, doc_end + 1, "graph")) .graph else if (keys.isEmbeddingArtifactKey(candidate)) .vector else if (keys.componentEquals(candidate, doc_end + 1, "resolution")) .resolution else .generated,
        else => null,
    };
}

pub const State = struct {
    epochs: [4]u64 = @splat(0),
    certified: u8 = 0,
    present: u8 = 0,
    pub fn encode(self: State) [80]u8 {
        var out: [80]u8 = @splat(0);
        @memcpy(out[0..8], "AFOOT001");
        for (self.epochs, 0..) |epoch, i| std.mem.writeInt(u64, out[8 + i * 8 ..][0..8], epoch, .little);
        out[40] = self.certified;
        out[41] = self.present;
        std.crypto.hash.Blake3.hash(out[0..48], out[48..80], .{});
        return out;
    }
    pub fn decode(raw: []const u8) !State {
        if (raw.len != 80 or !std.mem.eql(u8, raw[0..8], "AFOOT001") or raw[40] & ~all != 0 or raw[41] & ~all != 0 or
            !std.mem.allEqual(u8, raw[42..48], 0)) return error.ArtifactCatalogCorrupt;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(raw[0..48], &digest, .{});
        if (!std.mem.eql(u8, &digest, raw[48..])) return error.ArtifactCatalogCorrupt;
        var result: State = .{ .certified = raw[40], .present = raw[41] };
        for (&result.epochs, 0..) |*epoch, i| epoch.* = std.mem.readInt(u64, raw[8 + i * 8 ..][0..8], .little);
        return result;
    }
    pub fn observe(self: *State, puts: u8, deletes: u8) !void {
        for (&self.epochs, 0..) |*epoch, i| {
            const mask = @as(u8, 1) << @as(u3, @intCast(i));
            if ((puts | deletes) & mask == 0) continue;
            epoch.* = std.math.add(u64, epoch.*, 1) catch return error.ArtifactCatalogCorrupt;
        }
        self.certified = (self.certified | puts) & ~deletes;
        self.present |= puts;
    }
    pub fn finish(self: *State, epochs: [4]u64, seen: u8) void {
        for (self.epochs, epochs, 0..) |current, expected, i| {
            const mask = @as(u8, 1) << @as(u3, @intCast(i));
            if (current != expected) continue;
            self.certified |= mask;
            self.present = (self.present & ~mask) | (seen & mask);
        }
    }
};

pub fn load(txn: anytype) !State {
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return .{},
        else => return err,
    };
    return State.decode(raw);
}

/// Empty-root proof is one physical seek, repeated under the writer lock.
/// Existing roots without a record remain unknown until bounded reconciliation.
pub fn initializeEmpty(store: anytype) !void {
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openPhysicalCursorAdapter();
        defer cursor.close();
        if (try cursor.seekAtOrAfter("") != null) return;
    }
    var write = store.beginWriteTxn() catch |err| switch (err) {
        error.ReadOnly => return,
        else => return err,
    };
    errdefer write.abort();
    var cursor = try write.openPhysicalCursorAdapter();
    const empty = (try cursor.seekAtOrAfter("")) == null;
    cursor.close();
    if (!empty) {
        write.abort();
        return;
    }
    try write.put(key, &(State{ .certified = all }).encode());
    try write.commit();
}

/// Publication/import code can conservatively discard certification. Advancing
/// epochs prevents a pre-import maintenance cursor from restoring stale absence.
pub fn invalidate(store: anytype) !void {
    var write = try store.beginWriteTxn();
    errdefer write.abort();
    var state = try load(&write);
    try state.observe(0, all);
    try write.put(key, &state.encode());
    write.delete(scan_key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    try write.commit();
}

/// Constant-space transaction capture. One point read/write per transaction
/// touching artifacts, not one old-value probe per artifact. Mixed puts and
/// deletes deliberately remain uncertified, including same-key replacements.
pub const Capture = struct {
    puts: u8 = 0,
    deletes: u8 = 0,
    staged: bool = false,
    pub fn touch(self: *Capture, candidate: []const u8, value: ?[]const u8) !void {
        const family = classify(candidate) orelse return;
        if (self.staged) return error.RetainedEffectsMixedControl;
        if (value != null) self.puts |= bit(family) else self.deletes |= bit(family);
    }
    pub fn stage(self: *Capture, txn: anytype) !void {
        if (self.staged or self.puts | self.deletes == 0) return;
        var state = try load(txn);
        try state.observe(self.puts, self.deletes);
        try txn.put(key, &state.encode());
        self.staged = true;
    }
};

// Match the transfer cursor's public physical-key ceiling. A larger key is
// unsupported, not perpetual maintenance debt. Callers charge heap buffers.
const max_cursor = 1024 * 1024;
const Scan = struct {
    epochs: [4]u64,
    seen: u8 = 0,
    cursor: []const u8 = "",
    fn encode(self: Scan, alloc: std.mem.Allocator) ![]u8 {
        if (self.cursor.len > max_cursor) return error.OnlineMergeArtifactTailsUnsupported;
        const out = try alloc.alloc(u8, 73 + self.cursor.len);
        @memcpy(out[0..4], "AFS1");
        for (self.epochs, 0..) |epoch, i| std.mem.writeInt(u64, out[4 + i * 8 ..][0..8], epoch, .little);
        out[36] = self.seen;
        std.mem.writeInt(u32, out[37..41], @intCast(self.cursor.len), .little);
        @memcpy(out[41 .. out.len - 32], self.cursor);
        std.crypto.hash.Blake3.hash(out[0 .. out.len - 32], out[out.len - 32 ..][0..32], .{});
        return out;
    }
    fn decode(raw: []const u8) !Scan {
        if (raw.len < 73 or raw.len > 73 + max_cursor or !std.mem.eql(u8, raw[0..4], "AFS1") or
            raw[36] & ~all != 0 or std.mem.readInt(u32, raw[37..41], .little) != raw.len - 73) return error.ArtifactCatalogCorrupt;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(raw[0 .. raw.len - 32], &digest, .{});
        if (!std.mem.eql(u8, &digest, raw[raw.len - 32 ..])) return error.ArtifactCatalogCorrupt;
        var result: Scan = .{ .epochs = undefined, .seen = raw[36], .cursor = raw[41 .. raw.len - 32] };
        for (&result.epochs, 0..) |*epoch, i| epoch.* = std.mem.readInt(u64, raw[4 + i * 8 ..][0..8], .little);
        return result;
    }
};

/// One physical-key page, outside the DB apply/admission lock. No value
/// hydration, no full scan, and no pinned read transaction survives the call.
/// Durable cursor CAS prevents concurrent maintenance slices losing progress.
pub fn reconcilePage(alloc: std.mem.Allocator, store: anytype) !bool {
    var read = try store.beginReadTxnWithBlockCacheAdmission(.transient);
    defer read.abort();
    const initial = try load(&read);
    if (initial.certified == all) return true;
    const previous = read.get(scan_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    var scan: Scan = if (previous) |raw| try Scan.decode(raw) else .{ .epochs = initial.epochs };
    var cursor = try read.openPhysicalCursorAdapter();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(scan.cursor);
    if (entry) |item| if (scan.cursor.len != 0 and std.mem.eql(u8, item.key, scan.cursor)) {
        entry = try cursor.next();
    };
    var last: std.ArrayList(u8) = .empty;
    defer last.deinit(alloc);
    var count: usize = 0;
    var key_bytes: usize = 0;
    const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
    while (entry) |item| {
        if (count != 0 and (count >= 128 or key_bytes >= 64 * 1024 or item.key.len > 64 * 1024 or @import("antfly_platform").time.monotonicNs() >= deadline)) break;
        if (item.key.len > max_cursor) return error.OnlineMergeArtifactTailsUnsupported;
        if (classify(item.key)) |family| scan.seen |= bit(family);
        try last.resize(alloc, item.key.len);
        @memcpy(last.items, item.key);
        key_bytes += item.key.len;
        count += 1;
        entry = try cursor.next();
    }
    if (count != 0) scan.cursor = last.items;
    var write = try store.beginWriteTxn();
    errdefer write.abort();
    const current_progress = write.get(scan_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    const same = if (previous) |old| if (current_progress) |now| std.mem.eql(u8, old, now) else false else current_progress == null;
    if (!same) {
        write.abort();
        return false;
    }
    var current = try load(&write);
    if (entry == null) {
        current.finish(scan.epochs, scan.seen);
        try write.put(key, &current.encode());
        write.delete(scan_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    } else {
        const encoded = try scan.encode(alloc);
        defer alloc.free(encoded);
        try write.put(scan_key, encoded);
    }
    try write.commit();
    return current.certified == all;
}

test "artifact footprint absence requires matching family epoch and checksum" {
    var state: State = .{};
    const epochs = state.epochs;
    try state.observe(bit(.graph), 0);
    state.finish(epochs, 0);
    try std.testing.expectEqual(all, state.certified);
    try std.testing.expectEqual(bit(.graph), state.present);
    try state.observe(0, bit(.graph));
    try std.testing.expectEqual(all & ~bit(.graph), state.certified);
    state.finish(state.epochs, 0);
    try std.testing.expectEqual(@as(u8, 0), state.present);
    var encoded = state.encode();
    try std.testing.expectEqualDeep(state, try State.decode(&encoded));
    encoded[41] ^= 1;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, State.decode(&encoded));
    const stable_graph_epoch = state.epochs;
    try state.observe(bit(.vector), 0);
    state.finish(stable_graph_epoch, 0);
    try std.testing.expect(state.certified & bit(.graph) != 0);
    try std.testing.expect(state.present & bit(.graph) == 0);
    try std.testing.expect(state.present & bit(.vector) != 0);
}

test "artifact footprint capture coalesces physical mutations without old artifact reads" {
    const Fake = struct {
        raw: [80]u8,
        reads: usize = 0,
        writes: usize = 0,
        pub fn get(self: *@This(), candidate: []const u8) anyerror![]const u8 {
            try std.testing.expectEqualStrings(key, candidate);
            self.reads += 1;
            return &self.raw;
        }
        pub fn put(self: *@This(), candidate: []const u8, value: []const u8) !void {
            try std.testing.expectEqualStrings(key, candidate);
            _ = try State.decode(value);
            @memcpy(&self.raw, value);
            self.writes += 1;
        }
    };
    var txn: Fake = .{ .raw = (State{ .certified = all }).encode() };
    const graph = try keys.graphEdgeArtifactKeyAlloc(std.testing.allocator, "doc", "graph", "edge", "target");
    defer std.testing.allocator.free(graph);
    var capture: Capture = .{};
    for (0..1000) |_| try capture.touch(graph, "large values are never read by footprint capture");
    try std.testing.expectEqual(@as(usize, 0), txn.reads);
    try capture.stage(&txn);
    try capture.stage(&txn);
    try std.testing.expectEqual(@as(usize, 1), txn.reads);
    try std.testing.expectEqual(@as(usize, 1), txn.writes);
    try std.testing.expectEqual(bit(.graph), (try State.decode(&txn.raw)).present);
    try std.testing.expectEqual(Family.graph, classify(graph[0 .. graph.len - 1]).?);
    var deletion: Capture = .{};
    try deletion.touch(graph, null);
    try deletion.touch(graph, "same-transaction replacement stays conservative");
    try deletion.stage(&txn);
    try std.testing.expectEqual(@as(u8, 0), (try State.decode(&txn.raw)).certified & bit(.graph));
}

test "artifact footprint durable cursor checksum rejects forged progress" {
    const scan: Scan = .{ .epochs = .{ 1, 2, 3, 4 }, .seen = bit(.graph), .cursor = "bounded-last-key" };
    const encoded = try scan.encode(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    const decoded = try Scan.decode(encoded);
    try std.testing.expectEqualDeep(scan.epochs, decoded.epochs);
    try std.testing.expectEqual(scan.seen, decoded.seen);
    try std.testing.expectEqualStrings(scan.cursor, decoded.cursor);
    encoded[41] ^= 1;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, Scan.decode(encoded));
}

test "artifact footprint cursor supports large bounded keys and rejects unsupported size" {
    const alloc = std.testing.allocator;
    const large = try alloc.alloc(u8, max_cursor + 1);
    defer alloc.free(large);
    @memset(large, 'k');
    const scan: Scan = .{ .epochs = @splat(1), .cursor = large[0..max_cursor] };
    const encoded = try scan.encode(alloc);
    defer alloc.free(encoded);
    try std.testing.expectEqualSlices(u8, scan.cursor, (try Scan.decode(encoded)).cursor);
    try std.testing.expectError(error.OnlineMergeArtifactTailsUnsupported, (Scan{ .epochs = @splat(1), .cursor = large }).encode(alloc));
}
