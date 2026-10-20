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

//! Durable, bounded source-index activation. Clearing and scanning are
//! distinct phases, so an old same-config membership cannot survive re-add.
//! Live primary writes keep indexing throughout both phases. Scan reads the
//! current primary under the owner apply fence, never a stale afterimage.
const std = @import("std");
const index = @import("group_source_index.zig");
const groups = @import("group_sources.zig");
const queue = @import("group_source_queue.zig");
const A = std.mem.Allocator;
pub const Phase = enum { clear, scan, ready };
pub const Progress = struct { generation: u64, incarnation: u64 = 0, phase: Phase = .clear, cursor: []const u8 = "" };
pub const Materializer = struct {
    ptr: *anyopaque,
    read_fn: *const fn (*anyopaque, A, []const u8) anyerror!?std.json.Value,
};
pub fn selectedKey(a: A, definition: index.Definition) ![]u8 {
    var table: [16]u8 = undefined;
    std.mem.writeInt(u128, &table, definition.table_incarnation, .big);
    return std.mem.concat(a, u8, &.{ "\x00\x00G\x07", &table, &definition.configuration });
}
pub fn load(a: A, txn: anytype, definition: index.Definition, generation: u64) !Progress {
    const selected = try selectedKey(a, definition);
    const raw = txn.get(selected) catch |err| if (err == error.NotFound) return .{ .generation = generation, .incarnation = (try @import("group_source_retirement.zig").load(a, txn, 0)).incarnation } else return err;
    const payload = try queue.decode(selected, raw);
    const value = std.json.parseFromSliceLeaky(Progress, a, payload, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.GroupedSourceCorrupt;
    return if (value.generation == generation) value else .{ .generation = generation, .incarnation = value.incarnation };
}
pub fn requireReady(a: A, txn: anytype, definition: index.Definition, generation: u64) !void {
    if ((try load(a, txn, definition, generation)).phase != .ready) return error.GroupedSourceBaselinePending;
}
fn write(a: A, txn: anytype, definition: index.Definition, value: Progress) !void {
    const selected = try selectedKey(a, definition);
    const raw = try std.json.Stringify.valueAlloc(a, value, .{});
    try txn.put(selected, try queue.encode(a, selected, raw));
}
fn fromBase64(a: A, text: []const u8) ![]u8 {
    const decoder = std.base64.standard.Decoder;
    const raw = try a.alloc(u8, decoder.calcSizeForSlice(text) catch return error.GroupedSourceCorrupt);
    decoder.decode(raw, text) catch return error.GroupedSourceCorrupt;
    return raw;
}
fn toBase64(a: A, raw: []const u8) ![]const u8 {
    const encoder = std.base64.standard.Encoder;
    const text = try a.alloc(u8, encoder.calcSize(raw.len));
    return encoder.encode(text, raw);
}
/// `prepare` projects exactly one current row to an optional group member.
/// Physical visits, bytes, effects and stored cursors all have fixed bounds.
pub fn step(comptime Keys: type, a: A, txn: anytype, definition: index.Definition, generation: u64, expected_cursor: []const u8, materializer: Materializer, prepare: anytype) !void {
    const progress = try load(a, txn, definition, generation);
    if (!std.mem.eql(u8, progress.cursor, expected_cursor)) return error.EnrichmentSourceChanged;
    if (progress.phase == .ready) return;
    const page = try preparePage(a, txn, definition, generation);
    try applyPage(Keys, a, txn, definition, generation, page, materializer, prepare);
}
pub fn preparePage(a: A, txn: anytype, definition: index.Definition, generation: u64) !@import("group_source_page.zig").Page {
    const progress = try load(a, txn, definition, generation);
    var table: [16]u8 = undefined;
    std.mem.writeInt(u128, &table, definition.table_incarnation, .big);
    const prefix = if (progress.phase == .clear) try std.mem.concat(a, u8, &.{ index.forward_namespace, &table, &definition.configuration }) else "\x01";
    return @import("group_source_page.zig").prepare(a, txn, generation, progress.incarnation, if (progress.phase == .clear) .clear else .scan, progress.cursor, prefix);
}
pub fn applyPage(comptime Keys: type, a: A, txn: anytype, definition: index.Definition, generation: u64, page: @import("group_source_page.zig").Page, materializer: Materializer, prepare: anytype) !void {
    try page.validate();
    var progress = try load(a, txn, definition, generation);
    if (page.generation != generation or page.incarnation != progress.incarnation or progress.phase == .ready or !std.mem.eql(u8, progress.cursor, page.cursor) or page.phase != (if (progress.phase == .clear) @as(@FieldType(@import("group_source_page.zig").Page, "phase"), .clear) else .scan)) return;
    for (page.candidates) |candidate| {
        const key = try fromBase64(a, candidate.key);
        if (progress.phase == .clear) {
            const document = try index.forwardDocument(a, definition, key);
            if (comptime @hasDecl(@TypeOf(prepare), "owns")) {
                if (!prepare.owns(document)) {
                    try txn.delete(key);
                    continue;
                }
            }
            const projected = try projectOne(a, txn, definition, document, materializer, prepare);
            if (projected == null or !std.mem.eql(u8, key, try index.forwardKey(a, definition, projected.?))) try txn.delete(key);
            continue;
        }
        const raw = txn.get(key) catch |err| if (err == error.NotFound) continue else return err;
        if (!std.mem.eql(u8, &groups.digest(raw), &candidate.digest)) continue;
        const current = .{ .key = key, .value = raw };
        if (Keys.isStoredDocumentRowKey(current.key)) {
            const document = (try Keys.decodeStoredDocumentRowKeyAlloc(a, current.key)) orelse return error.GroupedSourceCorrupt;
            if (comptime @hasDecl(@TypeOf(prepare), "owns")) {
                if (!prepare.owns(document)) {
                    continue;
                }
            }
            _ = try projectOne(a, txn, definition, document, materializer, prepare);
        } else if (@import("group_source_transfer.zig").isKey(current.key)) {
            const transfer = @import("group_source_transfer.zig");
            const owner = try transfer.info(a, current.key);
            if (owner.kind == @import("group_source_identity.zig").reverse_kind and std.mem.eql(u8, owner.suffix, &definition.configuration)) {
                const owned = if (comptime @hasDecl(@TypeOf(prepare), "owns")) prepare.owns(owner.document) else true;
                if (owned) _ = try projectOne(a, txn, definition, owner.document, materializer, prepare);
            } else if (owner.kind == @import("group_source_identity.zig").outbox_kind) {
                const event = try index.readInvalidation(a, current.key, current.value);
                if (std.meta.eql(event.definition, definition)) {
                    const owned = if (comptime @hasDecl(@TypeOf(prepare), "owns")) prepare.owns(owner.document) else true;
                    if (owned) {
                        _ = try index.setDelivery(a, txn, current.key, current.value, false);
                        try transfer.reconstruct(a, txn, definition.table_incarnation, current.key, try txn.get(current.key));
                    } else {
                        try queue.removeSource(a, txn, current.key);
                        try @import("group_source_debt.zig").remove(a, txn, current.key, event);
                        try @import("group_source_order.zig").remove(a, txn, current.key, event);
                    }
                }
            } else if (owner.kind == @import("group_source_identity.zig").metadata_kind and (owner.suffix[0] == 1 or owner.suffix[0] == 2)) {
                const source_config = @import("group_source_identity.zig").configurationOf(owner.document);
                if (source_config != null and std.mem.eql(u8, &source_config.?, &definition.configuration)) {
                    const owned = if (comptime @hasDecl(@TypeOf(prepare), "owns")) prepare.owns(owner.document) else true;
                    if (owned) try transfer.reconstruct(a, txn, definition.table_incarnation, current.key, current.value) else try queue.removeGroup(a, txn, owner.document);
                }
            }
        }
    }
    if (page.done) {
        progress.phase = if (progress.phase == .clear) .scan else .ready;
        progress.cursor = "";
    } else progress.cursor = page.next;
    try write(a, txn, definition, progress);
}

fn projectOne(a: A, txn: anytype, definition: index.Definition, document: []const u8, materializer: Materializer, prepare: anytype) !?index.Projection {
    const value = try materializer.read_fn(materializer.ptr, a, document);
    const projection = try prepare.project(a, document, value);
    const revision = try txn.eventRevision(definition);
    const changed = try index.updateProjection(a, txn, definition, document, revision, 0, projection);
    if (!changed and projection != null) {
        const selected = try index.reverseKey(a, document, definition);
        const raw = try txn.get(selected);
        try @import("group_source_transfer.zig").reconstruct(a, txn, definition.table_incarnation, selected, raw);
        try index.repairPartition(a, txn, definition, projection.?, revision);
    }
    return projection;
}

test "group baseline clears stale covering rows scans current rows in bounded pages and survives restart" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Txn = struct {
        const Self = @This();
        a: A,
        values: std.StringHashMapUnmanaged([]const u8) = .empty,
        const Row = struct { key: []const u8, value: []const u8 };
        const Cursor = struct {
            txn: *Self,
            last: []const u8 = "",
            pub fn close(_: *@This()) void {}
            pub fn seekAtOrAfter(self: *@This(), after: []const u8) !?Row {
                var chosen: ?Row = null;
                var it = self.txn.values.iterator();
                while (it.next()) |entry| {
                    if (std.mem.order(u8, entry.key_ptr.*, after) == .lt) continue;
                    if (chosen == null or std.mem.order(u8, entry.key_ptr.*, chosen.?.key) == .lt) chosen = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
                }
                if (chosen) |row| self.last = row.key;
                return chosen;
            }
            pub fn next(self: *@This()) !?Row {
                return self.seekAtOrAfter(try std.mem.concat(self.txn.a, u8, &.{ self.last, "\x00" }));
            }
        };
        pub fn openPhysicalCursorAdapter(self: *Self) !Cursor {
            return .{ .txn = self };
        }
        pub fn get(self: *Self, selected: []const u8) ![]const u8 {
            return self.values.get(selected) orelse error.NotFound;
        }
        pub fn put(self: *Self, selected: []const u8, raw: []const u8) !void {
            try self.values.put(self.a, try self.a.dupe(u8, selected), try self.a.dupe(u8, raw));
        }
        pub fn delete(self: *Self, selected: []const u8) !void {
            _ = self.values.remove(selected);
        }
        pub fn eventRevision(self: *Self, definition: index.Definition) !u64 {
            return index.nextRevision(self.a, self, definition);
        }
    };
    const Keys = struct {
        pub fn isStoredDocumentRowKey(raw: []const u8) bool {
            return raw.len == 5 and raw[0] == 1 and std.ascii.isDigit(raw[1]) and std.ascii.isDigit(raw[2]) and std.ascii.isDigit(raw[3]) and std.ascii.isDigit(raw[4]);
        }
        pub fn decodeStoredDocumentRowKeyAlloc(alloc: A, raw: []const u8) !?[]u8 {
            return try alloc.dupe(u8, raw[1..]);
        }
    };
    const Fixture = struct {
        txn: *Txn,
        pub fn read(ptr: *anyopaque, alloc: A, document: []const u8) !?std.json.Value {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const selected = try std.mem.concat(alloc, u8, &.{ "\x01", document });
            const raw = self.txn.get(selected) catch |err| return if (err == error.NotFound) null else err;
            return try std.json.parseFromSliceLeaky(std.json.Value, alloc, raw, .{});
        }
        pub fn project(_: @This(), alloc: A, document: []const u8, value: ?std.json.Value) !?index.Projection {
            const row = value orelse return null;
            const config = groups.Config{ .group_by = "thread", .order_by = "ordinal", .segment_by = "turn", .field = "text" };
            const selected = (try groups.selectMember(alloc, config, document, 1, row, true)) orelse return null;
            return .{ .partition = .{ .group = selected.group, .segment = selected.member.segment, .cell = @divFloor(selected.member.ordinal, 128) }, .member = selected.member };
        }
    };
    var txn = Txn{ .a = a };
    var fixture = Fixture{ .txn = &txn };
    const materializer = Materializer{ .ptr = &fixture, .read_fn = Fixture.read };
    const definition = index.Definition{ .table_incarnation = 9, .configuration = groups.digest("config") };
    const stale = index.Projection{ .partition = .{ .group = "old", .segment = "old", .cell = 0 }, .member = .{ .document = "deleted", .version = 1, .ordinal = 1, .segment = "old", .text = "stale" } };
    _ = try index.updateProjection(a, &txn, definition, "deleted", try txn.eventRevision(definition), 1, stale);
    for (0..200) |i| {
        const document = try std.fmt.allocPrint(a, "{d:0>4}", .{i});
        const payload = try std.json.Stringify.valueAlloc(a, .{ .thread = "current", .ordinal = i, .turn = "turn", .text = "current" }, .{});
        try txn.put(try std.mem.concat(a, u8, &.{ "\x01", document }), payload);
    }
    // Physical derived keys can differ across replicas. Only the leader's
    // selected page controls the replicated cursor and authoritative effects.
    var replica = Txn{ .a = a, .values = try txn.values.clone(a) };
    try replica.put("\x010000-derived-only-on-follower", "local artifact");
    var replica_fixture = Fixture{ .txn = &replica };
    const replica_materializer = Materializer{ .ptr = &replica_fixture, .read_fn = Fixture.read };
    const clear_page = try preparePage(a, &txn, definition, 3);
    try applyPage(Keys, a, &replica, definition, 3, clear_page, replica_materializer, replica_fixture);
    try std.testing.expectError(error.GroupedSourceBaselinePending, requireReady(a, &txn, definition, 3));
    try step(Keys, a, &txn, definition, 3, "", materializer, fixture);
    try std.testing.expectError(error.NotFound, txn.get(try index.forwardKey(a, definition, stale)));
    try std.testing.expectEqual(Phase.scan, (try load(a, &txn, definition, 3)).phase);
    const selected_page = try preparePage(a, &txn, definition, 3);
    try applyPage(Keys, a, &replica, definition, 3, selected_page, replica_materializer, replica_fixture);

    try step(Keys, a, &txn, definition, 3, "", materializer, fixture);
    const resumed = try load(a, &txn, definition, 3);
    const replica_progress = try load(a, &replica, definition, 3);
    try std.testing.expectEqualStrings(resumed.cursor, replica_progress.cursor);
    try std.testing.expectEqual(resumed.phase, replica_progress.phase);

    try std.testing.expectEqual(Phase.scan, resumed.phase);
    try std.testing.expect(resumed.cursor.len > 0);
    // A later primary update is indexed before baseline reaches that row.
    try txn.put("\x010199", "{\"thread\":\"current\",\"ordinal\":199,\"turn\":\"turn\",\"text\":\"updated\"}");
    const projection = try fixture.project(a, "0199", try Fixture.read(&fixture, a, "0199"));
    _ = try index.updateProjection(a, &txn, definition, "0199", try txn.eventRevision(definition), 10, projection);
    try step(Keys, a, &txn, definition, 3, resumed.cursor, materializer, fixture);
    var passes: usize = 0;
    while ((try load(a, &txn, definition, 3)).phase != .ready) : (passes += 1) {
        try std.testing.expect(passes < 16);
        const current = try load(a, &txn, definition, 3);
        try step(Keys, a, &txn, definition, 3, current.cursor, materializer, fixture);
    }
    try requireReady(a, &txn, definition, 3);
    const point = try index.forwardKey(a, definition, projection.?);
    try std.testing.expectEqualStrings("updated", (try index.readProjection(a, definition, point, try txn.get(point))).member.text);
    try std.testing.expectError(error.EnrichmentSourceChanged, step(Keys, a, &txn, definition, 3, resumed.cursor, materializer, fixture));
    try invalidate(a, &txn, definition);
    try step(Keys, a, &txn, definition, 3, "", materializer, fixture);
    while ((try load(a, &txn, definition, 3)).phase == .clear) {
        const clearing = try load(a, &txn, definition, 3);
        try step(Keys, a, &txn, definition, 3, clearing.cursor, materializer, fixture);
    }
    const readded = try load(a, &txn, definition, 3);
    try std.testing.expectEqual(Phase.scan, readded.phase);
    try std.testing.expectEqual(@as(u64, 1), readded.incarnation);
    try applyPage(Keys, a, &txn, definition, 3, selected_page, materializer, fixture);
    const rejected = try load(a, &txn, definition, 3);
    try std.testing.expectEqualStrings(readded.cursor, rejected.cursor);
    try std.testing.expectEqual(readded.phase, rejected.phase);
    // Retirement may collect the per-definition progress. Its next activation
    // still inherits the table's monotone durable DDL incarnation, not zero.
    const retired_page = try preparePage(a, &txn, definition, 3);
    try txn.delete(try selectedKey(a, definition));
    try @import("group_source_retirement.zig").invalidate(a, &txn);
    try @import("group_source_retirement.zig").invalidate(a, &txn);
    const after_gc = try load(a, &txn, definition, 3);
    try std.testing.expect(after_gc.incarnation > retired_page.incarnation);
    try applyPage(Keys, a, &txn, definition, 3, retired_page, materializer, fixture);
    try std.testing.expectEqual(Phase.clear, (try load(a, &txn, definition, 3)).phase);
    const source_revision_before = (try index.readReverse(a, definition, try index.reverseKey(a, "0199", definition), try txn.get(try index.reverseKey(a, "0199", definition)))).member.version;
    var pending_keys: std.ArrayList([]const u8) = .empty;
    var pending_it = txn.values.iterator();
    while (pending_it.next()) |entry| if (@import("group_source_transfer.zig").isKey(entry.key_ptr.*)) {
        if ((try @import("group_source_transfer.zig").info(a, entry.key_ptr.*)).kind == @import("group_source_identity.zig").outbox_kind) try pending_keys.append(a, entry.key_ptr.*);
    };
    for (pending_keys.items) |selected| _ = try index.acknowledge(a, &txn, selected, try txn.get(selected));
    while ((try load(a, &txn, definition, 3)).phase != .ready) {
        const current = try load(a, &txn, definition, 3);
        try step(Keys, a, &txn, definition, 3, current.cursor, materializer, fixture);
    }
    const stable = try index.readReverse(a, definition, try index.reverseKey(a, "0199", definition), try txn.get(try index.reverseKey(a, "0199", definition)));
    try std.testing.expectEqual(source_revision_before, stable.member.version);
    var repairs: usize = 0;
    var repaired_it = txn.values.iterator();
    while (repaired_it.next()) |entry| if (@import("group_source_transfer.zig").isKey(entry.key_ptr.*)) {
        if ((try @import("group_source_transfer.zig").info(a, entry.key_ptr.*)).kind != @import("group_source_identity.zig").outbox_kind) continue;
        try std.testing.expect((try index.readInvalidation(a, entry.key_ptr.*, entry.value_ptr.*)).repair);
        repairs += 1;
    };
    try std.testing.expectEqual(@as(usize, 200), repairs);
}

/// Catalog activation and range ownership both change the covering index's
/// baseline obligation. A reused physical root must not inherit readiness
/// for a different logical range after split or merge.
pub fn activationGeneration(catalog_generation: u64, start: []const u8, end: []const u8) u64 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, catalog_generation, .big);
    hash.update(&bytes);
    for ([_][]const u8{ start, end }) |component| {
        std.mem.writeInt(u64, &bytes, component.len, .big);
        hash.update(&bytes);
        hash.update(component);
    }
    return std.mem.readInt(u64, hash.finalResult()[0..8], .big);
}

/// Stable across process restart and identical replicas. Schema changes require
/// a new source projection; catalog removal separately invalidates readiness.
pub fn sourceGeneration(schema_version: u64, namespace: [24]u8, start: []const u8, end: []const u8, root: u128) u64 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(&namespace);
    var owner: [16]u8 = undefined;
    std.mem.writeInt(u128, &owner, @import("group_source_owner_identity.zig").incarnation(namespace, root), .big);
    hash.update(&owner);
    var value: [8]u8 = undefined;
    std.mem.writeInt(u64, &value, activationGeneration(schema_version, start, end), .big);
    hash.update(&value);
    return std.mem.readInt(u64, hash.finalResult()[0..8], .big);
}
pub fn retirementGeneration(source: u64, digest: [32]u8) u64 {
    return activationGeneration(source, &digest, "");
}

/// Preserve the incarnation when resetting readiness so a delayed page from
/// a removed identical definition cannot advance a later baseline.
pub fn invalidate(a: A, txn: anytype, definition: index.Definition) !void {
    var progress = try load(a, txn, definition, 0);
    progress.incarnation = try std.math.add(u64, progress.incarnation, 1);
    try write(a, txn, definition, progress);
}

test "group baseline scope survives replica restart and fences Lite root replacement" {
    const ns: [24]u8 = @splat(1);
    try std.testing.expectEqual(sourceGeneration(4, ns, "a", "m", 1), sourceGeneration(4, ns, "a", "m", 999));
    try std.testing.expect(sourceGeneration(4, ns, "a", "m", 1) != sourceGeneration(4, ns, "a", "z", 1));
    try std.testing.expect(sourceGeneration(4, @splat(0), "", "", 1) != sourceGeneration(4, @splat(0), "", "", 999));
}
