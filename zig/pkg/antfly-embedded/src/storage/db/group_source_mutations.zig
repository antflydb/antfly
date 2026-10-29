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

//! Immutable group projection plans and primary-batch index effects. Filters
//! compile once per catalog generation. Point reads run under the primary
//! apply fence; generated mutations join the source row's existing atomic
//! backend batch, including transaction resolution and replicated writes.
const std = @import("std");
const groups = @import("group_sources.zig");
const index = @import("group_source_index.zig");
const graph = @import("query/graph_exec.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    entries: []const Entry,
    pub const Entry = struct {
        name: []const u8,
        source: groups.Config,
        configuration: groups.Digest,
        horizon: u16 = 0,
        horizons: []const u16 = &.{},
        filter: graph.CompiledPatternFilter,
    };
    pub fn init(a: A, templates: anytype) !Plan {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var entries: std.ArrayList(Entry) = .empty;
        for (templates) |template| {
            if (template.group_source_json.len == 0) continue;
            if (entries.items.len == 32) return error.GroupedSourceLimitExceeded;
            const source = try std.json.parseFromSliceLeaky(groups.Config, owned, template.group_source_json, .{ .allocate = .alloc_always });
            try source.validate();
            const name = try owned.dupe(u8, template.artifact_name);
            var horizon: u16 = 0;
            var horizons: std.ArrayList(u16) = .empty;
            for (templates) |consumer| {
                if (consumer.prior_context_json.len == 0) continue;
                var upstream = consumer.upstream_artifact_name;
                var depth: usize = 0;
                while (upstream.len > 0 and depth < templates.len) : (depth += 1) {
                    if (std.mem.eql(u8, upstream, name)) {
                        const prior = try std.json.parseFromSliceLeaky(groups.PriorContext, owned, consumer.prior_context_json, .{});
                        horizon = @max(horizon, prior.horizon_segments);
                        if (std.mem.indexOfScalar(u16, horizons.items, prior.horizon_segments) == null) try horizons.append(owned, prior.horizon_segments);
                        break;
                    }
                    var next: []const u8 = "";
                    for (templates) |candidate| if (std.mem.eql(u8, candidate.artifact_name, upstream)) {
                        next = candidate.upstream_artifact_name;
                        break;
                    };
                    upstream = next;
                }
            }
            try entries.append(owned, .{ .horizon = horizon, .horizons = try horizons.toOwnedSlice(owned), .name = name, .source = source, .configuration = groups.definitionDigest(name, source), .filter = if (source.filter == .null) .match_all else try graph.compilePatternFilter(owned, try groups.normalizedFilter(owned, source.filter)) });
        }
        return .{ .arena = arena, .entries = try entries.toOwnedSlice(owned) };
    }
    pub const Change = struct { entry: usize, projection: ?index.Projection };
    pub fn prepareRow(self: *const Plan, a: A, document: []const u8, value: V) ![]const Change {
        if (isVirtual(document)) return error.InvalidBatchRequest;
        const changes = try a.alloc(Change, self.entries.len);
        for (self.entries, changes, 0..) |entry, *change, i| {
            const matches = try entry.filter.matches(a, document, value);
            const selection = try groups.selectMember(a, entry.source, document, 1, value, matches);
            change.* = .{ .entry = i, .projection = if (selection) |selected| .{ .partition = .{ .group = try a.dupe(u8, selected.group), .segment = try a.dupe(u8, selected.member.segment), .cell = @divFloor(selected.member.ordinal, @as(i64, entry.source.window_ordinals)) }, .member = .{ .document = try a.dupe(u8, document), .text = try a.dupe(u8, selected.member.text), .ordinal = selected.member.ordinal, .segment = try a.dupe(u8, selected.member.segment), .version = 1 } } else null };
        }
        return changes;
    }
    pub const Projector = struct {
        entry: Entry,
        bounds: @import("group_source_reads.zig").Bounds = .{},
        pub fn owns(self: @This(), document: []const u8) bool {
            return self.bounds.contains(document);
        }
        pub fn project(self: @This(), a: A, document: []const u8, value: ?V) !?index.Projection {
            if (!self.owns(document)) return null;
            const row = value orelse return null;
            const selected = (try groups.selectMember(a, self.entry.source, document, 1, row, try self.entry.filter.matches(a, document, row))) orelse return null;
            return .{ .partition = .{ .group = selected.group, .segment = selected.member.segment, .cell = @divFloor(selected.member.ordinal, @as(i64, self.entry.source.window_ordinals)) }, .member = selected.member };
        }
    };
    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
const DocStore = @import("../docstore.zig").DocStore;
pub const OwnedCursor = struct {
    allocator: A,
    read: *DocStore.Txn,
    cursor: DocStore.Txn.CursorAdapter,
    pub fn seekAtOrAfter(self: *@This(), selected: []const u8) !?@import("../backend_erased.zig").Entry {
        return self.cursor.seekAtOrAfter(selected);
    }
    pub fn next(self: *@This()) !?@import("../backend_erased.zig").Entry {
        return self.cursor.next();
    }
    pub fn prev(self: *@This()) !?@import("../backend_erased.zig").Entry {
        return self.cursor.prev();
    }
    pub fn seekAtOrBefore(self: *@This(), selected: []const u8) !?@import("../backend_erased.zig").Entry {
        return self.cursor.seekAtOrBefore(selected);
    }
    pub fn close(self: *@This()) void {
        self.cursor.close();
        self.read.abort();
        self.allocator.destroy(self.read);
    }
};
pub const Reader = struct {
    ptr: *anyopaque,
    get_fn: *const fn (*anyopaque, A, []const u8) anyerror![]const u8,
    cursor_fn: *const fn (*anyopaque, A) anyerror!OwnedCursor,
    pub fn fromStore(store: anytype) Reader {
        const T = @TypeOf(store);
        const Bridge = struct {
            fn cursor(ptr: *anyopaque, a: A) !OwnedCursor {
                const typed: T = @ptrCast(@alignCast(ptr));
                const read = try a.create(DocStore.Txn);
                errdefer a.destroy(read);
                read.* = try typed.beginReadTxn();
                errdefer read.abort();
                return .{ .allocator = a, .read = read, .cursor = try read.openPhysicalCursorAdapter() };
            }
            fn get(ptr: *anyopaque, a: A, key: []const u8) ![]const u8 {
                const typed: T = @ptrCast(@alignCast(ptr));
                return typed.get(a, key);
            }
        };
        return .{ .ptr = store, .get_fn = Bridge.get, .cursor_fn = Bridge.cursor };
    }
};
pub const Mutation = struct { key: []const u8, value: ?[]const u8 };
pub const Overlay = struct {
    allocator: A,
    reader: Reader,
    effects: std.StringHashMapUnmanaged(?[]const u8) = .empty,
    native_revision: ?u64 = null,
    legacy_completion: bool = false,
    completion_root: u128 = 0,
    source_revision: ?u64 = null,
    owner_incarnation: u128 = 1,
    pub fn groupIncarnation(self: *const Overlay) u128 {
        return self.owner_incarnation;
    }
    pub fn openPhysicalCursorAdapter(self: *Overlay) !OwnedCursor {
        return self.reader.cursor_fn(self.reader.ptr, self.allocator);
    }
    pub fn get(self: *Overlay, key: []const u8) ![]const u8 {
        if (self.effects.getEntry(key)) |entry| return entry.value_ptr.* orelse error.NotFound;
        return self.reader.get_fn(self.reader.ptr, self.allocator, key);
    }
    pub fn put(self: *Overlay, key: []const u8, value: []const u8) !void {
        try self.effects.put(self.allocator, try self.allocator.dupe(u8, key), try self.allocator.dupe(u8, value));
    }
    pub fn delete(self: *Overlay, key: []const u8) !void {
        try self.effects.put(self.allocator, try self.allocator.dupe(u8, key), null);
    }
    pub fn sourceSettled(self: *Overlay, document: []const u8) !bool {
        const authority = (try @import("artifact/publication.zig").authority(self)) orelse return self.legacy_completion;
        const obligations = @import("artifact/producer_obligations.zig");
        const progress = (try obligations.load(self)) orelse return false;
        try progress.requireAuthority(authority);
        if (!progress.baseline_complete) return false;
        return @import("artifact/completion_progress.zig").groupReady(self.allocator, self, self.completion_root, document);
    }
    pub fn eventRevision(self: *Overlay, definition: index.Definition) !u64 {
        if (self.native_revision) |value| return value;
        const value = try index.reserveRevision(self.allocator, self, definition, self.source_revision);
        self.native_revision = value;
        return value;
    }

    pub fn stage(self: *Overlay, plan: *const Plan, table: u128, document: []const u8, revision: u64, value: ?V) !void {
        if (isVirtual(document)) return error.InvalidBatchRequest;
        for (plan.entries) |entry| {
            const definition = index.Definition{ .table_incarnation = table, .configuration = entry.configuration };
            const source_revision = try self.eventRevision(definition);
            const matches = if (value) |row| try entry.filter.matches(self.allocator, document, row) else false;
            const selection = if (value) |row| try groups.selectMember(self.allocator, entry.source, document, source_revision, row, matches) else null;
            _ = try index.updateProjection(self.allocator, self, definition, document, source_revision, revision, if (selection) |selected| .{ .partition = .{ .group = selected.group, .segment = selected.member.segment, .cell = @divFloor(selected.member.ordinal, @as(i64, entry.source.window_ordinals)) }, .member = selected.member } else null);
        }
    }
};
pub fn isVirtual(document: []const u8) bool {
    return @import("group_source_state.zig").isAnchor(document);
}

pub const table_identity_key = "\x00\x00__group_source_v1:table-identity";
pub fn tableIdentity(overlay: *Overlay, table_id: u64, root_incarnation: u128) !u128 {
    if (table_id != 0) return table_id;
    if (overlay.get(table_identity_key)) |raw| {
        if (raw.len != 48 or !std.mem.eql(u8, raw[16..], &groups.digest(raw[0..16]))) return error.GroupedSourceCorrupt;
        const value = std.mem.readInt(u128, raw[0..16], .big);
        if (value == 0) return error.GroupedSourceCorrupt;
        return value;
    } else |err| if (err != error.NotFound) return err;
    if (root_incarnation == 0) return error.DurableRootIncarnationUnavailable;
    var raw: [48]u8 = undefined;
    std.mem.writeInt(u128, raw[0..16], root_incarnation, .big);
    @memcpy(raw[16..], &groups.digest(raw[0..16]));
    try overlay.put(table_identity_key, &raw);
    return root_incarnation;
}
pub fn stagePrepared(overlay: *Overlay, plan: *const Plan, table: u128, document: []const u8, revision: u64, changes: []const Plan.Change) !void {
    for (changes) |change| {
        if (change.entry >= plan.entries.len) return error.InvalidGroupedSourceMutation;
        const entry = plan.entries[change.entry];
        const definition = index.Definition{ .table_incarnation = table, .configuration = entry.configuration };
        _ = try index.updateProjection(overlay.allocator, overlay, definition, document, try overlay.eventRevision(definition), revision, change.projection);
    }
}

pub fn currentRow(core: anytype, a: A, document: []const u8) !?V {
    var view = core.acquireSchemaView();
    defer if (view) |*value| value.release();
    const relational = if (view) |value| value.storageMode() == .relational else false;
    const rows = @import("relational_store.zig");
    const selected = if (relational) try rows.keyAlloc(a, document) else try @import("../internal_keys.zig").documentKeyAlloc(a, document);
    const raw = core.store.get(a, selected) catch |err| return if (err == error.NotFound) null else err;
    const bytes = if (relational) blk: {
        const version = try rows.rowSchemaVersion(raw);
        var historical = (try core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
        defer historical.release();
        break :blk try rows.decodeValueForSchemaAndLayoutAlloc(a, raw, historical.tableSchema().*, historical.physicalLayout());
    } else raw;
    return try std.json.parseFromSliceLeaky(V, a, bytes, .{ .allocate = .alloc_always });
}
