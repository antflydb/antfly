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

//! Covering relationship-type postings. Values are empty: metric topology
//! needs edge identity, not timestamps, weights or document metadata. Each
//! type range preserves the original reverse-key partition order.
const std = @import("std");
const backend = @import("../storage/backend_erased.zig");
const keys = @import("../storage/internal_keys.zig");
const Allocator = std.mem.Allocator;
pub const prefix = "meta:metric_type_edges:v2/";
pub const node_prefix = "meta:metric_type_nodes:v2/";
pub const ready_key = "meta:metric_type_edges_ready:v2";
pub const cursor_key = "meta:metric_type_edges_cursor:v2";
pub const active_key = "meta:metric_type_edges_active:v2";

pub const MaintenanceState = struct { active: bool, ready: bool };

/// First demand activates the covering index. Once activation/backfill starts,
/// keep it current even if the last filter is temporarily removed. Existing
/// partial indexes are detected once before recording an inactive marker; an
/// upgrade must never silently stop maintaining already persisted postings.
pub fn maintenanceState(batch: anytype, demanded: bool) !MaintenanceState {
    const marker = batch.get(active_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    var active = demanded;
    var was_active = false;
    if (marker) |raw| {
        if (!std.mem.eql(u8, raw, "0") and !std.mem.eql(u8, raw, "1")) return error.InvalidGraphMetricBuildManifest;
        was_active = raw[0] == '1';
        active = active or was_active;
        if (!active) return .{ .active = false, .ready = false };
    }
    const ready = if (batch.get(ready_key)) |_| true else |err| switch (err) {
        error.NotFound => false,
        else => return err,
    };
    active = active or ready;
    if (!active) active = if (batch.get(cursor_key)) |_| true else |err| switch (err) {
        error.NotFound => false,
        else => return err,
    };
    if (marker == null and !active) {
        var cursor = try batch.openCursor();
        defer cursor.close();
        if (try cursor.seekAtOrAfter(prefix)) |entry| active = std.mem.startsWith(u8, entry.key, prefix);
    }
    if (marker == null or (active and !was_active)) try batch.put(active_key, if (active) "1" else "0");
    return .{ .active = active, .ready = ready };
}

pub fn typePrefixAlloc(alloc: Allocator, kind: []const u8) ![]u8 {
    return rangePrefixAlloc(alloc, prefix, kind);
}

fn rangePrefixAlloc(alloc: Allocator, namespace: []const u8, kind: []const u8) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, namespace);
    try keys.appendEncodedComponent(&out, alloc, kind);
    return out.toOwnedSlice(alloc);
}

/// One membership ref per (type, endpoint), shared across every metric filter.
/// Posting existence makes maintenance/backfill idempotent in the same batch.
pub fn update(alloc: Allocator, batch: anytype, kind: []const u8, reverse_key: []const u8, source: []const u8, target: []const u8, present: bool) !void {
    const key = try keyAlloc(alloc, kind, reverse_key);
    defer alloc.free(key);
    const exists = if (batch.get(key)) |_| true else |err| switch (err) {
        error.NotFound => false,
        else => return err,
    };
    if (exists == present) return;
    const start = try rangePrefixAlloc(alloc, node_prefix, kind);
    defer alloc.free(start);
    for ([_][]const u8{ source, target }) |node| {
        const node_key = try std.mem.concat(alloc, u8, &.{ start, node });
        defer alloc.free(node_key);
        const current = if (batch.get(node_key)) |raw| blk: {
            if (raw.len != 8) return error.InvalidGraphMetricBuildManifest;
            break :blk std.mem.readInt(u64, raw[0..8], .little);
        } else |err| switch (err) {
            error.NotFound => @as(u64, 0),
            else => return err,
        };
        const next = if (present) std.math.add(u64, current, 1) catch return error.InvalidGraphMetricBuildManifest else std.math.sub(u64, current, 1) catch return error.InvalidGraphMetricBuildManifest;
        if (next == 0) try batch.delete(node_key) else {
            var raw: [8]u8 = undefined;
            std.mem.writeInt(u64, &raw, next, .little);
            try batch.put(node_key, &raw);
        }
    }
    if (present) try batch.put(key, "") else try batch.delete(key);
}

/// Transaction-scoped incidence deltas. Postings are staged immediately so
/// duplicate/reversed operations remain idempotent; endpoint counts are read
/// and written only once per distinct (type, node). The caller must hold the
/// store's write transaction across staging and flush, and abort on any error.
pub const Updates = struct {
    const Prefixes = struct { edge: []u8, node: []u8 };
    alloc: Allocator,
    deltas: std.StringHashMapUnmanaged(i64) = .empty,
    prefixes: std.StringHashMapUnmanaged(Prefixes) = .empty,
    scratch: std.ArrayListUnmanaged(u8) = .empty,

    pub fn init(alloc: Allocator) Updates {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Updates) void {
        var entries = self.deltas.keyIterator();
        while (entries.next()) |key| self.alloc.free(key.*);
        self.deltas.deinit(self.alloc);
        var kinds = self.prefixes.iterator();
        while (kinds.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.alloc.free(entry.value_ptr.edge);
            self.alloc.free(entry.value_ptr.node);
        }
        self.prefixes.deinit(self.alloc);
        self.scratch.deinit(self.alloc);
    }

    pub fn stage(self: *Updates, batch: anytype, kind: []const u8, reverse_key: []const u8, source: []const u8, target: []const u8, present: bool) !void {
        return self.stageKnown(batch, kind, reverse_key, source, target, present, null);
    }

    /// Net foreground mutations already know reverse-edge existence. Reuse it
    /// only after checking covering-index readiness in this same transaction.
    /// Backfill and arbitrary operation streams must pass null.
    pub fn stageKnown(self: *Updates, batch: anytype, kind: []const u8, reverse_key: []const u8, source: []const u8, target: []const u8, present: bool, known_exists: ?bool) !void {
        if (known_exists) |exists| if (exists == present) return;
        const start = self.prefixes.get(kind) orelse blk: {
            const owned_kind = try self.alloc.dupe(u8, kind);
            errdefer self.alloc.free(owned_kind);
            const node = try rangePrefixAlloc(self.alloc, node_prefix, kind);
            errdefer self.alloc.free(node);
            const edge = try typePrefixAlloc(self.alloc, kind);
            errdefer self.alloc.free(edge);
            const value = Prefixes{ .node = node, .edge = edge };
            try self.prefixes.put(self.alloc, owned_kind, value);
            break :blk value;
        };
        self.scratch.clearRetainingCapacity();
        try self.scratch.appendSlice(self.alloc, start.edge);
        try self.scratch.appendSlice(self.alloc, reverse_key);
        const exists = known_exists orelse if (batch.get(self.scratch.items)) |_| true else |err| switch (err) {
            error.NotFound => false,
            else => return err,
        };
        if (exists == present) return;
        if (present) try batch.put(self.scratch.items, "") else try batch.delete(self.scratch.items);
        for ([_][]const u8{ source, target }) |node| {
            self.scratch.clearRetainingCapacity();
            try self.scratch.appendSlice(self.alloc, start.node);
            try self.scratch.appendSlice(self.alloc, node);
            const entry = try self.deltas.getOrPut(self.alloc, self.scratch.items);
            if (!entry.found_existing) {
                // Remove the borrowed scratch key if ownership allocation fails.
                errdefer _ = self.deltas.remove(self.scratch.items);
                entry.key_ptr.* = try self.alloc.dupe(u8, self.scratch.items);
                entry.value_ptr.* = 0;
            }
            entry.value_ptr.* = std.math.add(i64, entry.value_ptr.*, if (present) 1 else -1) catch return error.InvalidGraphMetricBuildManifest;
        }
    }

    pub fn flush(self: *Updates, batch: anytype) !void {
        const ordered = try self.alloc.alloc([]const u8, self.deltas.count());
        defer self.alloc.free(ordered);
        var count: usize = 0;
        var entries = self.deltas.iterator();
        while (entries.next()) |entry| {
            if (entry.value_ptr.* == 0) continue;
            ordered[count] = entry.key_ptr.*;
            count += 1;
        }
        const sorted = ordered[0..count];
        std.mem.sort([]const u8, sorted, {}, struct {
            fn less(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.less);
        // Bound bulk-read scratch independently of ingestion batch size.
        var values: [256]?[]const u8 = undefined;
        var next_counts: [256]u64 = undefined;
        var offset: usize = 0;
        while (offset < sorted.len) {
            const page = sorted[offset..@min(sorted.len, offset + values.len)];
            try batch.getManySorted(page, values[0..page.len]);
            // Values may borrow the mutable batch. Decode all before any put.
            for (page, values[0..page.len], 0..) |key, value, i| {
                const current = if (value) |raw| blk: {
                    if (raw.len != 8) return error.InvalidGraphMetricBuildManifest;
                    break :blk std.mem.readInt(u64, raw[0..8], .little);
                } else 0;
                const next = @as(i128, current) + self.deltas.get(key).?;
                next_counts[i] = std.math.cast(u64, next) orelse return error.InvalidGraphMetricBuildManifest;
            }
            for (page, next_counts[0..page.len]) |key, next| {
                if (next == 0) try batch.delete(key) else {
                    var raw: [8]u8 = undefined;
                    std.mem.writeInt(u64, &raw, next, .little);
                    try batch.put(key, &raw);
                }
                self.deltas.getPtr(key).?.* = 0;
            }
            offset += page.len;
        }
    }
};

/// Merge selected type ranges by their raw suffix, deduplicating endpoints.
/// Memory is bounded by filter fanout, not graph cardinality. Each stream's
/// key borrows its own cursor until popped; the returned key lasts until next().
pub const MergedCursor = struct {
    const Stream = struct { raw: backend.Cursor, start: []u8, head: ?[]const u8 = null };
    const Heap = std.PriorityQueue(usize, []Stream, order);
    alloc: Allocator,
    streams: []Stream,
    heap: Heap,
    last: std.ArrayListUnmanaged(u8) = .empty,

    fn order(streams: []Stream, a: usize, b: usize) std.math.Order {
        return std.mem.order(u8, streams[a].head.?, streams[b].head.?);
    }

    pub fn init(alloc: Allocator, txn: anytype, types: []const []const u8, nodes: bool, after: []const u8) !MergedCursor {
        _ = try txn.get(ready_key);
        const streams = try alloc.alloc(Stream, types.len);
        var result = MergedCursor{ .alloc = alloc, .streams = streams, .heap = Heap.initContext(streams) };
        var initialized: usize = 0;
        errdefer {
            result.streams = streams[0..initialized];
            for (result.streams) |*stream| {
                stream.raw.close();
                alloc.free(stream.start);
            }
            result.heap.deinit(alloc);
            alloc.free(streams);
        }
        try result.heap.ensureTotalCapacity(alloc, types.len);
        for (types, streams, 0..) |kind, *stream, i| {
            const start = try rangePrefixAlloc(alloc, if (nodes) node_prefix else prefix, kind);
            const raw = txn.openCursor() catch |err| {
                alloc.free(start);
                return err;
            };
            stream.* = .{ .raw = raw, .start = start };
            initialized += 1;
            const seek = try std.mem.concat(alloc, u8, &.{ start, after });
            defer alloc.free(seek);
            var item = try stream.raw.seekAtOrAfter(seek);
            if (after.len > 0) if (item) |entry| if (std.mem.eql(u8, entry.key, seek)) {
                item = try stream.raw.next();
            };
            if (item) |entry| if (std.mem.startsWith(u8, entry.key, start)) {
                stream.head = entry.key[start.len..];
                try result.heap.push(alloc, i);
            };
        }
        return result;
    }

    pub fn deinit(self: *@This()) void {
        for (self.streams) |*stream| {
            stream.raw.close();
            self.alloc.free(stream.start);
        }
        self.alloc.free(self.streams);
        self.heap.deinit(self.alloc);
        self.last.deinit(self.alloc);
    }

    pub fn next(self: *@This()) !?[]const u8 {
        const first = self.heap.peek() orelse return null;
        self.last.clearRetainingCapacity();
        try self.last.appendSlice(self.alloc, self.streams[first].head.?);
        while (self.heap.peek()) |i| {
            if (!std.mem.eql(u8, self.streams[i].head.?, self.last.items)) break;
            _ = self.heap.pop();
            const stream = &self.streams[i];
            stream.head = null;
            if (try stream.raw.next()) |entry| if (std.mem.startsWith(u8, entry.key, stream.start)) {
                stream.head = entry.key[stream.start.len..];
                try self.heap.push(self.alloc, i);
            };
        }
        return self.last.items;
    }
};

pub fn keyAlloc(alloc: Allocator, kind: []const u8, reverse_key: []const u8) ![]u8 {
    const start = try typePrefixAlloc(alloc, kind);
    defer alloc.free(start);
    return std.mem.concat(alloc, u8, &.{ start, reverse_key });
}

pub const Entry = struct { key: []const u8, cursor: []const u8 };

pub const Cursor = struct {
    alloc: Allocator,
    raw: backend.Cursor,
    types: []const []const u8,
    lower: []const u8,
    upper: []const u8,
    resume_key: []const u8,
    type_index: usize = 0,
    started: bool = false,
    type_prefix: []u8 = &.{},
    exhausted: bool = false,

    pub fn init(alloc: Allocator, txn: anytype, filter: anytype, lower: []const u8, upper: []const u8, resume_key: []const u8) !Cursor {
        const types = try alloc.dupe([]const u8, if (filter.mode == .all) &.{} else filter.types);
        errdefer alloc.free(types);
        std.mem.sort([]const u8, types, {}, struct {
            fn less(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.less);
        // Index preparation is a durable prerequisite, not a silent scan
        // fallback that can change cursor interpretation across checkpoints.
        if (types.len > 0) _ = txn.get(ready_key) catch |err| switch (err) {
            error.NotFound => return error.InvalidGraphMetricBuildManifest,
            else => return err,
        };
        if (resume_key.len > 0) {
            var raw_resume: ?[]const u8 = if (types.len == 0) resume_key else null;
            for (types) |kind| {
                const start = try typePrefixAlloc(alloc, kind);
                defer alloc.free(start);
                if (std.mem.startsWith(u8, resume_key, start)) {
                    raw_resume = resume_key[start.len..];
                    break;
                }
            }
            const key = raw_resume orelse return error.InvalidGraphMetricBuildManifest;
            if (key.len == 0 or std.mem.startsWith(u8, key, "meta:") or
                (lower.len > 0 and std.mem.order(u8, key, lower) == .lt) or
                (upper.len > 0 and std.mem.order(u8, key, upper) != .lt))
                return error.InvalidGraphMetricBuildManifest;
        }
        return .{ .alloc = alloc, .raw = try txn.openCursor(), .types = types, .lower = lower, .upper = upper, .resume_key = resume_key };
    }

    pub fn deinit(self: *@This()) void {
        self.raw.close();
        self.alloc.free(self.types);
        self.alloc.free(self.type_prefix);
    }

    pub fn next(self: *@This()) !?Entry {
        if (self.exhausted) return null;
        if (self.types.len == 0) {
            var found = if (self.started) try self.raw.next() else blk: {
                self.started = true;
                const start = if (self.resume_key.len > 0) self.resume_key else self.lower;
                var item = if (start.len > 0) try self.raw.seekAtOrAfter(start) else try self.raw.first();
                if (item) |entry| if (self.resume_key.len > 0 and std.mem.eql(u8, entry.key, self.resume_key)) {
                    item = try self.raw.next();
                };
                break :blk item;
            };
            if (found) |entry| if (std.mem.startsWith(u8, entry.key, "meta:")) {
                found = try self.raw.seekAtOrAfter("meta;");
            };
            if (found) |entry| {
                if (self.upper.len == 0 or std.mem.order(u8, entry.key, self.upper) == .lt)
                    return .{ .key = entry.key, .cursor = entry.key };
            }
            self.exhausted = true;
            return null;
        }
        while (self.type_index < self.types.len) {
            const found = if (self.started) try self.raw.next() else blk: {
                self.alloc.free(self.type_prefix);
                self.type_prefix = &.{};
                self.type_prefix = try typePrefixAlloc(self.alloc, self.types[self.type_index]);
                const lower = try std.mem.concat(self.alloc, u8, &.{ self.type_prefix, self.lower });
                defer self.alloc.free(lower);
                const start = if (self.resume_key.len > 0 and std.mem.order(u8, self.resume_key, lower) == .gt) self.resume_key else lower;
                self.started = true;
                var item = try self.raw.seekAtOrAfter(start);
                if (item) |entry| if (self.resume_key.len > 0 and std.mem.eql(u8, entry.key, self.resume_key)) {
                    item = try self.raw.next();
                };
                break :blk item;
            };
            if (found) |entry| if (std.mem.startsWith(u8, entry.key, self.type_prefix)) {
                const key = entry.key[self.type_prefix.len..];
                if (self.upper.len == 0 or std.mem.order(u8, key, self.upper) == .lt)
                    return .{ .key = key, .cursor = entry.key };
            };
            self.type_index += 1;
            self.started = false;
        }
        self.exhausted = true;
        return null;
    }
};
