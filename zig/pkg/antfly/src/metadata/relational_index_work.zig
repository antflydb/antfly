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

//! Bounded durable work discovery and immutable cohort capture. Neither a page
//! nor its cursor is an execution capability. Receiving owners independently
//! obtain current metadata permission before applying any effect.
const std = @import("std");
const publication = @import("relational_index_publication.zig");
const local = @import("antfly_local_sources");
const progress = local.storage_db_relational_index_publication_progress;
const delivery = local.storage_db_relational_index_owner_job.private_claims.delivery;
const lifecycle = @import("table_lifecycle_lock.zig");

pub const max_discovery_records = 16;
pub const max_routes = 16;
pub const max_route_bytes = 4 * 1024 * 1024;
pub const max_wire_bytes = 32 * 1024 * 1024;
pub const max_cohort_bytes = 8 * 1024 * 1024;
pub const Identity = struct {
    table_id: u64,
    id: [16]u8,
    pub fn validate(self: Identity) !void {
        if (self.table_id == 0 or std.mem.allEqual(u8, &self.id, 0)) return error.InvalidGenerationPublication;
    }
    pub fn requireHead(self: Identity, head: progress.Head) !void {
        try self.validate();
        try head.validate();
        if (head.table_id != self.table_id or !std.mem.eql(u8, &head.plan_id, &self.id)) return error.GenerationPublicationChanged;
    }
};
pub const Query = union(enum) {
    /// Opaque position in the canonical lifecycle-key order, not numeric order.
    discover: u64,
    head: Identity,
    routes: struct { identity: Identity, ordinal: u16 = 0 },
    pub fn validate(self: Query) !void {
        switch (self) {
            .discover => {},
            .head => |identity| try identity.validate(),
            .routes => |request| {
                try request.identity.validate();
                if (request.ordinal >= progress.max_owners) return error.InvalidGenerationPublication;
            },
        }
    }
};
pub const Targets = struct {
    items: []const delivery.Target,
    pub fn jsonStringify(self: Targets, writer: anytype) !void {
        try writer.write(self.items);
    }
    pub fn jsonParse(alloc: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !Targets {
        if (try source.next() != .array_begin) return error.UnexpectedToken;
        var items: std.ArrayList(delivery.Target) = .empty;
        while (try source.peekNextTokenType() != .array_end) {
            if (items.items.len >= max_routes) return error.LengthMismatch;
            try items.append(alloc, try std.json.innerParse(delivery.Target, alloc, source, options));
        }
        _ = try source.next();
        return .{ .items = try items.toOwnedSlice(alloc) };
    }
};
pub const Routes = struct {
    head: progress.Head,
    first_ordinal: u16,
    table_name: []const u8,
    targets: Targets,
    next_ordinal: ?u16,
    pub fn validate(self: Routes, request: @FieldType(Query, "routes")) !void {
        try request.identity.requireHead(self.head);
        if (self.first_ordinal != request.ordinal or self.targets.items.len == 0 or self.targets.items.len > max_routes or
            self.table_name.len == 0 or self.table_name.len > 1024 or self.head.phase == .published or self.head.phase == .canceled) return error.InvalidCatalogRecord;
        const end = @as(usize, self.first_ordinal) + self.targets.items.len;
        if (end > self.head.owner_count or self.next_ordinal != (if (end == self.head.owner_count) @as(?u16, null) else @as(?u16, @intCast(end)))) return error.InvalidCatalogRecord;
        var bytes: usize = 0;
        for (self.targets.items, self.first_ordinal..) |target, ordinal| {
            try target.owner.validate();
            if (target.owner.ordinal != ordinal or target.owner.namespace.table_id != self.head.table_id or
                !std.mem.eql(u8, &target.owner.plan_id, &self.head.plan_id) or !std.mem.eql(u8, &target.owner.plan_digest, &self.head.plan_digest)) return error.InvalidCatalogRecord;
            bytes = std.math.add(usize, bytes, 1024) catch return error.IndexPublicationTooLarge;
            bytes = std.math.add(usize, bytes, target.start.len) catch return error.IndexPublicationTooLarge;
            bytes = std.math.add(usize, bytes, target.end.len) catch return error.IndexPublicationTooLarge;
        }
        if (bytes > max_route_bytes) return error.IndexPublicationTooLarge;
    }
};
pub const Response = union(enum) {
    discovered: struct { head: ?progress.Head, after_table: u64, exhausted: bool },
    head: progress.Head,
    routes: Routes,
};

fn currentHead(txn: anytype, group: u64, identity: Identity) !progress.Head {
    var key: [192]u8 = undefined;
    const head = try progress.Head.decode(try txn.get(try publication.headKey(&key, group, identity.table_id)));
    if (head.table_id != identity.table_id) return error.InvalidCatalogRecord;
    try identity.requireHead(head);
    if (head.phase != .published and head.phase != .canceled) {
        const lock = (try lifecycle.read(txn, group, head.table_id)) orelse return error.InvalidCatalogRecord;
        if (lock.kind != .index_build or !std.mem.eql(u8, &lock.id, &head.plan_id)) return error.InvalidCatalogRecord;
        try publication.requireActivePointer(txn, group, head);
    }
    return head;
}

/// At most max_discovery_records live lifecycle locks or max_routes owner/dispatch
/// pairs. Terminal heads are never scanned, even after repeated DDL churn.
/// Byte admission counts even routes whose keys are unusually large. No user
/// rows, schema-sized inputs, or catalog-wide diagnostic snapshots are read.
pub fn readJson(alloc: std.mem.Allocator, txn: anytype, group: u64, request: Query) ![]u8 {
    try request.validate();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const response: Response = switch (request) {
        .head => |identity| .{ .head = try currentHead(txn, group, identity) },
        .discover => |after| blk: {
            var prefix_buf: [192]u8 = undefined;
            var key_buf: [192]u8 = undefined;
            const prefix = try lifecycle.prefix(&prefix_buf, group);
            const seek = if (after == 0) prefix else try lifecycle.key(&key_buf, group, after);
            var cursor = try txn.openCursor();
            defer cursor.close();
            var row = try cursor.seekAtOrAfter(seek);
            if (after != 0) if (row) |item| if (std.mem.eql(u8, item.key, seek)) {
                row = try cursor.next();
            };
            var visited: usize = 0;
            var last = after;
            while (row) |item| : (row = try cursor.next()) {
                if (!std.mem.startsWith(u8, item.key, prefix)) break;
                if (visited == max_discovery_records) break;
                visited += 1;
                if (try lifecycle.groupFromKey(item.key) != group) return error.InvalidCatalogRecord;
                const table_id = std.fmt.parseUnsigned(u64, item.key[prefix.len..], 10) catch return error.InvalidCatalogRecord;
                const lock = try lifecycle.Lock.decode(item.value);
                last = table_id;
                if (lock.kind != .index_build) continue;
                const head = currentHead(txn, group, .{ .table_id = table_id, .id = lock.id }) catch |err| return switch (@as(anyerror, err)) {
                    error.GenerationPublicationChanged, error.NotFound => error.InvalidCatalogRecord,
                    else => err,
                };
                if (head.phase == .published or head.phase == .canceled) return error.InvalidCatalogRecord;
                break :blk .{ .discovered = .{ .head = head, .after_table = last, .exhausted = false } };
            }
            break :blk .{ .discovered = .{ .head = null, .after_table = last, .exhausted = row == null or !std.mem.startsWith(u8, row.?.key, prefix) } };
        },
        .routes => |query| blk: {
            const head = try currentHead(txn, group, query.identity);
            if (head.phase == .published or head.phase == .canceled or query.ordinal >= head.owner_count) return error.GenerationPublicationChanged;
            var targets: std.ArrayList(delivery.Target) = .empty;
            var table_name: ?[]const u8 = null;
            var bytes: usize = 0;
            var ordinal = query.ordinal;
            while (ordinal < head.owner_count and targets.items.len < max_routes) : (ordinal += 1) {
                var key: [192]u8 = undefined;
                const state = try progress.OwnerState.decode(try txn.get(try publication.ownerKey(&key, group, head.table_id, ordinal)));
                if (state.owner.ordinal != ordinal or state.owner.namespace.table_id != head.table_id or !std.mem.eql(u8, &state.owner.plan_id, &head.plan_id) or
                    !std.mem.eql(u8, &state.owner.plan_digest, &head.plan_digest)) return error.InvalidCatalogRecord;
                if (try publication.ownerOrdinal(txn, group, head.table_id, state.owner.group_id) != ordinal) return error.InvalidCatalogRecord;
                var dispatch = try publication.readDispatch(alloc, txn, group, state.owner);
                defer dispatch.deinit();
                const value = dispatch.value;
                if (table_name) |name| {
                    if (!std.mem.eql(u8, name, value.table_name)) return error.InvalidCatalogRecord;
                } else table_name = try a.dupe(u8, value.table_name);
                const start = value.owner.range.start_key;
                const end = value.owner.range.end_key orelse "";
                const cost = std.math.add(usize, 1024, std.math.add(usize, start.len, end.len) catch return error.IndexPublicationTooLarge) catch return error.IndexPublicationTooLarge;
                if (cost > max_route_bytes) return error.IndexPublicationTooLarge;
                if (cost > max_route_bytes - bytes) break;
                bytes += cost;
                try targets.append(a, .{ .owner = state.owner, .start = try a.dupe(u8, start), .end = try a.dupe(u8, end) });
            }
            const routes: Routes = .{ .head = head, .first_ordinal = query.ordinal, .table_name = table_name orelse return error.InvalidCatalogRecord, .targets = .{ .items = targets.items }, .next_ordinal = if (ordinal == head.owner_count) null else ordinal };
            try routes.validate(query);
            break :blk .{ .routes = routes };
        },
    };
    const bytes = try std.json.Stringify.valueAlloc(alloc, response, .{});
    errdefer alloc.free(bytes);
    if (bytes.len > max_wire_bytes) return error.IndexPublicationTooLarge;
    return bytes;
}

/// Capture crosses short read transactions but pins only immutable plan identity.
/// Mutable progress revisions may advance. Replaced/canceled plans and changed
/// dispatch identity fail closed; global disjoint coverage is checked at finish.
pub const Capture = struct {
    arena: std.heap.ArenaAllocator,
    identity: Identity,
    plan_digest: [32]u8,
    owner_count: u16,
    next_ordinal: ?u16 = 0,
    table_name: ?[]const u8 = null,
    targets: std.ArrayList(delivery.Target) = .empty,
    bytes: usize = 0,
    failed: bool = false,
    pub fn init(alloc: std.mem.Allocator, head: progress.Head) !Capture {
        try head.validate();
        return .{ .arena = std.heap.ArenaAllocator.init(alloc), .identity = .{ .table_id = head.table_id, .id = head.plan_id }, .plan_digest = head.plan_digest, .owner_count = head.owner_count };
    }
    pub fn deinit(self: *Capture) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn request(self: *const Capture) !Query {
        if (self.failed or self.next_ordinal == null) return error.GenerationPublicationChanged;
        return .{ .routes = .{ .identity = self.identity, .ordinal = self.next_ordinal.? } };
    }
    pub fn append(self: *Capture, routes: Routes) !void {
        if (self.failed) return error.GenerationPublicationChanged;
        errdefer self.failed = true;
        const query = try self.request();
        try routes.validate(query.routes);
        if (!std.mem.eql(u8, &routes.head.plan_digest, &self.plan_digest) or routes.head.owner_count != self.owner_count) return error.GenerationPublicationChanged;
        const a = self.arena.allocator();
        if (self.table_name) |name| {
            if (!std.mem.eql(u8, name, routes.table_name)) return error.GenerationPublicationChanged;
        } else self.table_name = try a.dupe(u8, routes.table_name);
        for (routes.targets.items) |target| {
            const cost = @sizeOf(delivery.Target) + @sizeOf(u64) + @sizeOf(u16) + target.start.len + target.end.len;
            if (cost > max_cohort_bytes - self.bytes) return error.IndexPublicationTooLarge;
            self.bytes += cost;
            try self.targets.append(a, .{ .owner = target.owner, .start = try a.dupe(u8, target.start), .end = try a.dupe(u8, target.end) });
        }
        self.next_ordinal = routes.next_ordinal;
    }
    pub fn finish(self: *const Capture, alloc: std.mem.Allocator) !delivery.Cohort {
        if (self.failed or self.next_ordinal != null or self.targets.items.len != self.owner_count) return error.GenerationPublicationChanged;
        return delivery.Cohort.init(alloc, self.targets.items);
    }
};

fn testCapture(alloc: std.mem.Allocator, count: u16) !void {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var head: progress.Head = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .candidate_digest = @splat(3), .reservation_digest = @splat(4), .table_id = 101, .owner_count = count };
    var capture = try Capture.init(alloc, head);
    defer capture.deinit();
    var first: u16 = 0;
    while (first < count) {
        const end: u16 = @intCast(@min(@as(usize, count), @as(usize, first) + max_routes));
        const targets = try a.alloc(delivery.Target, end - first);
        for (targets, first..) |*target, ordinal| {
            const group = 1000 + ordinal;
            target.* = .{
                .owner = .{ .plan_id = head.plan_id, .plan_digest = head.plan_digest, .ordinal = @intCast(ordinal), .group_id = group, .namespace = .{ .table_id = head.table_id, .shard_id = group, .range_id = group }, .authority = .native, .fence_digest = @splat(5) },
                .start = if (ordinal == 0) "" else try std.fmt.allocPrint(a, "{x:0>4}", .{ordinal}),
                .end = if (ordinal + 1 == count) "" else try std.fmt.allocPrint(a, "{x:0>4}", .{ordinal + 1}),
            };
        }
        // Progress may advance while immutable identity and routes stay pinned.
        head.revision += 1;
        try capture.append(.{ .head = head, .first_ordinal = first, .table_name = "items", .targets = .{ .items = targets }, .next_ordinal = if (end == count) null else end });
        first = end;
    }
    var cohort = try capture.finish(alloc);
    defer cohort.deinit();
    try std.testing.expectEqual(@as(usize, count), cohort.targets.len);
    try std.testing.expectEqual(@as(usize, count), cohort.by_ordinal.len);
    try std.testing.expectError(error.GenerationPublicationChanged, capture.request());
}

test "relational index publication work captures all 4096 owners in bounded pages and owns every allocation" {
    try testCapture(std.testing.allocator, progress.max_owners);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testCapture, .{@as(u16, 3)});
}

test "relational index publication work capture rejects duplicate pages replaced identities and incomplete coverage" {
    const head: progress.Head = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .candidate_digest = @splat(3), .reservation_digest = @splat(4), .table_id = 101, .owner_count = 2 };
    const owner: progress.Owner = .{ .plan_id = head.plan_id, .plan_digest = head.plan_digest, .ordinal = 0, .group_id = 301, .namespace = .{ .table_id = 101, .shard_id = 301, .range_id = 301 }, .authority = .native, .fence_digest = @splat(5) };
    const page: Routes = .{ .head = head, .first_ordinal = 0, .table_name = "items", .targets = .{ .items = &.{.{ .owner = owner, .start = "", .end = "m" }} }, .next_ordinal = 1 };
    var capture = try Capture.init(std.testing.allocator, head);
    defer capture.deinit();
    try std.testing.expectError(error.GenerationPublicationChanged, capture.finish(std.testing.allocator));
    try capture.append(page);
    try std.testing.expectError(error.InvalidCatalogRecord, capture.append(page));
    try std.testing.expectError(error.GenerationPublicationChanged, capture.request());
    var replaced = try Capture.init(std.testing.allocator, head);
    defer replaced.deinit();
    var foreign = page;
    foreign.head.plan_id[0] ^= 1;
    try std.testing.expectError(error.GenerationPublicationChanged, replaced.append(foreign));
    try std.testing.expect(replaced.failed);
}

test "relational index publication work discovery bounds unrelated live locks and ignores retained terminal heads" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const Entry = struct { key: []const u8, value: []const u8 };
    const Reader = struct {
        entries: []const Entry,
        head: progress.Head,
        head_bytes: [progress.Head.encoded_len]u8,
        lock_bytes: [lifecycle.encoded_len]u8,
        point_reads: usize = 0,
        unexpected_reads: usize = 0,
        const Cursor = struct {
            entries: []const Entry,
            index: usize = 0,
            fn close(_: *@This()) void {}
            fn seekAtOrAfter(self: *@This(), key: []const u8) !?Entry {
                self.index = 0;
                while (self.index < self.entries.len and std.mem.lessThan(u8, self.entries[self.index].key, key)) self.index += 1;
                return if (self.index == self.entries.len) null else self.entries[self.index];
            }
            fn next(self: *@This()) !?Entry {
                self.index += 1;
                return if (self.index >= self.entries.len) null else self.entries[self.index];
            }
        };
        fn openCursor(self: *@This()) !Cursor {
            return .{ .entries = self.entries };
        }
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            self.point_reads += 1;
            var buf: [192]u8 = undefined;
            if (std.mem.eql(u8, key, try publication.headKey(&buf, 13, self.head.table_id))) return &self.head_bytes;
            if (std.mem.eql(u8, key, try publication.activeKey(&buf, 13, self.head.table_id))) return &self.head.plan_id;
            if (std.mem.eql(u8, key, try lifecycle.key(&buf, 13, self.head.table_id))) return &self.lock_bytes;
            self.unexpected_reads += 1;
            return error.NotFound;
        }
    };
    const head: progress.Head = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .candidate_digest = @splat(3), .reservation_digest = @splat(4), .table_id = 2000, .owner_count = 1 };
    const lock = lifecycle.value(.index_build, head.plan_id);
    const other = lifecycle.value(.generation, @splat(7));
    const terminal_count = 1000;
    const lock_count = max_discovery_records + 1;
    const entries = try a.alloc(Entry, lock_count + 1 + terminal_count);
    var buf: [192]u8 = undefined;
    for (entries[0..lock_count], 1000..) |*entry, table| entry.* = .{ .key = try a.dupe(u8, try lifecycle.key(&buf, 13, table)), .value = &other };
    entries[lock_count] = .{ .key = try a.dupe(u8, try lifecycle.key(&buf, 13, head.table_id)), .value = &lock };
    const terminal_bytes = try a.alloc([progress.Head.encoded_len]u8, terminal_count);
    for (entries[lock_count + 1 ..], terminal_bytes, 2001..) |*entry, *bytes, table| {
        var terminal = head;
        terminal.table_id = table;
        terminal.phase = .canceled;
        terminal.revision = 4;
        terminal.cancellation_revision = 3;
        terminal.failure = .requested;
        terminal.counts = .{ 1, 1, 0, 0, 0, 1, 1 };
        bytes.* = try terminal.encode();
        entry.* = .{ .key = try a.dupe(u8, try publication.headKey(&buf, 13, table)), .value = bytes };
    }
    var reader: Reader = .{ .entries = entries, .head = head, .head_bytes = try head.encode(), .lock_bytes = lock };
    const first_bytes = try readJson(alloc, &reader, 13, .{ .discover = 0 });
    defer alloc.free(first_bytes);
    var first = try std.json.parseFromSlice(Response, alloc, first_bytes, .{});
    defer first.deinit();
    try std.testing.expect(first.value.discovered.head == null and !first.value.discovered.exhausted);
    try std.testing.expectEqual(@as(u64, 1000 + max_discovery_records - 1), first.value.discovered.after_table);
    try std.testing.expectEqual(@as(usize, 0), reader.point_reads);
    const found_bytes = try readJson(alloc, &reader, 13, .{ .discover = first.value.discovered.after_table });
    defer alloc.free(found_bytes);
    var found = try std.json.parseFromSlice(Response, alloc, found_bytes, .{});
    defer found.deinit();
    try std.testing.expectEqual(head.table_id, found.value.discovered.head.?.table_id);
    try std.testing.expectEqual(@as(usize, 3), reader.point_reads);
    const end_bytes = try readJson(alloc, &reader, 13, .{ .discover = found.value.discovered.after_table });
    defer alloc.free(end_bytes);
    var end = try std.json.parseFromSlice(Response, alloc, end_bytes, .{});
    defer end.deinit();
    try std.testing.expect(end.value.discovered.exhausted and end.value.discovered.head == null);
    try std.testing.expectEqual(@as(usize, 3), reader.point_reads);
    try std.testing.expectEqual(@as(usize, 0), reader.unexpected_reads);
}
