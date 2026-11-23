// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Owned compare-only native assignments. SQL never allocates a version here.
//! Builders borrow producer bytes; freezing copies once at an ownership boundary.
const std = @import("std");
const native = @import("antfly_local_sources").storage_document_revision_reservation;
const A = std.mem.Allocator;
pub const Handoff = native.Handoff;
pub const Scope = native.Scope;
pub const Reference = native.Reference;
pub const max_groups = 64;

pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    value: []const Handoff,
    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn merge(a: A, previous: []const Handoff, incoming: []const Handoff) !Owned {
        var builder: Builder = .{ .a = a };
        defer builder.deinit();
        try builder.add(previous);
        try builder.add(incoming);
        return builder.freeze(a);
    }
};

/// On an error discard the builder. Journal producers already own the borrowed
/// references until final folding; no growing assignment vector is recopied for
/// each individual row/trigger effect.
pub const Builder = struct {
    a: A,
    groups: std.AutoArrayHashMapUnmanaged([Scope.encoded_len]u8, Group) = .empty,
    owners: std.StringHashMapUnmanaged(usize) = .empty,
    bytes: usize = 0,
    own_keys: bool = false,
    const Group = struct { scope: Scope, rows: std.StringHashMapUnmanaged(u64) = .empty };
    pub fn deinit(self: *Builder) void {
        if (self.own_keys) {
            var keys = self.owners.keyIterator();
            while (keys.next()) |key| self.a.free(key.*);
        }
        for (self.groups.values()) |*group| group.rows.deinit(self.a);
        self.groups.deinit(self.a);
        self.owners.deinit(self.a);
    }
    pub fn add(self: *Builder, incoming: []const Handoff) !void {
        if (incoming.len > max_groups) return error.TransactionTooLarge;
        for (incoming) |handoff| {
            try handoff.validate();
            if (self.groups.count() != 0) {
                const root = self.groups.values()[0].scope;
                if (root.table_id != handoff.scope.table_id or root.begin_timestamp != handoff.scope.begin_timestamp or
                    !std.mem.eql(u8, &root.transaction_id, &handoff.scope.transaction_id) or
                    !std.mem.eql(u8, &root.principal_digest, &handoff.scope.principal_digest) or
                    !std.mem.eql(u8, &root.metadata_incarnation, &handoff.scope.metadata_incarnation)) return error.LockOwnerChanged;
            }
            const encoded = try handoff.scope.encode();
            const position = self.groups.getIndex(encoded) orelse position: {
                if (self.groups.count() == max_groups) return error.TransactionTooLarge;
                for (self.groups.values()) |held| if (held.scope.shard_id == handoff.scope.shard_id and held.scope.range_id == handoff.scope.range_id) return error.LockOwnerChanged;
                const index = self.groups.count();
                try self.groups.put(self.a, encoded, .{ .scope = handoff.scope });
                break :position index;
            };
            const group = &self.groups.values()[position];
            for (handoff.references) |ref| {
                if (self.owners.get(ref.key)) |owner| {
                    if (owner != position) return error.CatalogGenerationChanged;
                    if (group.rows.get(ref.key).? != ref.version) return error.VersionConflict;
                    continue;
                }
                if (self.owners.count() == native.max_rows or ref.key.len > native.max_key_bytes -| self.bytes) return error.TransactionTooLarge;
                try group.rows.ensureUnusedCapacity(self.a, 1);
                try self.owners.ensureUnusedCapacity(self.a, 1);
                const key = if (self.own_keys) try self.a.dupe(u8, ref.key) else ref.key;
                group.rows.putAssumeCapacity(key, ref.version);
                self.owners.putAssumeCapacity(key, position);
                self.bytes += ref.key.len;
            }
        }
    }
    pub fn view(self: *const Builder, a: A) ![]Handoff {
        const out = try a.alloc(Handoff, self.groups.count());
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |handoff| a.free(handoff.references);
            a.free(out);
        }
        for (self.groups.values(), out) |group, *handoff| {
            const refs = try a.alloc(Reference, group.rows.count());
            handoff.* = .{ .scope = group.scope, .references = refs };
            initialized += 1;
            var rows = group.rows.iterator();
            for (refs) |*ref| {
                const entry = rows.next().?;
                ref.* = .{ .key = entry.key_ptr.*, .version = entry.value_ptr.* };
            }
            std.mem.sort(Reference, refs, {}, struct {
                fn less(_: void, x: Reference, y: Reference) bool {
                    return std.mem.order(u8, x.key, y.key) == .lt;
                }
            }.less);
        }
        std.mem.sort(Handoff, out, {}, struct {
            fn less(_: void, x: Handoff, y: Handoff) bool {
                const left = x.scope.encode() catch unreachable;
                const right = y.scope.encode() catch unreachable;
                return std.mem.order(u8, &left, &right) == .lt;
            }
        }.less);
        return out;
    }
    pub fn freeze(self: *const Builder, a: A) !Owned {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const region = arena.allocator();
        const out = try self.view(region);
        for (out) |handoff| for (@constCast(handoff.references)) |*ref| {
            ref.key = try region.dupe(u8, ref.key);
        };
        return .{ .arena = arena, .value = out };
    }
};
pub fn freeView(a: A, view: []const Handoff) void {
    for (view) |handoff| a.free(handoff.references);
    a.free(view);
}

pub fn fromMutations(a: A, mutations: anytype) !Owned {
    var builder: Builder = .{ .a = a };
    defer builder.deinit();
    for (mutations) |mutation| if (mutation.document_revision) |stamp| {
        if (mutation.predicate_only or mutation.trigger_suppressed) continue;
        try builder.add(&.{.{ .scope = stamp.scope.*, .references = &.{.{ .key = mutation.key, .version = stamp.version }} }});
    };
    return builder.freeze(a);
}
pub fn lookup(handoffs: []const Handoff, key: []const u8) ?u64 {
    for (handoffs) |handoff| {
        var low: usize = 0;
        var high = handoff.references.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            switch (std.mem.order(u8, handoff.references[mid].key, key)) {
                .lt => low = mid + 1,
                .gt => high = mid,
                .eq => return handoff.references[mid].version,
            }
        }
    }
    return null;
}

fn checkOwned(a: A) !void {
    var builder: Builder = .{ .a = a, .own_keys = true };
    defer builder.deinit();
    const scope: Scope = .{ .metadata_incarnation = @splat('1'), .transaction_id = @splat(2), .principal_digest = @splat(3), .begin_timestamp = 4, .table_id = 5, .shard_id = 6, .range_id = 7 };
    {
        const key = try a.dupe(u8, "opaque\x00\xff");
        defer a.free(key);
        try builder.add(&.{.{ .scope = scope, .references = &.{.{ .key = key, .version = 9007199254740993 }} }});
    }
    try builder.add(&.{.{ .scope = scope, .references = &.{.{ .key = "a", .version = 9 }} }});
    try builder.add(&.{.{ .scope = scope, .references = &.{.{ .key = "a", .version = 9 }} }});
    var owned = try builder.freeze(a);
    defer owned.deinit();
    try std.testing.expectEqual(@as(usize, 2), owned.value[0].references.len);
    try std.testing.expectEqual(@as(?u64, 9007199254740993), lookup(owned.value, "opaque\x00\xff"));
    const bytes = try std.json.Stringify.valueAlloc(a, owned.value, .{});
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice([]const Handoff, a, bytes, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try std.testing.expectEqualDeep(owned.value, parsed.value);
    var clone = try Owned.merge(a, &.{}, parsed.value);
    defer clone.deinit();
    try std.testing.expectEqualDeep(owned.value, clone.value);
}
test "distributed txn document revision custody owns opaque assignments through allocation failure and canonical replay" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, checkOwned, .{});
}
test "distributed txn document revision custody rejects reassigned keys changed births and physical owner replacement" {
    const a = std.testing.allocator;
    const scope: Scope = .{ .metadata_incarnation = @splat('1'), .transaction_id = @splat(2), .principal_digest = @splat(3), .begin_timestamp = 4, .table_id = 5, .shard_id = 6, .range_id = 7 };
    const initial: Handoff = .{ .scope = scope, .references = &.{.{ .key = "a", .version = 9 }} };
    var owned = try Owned.merge(a, &.{}, &.{initial});
    defer owned.deinit();
    var changed = initial;
    changed.references = &.{.{ .key = "a", .version = 10 }};
    try std.testing.expectError(error.VersionConflict, Owned.merge(a, owned.value, &.{changed}));
    changed = initial;
    changed.scope.begin_timestamp += 1;
    try std.testing.expectError(error.LockOwnerChanged, Owned.merge(a, owned.value, &.{changed}));
    changed = initial;
    changed.scope.range_id += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, Owned.merge(a, owned.value, &.{changed}));
    try std.testing.expectEqual(@as(?u64, 9), lookup(owned.value, "a"));
}
