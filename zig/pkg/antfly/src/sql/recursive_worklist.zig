// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Typed delta rows and a SQL set-equality visited table, under query admission.
const std = @import("std");
const scalar = @import("scalar.zig");
const operators = @import("operators.zig");
const Datum = scalar.Datum;

pub const Worklist = struct {
    arena: std.heap.ArenaAllocator,
    rows: std.ArrayList(Row) = .empty,
    heads: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    all: bool,
    begin: usize = 0,
    end: usize = 0,
    complete: bool = false,
    const Row = struct { values: []const Datum, next: ?usize };

    pub fn init(alloc: std.mem.Allocator, all: bool) Worklist {
        return .{ .arena = .init(alloc), .all = all };
    }
    pub fn deinit(self: *Worklist) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn append(self: *Worklist, engine: anytype, values: []const Datum, limit: usize) !void {
        try engine.checkpoint();
        var hash: u64 = 0;
        if (!self.all) {
            var hasher = std.hash.Wyhash.init(0);
            for (values) |value| {
                var bytes: [9]u8 = undefined;
                bytes[0] = @intFromBool(value.sql_null);
                std.mem.writeInt(u64, bytes[1..9], if (value.sql_null) 0 else try scalar.semanticHash(value.value), .little);
                hasher.update(&bytes);
            }
            hash = hasher.final();
            var cursor = self.heads.get(hash);
            while (cursor) |index| {
                try engine.checkpoint();
                const row = self.rows.items[index];
                const equal = for (row.values, values) |left, right| {
                    if (left.sql_null != right.sql_null or (!left.sql_null and try scalar.compare(left.value, right.value) != .eq)) break false;
                } else true;
                if (equal) return;
                cursor = row.next;
            }
        }
        if (self.rows.items.len >= limit) return error.SqlProgramLimitExceeded;
        const alloc = self.arena.allocator();
        const owned = try alloc.alloc(Datum, values.len);
        for (values, owned) |value, *out| out.* = try operators.cloneDatum(alloc, value);
        const index = self.rows.items.len;
        try self.rows.append(alloc, .{ .values = owned, .next = if (self.all) null else self.heads.get(hash) });
        if (!self.all) try self.heads.put(alloc, hash, index);
    }
};
