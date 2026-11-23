// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Immutable COPY tuple births in the existing SQL session payload. Ordinary
//! row folding cannot retain these: UPDATE/DELETE must not erase an initially
//! frozen image, and a deleted key may be frozen again in the same generation.
//! No field in this module is accepted by the public mutation decoder.
const std = @import("std");
const A = std.mem.Allocator;
pub const max_rows = 4096;
pub const max_bytes = 8 << 20;
pub const max_generations = 64;

pub const Origin = struct {
    transaction: [16]u8,
    nonce: [16]u8,
    physical: [16]u8,
    schema_version: u32,

    pub fn validate(self: Origin) !void {
        if (std.mem.allEqual(u8, &self.transaction, 0) or std.mem.allEqual(u8, &self.nonce, 0) or
            std.mem.allEqual(u8, &self.physical, 0)) return error.InvalidTransactionSessionRecord;
    }
    pub fn eql(self: Origin, other: Origin) bool {
        return std.meta.eql(self, other);
    }
};

pub const Row = struct {
    key: []const u8,
    document: []const u8,
    json_null_fields: []const []const u8 = &.{},
    /// An UPDATE, DELETE or later birth ended this tuple's current lifetime.
    /// Keep the original image for nonlocking historical readers.
    superseded: bool = false,
    pub fn jsonStringify(self: Row, writer: anytype) !void {
        try writer.beginObject();
        try writer.objectField("key");
        try writer.write(self.key);
        try writer.objectField("document");
        try writer.write(self.document);
        try writer.objectField("json_null_fields");
        try writer.write(self.json_null_fields);
        if (self.superseded) {
            try writer.objectField("superseded");
            try writer.write(true);
        }
        try writer.endObject();
    }

    fn bytes(self: Row) !usize {
        if (self.key.len == 0 or self.key.len > 64 * 1024 or self.document.len == 0 or self.json_null_fields.len > 256)
            return error.InvalidTransactionSessionRecord;
        var size = @sizeOf(Row) +| self.key.len +| self.document.len +| (self.json_null_fields.len * @sizeOf([]const u8));
        for (self.json_null_fields) |name| {
            if (name.len == 0) return error.InvalidTransactionSessionRecord;
            size +|= name.len;
        }
        return size;
    }
    fn deinit(self: Row, a: A) void {
        a.free(self.key);
        a.free(self.document);
        for (self.json_null_fields) |name| a.free(name);
        a.free(self.json_null_fields);
    }
    fn clone(self: Row, a: A) !Row {
        const key = try a.dupe(u8, self.key);
        errdefer a.free(key);
        const document = try a.dupe(u8, self.document);
        errdefer a.free(document);
        const names = try a.alloc([]const u8, self.json_null_fields.len);
        var initialized: usize = 0;
        errdefer {
            for (names[0..initialized]) |name| a.free(name);
            a.free(names);
        }
        for (self.json_null_fields, names) |name, *out| {
            out.* = try a.dupe(u8, name);
            initialized += 1;
        }
        std.mem.sort([]const u8, names, {}, struct {
            fn less(_: void, lhs: []const u8, rhs: []const u8) bool {
                return std.mem.order(u8, lhs, rhs) == .lt;
            }
        }.less);
        for (names, 0..) |name, i| if (i != 0 and std.mem.eql(u8, names[i - 1], name)) return error.InvalidTransactionSessionRecord;
        return .{ .key = key, .document = document, .json_null_fields = names, .superseded = self.superseded };
    }
};

pub const Group = struct {
    origin: Origin,
    // The position is a generation-local tuple birth ordinal, independent of
    // transport order. Sorting row headers must never renumber this identity.
    rows: std.ArrayList(Row) = .empty,
    current_births: std.StringHashMapUnmanaged(usize) = .empty,

    fn deinit(self: *Group, a: A) void {
        for (self.rows.items) |row| row.deinit(a);
        self.rows.deinit(a);
        self.current_births.deinit(a);
    }
};

pub const Ledger = struct {
    groups: std.ArrayList(Group) = .empty,
    rows: usize = 0,
    retained_bytes: usize = 0,

    pub fn deinit(self: *Ledger, a: A) void {
        for (self.groups.items) |*group| group.deinit(a);
        self.groups.deinit(a);
        self.* = .{};
    }
    pub fn find(self: Ledger, origin: Origin) ?*const Group {
        for (self.groups.items) |*group| if (group.origin.eql(origin)) return group;
        return null;
    }
    /// Append immutable birth images. The index points only to surviving
    /// tuples, so invalidation never scans or changes their historical order.
    pub fn append(self: *Ledger, a: A, origin: Origin, rows: []const Row) !void {
        try origin.validate();
        if (rows.len == 0) return;
        if (rows.len > max_rows -| self.rows) return error.TransactionTooLarge;
        var bytes: usize = 0;
        for (rows) |row| bytes +|= try row.bytes();
        if (bytes > max_bytes -| self.retained_bytes) return error.TransactionTooLarge;
        var index: ?usize = null;
        for (self.groups.items, 0..) |group, i| {
            if (std.mem.eql(u8, &group.origin.physical, &origin.physical)) {
                if (!group.origin.eql(origin)) return error.CatalogGenerationChanged;
                index = i;
                break;
            }
            if (!std.mem.eql(u8, &group.origin.transaction, &origin.transaction) or
                std.mem.eql(u8, &group.origin.nonce, &origin.nonce)) return error.CatalogGenerationChanged;
        }
        const created = index == null;
        if (created) {
            if (self.groups.items.len == max_generations) return error.TransactionTooLarge;
            try self.groups.append(a, .{ .origin = origin });
            index = self.groups.items.len - 1;
        }
        const group = &self.groups.items[index.?];
        const previous = group.rows.items.len;
        errdefer {
            for (group.rows.items[previous..]) |row| row.deinit(a);
            group.rows.shrinkRetainingCapacity(previous);
            if (created) {
                group.deinit(a);
                self.groups.items.len -= 1;
            }
        }
        try group.rows.ensureUnusedCapacity(a, rows.len);
        try group.current_births.ensureUnusedCapacity(a, @intCast(rows.len));
        for (rows) |row| group.rows.appendAssumeCapacity(try row.clone(a));
        // All allocations precede updates to the old index and tuple flags.
        for (group.rows.items[previous..], previous..) |row, ordinal| {
            if (row.superseded) continue;
            const current = group.current_births.getOrPutAssumeCapacity(row.key);
            if (current.found_existing) group.rows.items[current.value_ptr.*].superseded = true;
            current.value_ptr.* = ordinal;
        }
        self.rows += rows.len;
        self.retained_bytes += bytes;
    }
    pub fn invalidateKey(self: *Ledger, physical: [16]u8, key: []const u8) void {
        for (self.groups.items) |*group| {
            if (!std.mem.eql(u8, &group.origin.physical, &physical)) continue;
            if (group.current_births.fetchRemove(key)) |birth| group.rows.items[birth.value].superseded = true;
            return;
        }
    }

    pub fn merge(self: *Ledger, a: A, other: Ledger) !void {
        for (other.groups.items) |group| try self.append(a, group.origin, group.rows.items);
    }
    pub fn clone(self: Ledger, a: A) !Ledger {
        var out: Ledger = .{};
        errdefer out.deinit(a);
        try out.merge(a, self);
        return out;
    }
    pub fn jsonStringify(self: Ledger, writer: anytype) !void {
        try writer.beginArray();
        for (self.groups.items) |group| try writer.write(.{ .origin = group.origin, .rows = group.rows.items });
        try writer.endArray();
    }
    pub fn parse(a: A, value: std.json.Value) !Ledger {
        if (value != .array or value.array.items.len > max_generations) return error.InvalidTransactionSessionRecord;
        var total: usize = 0;
        for (value.array.items) |group| {
            if (group != .object) return error.InvalidTransactionSessionRecord;
            const rows = group.object.get("rows") orelse return error.InvalidTransactionSessionRecord;
            if (rows != .array or rows.array.items.len == 0 or rows.array.items.len > max_rows -| total) return error.InvalidTransactionSessionRecord;
            total += rows.array.items.len;
        }
        const Wire = struct { origin: Origin, rows: []const Row };
        var decoded = try std.json.parseFromValue([]const Wire, a, value, .{});
        defer decoded.deinit();
        var out: Ledger = .{};
        errdefer out.deinit(a);
        for (decoded.value) |group| {
            if (out.find(group.origin) != null) return error.InvalidTransactionSessionRecord;
            try out.append(a, group.origin, group.rows);
        }
        return out;
    }
};

test "SQL generation intents frozen births retain multiplicity and allocation failure atomicity" {
    const Probe = struct {
        fn run(a: A) !void {
            var ledger: Ledger = .{};
            defer ledger.deinit(a);
            const origin: Origin = .{ .transaction = @splat(1), .nonce = @splat(2), .physical = @splat(3), .schema_version = 0 };
            const first = [_]Row{.{ .key = "z", .document = "{\"body\":null}", .json_null_fields = &.{"body"} }};
            try ledger.append(a, origin, &first);
            const second = [_]Row{ .{ .key = "a", .document = "{}" }, .{ .key = "z", .document = "{\"body\":\"second\"}" } };
            ledger.append(a, origin, &second) catch |err| {
                try std.testing.expectEqual(@as(usize, 1), ledger.rows);
                try std.testing.expectEqual(@as(usize, 1), ledger.groups.items[0].rows.items.len);
                return err;
            };
            try std.testing.expectEqual(@as(usize, 3), ledger.rows);
            try std.testing.expect(ledger.groups.items[0].rows.items[0].superseded);
            try std.testing.expect(!ledger.groups.items[0].rows.items[2].superseded);
            ledger.invalidateKey(origin.physical, "z");
            try std.testing.expect(ledger.groups.items[0].rows.items[2].superseded);
            try std.testing.expect(!ledger.groups.items[0].rows.items[1].superseded);
            const bytes = try std.json.Stringify.valueAlloc(a, ledger, .{});
            defer a.free(bytes);
            var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
            defer parsed.deinit();
            var restored = try Ledger.parse(a, parsed.value);
            defer restored.deinit(a);
            try std.testing.expectEqualDeep(ledger.groups.items[0].rows.items, restored.groups.items[0].rows.items);
            var point = try restored.clone(a);
            defer point.deinit(a);
            try ledger.append(a, origin, &second);
            try std.testing.expectEqual(@as(usize, 3), point.rows);
            var foreign = origin;
            foreign.nonce[0] ^= 1;
            try std.testing.expectError(error.CatalogGenerationChanged, ledger.append(a, foreign, &first));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}
