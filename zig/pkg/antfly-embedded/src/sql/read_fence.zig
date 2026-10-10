// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Owned read authority pins. The coordinator supplies current policy/catalog
//! validation; this state tracks when scheduling invalidates a previous check.
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Table = struct {
    pub const Policy = struct { generation: u64, catalog_epoch: u64 };
    database: []const u8,
    namespace: []const u8,
    name: []const u8,
    physical: []const u8,
    id: u64,
    schema_version: u32,
    catalog_revision: u64,
    filter: ?[]const u8,
    row_policy: ?Policy = null,

    fn clone(self: Table, alloc: Allocator) !Table {
        const database = try alloc.dupe(u8, self.database);
        errdefer alloc.free(database);
        const namespace = try alloc.dupe(u8, self.namespace);
        errdefer alloc.free(namespace);
        const name = try alloc.dupe(u8, self.name);
        errdefer alloc.free(name);
        const physical = try alloc.dupe(u8, self.physical);
        errdefer alloc.free(physical);
        const filter = if (self.filter) |value| try alloc.dupe(u8, value) else null;
        return .{ .database = database, .namespace = namespace, .name = name, .physical = physical, .id = self.id, .schema_version = self.schema_version, .catalog_revision = self.catalog_revision, .filter = filter, .row_policy = self.row_policy };
    }

    fn deinit(self: *Table, alloc: Allocator) void {
        alloc.free(self.database);
        alloc.free(self.namespace);
        alloc.free(self.name);
        alloc.free(self.physical);
        if (self.filter) |value| alloc.free(value);
        self.* = undefined;
    }

    fn sameName(self: Table, other: Table) bool {
        return std.mem.eql(u8, self.database, other.database) and
            std.mem.eql(u8, self.namespace, other.namespace) and
            std.mem.eql(u8, self.name, other.name);
    }

    fn sameBinding(self: Table, other: Table) bool {
        return self.id == other.id and self.schema_version == other.schema_version and
            self.catalog_revision == other.catalog_revision and std.mem.eql(u8, self.physical, other.physical) and
            std.meta.eql(self.row_policy, other.row_policy);
    }
};

pub const Guard = struct {
    pub const max_tables = 64;
    pub const Validator = struct {
        ptr: *anyopaque,
        check: *const fn (*anyopaque, Allocator, []const Table) anyerror!void,
    };

    alloc: Allocator,
    tables: std.ArrayList(Table) = .empty,
    checked_generation: ?u64 = null,
    validating: bool = false,

    pub fn init(alloc: Allocator) Guard {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Guard) void {
        for (self.tables.items) |*table| table.deinit(self.alloc);
        self.tables.deinit(self.alloc);
        self.* = undefined;
    }

    /// Copy before the first source read, including reads during Stream.open.
    /// A repeated name cannot acquire a newer binding or broader row policy.
    pub fn capture(self: *Guard, table: Table) !void {
        if (self.validating) return error.InvalidReadFenceTransition;
        for (self.tables.items) |existing| {
            if (!existing.sameName(table)) continue;
            if (!existing.sameBinding(table)) return error.CatalogGenerationChanged;
            if ((existing.filter == null) != (table.filter == null) or
                (existing.filter != null and !std.mem.eql(u8, existing.filter.?, table.filter.?))) return error.Forbidden;
            return;
        }
        if (self.tables.items.len == max_tables) return error.SqlProgramLimitExceeded;
        var owned = try table.clone(self.alloc);
        errdefer owned.deinit(self.alloc);
        try self.tables.append(self.alloc, owned);
        self.checked_generation = null;
    }

    /// A successful check is reusable only within the same runnable grant.
    /// Output delivery forces a check even when no scheduling yield occurred.
    pub fn check(self: *Guard, scratch: Allocator, generation: u64, force: bool, validator: Validator) !void {
        if (self.validating) return error.InvalidReadFenceTransition;
        if (self.tables.items.len == 0) return;
        if (!force and self.checked_generation == generation) return;
        self.checked_generation = null;
        self.validating = true;
        defer self.validating = false;
        try validator.check(validator.ptr, scratch, self.tables.items);
        self.checked_generation = generation;
    }
};

fn fixtureTable() Table {
    return .{ .database = "main", .namespace = "public", .name = "docs", .physical = "private-7", .id = 7, .schema_version = 3, .catalog_revision = 19, .filter = "{\"term\":{\"tenant\":\"admitted\"}}", .row_policy = .{ .generation = 2, .catalog_epoch = 3 } };
}

test "read fence validates each resumed grant and forced output without caching failure" {
    var guard = Guard.init(std.testing.allocator);
    defer guard.deinit();
    const Current = struct {
        calls: usize = 0,
        failure: ?anyerror = null,
        fn check(raw: *anyopaque, _: Allocator, tables: []const Table) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expectEqual(@as(usize, 1), tables.len);
            if (self.failure) |err| return err;
        }
    };
    var current: Current = .{};
    const validator: Guard.Validator = .{ .ptr = &current, .check = Current.check };
    try guard.capture(fixtureTable());
    try guard.check(std.testing.allocator, 1, false, validator);
    try guard.check(std.testing.allocator, 1, false, validator);
    try std.testing.expectEqual(@as(usize, 1), current.calls);
    // Model a cooperative yield, resource wait, and output wait separately.
    for ([_]u64{ 2, 3, 4 }) |generation| try guard.check(std.testing.allocator, generation, false, validator);
    try std.testing.expectEqual(@as(usize, 4), current.calls);
    try guard.check(std.testing.allocator, 4, true, validator);
    try std.testing.expectEqual(@as(usize, 5), current.calls);
    current.failure = error.Forbidden;
    try std.testing.expectError(error.Forbidden, guard.check(std.testing.allocator, 5, false, validator));
    try std.testing.expectError(error.Forbidden, guard.check(std.testing.allocator, 5, false, validator));
    try std.testing.expectEqual(@as(?u64, null), guard.checked_generation);
    current.failure = error.CatalogGenerationChanged;
    try std.testing.expectError(error.CatalogGenerationChanged, guard.check(std.testing.allocator, 5, false, validator));
    current.failure = null;
    try guard.check(std.testing.allocator, 5, false, validator);
    try std.testing.expectEqual(@as(usize, 9), current.calls);
}

test "read fence owns bounded immutable table and policy pins across allocation failure" {
    const a = std.testing.allocator;
    const Sweep = struct {
        fn run(alloc: Allocator) !void {
            var guard = Guard.init(alloc);
            defer guard.deinit();
            try guard.capture(fixtureTable());
            try guard.capture(fixtureTable());
            try std.testing.expectEqual(@as(usize, 1), guard.tables.items.len);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Sweep.run, .{});
    var guard = Guard.init(a);
    defer guard.deinit();
    var name = [_]u8{ 'd', 'o', 'c', 's' };
    var table = fixtureTable();
    table.name = &name;
    try guard.capture(table);
    name[0] = 'x';
    try std.testing.expectEqualStrings("docs", guard.tables.items[0].name);
    try std.testing.expectEqual(@as(u64, 2), guard.tables.items[0].row_policy.?.generation);
    table = fixtureTable();
    table.id += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, guard.capture(table));
    table = fixtureTable();
    table.filter = null;
    try std.testing.expectError(error.Forbidden, guard.capture(table));
    table = fixtureTable();
    table.row_policy.?.generation = 3;
    try std.testing.expectError(error.CatalogGenerationChanged, guard.capture(table));
    for (1..Guard.max_tables) |i| {
        var buffer: [32]u8 = undefined;
        table = fixtureTable();
        table.name = try std.fmt.bufPrint(&buffer, "docs-{d}", .{i});
        try guard.capture(table);
    }
    table.name = "overflow";
    try std.testing.expectError(error.SqlProgramLimitExceeded, guard.capture(table));
    try std.testing.expectEqual(Guard.max_tables, guard.tables.items.len);
}

test "read fence invalidates a grant when binding adds a table and protects validation ownership" {
    const a = std.testing.allocator;
    var guard = Guard.init(a);
    defer guard.deinit();
    const Current = struct {
        guard: *Guard,
        expected_tables: usize = 1,
        calls: usize = 0,

        fn check(raw: *anyopaque, alloc: Allocator, tables: []const Table) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expectEqual(self.expected_tables, tables.len);
            // A validator cannot mutate pins or recursively declare its own
            // check successful while the outer current-state read is pending.
            try std.testing.expectError(error.InvalidReadFenceTransition, self.guard.capture(fixtureTable()));
            try std.testing.expectError(error.InvalidReadFenceTransition, self.guard.check(alloc, 9, false, .{ .ptr = raw, .check = check }));
            // Validation scratch may use a protected allocator distinct from
            // the pins' original owner. Failure must leave those pins intact.
            const scratch = try alloc.alloc(u8, 32);
            defer alloc.free(scratch);
        }
    };
    var current: Current = .{ .guard = &guard };
    const validator: Guard.Validator = .{ .ptr = &current, .check = Current.check };
    try guard.capture(fixtureTable());
    try guard.check(a, 9, false, validator);
    var other = fixtureTable();
    other.name = "joined";
    try guard.capture(other);
    current.expected_tables = 2;
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, guard.check(failing.allocator(), 9, false, validator));
    try std.testing.expectEqual(@as(usize, 2), guard.tables.items.len);
    try std.testing.expectEqualStrings("joined", guard.tables.items[1].name);
    try std.testing.expectEqual(@as(?u64, null), guard.checked_generation);
    try std.testing.expect(!guard.validating);
    try guard.check(a, 9, false, validator);
    try std.testing.expectEqual(@as(usize, 3), current.calls);
}
