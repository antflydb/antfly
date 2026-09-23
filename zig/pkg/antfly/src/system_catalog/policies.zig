// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Catalog-owned, schema-bound row-policy definitions. These records are not
//! permission to enable RLS: owners must first implement pre-page filtering and
//! guarded old-image/new-image checks on every read and mutation route.
const std = @import("std");
const ast = @import("../sql/ast.zig");
const scalar = @import("../sql/scalar.zig");
const sql_catalog = @import("../sql/catalog.zig");
const setting_catalog = @import("../sql/setting_catalog.zig");

pub const CommandScope = packed struct(u4) {
    select: bool = false,
    insert: bool = false,
    update: bool = false,
    delete: bool = false,

    pub fn any(self: @This()) bool {
        return self.select or self.insert or self.update or self.delete;
    }
};

/// Persist the binder's typed instruction DAG, never SQL text interpreted by
/// a data owner. The publisher binds against the exact schema and setting view;
/// the recipient still checks bounds, dependencies and boolean result type.
pub const Predicate = struct {
    instructions: []const scalar.Instruction,
    root: u32,

    pub fn validate(self: @This(), columns: []const sql_catalog.Column, settings: *const setting_catalog.View) !void {
        if (self.instructions.len == 0 or self.instructions.len > 8192 or self.root >= self.instructions.len) return error.InvalidRowPolicyProgram;
        if (self.instructions[self.root].type.kind != ast.ColumnType.boolean) return error.InvalidRowPolicyProgram;
        for (self.instructions, 0..) |instruction, i| {
            switch (instruction.operation) {
                .literal => |value| {
                    const actual: ?ast.ColumnType = switch (value) {
                        .null => null,
                        .bool => .boolean,
                        .integer => .integer,
                        .float, .number_string => .number,
                        .string => .string,
                        .array, .object => .json,
                    };
                    if (actual != null and instruction.type.kind != actual) return error.InvalidRowPolicyProgram;
                },
                .column => |ordinal| {
                    if (ordinal >= columns.len or instruction.type.kind != columns[ordinal].type) return error.InvalidRowPolicyProgram;
                },
                .parameter => return error.InvalidRowPolicyProgram,
                .unary => |node| {
                    try predecessor(node.operand, i);
                    const operand = self.instructions[node.operand].type.kind;
                    switch (node.op) {
                        .not, .is_true, .is_not_true, .is_false, .is_not_false => if (instruction.type.kind != .boolean or operand != .boolean) return error.InvalidRowPolicyProgram,
                        .is_null, .is_not_null => if (instruction.type.kind != .boolean) return error.InvalidRowPolicyProgram,
                        .positive, .negative => if (instruction.type.kind == null or operand != instruction.type.kind or (operand != .integer and operand != .number)) return error.InvalidRowPolicyProgram,
                    }
                },
                .binary => |node| {
                    try predecessor(node.left, i);
                    try predecessor(node.right, i);
                    const left = self.instructions[node.left].type.kind;
                    const right = self.instructions[node.right].type.kind;
                    switch (node.op) {
                        .@"and", .@"or" => if (instruction.type.kind != .boolean or left != .boolean or right != .boolean) return error.InvalidRowPolicyProgram,
                        .eq, .neq, .lt, .lte, .gt, .gte, .is_distinct, .is_not_distinct, .like, .ilike => if (instruction.type.kind != .boolean) return error.InvalidRowPolicyProgram,
                        .add, .subtract, .multiply, .divide, .modulo => if (instruction.type.kind != .integer and instruction.type.kind != .number) return error.InvalidRowPolicyProgram,
                        .concat, .json_text => if (instruction.type.kind != .string) return error.InvalidRowPolicyProgram,
                        .json_get => if (instruction.type.kind != .json) return error.InvalidRowPolicyProgram,
                    }
                },
                .call => |node| {
                    if (node.args.len > 64 or node.function == .@"$single" or node.function == .@"$pattern_quantified") return error.InvalidRowPolicyProgram;
                    for (node.args) |arg| try predecessor(arg, i);
                    if (node.function == .current_setting) {
                        if (node.args.len != 0 or instruction.type.kind != .string) return error.InvalidRowPolicyProgram;
                        const identity = node.setting_identity orelse return error.InvalidRowPolicyProgram;
                        const definition = for (settings.definitions) |candidate| {
                            if (candidate.identity.id == identity.id) break candidate;
                        } else return error.SettingCatalogChanged;
                        if (definition.identity.generation != identity.generation or !definition.policy_sensitive or definition.session_writable) return error.SettingCatalogChanged;
                    } else {
                        if (node.setting_identity != null) return error.InvalidRowPolicyProgram;
                        const count = node.args.len;
                        const valid = switch (node.function) {
                            .abs, .lower, .upper, .length, .octet_length, .ceil, .floor, .round, .sqrt, .to_timestamp => count == 1,
                            .nullif, .power, .mod, .starts_with, .date_part, .date_trunc => count == 2,
                            .substring => count == 2 or count == 3,
                            .replace => count == 3,
                            .trim, .ltrim, .rtrim => count == 1 or count == 2,
                            .coalesce, .greatest, .least => count > 0,
                            .concat => true,
                            else => false,
                        };
                        if (!valid) return error.InvalidRowPolicyProgram;
                    }
                },
                .cast => |node| {
                    try predecessor(node.operand, i);
                    if (instruction.type.kind != node.type) return error.InvalidRowPolicyProgram;
                },
                .case_when => |node| {
                    if (node.branches.len > 256) return error.InvalidRowPolicyProgram;
                    for (node.branches) |branch| {
                        try predecessor(branch.condition, i);
                        try predecessor(branch.value, i);
                        if (self.instructions[branch.condition].type.kind != .boolean or self.instructions[branch.value].type.kind != instruction.type.kind) return error.InvalidRowPolicyProgram;
                    }
                    if (node.otherwise) |otherwise| {
                        try predecessor(otherwise, i);
                        if (self.instructions[otherwise].type.kind != instruction.type.kind) return error.InvalidRowPolicyProgram;
                    }
                },
                .in_list => |node| {
                    if (node.values.len > 1024) return error.InvalidRowPolicyProgram;
                    if (instruction.type.kind != .boolean) return error.InvalidRowPolicyProgram;
                    try predecessor(node.operand, i);
                    for (node.values) |value| try predecessor(value, i);
                },
            }
        }
    }
};

fn predecessor(node: u32, index: usize) !void {
    if (node >= index) return error.InvalidRowPolicyProgram;
}

pub const Record = struct {
    id: u64,
    generation: u64,
    table_id: u64,
    schema_version: u32,
    schema_digest: [32]u8,
    name: []const u8,
    commands: CommandScope,
    roles: []const []const u8,
    permissive: bool = true,
    using: ?Predicate = null,
    with_check: ?Predicate = null,

    pub fn validateShape(self: @This()) !void {
        if (self.id == 0 or self.generation == 0 or self.table_id == 0 or self.schema_version == 0 or
            self.name.len == 0 or self.name.len > 128 or !self.commands.any() or self.roles.len == 0 or self.roles.len > 256)
            return error.InvalidRowPolicyRecord;
        for (self.name) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '_') return error.InvalidRowPolicyRecord;
        for (self.roles, 0..) |role, i| {
            if (role.len == 0 or role.len > 128 or !std.unicode.utf8ValidateSlice(role)) return error.InvalidRowPolicyRecord;
            for (self.roles[0..i]) |prior| if (std.mem.eql(u8, prior, role)) return error.InvalidRowPolicyRecord;
        }
        if ((self.commands.select or self.commands.update or self.commands.delete) and self.using == null) return error.InvalidRowPolicyRecord;
        if ((self.commands.insert or self.commands.update) and self.with_check == null) return error.InvalidRowPolicyRecord;
        if (self.using == null and self.with_check == null) return error.InvalidRowPolicyRecord;
    }

    pub fn validate(self: @This(), table: sql_catalog.Table, schema_digest: [32]u8, settings: *const setting_catalog.View) !void {
        try self.validateShape();
        if (self.table_id != table.id or self.schema_version != table.schema_version or
            !std.mem.eql(u8, &self.schema_digest, &schema_digest) or table.storage_mode != .relational)
            return error.InvalidRowPolicyRecord;
        if (self.using) |program| try program.validate(table.columns, settings);
        if (self.with_check) |program| try program.validate(table.columns, settings);
    }
};

/// Internal-only Raft payload. Public policy DDL is deliberately unavailable
/// until all data owners can enforce the new generation.
pub const Command = struct {
    version: u16 = 1,
    expected_revision: u64,
    change: union(enum) { put: Record, drop: struct { id: u64, generation: u64, table_id: u64 } },
};

pub const Snapshot = struct {
    table_id: u64,
    schema_version: u32,
    schema_digest: [32]u8,
    policy_generation: u64,
    catalog_epoch: u64,
    principal: []const u8,
    database: []const u8,
    records: []const Record,
};

/// The owner returns a snapshot from one authenticated, linearizable catalog
/// cut, with all nested slices allocated in the supplied request arena (or
/// static). A production owner can clone a retained immutable epoch into that
/// arena; borrowed mutable Raft buffers are forbidden by this contract.
pub const Owner = struct {
    ptr: *anyopaque,
    load: *const fn (*anyopaque, std.mem.Allocator, u64, []const u8, []const u8) anyerror!Snapshot,
};

pub const View = struct {
    arena: std.heap.ArenaAllocator,
    snapshot: Snapshot,

    pub fn capture(backing: std.mem.Allocator, owner: Owner, table: sql_catalog.Table, schema_digest: [32]u8, settings: *const setting_catalog.View) !View {
        var arena = std.heap.ArenaAllocator.init(backing);
        errdefer arena.deinit();
        const alloc = arena.allocator();
        const raw = try owner.load(owner.ptr, alloc, table.id, settings.scope.principal, settings.scope.database);
        if (raw.table_id != table.id or raw.schema_version != table.schema_version or
            !std.mem.eql(u8, &raw.schema_digest, &schema_digest) or raw.policy_generation == 0 or
            raw.catalog_epoch != settings.epoch or
            !std.mem.eql(u8, raw.principal, settings.scope.principal) or
            !std.mem.eql(u8, raw.database, settings.scope.database) or raw.records.len > 1024)
            return error.RowPolicyCatalogChanged;
        for (raw.records, 0..) |record, i| {
            try record.validate(table, schema_digest, settings);
            for (raw.records[0..i]) |prior| if (prior.id == record.id or std.ascii.eqlIgnoreCase(prior.name, record.name)) return error.InvalidRowPolicyRecord;
        }
        return .{ .arena = arena, .snapshot = raw };
    }

    pub fn deinit(self: *View) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn requireCurrent(self: *const View, generation: u64, catalog_epoch: u64, schema_version: u32) !void {
        if (self.snapshot.policy_generation != generation or self.snapshot.catalog_epoch != catalog_epoch or self.snapshot.schema_version != schema_version)
            return error.RowPolicyCatalogChanged;
    }
};

test "row policy capture fences revocation and rejects client-writable setting dependencies" {
    const alloc = std.testing.allocator;
    const Setting = @import("settings.zig");
    const Fake = struct {
        fn load(_: *anyopaque, _: std.mem.Allocator, scope: Setting.Scope) !Setting.Snapshot {
            return .{ .scope = scope, .epoch = 7, .definitions = &.{.{ .identity = .{ .id = 4, .generation = 2 }, .name = "app.tenant", .kind = .string, .policy_sensitive = true, .default = .{ .string = "tenant-a" } }} };
        }
    };
    var dummy: u8 = 0;
    var settings = try setting_catalog.View.capture(alloc, .{ .ptr = &dummy, .load = Fake.load }, .{ .principal = "alice", .database = "main" }, &.{});
    defer settings.deinit();
    const table: sql_catalog.Table = .{ .id = 3, .physical_name = "table:3", .schema_version = 5, .columns = &.{.{ .name = "tenant", .path = "tenant", .type = .string }} };
    const predicate: Predicate = .{ .instructions = &.{.{ .type = .{ .kind = .boolean, .nullable = false }, .operation = .{ .literal = .{ .bool = true } } }}, .root = 0 };
    const record: Record = .{ .id = 9, .generation = 1, .table_id = 3, .schema_version = 5, .schema_digest = @splat(8), .name = "tenant_guard", .commands = .{ .select = true }, .roles = &.{"alice"}, .using = predicate };
    const FakePolicy = struct {
        fn load(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !Snapshot {
            return .{ .table_id = 3, .schema_version = 5, .schema_digest = @splat(8), .policy_generation = 11, .catalog_epoch = 7, .principal = "alice", .database = "main", .records = &.{record} };
        }
    };
    var view = try View.capture(alloc, .{ .ptr = &dummy, .load = FakePolicy.load }, table, @splat(8), &settings);
    defer view.deinit();
    try view.requireCurrent(11, 7, 5);
    try std.testing.expectError(error.RowPolicyCatalogChanged, view.requireCurrent(12, 7, 5));
    try std.testing.expectError(error.RowPolicyCatalogChanged, view.requireCurrent(11, 8, 5));
    var stale_table = table;
    stale_table.schema_version = 6;
    try std.testing.expectError(error.RowPolicyCatalogChanged, View.capture(alloc, .{ .ptr = &dummy, .load = FakePolicy.load }, stale_table, @splat(8), &settings));
    stale_table = table;
    stale_table.id = 4;
    try std.testing.expectError(error.RowPolicyCatalogChanged, View.capture(alloc, .{ .ptr = &dummy, .load = FakePolicy.load }, stale_table, @splat(8), &settings));
    const spoofed: Predicate = .{ .instructions = &.{
        .{ .type = .{ .kind = .string }, .operation = .{ .literal = .{ .string = "app.tenant" } } },
        .{ .type = .{ .kind = .string }, .operation = .{ .call = .{ .function = .current_setting, .args = &.{}, .setting_identity = .{ .id = 4, .generation = 1 } } } },
        .{ .type = .{ .kind = .boolean }, .operation = .{ .literal = .{ .bool = true } } },
    }, .root = 2 };
    try std.testing.expectError(error.SettingCatalogChanged, spoofed.validate(table.columns, &settings));
    const malformed: Predicate = .{ .instructions = &.{
        .{ .type = .{ .kind = .boolean }, .operation = .{ .literal = .{ .bool = true } } },
        .{ .type = .{ .kind = .boolean }, .operation = .{ .call = .{ .function = .starts_with, .args = &.{0} } } },
    }, .root = 1 };
    try std.testing.expectError(error.InvalidRowPolicyProgram, malformed.validate(table.columns, &settings));
}
