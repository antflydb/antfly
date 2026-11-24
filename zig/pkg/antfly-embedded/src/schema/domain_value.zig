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

//! Schema-owned domain programs over one typed VALUE slot. SQL assignment,
//! ingress and activation deltas share this prepared validator. Retained-row
//! restore must preserve domain provenance, rather than re-running coercions.
//! Preparation does not admit or activate a catalog generation.
const std = @import("std");
const domains = @import("../system_catalog/domain_types.zig");
const types = @import("../system_catalog/named_types.zig");
const enum_values = @import("../common/sql_enum_value.zig");
const schema = @import("../storage/schema.zig");
const vm = @import("relational_expression.zig");
const A = std.mem.Allocator;
pub const Value = vm.Value;
pub const Mode = enum { assignment, validated_contract };
pub const Violation = struct {
    /// Zero only for a standalone pre-admission program. Registry-owned
    /// programs always retain the exact catalog identity that failed.
    type_id: u64 = 0,
    kind: enum { not_null, check },
    constraint: ?[]const u8 = null,
};

/// Sparse type metadata lives with the immutable registry, not inline in every
/// scalar instruction. Symbolic references still need catalog rebinding.
pub const Binding = union(enum) { view: *const View, reference: domains.Reference };

pub const View = struct {
    definition: domains.Definition,
    column: schema.RelationalColumn,
    default: ?vm.Plan,
    checks: []vm.Plan,
    check_order: []u32,
    alloc: A,
    type_id: u64 = 0,
    identity: ?types.Identity = null,
    type_binding: Binding = undefined,
    base_depth: usize = 1,
    parent: ?*const View = null,
    enumeration: ?*const enum_values.View = null,

    const Base = union(enum) { domain: *const View, enumeration: *const enum_values.View };

    /// Borrowed executable handles expose identity only. Deserializing one
    /// cannot mint authority; callers must rebind through a pinned registry.
    pub fn jsonStringify(self: View, writer: anytype) !void {
        return writer.write(.{ .kind = "domain", .identity = self.identity });
    }

    pub fn jsonParse(_: A, source: anytype, _: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!View {
        return error.UnexpectedToken;
    }

    pub fn jsonParseFromValue(_: A, _: std.json.Value, _: std.json.ParseOptions) std.json.ParseFromValueError!View {
        return error.UnexpectedToken;
    }

    pub fn init(a: A, definition: domains.Definition) !View {
        return initResolved(a, definition, null);
    }

    fn initResolved(a: A, definition: domains.Definition, base: ?Base) !View {
        try definition.validate(a);
        const parent: ?*const View = if (base) |resolved| if (resolved == .domain) resolved.domain else null else null;
        const enumeration: ?*const enum_values.View = if (base) |resolved| switch (resolved) {
            .enumeration => |view| view,
            .domain => |view| view.enumeration,
        } else null;
        const base_depth = if (parent) |view| view.base_depth + 1 else 1;
        if (base_depth > Registry.max_depth) return error.SqlProgramLimitExceeded;
        const column: schema.RelationalColumn = if (parent) |view| view.column else if (enumeration) |view| .{
            .name = "value",
            .path = "value",
            .allows_null = true,
            .column_type = .enumeration,
            .named_type_id = view.record.identity.id,
        } else builtinColumn(switch (definition.base) {
            .builtin => |kind| kind,
            // The catalog registry must first resolve named bases against one
            // immutable cut. Never guess a base family from incoming values.
            .named => return error.SqlDomainBaseRequiresRegistry,
        }, definition.numeric_modifier);
        var copy = try definition.cloneOwned(a);
        errdefer copy.deinitOwned(a);
        var default: ?vm.Plan = null;
        errdefer if (default) |*plan| plan.deinit();
        if (definition.default_expression) |expression| {
            default = try compile(a, column, enumeration, expression, column.column_type, true);
            if (default.?.dependencies.len != 0) return error.InvalidSqlDomainDefault;
        }
        const checks = try a.alloc(vm.Plan, definition.checks.len);
        errdefer a.free(checks);
        var initialized: usize = 0;
        errdefer for (checks[0..initialized]) |*check| check.deinit();
        for (definition.checks, checks) |source, *plan| {
            plan.* = try compile(a, column, enumeration, source.expression, .boolean, false);
            initialized += 1;
        }
        const check_order = try a.alloc(u32, checks.len);
        errdefer a.free(check_order);
        for (check_order, 0..) |*index, i| index.* = @intCast(i);
        const Order = struct {
            fn less(items: []const domains.Check, left: u32, right: u32) bool {
                return std.mem.order(u8, items[left].name, items[right].name) == .lt;
            }
        };
        std.mem.sort(u32, check_order, copy.checks, Order.less);
        return .{ .definition = copy, .column = column, .default = default, .checks = checks, .check_order = check_order, .alloc = a, .base_depth = base_depth, .parent = parent, .enumeration = enumeration };
    }

    pub fn deinit(self: *View) void {
        for (self.checks) |*check| check.deinit();
        self.alloc.free(self.checks);
        self.alloc.free(self.check_order);
        if (self.default) |*plan| plan.deinit();
        self.definition.deinitOwned(self.alloc);
        self.* = undefined;
    }

    /// SQL NULL reaching this boundary is an assignment/cast, not the NULL
    /// introduced by a LEFT JOIN. Reads do not re-apply domain NOT NULL.
    /// validated_contract is a standalone invariant probe, not a restore or
    /// activation API. Use validateCoercion for assignments and Activation for
    /// predecessor/successor scans; NOT VALID still checks new base inputs.
    pub fn validate(self: *const View, execution: *vm.Execution, value: Value, mode: Mode) !?Violation {
        try execution.charge(1);
        if (self.parent) |parent| if (try parent.validate(execution, value, mode)) |violation| return violation;
        try self.validateBase(value);
        if (self.definition.not_null and value == .null) return .{ .type_id = self.type_id, .kind = .not_null };
        for (self.check_order) |index| {
            const plan = &self.checks[index];
            const check = self.definition.checks[index];
            try execution.charge(1);
            if (mode == .validated_contract and !check.validated) continue;
            switch (try plan.evaluateWithExecution(execution, &.{value})) {
                .null => {}, // CHECK UNKNOWN succeeds, independently of NOT NULL.
                .boolean => |accepted| if (!accepted) return .{ .type_id = self.type_id, .kind = .check, .constraint = check.name },
                else => return error.InvalidSqlDomainProgram,
            }
        }
        return null;
    }

    fn validateBase(self: *const View, value: Value) !void {
        if (value != .null) {
            const matches = switch (self.column.column_type) {
                .string => value == .string,
                .integer => value == .integer,
                .number => value == .number,
                .numeric => value == .numeric,
                .boolean => value == .boolean,
                .json => value == .json,
                .enumeration => value == .enumeration,
                else => false,
            };
            if (!matches) return error.InvalidRelationalExpressionInput;
            if (self.enumeration) |view| _ = try view.output(value.enumeration);
        }
    }

    /// Only a trusted compiled SQL descriptor/native reader may supply this
    /// witness. Client JSON and raw parameter values always use null. Keeping
    /// provenance on the prepared column avoids widening ordinary value slots.
    /// PostgreSQL does not re-coerce an already identical domain-typed value,
    /// including a NULL introduced by an empty scalar subquery/outer join.
    pub fn validateCoercion(self: *const View, execution: *vm.Execution, value: Value, source: ?domains.Reference) !?Violation {
        if (source) |witness| {
            const identity = self.identity orelse return error.InvalidSqlNamedType;
            if (witness.id != identity.id or witness.generation != identity.generation or !std.mem.eql(u8, &witness.origin_id, &identity.origin_id)) return error.CatalogGenerationChanged;
            try execution.charge(1);
            try self.validateBase(value);
            return null;
        }
        return self.validate(execution, value, .assignment);
    }

    /// No default and DEFAULT NULL are distinct. Column defaults take
    /// precedence in the caller; this never rewrites a stored historical row.
    pub fn defaultValue(self: *const View, execution: *vm.Execution) !?Value {
        const plan = if (self.default) |*plan| plan else return if (self.parent) |parent| parent.defaultValue(execution) else null;
        const value = try plan.evaluateWithExecution(execution, &.{});
        return value;
    }
};

fn compile(a: A, column: schema.RelationalColumn, enumeration: ?*const enum_values.View, bytes: []const u8, expected: schema.RelationalColumnType, default: bool) !vm.Plan {
    var expression = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .parse_numbers = false });
    defer expression.deinit();
    const table: schema.TableSchema = .{
        .storage_mode = .relational,
        .relational_columns = &.{column},
        .requires_public_schema = true,
        .requires_enum_expressions = enumeration != null,
        .named_types = if (enumeration) |view| &.{view.record} else &.{},
    };
    return if (default) vm.Plan.initDefault(a, table, expression.value, expected) else vm.Plan.init(a, table, expression.value, expected);
}

test "SQL domain prepared VALUE checks preserve exact numeric NULL and NOT VALID assignment semantics" {
    const a = std.testing.allocator;
    const definition: domains.Definition = .{
        .base = .{ .builtin = .numeric },
        .checks = &.{
            .{ .name = "positive", .expression =
            \\{"op":"gt","args":[{"op":"column","column":"value"},{"op":"literal","type":"numeric","value":"0"}]}
            },
            .{ .name = "bounded", .validated = false, .expression =
            \\{"op":"lt","args":[{"op":"column","column":"value"},{"op":"literal","type":"numeric","value":"9007199254740993.002"}]}
            },
        },
    };
    var view = try View.init(a, definition);
    defer view.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var bytes: usize = vm.max_allocated_bytes;
    var execution = vm.Execution.init(scratch.allocator(), &bytes);
    const good = try vm.numericJson(&execution, .{ .string = "9007199254740993.001" });
    const bad = try vm.numericJson(&execution, .{ .string = "9007199254740993.003" });
    const negative = try vm.numericJson(&execution, .{ .string = "-0.0000000000000001" });
    try std.testing.expectEqualDeep(@as(?Violation, null), try view.validate(&execution, good, .assignment));
    try std.testing.expectEqualDeep(@as(?Violation, null), try view.validate(&execution, .null, .assignment));
    try std.testing.expectEqualDeep(@as(?Violation, .{ .kind = .check, .constraint = "bounded" }), try view.validate(&execution, bad, .assignment));
    try std.testing.expectEqualDeep(@as(?Violation, null), try view.validate(&execution, bad, .validated_contract));
    try std.testing.expectEqualDeep(@as(?Violation, .{ .kind = .check, .constraint = "positive" }), try view.validate(&execution, negative, .validated_contract));
    view.definition.not_null = true;
    try std.testing.expectEqualDeep(@as(?Violation, .{ .kind = .not_null }), try view.validate(&execution, .null, .assignment));
}

test "SQL domain prepared defaults distinguish omission NULL and historical values" {
    const a = std.testing.allocator;
    var view = try View.init(a, .{ .base = .{ .builtin = .text }, .not_null = true, .default_expression =
        \\{"op":"literal","type":"string","value":"queued"}
    });
    defer view.deinit();
    var bytes: usize = vm.max_allocated_bytes;
    var execution = vm.Execution.init(a, &bytes);
    try std.testing.expectEqualStrings("queued", (try view.defaultValue(&execution)).?.string);
    try std.testing.expectEqualDeep(@as(?Violation, .{ .kind = .not_null }), try view.validate(&execution, .null, .assignment));
    var absent = try View.init(a, .{ .base = .{ .builtin = .text } });
    defer absent.deinit();
    try std.testing.expectEqual(@as(?Value, null), try absent.defaultValue(&execution));
    var explicit_null = try View.init(a, .{ .base = .{ .builtin = .text }, .default_expression =
        \\{"op":"literal","type":"string","value":null}
    });
    defer explicit_null.deinit();
    try std.testing.expect((try explicit_null.defaultValue(&execution)).? == .null);
    try std.testing.expectError(error.InvalidSqlDomainDefault, View.init(a, .{ .base = .{ .builtin = .text }, .default_expression =
        \\{"op":"column","column":"value"}
    }));
    try std.testing.expectError(error.RelationalIndexColumnNotFound, View.init(a, .{ .base = .{ .builtin = .text }, .checks = &.{.{ .name = "bad", .expression =
        \\{"op":"is_not_null","args":[{"op":"column","column":"other"}]}
    }} }));
}

test "SQL domain prepared programs unwind every allocation failure" {
    const Harness = struct {
        fn run(a: A) !void {
            var view = try View.init(a, .{ .base = .{ .builtin = .text }, .default_expression =
                \\{"op":"literal","type":"string","value":"queued"}
            , .checks = &.{.{ .name = "present", .expression =
                \\{"op":"is_not_null","args":[{"op":"column","column":"value"}]}
            }} });
            defer view.deinit();
            var bytes: usize = vm.max_allocated_bytes;
            var execution = vm.Execution.init(a, &bytes);
            try std.testing.expectEqualStrings("queued", (try view.defaultValue(&execution)).?.string);
            try std.testing.expectEqualDeep(@as(?Violation, null), try view.validate(&execution, .{ .string = "done" }, .assignment));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

fn builtinColumn(kind: @import("../common/sql_builtin_type.zig").Type, modifier: ?@import("../common/sql_builtin_type.zig").NumericModifier) schema.RelationalColumn {
    return schema.builtinTypeColumn(kind, modifier);
}

/// Stable-address preparation directory. Resolve the entire base DAG once;
/// every row subsequently borrows a prepared chain without catalog access.
pub const Registry = struct {
    pub const max_types = 4096;
    pub const max_depth = 64;
    pub const max_bytes = 16 * 1024 * 1024;
    pub const Executable = union(enum) { enumeration: enum_values.View, domain: View };
    pub const Entry = struct { record: *const types.Record, executable: ?Executable = null };
    records: []types.Record,
    entries: []Entry,
    alloc: A,

    pub fn init(a: A, input: []const types.Record) !Registry {
        if (input.len > max_types) return error.SqlProgramLimitExceeded;
        const records = try a.alloc(types.Record, input.len);
        var initialized: usize = 0;
        errdefer {
            for (records[0..initialized]) |*record| record.deinitOwned(a);
            a.free(records);
        }
        const entries = try a.alloc(Entry, input.len);
        for (entries, records) |*entry, *record| entry.* = .{ .record = record };
        errdefer {
            for (entries) |*entry| if (entry.executable) |*executable| switch (executable.*) {
                .enumeration => |*view| view.deinit(a),
                .domain => |*view| view.deinit(),
            };
            a.free(entries);
        }
        var origins: std.AutoHashMapUnmanaged([16]u8, void) = .empty;
        defer origins.deinit(a);
        var previous: u64 = 0;
        var bytes: usize = 0;
        for (input, records) |record, *owned| {
            try record.validate(a);
            if (record.identity.id <= previous or (try origins.getOrPut(a, record.identity.origin_id)).found_existing) return error.InvalidSqlNamedType;
            previous = record.identity.id;
            const encoded = try record.encode(a);
            defer a.free(encoded);
            bytes +|= encoded.len;
            if (bytes > max_bytes) return error.SqlProgramLimitExceeded;
            owned.* = try record.cloneOwned(a);
            initialized += 1;
        }
        var registry: Registry = .{ .records = records, .entries = entries, .alloc = a };
        const states = try a.alloc(u8, entries.len);
        defer a.free(states);
        @memset(states, 0);
        for (0..entries.len) |index| try registry.prepare(index, states, 0);
        return registry;
    }

    pub fn deinit(self: *Registry) void {
        // No destructor traverses borrowed parents. Release all programs before
        // their source records; input order need not be topological.
        for (self.entries) |*entry| if (entry.executable) |*executable| switch (executable.*) {
            .enumeration => |*view| view.deinit(self.alloc),
            .domain => |*view| view.deinit(),
        };
        for (self.records) |*record| record.deinitOwned(self.alloc);
        self.alloc.free(self.records);
        self.alloc.free(self.entries);
        self.* = undefined;
    }
    pub fn byId(self: *const Registry, id: u64) ?*const Entry {
        const index = self.indexOf(id) orelse return null;
        return &self.entries[index];
    }
    fn indexOf(self: *const Registry, id: u64) ?usize {
        var lower: usize = 0;
        var upper = self.entries.len;
        while (lower < upper) {
            const middle = lower + (upper - lower) / 2;
            const current = self.entries[middle].record.identity.id;
            if (current == id) return middle;
            if (current < id) lower = middle + 1 else upper = middle;
        }
        return null;
    }
    fn prepare(self: *Registry, index: usize, states: []u8, depth: usize) anyerror!void {
        if (states[index] == 2) return;
        if (states[index] == 1) return error.SqlDomainBaseCycle;
        if (depth >= max_depth) return error.SqlProgramLimitExceeded;
        states[index] = 1;
        const entry = &self.entries[index];
        entry.executable = switch (entry.record.definition) {
            .labels => .{ .enumeration = try enum_values.View.init(self.alloc, entry.record.*) },
            .domain => |definition| domain: {
                var base: ?View.Base = null;
                if (definition.base == .named) {
                    const reference = definition.base.named;
                    const parent_index = self.indexOf(reference.id) orelse return error.InvalidSqlNamedType;
                    const parent = &self.entries[parent_index];
                    if (reference.generation != parent.record.identity.generation or !std.mem.eql(u8, &reference.origin_id, &parent.record.identity.origin_id)) return error.InvalidSqlNamedType;
                    try self.prepare(parent_index, states, depth + 1);
                    base = switch (parent.executable.?) {
                        .enumeration => |*view| .{ .enumeration = view },
                        .domain => |*view| .{ .domain = view },
                    };
                }
                break :domain .{ .domain = try View.initResolved(self.alloc, definition, base) };
            },
        };
        if (entry.executable.? == .domain) {
            entry.executable.?.domain.type_id = entry.record.identity.id;
            entry.executable.?.domain.identity = entry.record.identity;
            entry.executable.?.domain.type_binding = .{ .view = &entry.executable.?.domain };
        }
        states[index] = 2;
    }
};

test "SQL domain registry resolves forward bases once retains enum identity and rejects cycles and rebound generations" {
    const a = std.testing.allocator;
    const parent_identity = try types.Identity.init(9, 2, 1, @splat(2));
    const child_identity = try types.Identity.init(4, 2, 1, @splat(3));
    const reference: domains.Reference = .{ .id = 9, .origin_id = parent_identity.origin_id, .generation = 1 };
    const parent: types.Record = .{ .identity = parent_identity, .owner = .auth_disabled, .definition = .{ .domain = .{
        .base = .{ .builtin = .text },
        .not_null = true,
        .default_expression =
        \\{"op":"literal","type":"string","value":"queued"}
        ,
        .checks = &.{.{ .name = "queued", .expression =
        \\{"op":"eq","args":[{"op":"column","column":"value"},{"op":"literal","type":"string","value":"queued"}]}
        }},
    } } };
    const child: types.Record = .{ .identity = child_identity, .owner = .auth_disabled, .definition = .{ .domain = .{ .base = .{ .named = reference } } } };
    var registry = try Registry.init(a, &.{ child, parent });
    defer registry.deinit();
    const view = &registry.byId(4).?.executable.?.domain;
    var bytes: usize = vm.max_allocated_bytes;
    var execution = vm.Execution.init(a, &bytes);
    try std.testing.expectEqualStrings("queued", (try view.defaultValue(&execution)).?.string);
    try std.testing.expectEqualDeep(@as(?Violation, .{ .type_id = 9, .kind = .not_null }), try view.validate(&execution, .null, .assignment));
    try std.testing.expectEqualDeep(@as(?Violation, .{ .type_id = 9, .kind = .check, .constraint = "queued" }), try view.validate(&execution, .{ .string = "other" }, .assignment));
    try std.testing.expectError(error.InvalidRelationalExpressionInput, view.validate(&execution, .{ .integer = 5 }, .assignment));
    var rebound = child;
    rebound.definition.domain.base.named.generation += 1;
    try std.testing.expectError(error.InvalidSqlNamedType, Registry.init(a, &.{ rebound, parent }));
    rebound = child;
    rebound.definition.domain.base.named.origin_id = @splat(8);
    try std.testing.expectError(error.InvalidSqlNamedType, Registry.init(a, &.{ rebound, parent }));
    var cycle = parent;
    cycle.definition.domain.base = .{ .named = .{ .id = 4, .generation = 1, .origin_id = child_identity.origin_id } };
    try std.testing.expectError(error.SqlDomainBaseCycle, Registry.init(a, &.{ child, cycle }));
    try std.testing.expectError(error.InvalidSqlNamedType, Registry.init(a, &.{child}));

    var labels = try types.Owned.create(a, &.{ "queued", "done" });
    defer labels.deinit();
    const enumeration: types.Record = .{ .identity = parent_identity, .owner = .auth_disabled, .definition = labels.definition };
    var enum_registry = try Registry.init(a, &.{ child, enumeration });
    defer enum_registry.deinit();
    const enum_view = &enum_registry.byId(9).?.executable.?.enumeration;
    const domain = &enum_registry.byId(4).?.executable.?.domain;
    try std.testing.expect(domain.column.column_type == .enumeration);
    try std.testing.expectEqualDeep(@as(?Violation, null), try domain.validate(&execution, .{ .enumeration = try enum_view.input("done") }, .assignment));
    try std.testing.expectError(error.InvalidRelationalExpressionInput, domain.validate(&execution, .{ .string = "done" }, .assignment));
}

test "SQL domain registry cold ownership unwinds complete dependency preparation allocation failures" {
    const Harness = struct {
        fn run(a: A) !void {
            const identity = try types.Identity.init(2, 1, 1, @splat(1));
            const child: types.Record = .{
                .identity = try types.Identity.init(1, 1, 1, @splat(2)),
                .owner = .auth_disabled,
                .definition = .{ .domain = .{ .base = .{ .named = .{ .id = 2, .origin_id = identity.origin_id, .generation = 1 } } } },
            };
            const parent: types.Record = .{
                .identity = identity,
                .owner = .{ .role = .{ .name = "owner", .instance_id = @splat(1) } },
                .definition = .{ .domain = .{ .base = .{ .builtin = .text }, .default_expression =
                \\{"op":"literal","type":"string","value":"queued"}
                , .checks = &.{.{ .name = "present", .expression =
                \\{"op":"is_not_null","args":[{"op":"column","column":"value"}]}
                }} } },
            };
            var registry = try Registry.init(a, &.{ child, parent });
            defer registry.deinit();
            const view = &registry.byId(1).?.executable.?.domain;
            var bytes: usize = vm.max_allocated_bytes;
            var execution = vm.Execution.init(a, &bytes);
            try std.testing.expectEqualStrings("queued", (try view.defaultValue(&execution)).?.string);
            try std.testing.expectEqualDeep(@as(?Violation, null), try view.validate(&execution, .{ .string = "done" }, .assignment));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL domain prepared text validation borrows programs without row allocations" {
    const a = std.testing.allocator;
    var view = try View.init(a, .{ .base = .{ .builtin = .text }, .not_null = true, .checks = &.{.{ .name = "state", .expression =
        \\{"op":"in_list","args":[{"op":"column","column":"value"},{"op":"literal","type":"string","value":"queued"},{"op":"literal","type":"string","value":"done"}]}
    }} });
    defer view.deinit();
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var bytes: usize = vm.max_allocated_bytes;
    var execution = vm.Execution.init(denied.allocator(), &bytes);
    const count = 100_000;
    for (0..count) |_| try std.testing.expectEqualDeep(@as(?Violation, null), try view.validate(&execution, .{ .string = "done" }, .assignment));
    try std.testing.expect(!denied.has_induced_failure);
}

test "SQL domain prepared base depth is bounded independently of catalog identifier order" {
    const a = std.testing.allocator;
    const count = Registry.max_depth + 1;
    var records: [count]types.Record = undefined;
    for (&records, 0..) |*record, index| {
        const identity = try types.Identity.init(index + 1, 1, 1, @splat(@intCast(index + 1)));
        record.* = .{ .identity = identity, .owner = .auth_disabled, .definition = .{ .domain = .{
            .base = if (index == 0) .{ .builtin = .text } else .{ .named = .{
                .id = records[index - 1].identity.id,
                .origin_id = records[index - 1].identity.origin_id,
                .generation = 1,
            } },
        } } };
    }
    // Every parent has already been prepared when its child is visited. A DFS
    // recursion limit alone therefore cannot bound the execution chain.
    try std.testing.expectError(error.SqlProgramLimitExceeded, Registry.init(a, &records));
}

test "SQL domain CHECK evaluation follows PostgreSQL alphabetical order after NOT NULL" {
    const a = std.testing.allocator;
    const expression =
        \\{"op":"eq","args":[{"op":"column","column":"value"},{"op":"literal","type":"string","value":"good"}]}
    ;
    var view = try View.init(a, .{ .base = .{ .builtin = .text }, .not_null = true, .checks = &.{
        .{ .name = "z", .expression = expression },
        .{ .name = "a", .expression = expression },
    } });
    defer view.deinit();
    var bytes: usize = vm.max_allocated_bytes;
    var execution = vm.Execution.init(a, &bytes);
    try std.testing.expectEqualStrings("a", (try view.validate(&execution, .{ .string = "bad" }, .assignment)).?.constraint.?);
    try std.testing.expectEqual(.not_null, (try view.validate(&execution, .null, .assignment)).?.kind);
    try std.testing.expectEqualStrings("z", view.definition.checks[0].name);
}

/// A preparation-only restrictive delta between pinned executable contracts.
/// It grants no publication authority. The epoch coordinator still owns the
/// catalog cut, owner barriers, row snapshot and atomic final installation.
pub const Activation = struct {
    next: *const View,
    require_not_null: bool,
    checks: []u32,
    parent: ?*Activation,
    has_work: bool,
    alloc: A,

    pub fn init(a: A, previous: *const View, next: *const View) !Activation {
        const before = previous.identity orelse return error.InvalidSqlNamedType;
        const after = next.identity orelse return error.InvalidSqlNamedType;
        if (before.id != after.id or !std.mem.eql(u8, &before.origin_id, &after.origin_id) or
            after.generation < before.generation or after.generation - before.generation > 1) return error.CatalogGenerationChanged;
        if (previous.column.column_type != next.column.column_type or previous.column.sql_element_type != next.column.sql_element_type or
            !@import("../common/sql_builtin_type.zig").NumericModifier.eql(previous.column.numeric_modifier, next.column.numeric_modifier)) return error.InvalidSqlNamedType;
        if ((previous.enumeration == null) != (next.enumeration == null)) return error.InvalidSqlNamedType;
        if (previous.enumeration) |old| if (!old.record.sameValueDomain(next.enumeration.?.record)) return error.InvalidSqlNamedType;
        if ((previous.parent == null) != (next.parent == null)) return error.InvalidSqlNamedType;
        var parent: ?*Activation = null;
        errdefer if (parent) |plan| {
            plan.deinit();
            a.destroy(plan);
        };
        if (previous.parent) |old| {
            const plan = try a.create(Activation);
            errdefer a.destroy(plan);
            plan.* = try init(a, old, next.parent.?);
            parent = plan;
        }
        var checks: std.ArrayList(u32) = .empty;
        defer checks.deinit(a);
        var cursor: usize = 0;
        for (next.check_order) |index| {
            const current = next.definition.checks[index];
            if (!current.validated) continue;
            while (cursor < previous.check_order.len and std.mem.order(u8, previous.definition.checks[previous.check_order[cursor]].name, current.name) == .lt) : (cursor += 1) {}
            const old_index: ?u32 = if (cursor < previous.check_order.len and std.mem.eql(u8, previous.definition.checks[previous.check_order[cursor]].name, current.name)) previous.check_order[cursor] else null;
            const unchanged = if (old_index) |old| previous.definition.checks[old].validated and
                std.mem.eql(u8, &previous.checks[old].fingerprint, &next.checks[index].fingerprint) else false;
            if (!unchanged) try checks.append(a, index);
        }
        const require_not_null = next.definition.not_null and !previous.definition.not_null;
        // A contract cannot change behind an unchanged generation. Presentation
        // metadata is absent from these immutable executable definitions.
        if (after.generation == before.generation and !sameContract(previous.definition, next.definition)) return error.CatalogGenerationChanged;
        const has_work = require_not_null or checks.items.len != 0 or (if (parent) |plan| plan.has_work else false);
        return .{ .next = next, .require_not_null = require_not_null, .checks = try checks.toOwnedSlice(a), .parent = parent, .has_work = has_work, .alloc = a };
    }

    pub fn deinit(self: *Activation) void {
        if (self.parent) |plan| {
            plan.deinit();
            self.alloc.destroy(plan);
        }
        self.alloc.free(self.checks);
        self.* = undefined;
    }

    pub fn validate(self: *const Activation, execution: *vm.Execution, value: Value) !?Violation {
        try execution.charge(1);
        try self.next.validateBase(value);
        if (self.parent) |parent| if (parent.has_work) if (try parent.validate(execution, value)) |violation| return violation;
        if (self.require_not_null and value == .null) return .{ .type_id = self.next.type_id, .kind = .not_null };
        for (self.checks) |index| {
            try execution.charge(1);
            switch (try self.next.checks[index].evaluateWithExecution(execution, &.{value})) {
                .null => {},
                .boolean => |accepted| if (!accepted) return .{ .type_id = self.next.type_id, .kind = .check, .constraint = self.next.definition.checks[index].name },
                else => return error.InvalidSqlDomainProgram,
            }
        }
        return null;
    }
};

fn sameContract(left: domains.Definition, right: domains.Definition) bool {
    if (std.meta.activeTag(left.base) != std.meta.activeTag(right.base)) return false;
    switch (left.base) {
        .builtin => |kind| if (right.base.builtin != kind) return false,
        .named => |reference| if (!std.meta.eql(reference, right.base.named)) return false,
    }
    if (!@import("../common/sql_builtin_type.zig").NumericModifier.eql(left.numeric_modifier, right.numeric_modifier) or left.not_null != right.not_null or left.checks.len != right.checks.len) return false;
    if ((left.default_expression == null) != (right.default_expression == null)) return false;
    if (left.default_expression) |expression| if (!std.mem.eql(u8, expression, right.default_expression.?)) return false;
    for (left.checks, right.checks) |before, after| if (before.validated != after.validated or !std.mem.eql(u8, before.name, after.name) or !std.mem.eql(u8, before.expression, after.expression)) return false;
    return true;
}

test "SQL domain trusted coercion distinguishes typed NULL from unqualified input and fences stale provenance" {
    const a = std.testing.allocator;
    const identity = try types.Identity.init(7, 2, 2, @splat(8));
    const record: types.Record = .{ .identity = identity, .owner = .auth_disabled, .definition = .{ .domain = .{ .base = .{ .builtin = .text }, .not_null = true } } };
    var registry = try Registry.init(a, &.{record});
    defer registry.deinit();
    const view = &registry.byId(7).?.executable.?.domain;
    var bytes: usize = vm.max_allocated_bytes;
    var execution = vm.Execution.init(a, &bytes);
    const witness: domains.Reference = .{ .id = identity.id, .origin_id = identity.origin_id, .generation = identity.generation };
    try std.testing.expectEqual(.not_null, (try view.validateCoercion(&execution, .null, null)).?.kind);
    try std.testing.expectEqual(@as(?Violation, null), try view.validateCoercion(&execution, .null, witness));
    var stale = witness;
    stale.generation -= 1;
    try std.testing.expectError(error.CatalogGenerationChanged, view.validateCoercion(&execution, .null, stale));
    stale = witness;
    stale.origin_id = @splat(9);
    try std.testing.expectError(error.CatalogGenerationChanged, view.validateCoercion(&execution, .null, stale));
    try std.testing.expectError(error.InvalidRelationalExpressionInput, view.validateCoercion(&execution, .{ .integer = 1 }, witness));
}

test "SQL domain activation delta scans newly validated rules without rechecking unchanged NOT NULL or NOT VALID rules" {
    const a = std.testing.allocator;
    const condition =
        \\{"op":"eq","args":[{"op":"column","column":"value"},{"op":"literal","type":"string","value":"good"}]}
    ;
    var previous: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(8)), .owner = .auth_disabled, .definition = .{ .domain = .{
        .base = .{ .builtin = .text },
        .not_null = true,
        .checks = &.{.{ .name = "state", .expression = condition, .validated = false }},
    } } };
    var next = previous;
    next.identity.generation += 1;
    next.definition.domain.default_expression =
        \\{"op":"literal","type":"string","value":"good"}
    ;
    var old = try Registry.init(a, &.{previous});
    defer old.deinit();
    var changed_default = try Registry.init(a, &.{next});
    defer changed_default.deinit();
    var default_plan = try Activation.init(a, &old.byId(7).?.executable.?.domain, &changed_default.byId(7).?.executable.?.domain);
    defer default_plan.deinit();
    try std.testing.expect(!default_plan.has_work);
    var bytes: usize = vm.max_allocated_bytes;
    var execution = vm.Execution.init(a, &bytes);
    // This NULL may have arrived through an already-domain-typed subquery.
    try std.testing.expectEqual(@as(?Violation, null), try default_plan.validate(&execution, .null));
    try std.testing.expectEqual(@as(?Violation, null), try default_plan.validate(&execution, .{ .string = "old-invalid" }));
    next.definition.domain.checks = &.{.{ .name = "state", .expression = condition, .validated = true }};
    var validated = try Registry.init(a, &.{next});
    defer validated.deinit();
    var check_plan = try Activation.init(a, &old.byId(7).?.executable.?.domain, &validated.byId(7).?.executable.?.domain);
    defer check_plan.deinit();
    try std.testing.expect(check_plan.has_work and !check_plan.require_not_null);
    try std.testing.expectEqualStrings("state", (try check_plan.validate(&execution, .{ .string = "old-invalid" })).?.constraint.?);
    try std.testing.expectEqual(@as(?Violation, null), try check_plan.validate(&execution, .null));
    previous.definition.domain.not_null = false;
    var nullable = try Registry.init(a, &.{previous});
    defer nullable.deinit();
    var null_plan = try Activation.init(a, &nullable.byId(7).?.executable.?.domain, &validated.byId(7).?.executable.?.domain);
    defer null_plan.deinit();
    try std.testing.expectEqual(.not_null, (try null_plan.validate(&execution, .null)).?.kind);
    next.identity.generation = previous.identity.generation;
    var forged = try Registry.init(a, &.{next});
    defer forged.deinit();
    try std.testing.expectError(error.CatalogGenerationChanged, Activation.init(a, &old.byId(7).?.executable.?.domain, &forged.byId(7).?.executable.?.domain));
}

test "SQL domain activation delta ownership unwinds every allocation failure across nested bases" {
    const a = std.testing.allocator;
    const parent_identity = try types.Identity.init(9, 2, 1, @splat(4));
    const parent: types.Record = .{ .identity = parent_identity, .owner = .auth_disabled, .definition = .{ .domain = .{ .base = .{ .builtin = .text } } } };
    const child: types.Record = .{ .identity = try types.Identity.init(7, 2, 1, @splat(5)), .owner = .auth_disabled, .definition = .{ .domain = .{ .base = .{ .named = .{ .id = 9, .origin_id = parent_identity.origin_id, .generation = 1 } } } } };
    var before = try Registry.init(a, &.{ child, parent });
    defer before.deinit();
    var next_parent = parent;
    next_parent.identity.generation += 1;
    next_parent.definition.domain.not_null = true;
    var next_child = child;
    next_child.identity.generation += 1;
    next_child.definition.domain.base.named.generation += 1;
    var after = try Registry.init(a, &.{ next_child, next_parent });
    defer after.deinit();
    const Harness = struct {
        fn run(alloc: A, old: *const View, next: *const View) !void {
            var plan = try Activation.init(alloc, old, next);
            defer plan.deinit();
            try std.testing.expect(plan.has_work and !plan.require_not_null);
            var bytes: usize = vm.max_allocated_bytes;
            var execution = vm.Execution.init(alloc, &bytes);
            const violation = (try plan.validate(&execution, .null)).?;
            try std.testing.expectEqual(@as(u64, 9), violation.type_id);
            try std.testing.expectEqual(.not_null, violation.kind);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Harness.run, .{ &before.byId(7).?.executable.?.domain, &after.byId(7).?.executable.?.domain });
}
