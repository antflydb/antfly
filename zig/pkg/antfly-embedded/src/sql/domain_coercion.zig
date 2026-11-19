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

//! Typed scalar admission through the same immutable domain programs used by
//! native rows. This boundary never reparses SQL/JSON or resolves catalog names.
const std = @import("std");
const values = @import("../schema/domain_value.zig");
const vm = @import("../schema/relational_expression.zig");
const types = @import("../system_catalog/named_types.zig");
const scalar = @import("scalar.zig");
const A = std.mem.Allocator;

pub fn capture(a: A, sources: []const *const values.View) anyerror!values.Registry {
    var records: std.ArrayList(types.Record) = .empty;
    defer records.deinit(a);
    var identities: std.AutoHashMapUnmanaged(u64, types.Record) = .empty;
    defer identities.deinit(a);
    for (sources) |source| {
        var current: ?*const values.View = source;
        while (current) |view| {
            const identity = view.identity orelse return error.InvalidSqlNamedType;
            const captured: types.Record = .{ .identity = identity, .owner = .auth_disabled, .definition = .{ .domain = view.definition } };
            const slot = try identities.getOrPut(a, identity.id);
            if (slot.found_existing) {
                try requireSame(a, slot.value_ptr.*, captured);
                break;
            }
            slot.value_ptr.* = captured;
            if (records.items.len >= values.Registry.max_types) return error.SqlProgramLimitExceeded;
            try records.append(a, .{ .identity = identity, .owner = .auth_disabled, .definition = .{ .domain = view.definition } });
            if (view.enumeration) |enumeration| {
                const root = try identities.getOrPut(a, enumeration.record.identity.id);
                if (root.found_existing) {
                    try requireSame(a, root.value_ptr.*, enumeration.record);
                } else {
                    root.value_ptr.* = enumeration.record;
                    try records.append(a, enumeration.record);
                }
            }
            current = view.parent;
        }
    }
    std.mem.sort(types.Record, records.items, {}, struct {
        fn less(_: void, left: types.Record, right: types.Record) bool {
            return left.identity.id < right.identity.id;
        }
    }.less);
    return values.Registry.init(a, records.items);
}

fn requireSame(a: A, left: types.Record, right: types.Record) !void {
    if (!std.meta.eql(left.identity, right.identity)) return error.CatalogGenerationChanged;
    // The same immutable identity must never pick the first of contradictory
    // contracts. Most repeated references share slices and need no allocation.
    if (std.meta.eql(left.definition, right.definition)) return;
    const before = try std.json.Stringify.valueAlloc(a, left.definition, .{});
    defer a.free(before);
    const after = try std.json.Stringify.valueAlloc(a, right.definition, .{});
    defer a.free(after);
    if (!std.mem.eql(u8, before, after)) return error.InvalidSqlNamedType;
}

pub fn validate(execution: *vm.Execution, view: *const values.View, datum: scalar.Datum, source: ?scalar.EnumReference) anyerror!void {
    return validateInner(execution, view, datum, source) catch |err| switch (err) {
        error.RelationalExpressionBudgetExceeded => error.SqlProgramLimitExceeded,
        error.InvalidRelationalExpressionInput => error.SqlTypeMismatch,
        else => err,
    };
}

fn validateInner(execution: *vm.Execution, view: *const values.View, datum: scalar.Datum, source: ?scalar.EnumReference) anyerror!void {
    try datum.validateTemporal();
    var roots: @import("row_value.zig").JsonRow = undefined;
    roots.init(execution.alloc, execution.bytes.*, &execution.numeric);
    defer roots.deinit();
    try roots.bindAdmission(execution.bytes);
    var numeric_bytes: ?[]u8 = null;
    defer if (numeric_bytes) |bytes| execution.alloc.free(bytes);
    const value: vm.Value = if (datum.sql_null) .null else switch (view.column.column_type) {
        .string => if (datum.value == .string and datum.extension == null and datum.numeric == null and datum.array == null) .{ .string = datum.value.string } else return error.SqlTypeMismatch,
        .integer => if (datum.value == .integer and datum.extension == null and datum.numeric == null and datum.array == null) .{ .integer = datum.value.integer } else return error.SqlTypeMismatch,
        .number => if (datum.value == .float and datum.extension == null and datum.numeric == null and datum.array == null) .{ .number = datum.value.float } else return error.SqlTypeMismatch,
        .boolean => if (datum.value == .bool and datum.extension == null and datum.numeric == null and datum.array == null) .{ .boolean = datum.value.bool } else return error.SqlTypeMismatch,
        .enumeration => .{ .enumeration = datum.enumeration() orelse return error.SqlTypeMismatch },
        .json => if (datum.extension == null and datum.numeric == null and datum.array == null) .{ .json = try roots.pinValue(datum.value) } else return error.SqlTypeMismatch,
        .numeric => blk: {
            const number = datum.numeric orelse return error.SqlTypeMismatch;
            const codec = @import("numeric_binary.zig");
            const count = try codec.encodedSize(&execution.numeric, number.*);
            if (count > execution.bytes.*) return execution.limit();
            execution.bytes.* -= count;
            numeric_bytes = try codec.encodeAlloc(&execution.numeric, number.*);
            break :blk .{ .numeric = numeric_bytes.? };
        },
        else => return error.UnsupportedSqlExecution,
    };
    if (try view.validateCoercion(execution, value, if (source) |identity| .{ .id = identity.id, .origin_id = identity.origin_id, .generation = identity.generation } else null)) |violation|
        return switch (violation.kind) {
            .not_null => error.SqlNotNullViolation,
            .check => error.RelationalCheckViolation,
        };
}
