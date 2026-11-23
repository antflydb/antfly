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

//! Statement-owned domain provenance. References originate in pinned compiler
//! descriptors; neither JSON values nor public parameter metadata can mint them.
const std = @import("std");
const catalog = @import("catalog.zig");
const rows = @import("../schema/domain_row.zig");
const values = @import("../schema/domain_value.zig");
pub const Reference = @import("../system_catalog/domain_types.zig").Reference;

pub fn reference(view: ?*const values.View) ?Reference {
    const identity = (view orelse return null).identity orelse return null;
    return .{ .id = identity.id, .origin_id = identity.origin_id, .generation = identity.generation };
}

/// Build once per immutable statement/arm, then borrow for every affected row.
/// INSERT VALUES may build one per distinct expression row. Only domain fields
/// occupy this directory; physical ordinals remain valid across projections.
pub const Builder = struct {
    schema_version: u32,
    inputs: []rows.Input,

    pub fn init(a: std.mem.Allocator, table: catalog.Table, preserve_existing: bool) !Builder {
        var count: usize = 0;
        for (table.columns) |column| if (column.domain_view != null) {
            count += 1;
        };
        const inputs = try a.alloc(rows.Input, count);
        errdefer a.free(inputs);
        var index: usize = 0;
        for (table.columns) |column| if (column.domain_view != null) {
            inputs[index] = .{ .ordinal = column.storage_ordinal orelse return error.InvalidSqlBackendResponse, .source = if (preserve_existing and !column.generated) reference(column.domain_view) else null };
            index += 1;
        };
        std.mem.sort(rows.Input, inputs, {}, struct {
            fn less(_: void, left: rows.Input, right: rows.Input) bool {
                return left.ordinal < right.ordinal;
            }
        }.less);
        for (inputs, 0..) |input, i| if (i != 0 and inputs[i - 1].ordinal == input.ordinal) return error.InvalidSqlBackendResponse;
        return .{ .schema_version = table.schema_version, .inputs = inputs };
    }

    pub fn clone(self: Builder, a: std.mem.Allocator) !Builder {
        return .{ .schema_version = self.schema_version, .inputs = if (self.inputs.len == 0) &.{} else try a.dupe(rows.Input, self.inputs) };
    }

    pub fn set(self: Builder, target: catalog.Column, source: ?*const values.View, use_default: bool) !void {
        if (target.domain_view == null) return;
        const ordinal = target.storage_ordinal orelse return error.InvalidSqlBackendResponse;
        var lower: usize = 0;
        var upper = self.inputs.len;
        while (lower < upper) {
            const middle = lower + (upper - lower) / 2;
            if (self.inputs[middle].ordinal < ordinal) lower = middle + 1 else upper = middle;
        }
        if (lower == self.inputs.len or self.inputs[lower].ordinal != ordinal) return error.InvalidSqlBackendResponse;
        // A different logical domain is a fresh coercion. Retain a same-ID
        // reference even when stale, so the native epoch fence rejects it.
        const witness = if (use_default or target.generated) null else reference(source);
        const compatible = if (witness) |value| if (value.id == target.domain_view.?.identity.?.id) witness else null else null;
        self.inputs[lower] = .{ .ordinal = ordinal, .source = compatible, .use_default = use_default };
    }

    pub fn admission(self: Builder) ?rows.Admission {
        return if (self.inputs.len == 0) null else .{ .schema_version = self.schema_version, .inputs = self.inputs };
    }
};

test "SQL domain mutation provenance is sparse sorted and rejects projection ordinals" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const types = @import("../system_catalog/named_types.zig");
    var registry = try values.Registry.init(a, &.{types.Record{
        .identity = try types.Identity.init(7, 2, 1, @splat(8)),
        .owner = .auth_disabled,
        .definition = .{ .domain = .{ .base = .{ .builtin = .text }, .not_null = true } },
    }});
    defer registry.deinit();
    const view = &registry.byId(7).?.executable.?.domain;
    const first: catalog.Column = .{ .name = "a", .path = "a", .type = .string, .domain_view = view, .storage_ordinal = 9 };
    const second: catalog.Column = .{ .name = "b", .path = "b", .type = .string, .domain_view = view, .storage_ordinal = 2 };
    const table: catalog.Table = .{ .id = 1, .physical_name = "docs", .schema_version = 3, .columns = &.{ first, second } };
    const builder = try Builder.init(arena.allocator(), table, true);
    try std.testing.expectEqual(@as(u32, 2), builder.inputs[0].ordinal);
    try std.testing.expectEqual(@as(u64, 7), builder.inputs[0].source.?.id);
    try builder.set(first, null, false);
    try std.testing.expect(builder.inputs[1].source == null);
    try std.testing.expect(builder.inputs[0].source != null);
    try builder.set(first, view, false);
    try std.testing.expect(builder.inputs[1].source != null);
    try builder.set(first, view, true);
    try std.testing.expect(builder.inputs[1].source == null and builder.inputs[1].use_default);
    var forged = first;
    forged.storage_ordinal = null;
    try std.testing.expectError(error.InvalidSqlBackendResponse, builder.set(forged, view, false));
}
