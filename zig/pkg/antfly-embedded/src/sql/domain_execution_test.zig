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

const std = @import("std");
const scalar = @import("scalar.zig");
const compiler = @import("compiler.zig");
const values = @import("../schema/domain_value.zig");
const types = @import("../system_catalog/named_types.zig");
const A = std.mem.Allocator;
const Resolver = struct {
    view: *const values.View,
    calls: usize = 0,
    fn resolve(raw: *anyopaque, _: A, name: @import("ast.zig").Name) !scalar.Type {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (!std.mem.eql(u8, name.table, "status")) return error.SqlNamedTypeNotFound;
        self.calls += 1;
        return scalar.domainType(self.view);
    }
    fn limits(self: *@This()) scalar.BindLimits {
        return .{ .named_types = .{ .ptr = self, .resolve = resolve } };
    }
};
fn record() !types.Record {
    return .{ .identity = try types.Identity.init(7, 2, 1, @splat(8)), .owner = .auth_disabled, .definition = .{ .domain = .{
        .base = .{ .builtin = .text },
        .not_null = true,
        .checks = &.{.{ .name = "unblocked", .expression =
        \\{"op":"ne","args":[{"op":"column","column":"value"},{"op":"literal","type":"string","value":"blocked"}]}
        }},
    } } };
}

test "SQL domain casts own prepared contracts enforce NULL and retain lazy branch semantics" {
    const a = std.testing.allocator;
    const cases = [_]struct { sql: []const u8, failure: ?anyerror = null, domain: bool = true }{
        .{ .sql = "'queued'::status" },
        .{ .sql = "NULL::status", .failure = error.SqlNotNullViolation },
        .{ .sql = "'blocked'::status", .failure = error.RelationalCheckViolation },
        .{ .sql = "CASE WHEN false THEN 'blocked'::status ELSE 'queued'::status END" },
        .{ .sql = "coalesce(NULL::status,'queued'::status)", .failure = error.SqlNotNullViolation },
        .{ .sql = "('queued'::status)::text", .domain = false },
        .{ .sql = "coalesce('queued'::status,NULL)", .domain = false },
    };
    for (cases) |case| {
        errdefer std.debug.print("domain SQL case: {s}\n", .{case.sql});
        var program = build: {
            var registry = try values.Registry.init(a, &.{try record()});
            defer registry.deinit();
            var source = try compiler.compileScalar(a, case.sql, .{});
            defer source.deinit();
            var resolver: Resolver = .{ .view = &registry.byId(7).?.executable.?.domain };
            break :build try scalar.bind(a, source.expression, &.{}, &.{}, resolver.limits());
        };
        defer program.deinit();
        try std.testing.expect(program.domain_registry != null);
        try std.testing.expectEqual(case.domain, program.output_type.domainView() != null);
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        if (case.failure) |failure| {
            try std.testing.expectError(failure, program.evaluate(region.allocator(), &.{}, &.{}, .{}));
        } else {
            const result = try program.evaluate(region.allocator(), &.{}, &.{}, .{});
            try std.testing.expectEqualStrings("queued", result.value.string);
        }
    }
}

test "SQL domain casts accept trusted typed NULL while raw parameters must cross fresh admission" {
    const a = std.testing.allocator;
    var registry = try values.Registry.init(a, &.{try record()});
    defer registry.deinit();
    const view = &registry.byId(7).?.executable.?.domain;
    var resolver: Resolver = .{ .view = view };
    var expression = try compiler.compileScalar(a, "n::status", .{});
    defer expression.deinit();
    var program = try scalar.bind(a, expression.expression, &.{.{ .name = "n", .type = .string, .element_type = .text, .domain_view = view }}, &.{}, resolver.limits());
    defer program.deinit();
    const nulls = [_]scalar.Datum{.{}};
    const result = try program.evaluate(a, &nulls, &.{}, .{});
    try std.testing.expect(result.sql_null);
    var parameter = try compiler.compileScalar(a, "$1::status", .{});
    defer parameter.deinit();
    var typed = try scalar.bindTyped(a, parameter.expression, &.{}, &.{try scalar.domainType(view)}, resolver.limits());
    defer typed.deinit();
    try std.testing.expectError(error.SqlNotNullViolation, typed.evaluate(a, &.{}, &.{.null}, .{}));
}

test "SQL domain casts distinguish another domain from stale instances and share execution budgets" {
    const a = std.testing.allocator;
    const target = try record();
    var other = target;
    other.identity = try types.Identity.init(9, 2, 1, @splat(9));
    var registry = try values.Registry.init(a, &.{ target, other });
    defer registry.deinit();
    const view = &registry.byId(7).?.executable.?.domain;
    const other_view = &registry.byId(9).?.executable.?.domain;
    var resolver: Resolver = .{ .view = view };
    var expression = try compiler.compileScalar(a, "n::status", .{});
    defer expression.deinit();
    var program = try scalar.bind(a, expression.expression, &.{.{ .name = "n", .type = .string, .element_type = .text, .domain_view = other_view }}, &.{}, resolver.limits());
    defer program.deinit();
    try std.testing.expectError(error.SqlNotNullViolation, program.evaluate(a, &.{.{}}, &.{}, .{}));
    // The physical text identity cast and domain checks need no row allocation.
    var none = std.heap.FixedBufferAllocator.init(&.{});
    const result = try program.evaluate(none.allocator(), &.{scalar.Datum.json(.{ .string = "queued" })}, &.{}, .{});
    try std.testing.expectEqualStrings("queued", result.value.string);
    try std.testing.expectEqual(@as(usize, 0), none.end_index);
    try std.testing.expectError(error.SqlProgramLimitExceeded, program.evaluate(a, &.{scalar.Datum.json(.{ .string = "queued" })}, &.{}, .{ .steps = 3 }));
    var old = target;
    old.identity.generation += 1;
    var rebound = try values.Registry.init(a, &.{old});
    defer rebound.deinit();
    try std.testing.expectError(error.CatalogGenerationChanged, scalar.bind(a, expression.expression, &.{.{ .name = "n", .type = .string, .element_type = .text, .domain_view = &rebound.byId(7).?.executable.?.domain }}, &.{}, resolver.limits()));
    // One program cannot capture two generations under one immutable type ID.
    // capture() rejects this at binding, rather than silently selecting one.
}

test "SQL domain parameter frames own contracts and reject forged typed input and every allocation failure" {
    const Harness = struct {
        fn run(a: A) !void {
            var frame = build: {
                var registry = try values.Registry.init(a, &.{try record()});
                defer registry.deinit();
                const view = &registry.byId(7).?.executable.?.domain;
                break :build try @import("parameter_frame.zig").Frame.prepare(a, &.{try scalar.domainType(view)}, &.{.{ .text = "queued" }}, .{});
            };
            defer frame.deinit();
            try std.testing.expect(frame.descriptors[0].domainView() != null);
            var source = try compiler.compileScalar(a, "$1::status", .{});
            defer source.deinit();
            var resolver: Resolver = .{ .view = frame.descriptors[0].domainView().? };
            var program = try scalar.bindTyped(a, source.expression, &.{}, frame.descriptors, resolver.limits());
            defer program.deinit();
            var region = std.heap.ArenaAllocator.init(a);
            defer region.deinit();
            const result = try program.evaluate(region.allocator(), &.{}, &.{.{ .string = "queued" }}, .{});
            try std.testing.expectEqualStrings("queued", result.value.string);
            if (@import("parameter_frame.zig").Frame.prepare(a, frame.descriptors, &.{.sql_null}, .{})) |unexpected| {
                var admitted = unexpected;
                admitted.deinit();
                return error.TestUnexpectedResult;
            } else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.SqlNotNullViolation, err);
            }
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL domain numeric casts retain exact comparisons and cannot bypass checks through decimal templates" {
    const a = std.testing.allocator;
    var declaration = try compiler.compile(a, "CREATE DOMAIN positive_amount AS numeric CHECK (VALUE > 0)", .{});
    defer declaration.deinit();
    var lowered = try @import("domain_definition.zig").Prepared.init(a, "positive_amount", declaration.statement.catalog_ddl.domain_definition.?.create);
    defer lowered.deinit();
    var number_type = try record();
    number_type.definition = .{ .domain = lowered.definition };
    var registry = try values.Registry.init(a, &.{number_type});
    defer registry.deinit();
    var resolver: Resolver = .{ .view = &registry.byId(7).?.executable.?.domain };
    for ([_]struct { sql: []const u8, fail: bool }{
        .{ .sql = "'1e-400'::status", .fail = false },
        .{ .sql = "'-1e-400'::status", .fail = true },
        .{ .sql = "('-1e-400'::status)+0.0", .fail = true },
        .{ .sql = "CASE WHEN false THEN '-1e-400'::status ELSE '1e-400'::status END", .fail = false },
    }) |case| {
        errdefer std.debug.print("numeric domain SQL case: {s}\n", .{case.sql});
        var source = try compiler.compileScalar(a, case.sql, .{});
        defer source.deinit();
        var program = try scalar.bind(a, source.expression, &.{}, &.{}, resolver.limits());
        defer program.deinit();
        try std.testing.expect(program.decimal_plan == null);
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        if (case.fail) {
            try std.testing.expectError(error.RelationalCheckViolation, program.evaluate(region.allocator(), &.{}, &.{}, .{}));
        } else {
            const result = try program.evaluate(region.allocator(), &.{}, &.{}, .{});
            try std.testing.expect(result.numeric != null and !result.sql_null);
            try std.testing.expect(!result.numeric.?.negative and !result.numeric.?.isZero());
        }
    }
}

test "SQL domain enum backed casts own complete ancestor directories after their producer is released" {
    const a = std.testing.allocator;
    var program = build: {
        var labels = try types.Owned.create(a, &.{ "queued", "done" });
        defer labels.deinit();
        const root: types.Record = .{ .identity = try types.Identity.init(9, 2, 1, @splat(9)), .owner = .auth_disabled, .definition = labels.definition };
        var child = try record();
        child.definition.domain.base = .{ .named = .{ .id = 9, .origin_id = root.identity.origin_id, .generation = 1 } };
        child.definition.domain.checks = &.{};
        var registry = try values.Registry.init(a, &.{ child, root });
        defer registry.deinit();
        var resolver: Resolver = .{ .view = &registry.byId(7).?.executable.?.domain };
        var source = try compiler.compileScalar(a, "'queued'::status", .{});
        defer source.deinit();
        break :build try scalar.bind(a, source.expression, &.{}, &.{}, resolver.limits());
    };
    defer program.deinit();
    try std.testing.expectEqual(@as(usize, 2), program.domain_registry.?.records.len);
    try std.testing.expect(program.output_type.enum_view != null);
    const result = try program.evaluate(a, &.{}, &.{}, .{});
    try std.testing.expectEqualStrings("queued", result.enumeration().?.text);
}

test "SQL domain executable descriptors serialize references without minting wire authority" {
    const a = std.testing.allocator;
    var registry = try values.Registry.init(a, &.{try record()});
    defer registry.deinit();
    const view = &registry.byId(7).?.executable.?.domain;
    const descriptor = try scalar.domainType(view);
    const bytes = try std.json.Stringify.valueAlloc(a, descriptor, .{});
    defer a.free(bytes);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "domain_type") != null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "checks") == null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "allocator") == null);
    var decoded = try std.json.parseFromSlice(scalar.Type, a, bytes, .{});
    defer decoded.deinit();
    try std.testing.expect(decoded.value.domainView() == null and decoded.value.domainReference() != null);
    try scalar.validateParameterDescriptorWire(decoded.value);
    try std.testing.expectError(error.InvalidSqlParameters, scalar.validateParameterType(decoded.value));
    try std.testing.expectError(error.InvalidSqlParameters, @import("parameter_frame.zig").Frame.prepare(a, &.{decoded.value}, &.{.{ .text = "queued" }}, .{}));
    const handle = try std.json.Stringify.valueAlloc(a, view.*, .{});
    defer a.free(handle);
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(values.View, a, handle, .{}));
}

test "SQL domain program capture rejects contradictory contracts under one immutable identity" {
    const a = std.testing.allocator;
    var root = try values.Registry.init(a, &.{try record()});
    defer root.deinit();
    var changed = try record();
    changed.definition.domain.checks = &.{};
    var other = try values.Registry.init(a, &.{changed});
    defer other.deinit();
    var resolver: Resolver = .{ .view = &root.byId(7).?.executable.?.domain };
    var expression = try compiler.compileScalar(a, "n::status", .{});
    defer expression.deinit();
    try std.testing.expectError(error.InvalidSqlNamedType, scalar.bind(a, expression.expression, &.{.{ .name = "n", .type = .string, .element_type = .text, .domain_view = &other.byId(7).?.executable.?.domain }}, &.{}, resolver.limits()));
}

test "SQL domain explicit cast infers an unknown parameter domain while declared base parameters retain their identity" {
    const a = std.testing.allocator;
    var registry = try values.Registry.init(a, &.{try record()});
    defer registry.deinit();
    var resolver: Resolver = .{ .view = &registry.byId(7).?.executable.?.domain };
    var source = try compiler.compileScalar(a, "$1::status", .{});
    defer source.deinit();
    var inferred = try scalar.bind(a, source.expression, &.{}, &.{}, resolver.limits());
    defer inferred.deinit();
    try std.testing.expect(inferred.parameter_descriptors[0].domainView() != null);
    try std.testing.expectError(error.SqlNotNullViolation, inferred.evaluate(a, &.{}, &.{.null}, .{}));
    var explicit = try scalar.bindTyped(a, source.expression, &.{}, &.{.{ .kind = .string, .element_type = .text }}, resolver.limits());
    defer explicit.deinit();
    try std.testing.expect(explicit.parameter_descriptors[0].domainView() == null);
    try std.testing.expectError(error.SqlNotNullViolation, explicit.evaluate(a, &.{}, &.{.null}, .{}));
}
