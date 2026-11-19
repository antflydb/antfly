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

//! Trigger definition codecs and bounded execution through the shared SQL owner.
const std = @import("std");
const local = @import("antfly_local_sources");
const routines = local.sql_catalog.routines;
const procedures = routines.procedures;
const A = std.mem.Allocator;

test "SQL trigger owned definition codec preserves ordinary envelopes and rejects conflicting return contracts" {
    const record: routines.records.Record = .{ .id = 1, .generation = 1, .definition = .{ .arguments = &.{}, .result = .{ .scalar = .int64 }, .body = "SELECT 1", .owner = .auth_disabled } };
    const a = std.testing.allocator;
    const ordinary = try record.encode(a);
    defer a.free(ordinary);
    try std.testing.expect(std.mem.indexOf(u8, ordinary, "returns_trigger") == null);
    var trigger = record;
    trigger.definition.returns_trigger = true;
    trigger.definition.result = null;
    trigger.definition.language = .plpgsql;
    trigger.definition.body = "BEGIN RETURN NEW; END";
    const bytes = try trigger.encode(a);
    defer a.free(bytes);
    var owned = try routines.records.Record.decode(a, bytes);
    defer owned.deinit();
    @memset(bytes, 0);
    try std.testing.expect(owned.value.definition.returns_trigger);
    try std.testing.expectEqualStrings("BEGIN RETURN NEW; END", owned.value.definition.body);
    trigger.definition.result = record.definition.result;
    try std.testing.expectError(error.InvalidSqlRoutineRecord, trigger.validate());
    trigger.definition.result = null;
    trigger.kind = .procedure;
    try std.testing.expectError(error.InvalidSqlRoutineRecord, trigger.validate());
}

test "SQL trigger preparation lazy binding notice delivery and decode clean every allocation failure" {
    const Probe = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn run(a: A) !void {
            var region: std.heap.ArenaAllocator = .init(a);
            defer region.deinit();
            const arena = region.allocator();
            const record: routines.records.Record = .{ .id = 1, .generation = 1, .definition = .{ .arguments = &.{}, .returns_trigger = true, .language = .plpgsql, .body = "BEGIN RAISE NOTICE 'audit'; PERFORM 1+2; RETURN NEW; END", .owner = .auth_disabled } };
            const bytes = try record.encode(a);
            defer a.free(bytes);
            var decoded = try routines.records.Record.decode(a, bytes);
            defer decoded.deinit();
            const prepared = try procedures.prepare(arena, decoded.value, null, null, null);
            var marker: u8 = 0;
            const backend: local.sql_catalog.Backend = .{ .ptr = &marker, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = checkpoint } };
            const context: local.sql_runtime.Context = .{ .alloc = a, .arena = arena, .backend = backend, .binding = undefined, .parameters = &.{}, .limits = .{} };
            const row: local.sql_catalog.Row = .{ .id = "row", .version = 1, .value = .null };
            const output = try procedures.executeTrigger(context, prepared, .{ .event = .insert, .new = &row });
            try std.testing.expectEqual(&row, output.row.?);
            try std.testing.expectEqual(@as(usize, 1), output.notices.len);
            try std.testing.expectEqualStrings("audit", output.notices[0].message);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "SQL trigger invalid declarations retain specific PostgreSQL diagnostics" {
    const cases = .{
        .{ "CREATE FUNCTION f(integer) RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$", error.SqlTriggerDeclaredArguments, "Trigger functions cannot have declared arguments." },
        .{ "CREATE FUNCTION f() RETURNS trigger LANGUAGE sql AS $$SELECT 1$$", error.SqlTriggerSqlLanguage, "SQL functions cannot return type trigger." },
    };
    inline for (cases) |entry| {
        try std.testing.expectError(entry[1], local.sql_compiler.compile(std.testing.allocator, entry[0], .{}));
        const diagnostic = local.sql_errors.describe(entry[1]);
        try std.testing.expectEqualStrings("42P13", diagnostic.code);
        try std.testing.expectEqualStrings(entry[2], diagnostic.message);
    }
    try std.testing.expectError(error.SqlTriggerReturnValueRequired, procedures.compileBody(std.testing.allocator, .{ .id = 1, .generation = 1, .definition = .{ .arguments = &.{}, .returns_trigger = true, .language = .plpgsql, .body = "BEGIN RETURN; END", .owner = .auth_disabled } }));
}
