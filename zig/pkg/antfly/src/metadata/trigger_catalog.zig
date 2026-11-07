// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Transactional reverse edges for table triggers. Executable bodies stay in
//! the shared routine catalog. These compact edges make function retirement an
//! indexed check and are maintained by every authoritative table publication.
const std = @import("std");
const local = @import("antfly_local_sources");
const triggers = local.sql_catalog.triggers;
const routines = local.sql_catalog.routines.records;
const storage = @import("../system_catalog/storage.zig");
const Table = local.common_topology_records.TableRecord;
const A = std.mem.Allocator;

pub fn schemaState(a: A, table: Table) !?std.json.Parsed(triggers.State) {
    if (table.schema_json.len == 0) return null;
    var root = try std.json.parseFromSlice(std.json.Value, a, table.schema_json, .{});
    defer root.deinit();
    if (root.value != .object) return error.InvalidSqlTriggerDefinition;
    const value = root.value.object.get("sql_triggers") orelse return null;
    var state = try local.schema_mod.table_triggers.parseValue(a, value);
    errdefer state.deinit();
    if (state.value.table_id != table.table_id) return error.InvalidSqlTriggerDefinition;
    return state;
}
/// Native callers can avoid reading a catalog incarnation during legacy/raw
/// table bootstrap when neither schema has attachment metadata.
pub fn present(a: A, before: ?Table, after: ?Table) !bool {
    if (before) |table| {
        var parsed = try schemaState(a, table);
        defer if (parsed) |*state| state.deinit();
        if (parsed != null) return true;
    }
    if (after) |table| {
        var parsed = try schemaState(a, table);
        defer if (parsed) |*state| state.deinit();
        if (parsed != null) return true;
    }
    return false;
}

fn routinePrefix(buffer: []u8, group: u64, routine: u64) ![]const u8 {
    if (group == 0 or routine == 0) return error.InvalidSqlTriggerDefinition;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:table_triggers:routine:{x:0>16}:", .{ group, routine });
}
fn edgeKey(buffer: []u8, group: u64, table: u64, entry: triggers.Entry) ![]const u8 {
    var prefix: [160]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}:{x:0>16}", .{ try routinePrefix(&prefix, group, entry.function.id), table, entry.id });
}
fn edgeValue(buffer: []u8, generation: u64, entry: triggers.Entry) ![]const u8 {
    return std.fmt.bufPrint(buffer, "{x:0>16}:{x:0>8}", .{ generation, entry.generation });
}
fn requireRoutine(a: A, txn: anytype, group: u64, id: u64) !void {
    var resource = (try storage.getById(a, txn, group, .routine, id)) orelse return error.CatalogGenerationChanged;
    defer resource.deinit();
    var record = try routines.Record.decode(a, resource.value.source_json);
    defer record.deinit();
    if (record.value.id != id or record.value.kind != .function or !record.value.definition.returns_trigger or record.value.definition.arguments.len != 0) return error.InvalidSqlTriggerDefinition;
}

/// before/after are the exact decoded table rows in the existing metadata
/// transaction. Caller owns table binding authorization and generation CAS.
/// Dropping a table removes its edges in the same transaction as the table.
pub fn stageTable(a: A, txn: anytype, group: u64, before: ?Table, after: ?Table, incarnation: [16]u8) !void {
    return tableTransition(a, txn, group, before, after, incarnation, true);
}

pub fn validateTable(a: A, txn: anytype, group: u64, before: ?Table, after: ?Table, incarnation: [16]u8) !void {
    return tableTransition(a, txn, group, before, after, incarnation, false);
}

const TransitionPhase = enum { source, destination, identity, generation, routines, edges, write_edges };
var diagnosed_transition = std.atomic.Value(bool).init(false);
fn tableTransition(a: A, txn: anytype, group: u64, before: ?Table, after: ?Table, incarnation: [16]u8, apply: bool) !void {
    var phase: TransitionPhase = .source;
    return tableTransitionPhased(a, txn, group, before, after, incarnation, apply, &phase) catch |failure| {
        if (@import("builtin").mode == .debug and failure == error.InvalidSqlTriggerDefinition and !diagnosed_transition.swap(true, .monotonic)) std.debug.print("trigger catalog rejected phase={s} class={s} table={d} apply={}\n", .{ @tagName(phase), @errorName(failure), if (after) |value| value.table_id else if (before) |value| value.table_id else 0, apply });
        return failure;
    };
}
fn tableTransitionPhased(a: A, txn: anytype, group: u64, before: ?Table, after: ?Table, incarnation: [16]u8, apply: bool, phase: *TransitionPhase) !void {
    var previous = if (before) |table| try schemaState(a, table) else null;
    defer if (previous) |*state| state.deinit();
    phase.* = .destination;
    var next = if (after) |table| try schemaState(a, table) else null;
    defer if (next) |*state| state.deinit();
    if (previous == null and next == null) return;
    phase.* = .identity;
    if (std.mem.allEqual(u8, &incarnation, 0)) return error.InvalidSqlTriggerDefinition;
    if (previous) |state| if (!std.mem.eql(u8, &state.value.metadata_incarnation, &incarnation)) return error.CatalogGenerationChanged;
    if (next) |state| if (!std.mem.eql(u8, &state.value.metadata_incarnation, &incarnation)) return error.CatalogGenerationChanged;
    const table = if (after) |value| value.table_id else before.?.table_id;
    if (table == 0 or (before != null and before.?.table_id != table)) return error.InvalidSqlTriggerDefinition;
    if (next) |state| {
        phase.* = .generation;
        if (previous) |old| {
            if (old.value.effectiveLogicalRelationId() != state.value.effectiveLogicalRelationId()) return error.InvalidSqlTriggerDefinition;
            if (state.value.generation < old.value.generation or state.value.generation - old.value.generation > 1 or state.value.next_id < old.value.next_id) return error.InvalidSqlTriggerDefinition;
            if (state.value.generation == old.value.generation) {
                const prior_bytes = try old.value.encode(a);
                defer a.free(prior_bytes);
                const next_bytes = try state.value.encode(a);
                defer a.free(next_bytes);
                if (!std.mem.eql(u8, prior_bytes, next_bytes)) return error.InvalidSqlTriggerDefinition;
            }
        }
        phase.* = .routines;
        for (state.value.entries) |entry| try requireRoutine(a, txn, group, entry.function.id);
    } else if (after != null) {
        // Omitting a private field from a public schema update must preserve
        // it, never silently remove attachments or reset identity high water.
        return error.InvalidSqlTriggerDefinition;
    }
    phase.* = .edges;
    var key: [256]u8 = undefined;
    var value: [32]u8 = undefined;
    if (previous) |state| for (state.value.entries) |entry| {
        const expected = try edgeValue(&value, state.value.generation, entry);
        const stored = txn.get(try edgeKey(&key, group, table, entry)) catch |err| return if (err == error.NotFound) error.InvalidSqlTriggerDefinition else err;
        if (!std.mem.eql(u8, stored, expected)) return error.InvalidSqlTriggerDefinition;
    };
    if (!apply) return;
    phase.* = .write_edges;
    if (previous) |state| for (state.value.entries) |entry| try txn.delete(try edgeKey(&key, group, table, entry));
    if (next) |state| for (state.value.entries) |entry| try txn.put(try edgeKey(&key, group, table, entry), try edgeValue(&value, state.value.generation, entry));
}

/// Indexed early exit: never materialize every referencing table just to
/// decide RESTRICT or verify an executable ABI during replacement.
pub fn hasAttachments(txn: anytype, group: u64, routine: u64) !bool {
    var buffer: [160]u8 = undefined;
    const prefix = try routinePrefix(&buffer, group, routine);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const row = (try cursor.seekAtOrAfter(prefix)) orelse return false;
    if (!std.mem.startsWith(u8, row.key, prefix)) return false;
    if (row.key.len != prefix.len + 33 or row.value.len != 25) return error.InvalidSqlTriggerDefinition;
    return true;
}

pub fn requireCompatibleRoutine(txn: anytype, group: u64, previous: routines.Record, replacement: ?routines.Record) !void {
    if (!try hasAttachments(txn, group, previous.id)) return;
    const next = replacement orelse return error.SqlDependentCallable;
    if (next.id != previous.id or next.kind != .function or !next.definition.returns_trigger or next.definition.arguments.len != 0) return error.SqlInvalidFunctionDefinition;
}

pub const Reference = struct { table_id: u64, trigger_id: u64, state_generation: u64, trigger_generation: u32 };
pub const ReferencePage = struct { entries: []const Reference, after: ?Reference = null };

/// A cascade coordinator can stream a bounded exact dependency set, then
/// rejoin each immutable table/schema before constructing atomic replacements.
/// Names never identify either end of a dependency edge.
pub fn references(a: A, txn: anytype, group: u64, routine: u64, after: ?Reference, limit: usize) !ReferencePage {
    if (limit == 0 or limit > 256) return error.SqlProgramLimitExceeded;
    var buffer: [160]u8 = undefined;
    const prefix = try routinePrefix(&buffer, group, routine);
    var seek_buffer: [256]u8 = undefined;
    const seek = if (after) |last| try std.fmt.bufPrint(&seek_buffer, "{s}{x:0>16}:{x:0>16}", .{ prefix, last.table_id, last.trigger_id }) else prefix;
    var cursor = try txn.openCursor();
    defer cursor.close();
    var result: std.ArrayList(Reference) = .empty;
    errdefer result.deinit(a);
    var next = try cursor.seekAtOrAfter(seek);
    while (next) |row| : (next = try cursor.next()) {
        if (!std.mem.startsWith(u8, row.key, prefix)) break;
        if (after != null and std.mem.eql(u8, row.key, seek)) continue;
        if (row.key.len != prefix.len + 33 or row.key[prefix.len + 16] != ':' or row.value.len != 25 or row.value[16] != ':') return error.InvalidSqlTriggerDefinition;
        const entry: Reference = .{
            .table_id = std.fmt.parseInt(u64, row.key[prefix.len..][0..16], 16) catch return error.InvalidSqlTriggerDefinition,
            .trigger_id = std.fmt.parseInt(u64, row.key[prefix.len + 17 ..], 16) catch return error.InvalidSqlTriggerDefinition,
            .state_generation = std.fmt.parseInt(u64, row.value[0..16], 16) catch return error.InvalidSqlTriggerDefinition,
            .trigger_generation = std.fmt.parseInt(u32, row.value[17..], 16) catch return error.InvalidSqlTriggerDefinition,
        };
        if (entry.table_id == 0 or entry.trigger_id == 0 or entry.state_generation == 0 or entry.trigger_generation == 0) return error.InvalidSqlTriggerDefinition;
        var canonical_key: [256]u8 = undefined;
        var canonical_value: [32]u8 = undefined;
        if (!std.mem.eql(u8, row.key, try std.fmt.bufPrint(&canonical_key, "{s}{x:0>16}:{x:0>16}", .{ prefix, entry.table_id, entry.trigger_id })) or
            !std.mem.eql(u8, row.value, try std.fmt.bufPrint(&canonical_value, "{x:0>16}:{x:0>8}", .{ entry.state_generation, entry.trigger_generation }))) return error.InvalidSqlTriggerDefinition;
        if (result.items.len == limit) {
            const last = result.items[result.items.len - 1];
            return .{ .entries = try result.toOwnedSlice(a), .after = last };
        }
        try result.append(a, entry);
    }
    return .{ .entries = try result.toOwnedSlice(a) };
}

pub fn requireReference(a: A, table: Table, routine: u64, reference: Reference) !void {
    if (table.table_id != reference.table_id) return error.InvalidSqlTriggerDefinition;
    var parsed = (try schemaState(a, table)) orelse return error.InvalidSqlTriggerDefinition;
    defer parsed.deinit();
    if (parsed.value.generation != reference.state_generation) return error.CatalogGenerationChanged;
    for (parsed.value.entries) |entry| if (entry.id == reference.trigger_id) {
        if (entry.generation != reference.trigger_generation or entry.function.id != routine) return error.CatalogGenerationChanged;
        return;
    };
    return error.InvalidSqlTriggerDefinition;
}

test "trigger catalog stages routine edges atomically and previews without writes" {
    const Fake = struct {
        const Self = @This();
        a: A,
        values: std.StringHashMapUnmanaged([]const u8) = .empty,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.values.get(key) orelse error.NotFound;
        }
        fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try self.values.put(self.a, try self.a.dupe(u8, key), try self.a.dupe(u8, value));
        }
        fn delete(self: *@This(), key: []const u8) !void {
            _ = self.values.remove(key);
        }
        const Row = struct { key: []const u8, value: []const u8 };
        const Cursor = struct {
            owner: *Self,
            keys: []const []const u8,
            index: usize = 0,
            fn close(_: *@This()) void {}
            fn seekAtOrAfter(self: *@This(), key: []const u8) !?Row {
                self.index = 0;
                while (self.index < self.keys.len and std.mem.order(u8, self.keys[self.index], key) == .lt) self.index += 1;
                return self.current();
            }
            fn current(self: *@This()) ?Row {
                if (self.index == self.keys.len) return null;
                return .{ .key = self.keys[self.index], .value = self.owner.values.get(self.keys[self.index]).? };
            }
            fn next(self: *@This()) !?Row {
                if (self.index < self.keys.len) self.index += 1;
                return self.current();
            }
        };
        fn openCursor(self: *@This()) !Cursor {
            const keys = try self.a.alloc([]const u8, self.values.count());
            var iterator = self.values.keyIterator();
            var i: usize = 0;
            while (iterator.next()) |key| : (i += 1) keys[i] = key.*;
            std.mem.sort([]const u8, keys, {}, struct {
                fn less(_: void, x: []const u8, y: []const u8) bool {
                    return std.mem.order(u8, x, y) == .lt;
                }
            }.less);
            return .{ .owner = self, .keys = keys };
        }
    };
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn: Fake = .{ .a = a };
    const record: routines.Record = .{ .id = 9, .generation = 1, .definition = .{ .returns_trigger = true, .arguments = &.{}, .language = .plpgsql, .body = "BEGIN RETURN NEW; END", .owner = .auth_disabled } };
    const resource: local.system_catalog_domain.Resource = .{ .id = 9, .kind = .routine, .parent_id = 2, .name = "touch", .signature = "", .source_json = try record.encode(a) };
    try txn.put(try storage.recordKeyAlloc(a, 1, .routine, 9), try std.json.Stringify.valueAlloc(a, resource, .{}));
    const before: Table = .{ .table_id = 7, .name = "t", .schema_json = "{}" };
    const initial: triggers.State = .{ .metadata_incarnation = @splat(1), .table_id = 7, .generation = 1 };
    const state = try triggers.prepare(a, initial, .{ .name = "touch", .timing = .before, .events = .{ .update = true }, .level = .row, .function = .{ .id = 9, .namespace_id = 2, .database = "antfly", .namespace = "public", .name = "touch" } }, false);
    var after = before;
    after.schema_json = try std.fmt.allocPrint(a, "{{\"sql_triggers\":{s}}}", .{try state.encode(a)});
    try validateTable(a, &txn, 1, before, after, @splat(1));
    try std.testing.expect(!try hasAttachments(&txn, 1, 9));
    try stageTable(a, &txn, 1, before, after, @splat(1));
    try std.testing.expect(try hasAttachments(&txn, 1, 9));
    var second_routine = record;
    second_routine.id = 10;
    var second_resource = resource;
    second_resource.id = 10;
    second_resource.name = "second";
    second_resource.source_json = try second_routine.encode(a);
    try txn.put(try storage.recordKeyAlloc(a, 1, .routine, 10), try std.json.Stringify.valueAlloc(a, second_resource, .{}));
    const second_state = try triggers.prepare(a, state, .{ .name = "second", .timing = .before, .events = .{ .update = true }, .level = .row, .function = .{ .id = 10, .namespace_id = 2, .database = "antfly", .namespace = "public", .name = "second" } }, false);
    var second_table = after;
    second_table.schema_json = try std.fmt.allocPrint(a, "{{\"sql_triggers\":{s}}}", .{try second_state.encode(a)});
    try validateTable(a, &txn, 1, after, second_table, @splat(1));
    try stageTable(a, &txn, 1, after, second_table, @splat(1));
    try std.testing.expect(try hasAttachments(&txn, 1, 10));
    const second_page = try references(a, &txn, 1, 10, null, 1);
    try requireReference(a, second_table, 10, second_page.entries[0]);
    after = second_table;
    try std.testing.expectError(error.SqlDependentCallable, requireCompatibleRoutine(&txn, 1, record, null));
    try requireCompatibleRoutine(&txn, 1, record, record);
    const page = try references(a, &txn, 1, 9, null, 1);
    try std.testing.expectEqual(@as(usize, 1), page.entries.len);
    try requireReference(a, after, 9, page.entries[0]);
    try std.testing.expectError(error.InvalidSqlTriggerDefinition, validateTable(a, &txn, 1, after, before, @splat(1)));
    try stageTable(a, &txn, 1, after, null, @splat(1));
    try std.testing.expect(!try hasAttachments(&txn, 1, 9));
    try std.testing.expect(!try hasAttachments(&txn, 1, 10));
}

/// Capture current labels/body by immutable routine identity. Attachments keep
/// historical display labels after SET SCHEMA/rename; they never re-resolve a
/// spelling or assert their creation namespace is the routine's current home.
/// Caller pins this read to its statement metadata revision and authenticates
/// the private capture request independently of these data-only identifiers.
pub fn captureRoutine(a: A, txn: anytype, group: u64, id: u64) !local.sql_catalog.trigger_manifest.Routine {
    var resource = (try storage.getById(a, txn, group, .routine, id)) orelse return error.CatalogGenerationChanged;
    defer resource.deinit();
    var namespace = (try storage.getById(a, txn, group, .namespace, resource.value.parent_id)) orelse return error.CatalogGenerationChanged;
    defer namespace.deinit();
    var database = (try storage.getById(a, txn, group, .database, namespace.value.parent_id)) orelse return error.CatalogGenerationChanged;
    defer database.deinit();
    const result: local.sql_catalog.trigger_manifest.Routine = .{
        .reference = .{ .id = id, .namespace_id = namespace.value.id, .database = try a.dupe(u8, database.value.name), .namespace = try a.dupe(u8, namespace.value.name), .name = try a.dupe(u8, resource.value.name) },
        .record = try std.json.parseFromSliceLeaky(routines.Record, a, resource.value.source_json, .{ .allocate = .alloc_always }),
    };
    try result.validate();
    return result;
}
