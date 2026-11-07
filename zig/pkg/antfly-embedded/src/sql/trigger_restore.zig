// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Pure identity handoff for the existing staged generation plan. Import and
//! replay never execute a VM or resolve an object by its display name.
const std = @import("std");
const triggers = @import("../system_catalog/triggers.zig");
const attributes = @import("../system_catalog/column_identities.zig");
const routines = @import("../system_catalog/routines.zig");
const parser = @import("sql_parser");
const A = std.mem.Allocator;
pub const ColumnMove = struct { source_id: u64, destination_id: u64 };
pub const RoutineMove = struct {
    source_id: u64,
    /// The trusted producer/native catalog validates this immutable object and
    /// trigger ABI in the same transaction as attachment/reverse-edge publish.
    destination: routines.RoutineReference,
    /// Statement-authorized public restore pins this current generation.
    /// Zero is reserved for canonical maintenance handoffs, whose native
    /// publication still validates the real current function/trigger ABI.
    destination_generation: u32 = 0,
};
pub const Destination = struct {
    table_id: u64,
    logical_relation_id: ?u64 = null,
    metadata_incarnation: [16]u8,
    columns: attributes.State,
    columns_mapping: []const ColumnMove = &.{},
    routines_mapping: []const RoutineMove = &.{},
};

/// All returned strings/arrays belong to the caller's publication region.
/// Source attributes come from the immutable backup manifest or captured old
/// generation, never a reconstructed ordinal directory after schema churn.
pub fn prepare(a: A, original: triggers.State, source_columns: ?attributes.State, destination: Destination) !triggers.State {
    try original.validate();
    try destination.columns.validate(a);
    if (destination.table_id == 0 or destination.table_id != destination.columns.table_id or std.mem.allEqual(u8, &destination.metadata_incarnation, 0)) return error.InvalidSqlTriggerDefinition;
    if (source_columns) |columns| {
        try columns.validate(a);
        if (columns.table_id != original.table_id) return error.CatalogGenerationChanged;
    }
    if (destination.columns_mapping.len > attributes.max_columns or destination.routines_mapping.len > triggers.max_entries) return error.SqlProgramLimitExceeded;
    for (destination.columns_mapping, 0..) |mapping, index| {
        if (mapping.source_id == 0 or mapping.destination_id == 0) return error.InvalidSqlTriggerDefinition;
        for (destination.columns_mapping[0..index]) |previous| if (previous.source_id == mapping.source_id or previous.destination_id == mapping.destination_id) return error.InvalidSqlTriggerDefinition;
        const source = (source_columns orelse return error.CatalogGenerationChanged).columnById(mapping.source_id) orelse return error.CatalogGenerationChanged;
        const target = destination.columns.columnById(mapping.destination_id) orelse return error.CatalogGenerationChanged;
        if (!std.mem.eql(u8, &source.signature, &target.signature)) return error.InvalidSqlTriggerDefinition;
    }
    for (destination.routines_mapping, 0..) |mapping, index| {
        if (mapping.source_id == 0) return error.InvalidSqlTriggerDefinition;
        try mapping.destination.validate();
        for (destination.routines_mapping[0..index]) |previous| if (previous.source_id == mapping.source_id) return error.InvalidSqlTriggerDefinition;
        const used = for (original.entries) |entry| {
            if (entry.function.id == mapping.source_id) break true;
        } else false;
        if (!used) return error.InvalidSqlTriggerDefinition;
    }
    if (destination.logical_relation_id) |id| if (id == 0 or id > std.math.maxInt(u32)) return error.InvalidSqlTriggerDefinition;
    var next = original;
    next.table_id = destination.table_id;
    // Identity comes exclusively from the admitted destination binding; a
    // copied archive never donates its source SQL relation ID.
    next.logical_relation_id = if (destination.logical_relation_id) |id| if (id == destination.table_id) null else id else null;
    next.metadata_incarnation = destination.metadata_incarnation;
    next.generation = std.math.add(u64, original.generation, 1) catch return error.SqlProgramLimitExceeded;
    const entries = try a.dupe(triggers.Entry, original.entries);
    next.entries = entries;
    for (entries) |*entry| {
        const moved = for (destination.routines_mapping) |mapping| {
            if (mapping.source_id == entry.function.id) break mapping.destination;
        } else null;
        if (moved) |reference| entry.function = reference else if (!std.mem.eql(u8, &original.metadata_incarnation, &destination.metadata_incarnation)) return error.CatalogGenerationChanged;
        const ids = try a.alloc(u64, entry.update_columns.len);
        for (entry.update_columns, ids) |id, *target| {
            const source = (source_columns orelse return error.CatalogGenerationChanged).columnById(id) orelse return error.CatalogGenerationChanged;
            target.* = (try destinationColumn(source, destination)).id;
        }
        std.mem.sort(u64, ids, {}, std.sort.asc(u64));
        entry.update_columns = ids;
        if (entry.predicate) |predicate| entry.predicate = try rewritePredicate(a, predicate, source_columns, destination);
        entry.generation = std.math.add(u32, entry.generation, 1) catch return error.SqlProgramLimitExceeded;
    }
    try next.validate();
    const encoded = try next.encode(a);
    defer a.free(encoded);
    return std.json.parseFromSliceLeaky(triggers.State, a, encoded, .{ .allocate = .alloc_always });
}
fn destinationColumn(source: attributes.Column, destination: Destination) !attributes.Column {
    const explicit = for (destination.columns_mapping) |mapping| {
        if (mapping.source_id == source.id) break mapping.destination_id;
    } else null;
    const found = if (explicit) |id| destination.columns.columnById(id) else byName(destination.columns, source.name);
    const target = found orelse return error.CatalogGenerationChanged;
    if (!std.mem.eql(u8, &target.signature, &source.signature)) return error.InvalidSqlTriggerDefinition;
    return target;
}
fn byName(columns: attributes.State, name: []const u8) ?attributes.Column {
    for (columns.columns) |column| if (std.mem.eql(u8, column.name, name)) return column;
    return null;
}
fn rowQualifier(sql: []const u8, token: parser.token.Token) bool {
    if (token.kind != .identifier) return false;
    const quoted = token.source_start < sql.len and sql[token.source_start] == '"';
    return if (quoted) std.mem.eql(u8, token.text, "new") or std.mem.eql(u8, token.text, "old") else std.ascii.eqlIgnoreCase(token.text, "new") or std.ascii.eqlIgnoreCase(token.text, "old");
}
/// Rewrite only lexed OLD/NEW field selectors. A string, comment, dollar quote,
/// JSON key, or qualified function named old.field never becomes a dependency.
fn rewritePredicate(a: A, sql: []const u8, source: ?attributes.State, destination: Destination) ![]const u8 {
    var tokens = try parser.lexer.tokenizeAlloc(a, sql);
    defer parser.lexer.freeTokens(a, &tokens);
    var out: std.ArrayList(u8) = .empty;
    var consumed: usize = 0;
    for (tokens.items, 0..) |token, index| {
        if (!rowQualifier(sql, token) or index + 2 >= tokens.items.len or tokens.items[index + 1].kind != .dot or tokens.items[index + 2].kind != .identifier) continue;
        var end = index + 3;
        while (end + 1 < tokens.items.len and tokens.items[end].kind == .dot and tokens.items[end + 1].kind == .identifier) end += 2;
        if (end < tokens.items.len and tokens.items[end].kind == .lparen) continue;
        const field = tokens.items[index + 2];
        const original = byName(source orelse return error.CatalogGenerationChanged, field.text) orelse return error.CatalogGenerationChanged;
        const target = try destinationColumn(original, destination);
        if (std.mem.eql(u8, original.name, target.name)) continue;
        if (field.source_start < consumed or field.source_end > sql.len) return error.InvalidSqlTriggerDefinition;
        try out.appendSlice(a, sql[consumed..field.source_start]);
        try out.append(a, '"');
        for (target.name) |byte| {
            try out.append(a, byte);
            if (byte == '"') try out.append(a, byte);
        }
        try out.append(a, '"');
        consumed = field.source_end;
    }
    try out.appendSlice(a, sql[consumed..]);
    if (out.items.len > 65536) return error.SqlProgramLimitExceeded;
    return out.toOwnedSlice(a);
}

test "SQL trigger staged restore rebinds physical and attribute identities without executing or adopting routine names" {
    var region: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    const source: attributes.State = .{ .format = 1, .table_id = 7, .epoch = .{ .version = 1, .digest = @splat(2) }, .next_id = 12, .columns = &.{.{ .id = 11, .name = "a", .signature = @splat(3) }} };
    const columns: attributes.State = .{ .format = 1, .table_id = 8, .epoch = .{ .version = 2, .digest = @splat(4) }, .next_id = 4, .columns = &.{.{ .id = 3, .name = "b\"column", .signature = @splat(3) }} };
    const reference: routines.RoutineReference = .{ .database = "d", .namespace = "public", .name = "old_function_name", .namespace_id = 2, .id = 9 };
    const original: triggers.State = .{ .metadata_incarnation = @splat(1), .table_id = 7, .generation = 10, .next_id = 2, .entries = &.{.{ .id = 1, .generation = 1, .name = "touch", .timing = .before, .events = .{ .update = true }, .level = .row, .function = reference, .update_columns = &.{11}, .predicate = "NEW.a > 1 AND OLD.a IS NULL AND old.a() = 2 AND 'new.a' = 'new.a' /* old.a */" }} };
    var destination: Destination = .{ .table_id = 8, .metadata_incarnation = @splat(1), .columns = columns, .columns_mapping = &.{.{ .source_id = 11, .destination_id = 3 }} };
    const restored = try prepare(a, original, source, destination);
    try std.testing.expectEqual(@as(u64, 8), restored.table_id);
    try std.testing.expectEqual(@as(u64, 11), restored.generation);
    try std.testing.expectEqual(@as(u64, 1), restored.entries[0].id);
    try std.testing.expectEqual(@as(u64, 9), restored.entries[0].function.id);
    try std.testing.expectEqual(@as(u64, 3), restored.entries[0].update_columns[0]);
    try std.testing.expectEqualStrings("NEW.\"b\"\"column\" > 1 AND OLD.\"b\"\"column\" IS NULL AND old.a() = 2 AND 'new.a' = 'new.a' /* old.a */", restored.entries[0].predicate.?);
    try std.testing.expectError(error.CatalogGenerationChanged, prepare(a, original, null, destination));
    destination.metadata_incarnation = @splat(5);
    try std.testing.expectError(error.CatalogGenerationChanged, prepare(a, original, source, destination));
    destination.routines_mapping = &.{.{ .source_id = 9, .destination = .{ .database = "d", .namespace = "other", .name = "destination_function_name", .namespace_id = 12, .id = 19 } }};
    const moved = try prepare(a, original, source, destination);
    try std.testing.expectEqual(@as(u64, 19), moved.entries[0].function.id);
    try std.testing.expectEqual(@as([16]u8, @splat(5)), moved.metadata_incarnation);
    var incompatible = columns;
    incompatible.columns = &.{.{ .id = 3, .name = "b\"column", .signature = @splat(6) }};
    destination.columns = incompatible;
    try std.testing.expectError(error.InvalidSqlTriggerDefinition, prepare(a, original, source, destination));
}

/// Semantic source directory digest, excluding descriptions which carry no
/// executable identity and may change independently of the table schema cut.
pub fn sourceColumnsDigest(a: A, columns: attributes.State) ![32]u8 {
    try columns.validate(a);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/sql-trigger-restore/source-columns/v1\x00");
    var number: [8]u8 = undefined;
    for ([_]u64{ columns.table_id, columns.epoch.version, columns.next_id, columns.columns.len }) |value| {
        std.mem.writeInt(u64, &number, value, .little);
        hash.update(&number);
    }
    hash.update(&columns.epoch.digest);
    for (columns.columns) |column| {
        std.mem.writeInt(u64, &number, column.id, .little);
        hash.update(&number);
        std.mem.writeInt(u64, &number, column.name.len, .little);
        hash.update(&number);
        hash.update(column.name);
        hash.update(&column.signature);
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

test "SQL trigger restore source directory digest binds identity churn and ignores descriptions" {
    const a = std.testing.allocator;
    const original: attributes.State = .{ .format = 1, .table_id = 7, .epoch = .{ .version = 2, .digest = @splat(1) }, .next_id = 12, .columns = &.{.{ .id = 11, .name = "a", .signature = @splat(3) }} };
    const first = try sourceColumnsDigest(a, original);
    var annotated = original;
    annotated.columns = &.{.{ .id = 11, .name = "a", .signature = @splat(3), .description = "presentation-only" }};
    try std.testing.expectEqual(first, try sourceColumnsDigest(a, annotated));
    var forged = original;
    forged.columns = &.{.{ .id = 3, .name = "a", .signature = @splat(3) }};
    try std.testing.expect(!std.mem.eql(u8, &first, &try sourceColumnsDigest(a, forged)));
    forged = original;
    forged.next_id += 1;
    try std.testing.expect(!std.mem.eql(u8, &first, &try sourceColumnsDigest(a, forged)));
}

test "SQL trigger restore takes logical identity only from the admitted destination" {
    var region: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    const original: triggers.State = .{ .metadata_incarnation = @splat(1), .table_id = 7, .logical_relation_id = 3, .generation = 1 };
    const columns: attributes.State = .{ .format = 1, .table_id = 11, .epoch = .{ .version = 1, .digest = @splat(2) }, .next_id = 1, .columns = &.{} };
    const independent = try prepare(a, original, null, .{ .table_id = 11, .metadata_incarnation = @splat(1), .columns = columns });
    try std.testing.expectEqual(@as(u64, 11), independent.effectiveLogicalRelationId());
    const same_object = try prepare(a, original, null, .{ .table_id = 11, .logical_relation_id = 3, .metadata_incarnation = @splat(1), .columns = columns });
    try std.testing.expectEqual(@as(u64, 3), same_object.effectiveLogicalRelationId());
    try std.testing.expectEqual(@as(u64, 11), same_object.table_id);
}
