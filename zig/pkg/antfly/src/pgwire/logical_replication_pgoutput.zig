// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! pgoutput protocol v1. Inputs are captured canonical SQL text, never JSON
//! re-serialization or current-row polling. This codec grants no publication,
//! slot, snapshot, transaction or raw-row authority.
const std = @import("std");
const publisher = @import("antfly_local_sources").system_catalog_logical_replication_publisher;
pub const Cell = union(enum) { null_value, unchanged_toast, text: []const u8 };
pub const Tuple = []const Cell;
pub const OldTuple = union(enum) { key: Tuple, full: Tuple };
pub const Change = union(enum) {
    insert: struct { relation_oid: u32, values: Tuple },
    update: struct { relation_oid: u32, before: ?OldTuple = null, after: Tuple },
    delete: struct { relation_oid: u32, before: OldTuple },
    truncate: struct { relation_oids: []const u32, cascade: bool, restart_identity: bool },
};
const pg_epoch_unix_us: u64 = 946684800000000;
fn timestamp(unix_us: u64) !i64 {
    const delta: i128 = @as(i128, unix_us) - pg_epoch_unix_us;
    return std.math.cast(i64, delta) orelse error.InvalidLogicalPublisher;
}
fn cstring(writer: *std.Io.Writer, text: []const u8) !void {
    if (std.mem.indexOfScalar(u8, text, 0) != null) return error.InvalidLogicalPublisher;
    try writer.writeAll(text);
    try writer.writeByte(0);
}
pub fn begin(writer: *std.Io.Writer, final_lsn: u64, commit_unix_us: u64, xid: u32) !void {
    if (final_lsn == 0 or xid == 0) return error.InvalidLogicalPublisher;
    const time = try timestamp(commit_unix_us);
    try writer.writeByte('B');
    try writer.writeInt(u64, final_lsn, .big);
    try writer.writeInt(i64, time, .big);
    try writer.writeInt(u32, xid, .big);
}
pub fn commit(writer: *std.Io.Writer, commit_lsn: u64, end_lsn: u64, commit_unix_us: u64) !void {
    if (commit_lsn == 0 or end_lsn <= commit_lsn) return error.InvalidLogicalPublisher;
    const time = try timestamp(commit_unix_us);
    try writer.writeAll("C\x00");
    try writer.writeInt(u64, commit_lsn, .big);
    try writer.writeInt(u64, end_lsn, .big);
    try writer.writeInt(i64, time, .big);
}
pub fn validateProjection(description: publisher.RelationDescription, columns: []const publisher.Column) !void {
    try description.validate();
    if (columns.len > description.columns.len) return error.InvalidLogicalPublisher;
    var index: usize = 0;
    var prior: u16 = 0;
    for (columns) |column| {
        if (column.attnum <= prior) return error.InvalidLogicalPublisher;
        while (index < description.columns.len and description.columns[index].attnum < column.attnum) index += 1;
        if (index == description.columns.len) return error.InvalidLogicalPublisher;
        const original = description.columns[index];
        if (original.attnum != column.attnum or !std.mem.eql(u8, original.name, column.name) or original.type_oid != column.type_oid or
            original.type_modifier != column.type_modifier or original.key != column.key or original.generated != column.generated) return error.InvalidLogicalPublisher;
        prior = column.attnum;
        index += 1;
    }
}

pub fn relation(writer: *std.Io.Writer, description: publisher.RelationDescription) !void {
    return relationProjection(writer, description, description.columns);
}

/// Wire projections may contain zero columns (e.g. generated-only tables with
/// generated-column publication disabled). The captured physical description
/// remains complete, schema-bound and independently validated.
pub fn relationProjection(writer: *std.Io.Writer, description: publisher.RelationDescription, columns: []const publisher.Column) !void {
    try validateProjection(description, columns);
    try relationHeader(writer, description, columns.len);
    for (columns) |column| try relationColumn(writer, description.replica_identity, column);
}

fn relationHeader(writer: *std.Io.Writer, description: publisher.RelationDescription, count: usize) !void {
    try writer.writeByte('R');
    try writer.writeInt(u32, description.relation_oid, .big);
    try cstring(writer, description.namespace);
    try cstring(writer, description.name);
    try writer.writeByte(switch (description.replica_identity) {
        .default_key => 'd',
        .full => 'f',
        .nothing => 'n',
        .index => 'i',
    });
    try writer.writeInt(u16, @intCast(count), .big);
}

fn relationColumn(writer: *std.Io.Writer, identity: publisher.ReplicaIdentity, column: publisher.Column) !void {
    try writer.writeByte(@intFromBool(identity == .full or column.key));
    try cstring(writer, column.name);
    try writer.writeInt(u32, column.type_oid, .big);
    try writer.writeInt(i32, column.type_modifier, .big);
}

pub fn generatedMode(roster: publisher.PublishedRelation, selected: []const u64) !@import("antfly_local_sources").system_catalog_logical_replication.GeneratedColumns {
    try roster.validate();
    if (selected.len == 0 or selected.len > publisher.max_publications) return error.InvalidLogicalPublisher;
    for (selected, 0..) |id, i| if (id == 0 or (i != 0 and selected[i - 1] >= id)) return error.InvalidLogicalPublisher;
    const has_generated = for (roster.relation.columns) |column| {
        if (column.generated) break true;
    } else false;
    var generated: ?@import("antfly_local_sources").system_catalog_logical_replication.GeneratedColumns = null;
    var index: usize = 0;
    for (roster.memberships) |membership| {
        while (index < selected.len and selected[index] < membership.publication.id) index += 1;
        if (index == selected.len) break;
        if (selected[index] != membership.publication.id) continue;
        if (generated) |prior| if (has_generated and prior != membership.parameters.publish_generated_columns) return error.UnsupportedSqlShape;
        generated = membership.parameters.publish_generated_columns;
    }
    return generated orelse error.InvalidLogicalPublisher;
}

/// TRUNCATE has no tuple payload but still announces the same selected column
/// shape as row delivery. Filter without allocating a full column-array copy.
pub fn relationForPublications(writer: *std.Io.Writer, roster: publisher.PublishedRelation, selected: []const u64) !void {
    const stored = try generatedMode(roster, selected) == .stored;
    var count: usize = 0;
    for (roster.relation.columns) |column| if (stored or !column.generated) {
        count += 1;
    };
    try relationHeader(writer, roster.relation, count);
    for (roster.relation.columns) |column| if (stored or !column.generated) try relationColumn(writer, roster.relation.replica_identity, column);
}
pub fn originMessage(writer: *std.Io.Writer, value: publisher.Origin) !void {
    if (value.position == 0 or value.name.len == 0 or value.name.len > 1024 or !std.unicode.utf8ValidateSlice(value.name) or std.mem.indexOfScalar(u8, value.name, 0) != null) return error.InvalidLogicalPublisher;
    try writer.writeByte('O');
    try writer.writeInt(u64, value.position, .big);
    try cstring(writer, value.name);
}

fn validateTuple(values: Tuple, allow_unchanged: bool) !void {
    if (values.len > publisher.max_columns) return error.InvalidLogicalPublisher;
    for (values) |cell| switch (cell) {
        .unchanged_toast => if (!allow_unchanged) return error.InvalidLogicalPublisher,
        .text => |text| if (text.len > std.math.maxInt(i32)) return error.LogicalReplicationTupleTooLarge,
        .null_value => {},
    };
}
fn tuple(writer: *std.Io.Writer, values: Tuple) !void {
    try writer.writeInt(u16, @intCast(values.len), .big);
    for (values) |cell| switch (cell) {
        .null_value => try writer.writeByte('n'),
        .unchanged_toast => try writer.writeByte('u'),
        .text => |text| {
            try writer.writeByte('t');
            try writer.writeInt(i32, @intCast(text.len), .big);
            try writer.writeAll(text);
        },
    };
}
fn old(writer: *std.Io.Writer, before: OldTuple) !void {
    switch (before) {
        .key => |values| {
            try writer.writeByte('K');
            try tuple(writer, values);
        },
        .full => |values| {
            try writer.writeByte('O');
            try tuple(writer, values);
        },
    }
}
pub fn change(writer: *std.Io.Writer, value: Change) !void {
    // Validate the complete message before its first byte becomes visible.
    switch (value) {
        .insert => |row| {
            if (row.relation_oid == 0) return error.InvalidLogicalPublisher;
            try validateTuple(row.values, false);
        },
        .update => |row| {
            if (row.relation_oid == 0) return error.InvalidLogicalPublisher;
            try validateTuple(row.after, true);
            if (row.before) |before| switch (before) {
                .key, .full => |values| try validateTuple(values, false),
            };
        },
        .delete => |row| {
            if (row.relation_oid == 0) return error.InvalidLogicalPublisher;
            switch (row.before) {
                .key, .full => |values| try validateTuple(values, false),
            }
        },
        .truncate => |tables| {
            if (tables.relation_oids.len == 0 or tables.relation_oids.len > publisher.max_relations) return error.InvalidLogicalPublisher;
            for (tables.relation_oids) |id| if (id == 0) return error.InvalidLogicalPublisher;
        },
    }
    switch (value) {
        .insert => |row| {
            try writer.writeByte('I');
            try writer.writeInt(u32, row.relation_oid, .big);
            try writer.writeByte('N');
            try tuple(writer, row.values);
        },
        .update => |row| {
            try writer.writeByte('U');
            try writer.writeInt(u32, row.relation_oid, .big);
            if (row.before) |before| try old(writer, before);
            try writer.writeByte('N');
            try tuple(writer, row.after);
        },
        .delete => |row| {
            try writer.writeByte('D');
            try writer.writeInt(u32, row.relation_oid, .big);
            try old(writer, row.before);
        },
        .truncate => |tables| {
            try writer.writeByte('T');
            try writer.writeInt(u32, @intCast(tables.relation_oids.len), .big);
            try writer.writeByte(@as(u8, @intFromBool(tables.cascade)) | (@as(u8, @intFromBool(tables.restart_identity)) << 1));
            for (tables.relation_oids) |id| try writer.writeInt(u32, id, .big);
        },
    }
}

/// Physical WAL envelope carries the actual native-issued logical journal
/// positions. The server must have validated the slot lease before this call.
pub fn xlogData(writer: *std.Io.Writer, start: u64, server_end: u64, now_unix_us: u64, payload: []const u8) !void {
    if (start == 0 or server_end < start) return error.InvalidLogicalPublisher;
    const time = try timestamp(now_unix_us);
    try writer.writeByte('w');
    try writer.writeInt(u64, start, .big);
    try writer.writeInt(u64, server_end, .big);
    try writer.writeInt(i64, time, .big);
    try writer.writeAll(payload);
}
pub fn keepalive(writer: *std.Io.Writer, server_end: u64, now_unix_us: u64, reply_requested: bool) !void {
    if (server_end == 0) return error.InvalidLogicalPublisher;
    const time = try timestamp(now_unix_us);
    try writer.writeByte('k');
    try writer.writeInt(u64, server_end, .big);
    try writer.writeInt(i64, time, .big);
    try writer.writeByte(@intFromBool(reply_requested));
}
pub const StandbyStatus = struct { received: u64, flushed: u64, applied: u64, client_time_pg_us: i64, reply_requested: bool };
pub fn standbyStatus(bytes: []const u8) !StandbyStatus {
    if (bytes.len != 34 or bytes[0] != 'r' or bytes[33] > 1) return error.InvalidLogicalReplicationMessage;
    const result: StandbyStatus = .{
        .received = std.mem.readInt(u64, bytes[1..9], .big),
        .flushed = std.mem.readInt(u64, bytes[9..17], .big),
        .applied = std.mem.readInt(u64, bytes[17..25], .big),
        .client_time_pg_us = std.mem.readInt(i64, bytes[25..33], .big),
        .reply_requested = bytes[33] != 0,
    };
    if (result.applied > result.flushed or result.flushed > result.received) return error.InvalidLogicalReplicationMessage;
    // Client timestamps are diagnostic only, never a native lease clock.
    return result;
}

test "pgoutput v1 preserves SQL null text JSON null and actual beforeimages" {
    var bytes: [512]u8 = undefined;
    var writer = std.Io.Writer.fixed(&bytes);
    const row = [_]Cell{ .null_value, .{ .text = "null" }, .{ .text = "" } };
    try change(&writer, .{ .insert = .{ .relation_oid = 42, .values = &row } });
    try std.testing.expectEqualSlices(u8, "I\x00\x00\x00*N\x00\x03nt\x00\x00\x00\x04nullt\x00\x00\x00\x00", writer.buffered());
    var invalid = std.Io.Writer.fixed(&bytes);
    try std.testing.expectError(error.InvalidLogicalPublisher, change(&invalid, .{ .delete = .{ .relation_oid = 42, .before = .{ .full = &.{.unchanged_toast} } } }));
    try std.testing.expectEqual(@as(usize, 0), invalid.buffered().len);
}

test "pgoutput standby feedback never treats client clock as custody authority" {
    var bytes: [34]u8 = @splat(0);
    bytes[0] = 'r';
    std.mem.writeInt(u64, bytes[1..9], 3, .big);
    std.mem.writeInt(u64, bytes[9..17], 2, .big);
    std.mem.writeInt(u64, bytes[17..25], 1, .big);
    std.mem.writeInt(i64, bytes[25..33], -1, .big);
    const value = try standbyStatus(&bytes);
    try std.testing.expectEqual(@as(i64, -1), value.client_time_pg_us);
    bytes[33] = 2;
    try std.testing.expectError(error.InvalidLogicalReplicationMessage, standbyStatus(&bytes));
}

test "logical publisher FULL replica identity marks every actual published column as key" {
    var output = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer output.deinit();
    const description: publisher.RelationDescription = .{ .logical_relation_id = 7, .physical_table_id = 9, .database_id = 1, .namespace_id = 2, .relation_oid = 7, .namespace = "public", .name = "full_identity", .schema_version = 1, .schema_digest = @splat(3), .replica_identity = .full, .columns = &.{.{ .attnum = 1, .name = "payload", .type_oid = 3802, .key = false }} };
    try relation(&output.writer, description);
    const bytes = output.written();
    // R + OID + two C strings + identity + column count.
    const flag_offset = 1 + 4 + description.namespace.len + 1 + description.name.len + 1 + 1 + 2;
    try std.testing.expectEqual(@as(u8, 1), bytes[flag_offset]);
    try std.testing.expectEqual(@as(u32, 3802), std.mem.readInt(u32, bytes[flag_offset + 1 + "payload".len + 1 ..][0..4], .big));
}

test "logical publisher PostgreSQL generated projection wire reference covers rows and conflicting publications" {
    const a0 = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(a0);
    defer region.deinit();
    const a = region.allocator();
    const Reference = struct {
        postgres_major: u32,
        entries: []const struct {
            table: []const u8,
            mode: enum { none, stored, mixed },
            change: enum { insert, truncate },
            types: ?[]const u8 = null,
            messages: []const []const u8 = &.{},
            sqlstate: ?[]const u8 = null,
        },
    };
    const reference = try std.json.parseFromSliceLeaky(Reference, a, @embedFile("fixtures/logical_delivery_reference.json"), .{});
    try std.testing.expect(reference.postgres_major >= 18);
    try std.testing.expectEqual(@as(usize, 15), reference.entries.len);
    for (reference.entries) |expected| {
        if (expected.change == .truncate) continue;
        const only_generated = std.mem.eql(u8, expected.table, "onlygen");
        const ordinary = std.mem.eql(u8, expected.table, "ordinary");
        const generated_key = std.mem.eql(u8, expected.table, "keygen");
        const columns = [_]publisher.Column{
            .{ .attnum = 1, .name = if (only_generated) "g" else "id", .type_oid = 23, .key = !only_generated and !generated_key, .generated = only_generated },
            .{ .attnum = 2, .name = "g", .type_oid = 23, .key = generated_key, .generated = true },
        };
        const count: usize = if (only_generated or ordinary) 1 else 2;
        const roster: publisher.PublishedRelation = .{ .relation = .{ .logical_relation_id = 100, .physical_table_id = 20, .database_id = 1, .namespace_id = 2, .relation_oid = 100, .namespace = "public", .name = expected.table, .schema_version = 1, .schema_digest = @splat(1), .replica_identity = .default_key, .columns = columns[0..count] }, .memberships = &.{
            .{ .publication = .{ .id = 30, .generation = 1, .digest = @splat(2) }, .parameters = .{} },
            .{ .publication = .{ .id = 40, .generation = 1, .digest = @splat(3) }, .parameters = .{ .publish_generated_columns = .stored } },
        } };
        const selected: []const u64 = switch (expected.mode) {
            .none => &.{30},
            .stored => &.{40},
            .mixed => &.{ 30, 40 },
        };
        var output = std.Io.Writer.Allocating.init(a);
        if (expected.sqlstate) |code| {
            try std.testing.expectEqualStrings("0A000", code);
            try std.testing.expectError(error.UnsupportedSqlShape, relationForPublications(&output.writer, roster, selected));
            try std.testing.expectEqual(@as(usize, 0), output.written().len);
            continue;
        }
        try std.testing.expectEqual(@as(usize, 2), expected.messages.len);
        const expected_relation = try a.alloc(u8, expected.messages[0].len / 2);
        _ = try std.fmt.hexToBytes(expected_relation, expected.messages[0]);
        const expected_change = try a.alloc(u8, expected.messages[1].len / 2);
        _ = try std.fmt.hexToBytes(expected_change, expected.messages[1]);
        try relationForPublications(&output.writer, roster, selected);
        try std.testing.expectEqualSlices(u8, expected_relation, output.written());
        output.clearRetainingCapacity();
        const include_generated = try generatedMode(roster, selected) == .stored;
        var tuple_cells: [2]Cell = undefined;
        var length: usize = 0;
        for (roster.relation.columns) |column| if (include_generated or !column.generated) {
            tuple_cells[length] = .{ .text = if (column.attnum == 1) "1" else "2" };
            length += 1;
        };
        try change(&output.writer, .{ .insert = .{ .relation_oid = 100, .values = tuple_cells[0..length] } });
        try std.testing.expectEqualSlices(u8, expected_change, output.written());
    }
}
