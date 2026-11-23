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

//! Actual immutable AROW -> pgoutput text. No current row/schema lookup,
//! default/trigger replay or domain-OID flattening occurs on delivery. The
//! caller retains the authenticated outbox page and supplies bounded scratch.
const std = @import("std");
const local = @import("antfly_local_sources");
const wire = local.system_catalog_logical_replication_publisher;
const outbox = local.storage_db_logical_replication_outbox_contract;
const rows = local.storage_db_algebraic_relational_row_codec;
const schema = local.storage_schema;
const backend = @import("backend.zig");
const values = @import("values.zig");
const A = std.mem.Allocator;
const PreparedSchema = struct { digest: [32]u8, table: schema.TableSchema, layout: rows.PhysicalLayout, columns: []const backend.Column };

const PreparedCell = struct { column: backend.Column, value: ?backend.Cell, bytes: i32 = 0 };
const PreparedTuple = struct {
    cells: []const PreparedCell,
    encoded_bytes: usize,

    fn write(self: PreparedTuple, a: A, writer: *std.Io.Writer) !void {
        try writer.writeInt(u16, @intCast(self.cells.len), .big);
        for (self.cells) |cell| {
            if (cell.value) |native| {
                try writer.writeByte('t');
                try writer.writeInt(i32, cell.bytes, .big);
                try values.encodeCellInto(a, writer, cell.column, 0, native, @intCast(cell.bytes));
            } else try writer.writeByte('n');
        }
    }
};

/// Validated immutable logical values plus exact wire lengths, not an encoded
/// row-sized buffer. Retain the preparation arena, Encoder and input page until
/// write returns. A writer failure invalidates the containing delivery stream.
pub const PreparedChange = struct {
    relation_oid: u32,
    full_identity: bool,
    before: ?PreparedTuple,
    after: ?PreparedTuple,
    encoded_bytes: usize,

    pub fn write(self: PreparedChange, a: A, writer: *std.Io.Writer) !void {
        try writer.writeByte(if (self.before == null) 'I' else if (self.after != null) 'U' else 'D');
        try writer.writeInt(u32, self.relation_oid, .big);
        if (self.before) |tuple| {
            try writer.writeByte(if (self.full_identity) 'O' else 'K');
            try tuple.write(a, writer);
        }
        if (self.after) |tuple| {
            try writer.writeByte('N');
            try tuple.write(a, writer);
        }
    }
};

pub const Encoder = struct {
    relation: wire.RelationDescription,
    schemas: []const PreparedSchema,
    /// Result uses the supplied owning arena. It retains native layouts and
    /// named-type views, not a second copy of captured row payloads.
    pub fn initLeaky(a: A, relation: wire.RelationDescription, captured: []const outbox.Schema) !Encoder {
        try relation.validate();
        if (captured.len == 0) return error.InvalidLogicalOutbox;
        const prepared_schemas = try a.alloc(PreparedSchema, captured.len);
        for (captured, prepared_schemas, 0..) |item, *target, index| {
            try item.validate();
            for (captured[0..index]) |prior| if (std.mem.eql(u8, &prior.digest, &item.digest)) return error.InvalidLogicalOutbox;
            const table = try schema.deserializeSchema(a, item.encoded);
            if (table.storage_mode != .relational or table.version > relation.schema_version) return error.RelationalRowSchemaMismatch;
            if (table.version == relation.schema_version and !std.mem.eql(u8, &try rows.PhysicalLayout.schemaDigest(a, table), &relation.schema_digest)) return error.LogicalCaptureControlChanged;
            const columns = try local.sql_catalog.relationalColumnsLeaky(a, table);
            const physical = try a.alloc(backend.Column, columns.len);
            for (columns, physical) |column, *output| output.* = .{ .name = column.name, .type = switch (column.type) {
                inline else => |tag| @field(backend.Type, @tagName(tag)),
            }, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .temporal_kind = column.temporal_kind, .enum_view = column.enum_view, .domain_view = column.domain_view };
            for (relation.columns) |column| {
                const ordinal: usize = column.attnum - 1;
                if (ordinal >= physical.len) continue; // genuinely absent historical column = SQL NULL
                // ParameterDescription preserves named-domain identity;
                // RowDescription intentionally flattens it and is wrong here.
                if (try values.parameterOid(try values.parameterFromColumn(physical[ordinal])) != column.type_oid or
                    try values.columnModifier(physical[ordinal]) != column.type_modifier) return error.RelationalIndexColumnTypeMismatch;
                if (table.version == relation.schema_version and !std.mem.eql(u8, physical[ordinal].name, column.name)) return error.RelationalRowSchemaMismatch;
            }
            target.* = .{ .digest = item.digest, .table = table, .layout = try rows.PhysicalLayout.init(a, table), .columns = physical };
        }
        return .{ .relation = relation, .schemas = prepared_schemas };
    }
    pub fn initProjectionLeaky(a: A, relation: wire.RelationDescription, captured: []const outbox.Schema, columns: []const wire.Column) !Encoder {
        try @import("logical_replication_pgoutput.zig").validateProjection(relation, columns);
        var result = try initLeaky(a, relation, captured);
        result.relation.columns = columns;
        return result;
    }
    fn prepared(self: Encoder, image: outbox.Image) !*const PreparedSchema {
        for (self.schemas) |*item| if (std.mem.eql(u8, &item.digest, &image.schema_digest)) {
            if (try rows.rowSchemaVersion(image.row) != item.table.version) return error.RelationalRowSchemaMismatch;
            return item;
        };
        return error.InvalidLogicalOutbox;
    }
    fn prepareTuple(self: Encoder, a: A, image: outbox.Image, key_only: bool) !PreparedTuple {
        const source = try self.prepared(image);
        const row = try rows.ordinalRowView(image.row, source.table, &source.layout);
        const cells = try a.alloc(PreparedCell, self.relation.columns.len);
        var encoded_bytes: usize = 2;
        for (self.relation.columns, cells) |column, *cell| {
            cell.* = .{ .column = .{ .name = column.name, .type = .unknown }, .value = null };
            encoded_bytes = try std.math.add(usize, encoded_bytes, 1);
            const ordinal: usize = column.attnum - 1;
            if ((key_only and !column.key) or ordinal >= source.columns.len) {
                if (column.key and ordinal >= source.columns.len) return error.RelationalRowSchemaMismatch;
                continue;
            }
            // The immutable transport page is pinned through writing. Ordinary
            // text/blob/JSON values may borrow it; cursor-independent query
            // projections deliberately keep using the owning decoder.
            const datum = switch (source.table.relational_columns[ordinal].column_type) {
                .string, .blob, .json => if (try row.findCell(ordinal)) |physical_cell|
                    if (physical_cell.is_null) local.sql_scalar.Datum{} else local.sql_scalar.Datum.json(try rows.borrowedJsonValueFromCellAlloc(a, source.table.relational_columns[ordinal], physical_cell))
                else
                    local.sql_scalar.Datum{},
                else => (try row.sqlDatumAlloc(a, ordinal)) orelse local.sql_scalar.Datum{},
            };
            try datum.validateTemporal();
            if (datum.sql_null) {
                if (key_only and column.key) return error.InvalidLogicalOutbox;
                continue;
            }
            const native = try nativeCell(a, source.columns[ordinal], datum);
            var size = std.Io.Writer.Discarding.init(&.{});
            try values.encodeCellInto(a, &size.writer, source.columns[ordinal], 0, native, std.math.maxInt(i32));
            const length = std.math.cast(i32, size.fullCount()) orelse return error.LogicalReplicationTupleTooLarge;
            encoded_bytes = try std.math.add(usize, encoded_bytes, 4 + @as(usize, @intCast(length)));
            cell.* = .{ .column = source.columns[ordinal], .value = native, .bytes = length };
        }
        return .{ .cells = cells, .encoded_bytes = encoded_bytes };
    }
    /// Decode and validate each image once before framing a message. Output
    /// size includes every tuple tag/length; oversized records fail before the
    /// first socket byte. No defaults, triggers or current schema are consulted.
    pub fn prepare(self: Encoder, scratch: A, change: outbox.Change) !PreparedChange {
        try change.validate(outbox.max_page_wire_bytes);
        const full = self.relation.replica_identity == .full;
        if (change.before != null and !full) {
            if (self.relation.replica_identity == .nothing) return error.InvalidLogicalOutbox;
            const has_key = for (self.relation.columns) |column| {
                if (column.key) break true;
            } else false;
            if (!has_key) return error.InvalidLogicalOutbox;
        }
        const before = if (change.before) |image| try self.prepareTuple(scratch, image, !full) else null;
        const after = if (change.after) |image| try self.prepareTuple(scratch, image, false) else null;
        var encoded_bytes: usize = 5;
        for ([_]?PreparedTuple{ before, after }) |tuple| if (tuple) |present| {
            encoded_bytes = try std.math.add(usize, encoded_bytes, 1 + present.encoded_bytes);
        };
        // CopyData includes its own length and the XLogData fixed header.
        if (encoded_bytes > std.math.maxInt(u32) - 29) return error.LogicalReplicationTupleTooLarge;
        return .{ .relation_oid = self.relation.relation_oid, .full_identity = full, .before = before, .after = after, .encoded_bytes = encoded_bytes };
    }

    pub fn write(self: Encoder, scratch: A, writer: *std.Io.Writer, change: outbox.Change) !void {
        const prepared_change = try self.prepare(scratch, change);
        try prepared_change.write(scratch, writer);
    }
};
fn nativeCell(a: A, column: backend.Column, datum: local.sql_scalar.Datum) !backend.Cell {
    try datum.validateTemporal();
    if (datum.sql_null or datum.isVoid()) return error.InvalidLogicalOutbox;
    if (datum.bytes()) |bytes| return .{ .bytea = bytes, .sql_null = false };
    if (datum.enumeration()) |atom| return .{ .enumeration = atom, .sql_null = false };
    if (datum.range()) |range| return .{ .range = range, .sql_null = false };
    if (datum.array) |array| return .{ .array = array, .sql_null = false };
    if (datum.numeric) |number| return .{ .numeric = number, .sql_null = false };
    if (datum.temporal()) |temporal| {
        if (column.type != .datetime or temporal.kind() != (column.temporal_kind orelse .timestamp_tz)) return error.InvalidLogicalOutbox;
        if (temporal.kind() == .timestamp_tz) return .{ .value = try values.timestampEnvelope(a, try local.sql_scalar.timestampResult(datum, .datetime)), .sql_null = false };
        var text: [local.sql_temporal_text.output_capacity]u8 = undefined;
        return .{ .value = .{ .string = try a.dupe(u8, try local.sql_temporal_text.format(temporal.*, &text)) }, .sql_null = false };
    }
    return .{ .value = datum.value, .sql_null = false };
}

/// One linear captured participant stream. It owns only fixed digest state,
/// not row buffers; the caller releases each real Data page after encoding it.
/// Checking this is mandatory BEFORE the transaction's pgoutput COMMIT.
pub const CaptureStream = struct {
    reference: wire.CaptureRef,
    after: u64 = 0,
    hash: std.crypto.hash.Blake3,
    encoded_rows: u64 = 0,
    failed: bool = false,
    pub fn init(reference: wire.CaptureRef) !CaptureStream {
        try reference.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/logical-outbox-changes/v1\x00");
        return .{ .reference = reference, .hash = hash };
    }
    /// This consumes the next exact page, never a retry/reordered subset.
    /// On ANY error the caller abandons the uncommitted pgoutput transaction.
    pub fn accept(self: *CaptureStream, a: A, request: outbox.PageRequest, page: outbox.Page) !void {
        if (self.failed) return error.InvalidLogicalOutbox;
        errdefer self.failed = true;
        if (!request.reference.sameLogical(self.reference) or request.after != self.after) return error.InvalidLogicalOutbox;
        try request.require(page);
        try page.header.validate();
        try page.roster.validate();
        if (!std.mem.eql(u8, &page.header.rows_digest, &self.reference.changes_digest) or
            !std.mem.eql(u8, &try local.system_catalog_logical_replication_capture.canonicalDigest(a, "antfly/logical-capture-roster/v1\x00", page.roster), &self.reference.roster_digest)) return error.InvalidLogicalOutbox;
        // A page may repeat a schema across a boundary; every supplied schema
        // still hashes to the exact immutable digest retained in each image.
        for (page.schemas) |captured| try captured.validate();
        for (page.changes) |change| {
            if (self.after == std.math.maxInt(u64) or change.ordinal != self.after + 1) return error.InvalidLogicalOutbox;
            const length = try change.encodedLen(outbox.max_page_wire_bytes);
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, length, .little);
            self.hash.update(&bytes);
            try change.hashEncoded(&self.hash, outbox.max_page_wire_bytes);
            self.encoded_rows = try std.math.add(u64, self.encoded_rows, length);
            if (self.encoded_rows > self.reference.encoded_bytes) return error.InvalidLogicalOutbox;
            self.after = change.ordinal;
        }
    }
    pub fn finish(self: CaptureStream) !void {
        if (self.failed) return error.InvalidLogicalOutbox;
        if (self.after != self.reference.change_count) return error.LogicalCaptureReceiptMissing;
        var actual: [32]u8 = undefined;
        var hash = self.hash;
        hash.final(&actual);
        if (!std.mem.eql(u8, &actual, &self.reference.changes_digest)) return error.InvalidLogicalOutbox;
    }
};

test "logical publisher large pinned rows prepare and stream without payload sized scratch" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const text = try a.alloc(u8, 8 * 1024 * 1024);
    @memset(text, 'x');
    const columns = [_]schema.RelationalColumn{.{ .name = "body", .path = "body", .column_type = .string }};
    const table: schema.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &columns };
    const raw = try rows.serializeOrdinal(a, table.version, &columns, &.{.{ .ordinal = 0, .path = "body", .value_type = .bytes_val, .value = .{ .bytes_val = text } }}, @splat(0));
    const encoded_schema = try schema.serializeSchema(a, table);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/logical-outbox-schema/v1\x00");
    hash.update(encoded_schema);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    const captured = [_]outbox.Schema{.{ .digest = digest, .encoded = encoded_schema }};
    const relation: wire.RelationDescription = .{ .logical_relation_id = 100, .physical_table_id = 200, .database_id = 1, .namespace_id = 2, .relation_oid = 100, .namespace = "public", .name = "wide", .schema_version = 1, .schema_digest = try rows.PhysicalLayout.schemaDigest(a, table), .replica_identity = .full, .columns = &.{.{ .attnum = 1, .name = "body", .type_oid = 25 }} };
    const encoder = try Encoder.initLeaky(a, relation, &captured);
    const change: outbox.Change = .{ .ordinal = 1, .key = "row", .before = null, .after = .{ .schema_digest = digest, .row = raw } };
    var buffer: [16 * 1024]u8 = undefined;
    var scratch = std.heap.FixedBufferAllocator.init(&buffer);
    const prepared_change = try encoder.prepare(scratch.allocator(), change);
    try std.testing.expectEqual(text.len + 13, prepared_change.encoded_bytes);
    const cell = prepared_change.after.?.cells[0].value.?.value.string;
    try std.testing.expect(@intFromPtr(cell.ptr) >= @intFromPtr(raw.ptr) and @intFromPtr(cell.ptr) + cell.len <= @intFromPtr(raw.ptr) + raw.len);
    var output = std.Io.Writer.Discarding.init(&.{});
    // Once prepared, this scalar message needs no allocator or row-size buffer.
    try prepared_change.write(std.testing.failing_allocator, &output.writer);
    try std.testing.expectEqual(prepared_change.encoded_bytes, output.fullCount());
    var failure = std.Io.Writer.fixed(&.{});
    try std.testing.expectError(error.WriteFailed, prepared_change.write(std.testing.failing_allocator, &failure));
    std.debug.print("logical publisher pinned row payload_bytes={d} preparation_bytes={d} wire_bytes={d}\n", .{ text.len, scratch.end_index, prepared_change.encoded_bytes });
}

test "logical publisher retained AROW encodes exact numeric JSON null SQL null and key before images" {
    const a0 = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a0);
    defer arena.deinit();
    const a = arena.allocator();
    const columns = [_]schema.RelationalColumn{
        .{ .name = "id", .path = "id", .column_type = .integer },
        .{ .name = "literal", .path = "literal", .column_type = .json, .is_json = true, .json_kind = .any },
        .{ .name = "sql_null", .path = "sql_null", .column_type = .json, .is_json = true, .json_kind = .any, .allows_null = true },
    };
    const table: schema.TableSchema = .{ .version = 2, .storage_mode = .relational, .relational_columns = &columns };
    const cells = [_]rows.Cell{
        .{ .ordinal = 0, .path = "id", .value_type = .i64_val, .value = .{ .i64_val = 9007199254740993 } },
        .{ .ordinal = 1, .path = "literal", .value_type = .bytes_val, .is_json = true, .value = .{ .bytes_val = "null" } },
        .{ .ordinal = 2, .path = "sql_null", .value_type = .bytes_val, .is_json = true, .is_null = true, .value = .{ .bytes_val = "" } },
    };
    const raw = try rows.serializeOrdinal(a, table.version, &columns, &cells, @splat(0));
    const encoded_schema = try schema.serializeSchema(a, table);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/logical-outbox-schema/v1\x00");
    hash.update(encoded_schema);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    const captured = [_]outbox.Schema{.{ .digest = digest, .encoded = encoded_schema }};
    const description: wire.RelationDescription = .{ .logical_relation_id = 100, .physical_table_id = 200, .database_id = 1, .namespace_id = 2, .relation_oid = 100, .namespace = "public", .name = "published", .schema_version = 2, .schema_digest = try rows.PhysicalLayout.schemaDigest(a, table), .replica_identity = .default_key, .columns = &.{ .{ .attnum = 1, .name = "id", .type_oid = 20, .key = true }, .{ .attnum = 2, .name = "literal", .type_oid = 3802 }, .{ .attnum = 3, .name = "sql_null", .type_oid = 3802 } } };
    const changed: outbox.Change = .{ .ordinal = 1, .key = &.{ 0xff, 0 }, .before = null, .after = .{ .schema_digest = digest, .row = raw } };
    const encoded = try Encoder.initLeaky(a, description, &captured);
    const pg = @import("logical_replication_pgoutput.zig");
    var actual = std.Io.Writer.Allocating.init(a);
    const prepared_insert = try encoded.prepare(a, changed);
    try prepared_insert.write(a, &actual.writer);
    try std.testing.expectEqual(actual.written().len, prepared_insert.encoded_bytes);
    var expected = std.Io.Writer.Allocating.init(a);
    try pg.change(&expected.writer, .{ .insert = .{ .relation_oid = 100, .values = &.{ .{ .text = "9007199254740993" }, .{ .text = "null" }, .null_value } } });
    try std.testing.expectEqualSlices(u8, expected.written(), actual.written());
    var updated = changed;
    updated.before = changed.after;
    var actual_update = std.Io.Writer.Allocating.init(a);
    const prepared_update = try encoded.prepare(a, updated);
    try prepared_update.write(a, &actual_update.writer);
    try std.testing.expectEqual(actual_update.written().len, prepared_update.encoded_bytes);
    var expected_update = std.Io.Writer.Allocating.init(a);
    try pg.change(&expected_update.writer, .{ .update = .{ .relation_oid = 100, .before = .{ .key = &.{ .{ .text = "9007199254740993" }, .null_value, .null_value } }, .after = &.{ .{ .text = "9007199254740993" }, .{ .text = "null" }, .null_value } } });
    try std.testing.expectEqualSlices(u8, expected_update.written(), actual_update.written());
    // A wire projection keeps original ordinals and physical schema identity;
    // generated-only tables may legitimately publish a zero-column tuple.
    var generated_columns = try a.dupe(wire.Column, description.columns);
    generated_columns[1].generated = true;
    var generated_description = description;
    generated_description.columns = generated_columns;
    const generated_projection = [_]wire.Column{ generated_columns[0], generated_columns[2] };
    const projected = try Encoder.initProjectionLeaky(a, generated_description, &captured, &generated_projection);
    var projected_output = std.Io.Writer.Allocating.init(a);
    try projected.write(a, &projected_output.writer, changed);
    var projected_expected = std.Io.Writer.Allocating.init(a);
    try pg.change(&projected_expected.writer, .{ .insert = .{ .relation_oid = 100, .values = &.{ .{ .text = "9007199254740993" }, .null_value } } });
    try std.testing.expectEqualSlices(u8, projected_expected.written(), projected_output.written());
    const zero_projection = try Encoder.initProjectionLeaky(a, generated_description, &captured, &.{});
    var empty_output = std.Io.Writer.Allocating.init(a);
    try zero_projection.write(a, &empty_output.writer, changed);
    try std.testing.expectEqualStrings("I\x00\x00\x00dN\x00\x00", empty_output.written());
    var zero_relation = std.Io.Writer.Allocating.init(a);
    try pg.relationProjection(&zero_relation.writer, generated_description, &.{});
    const relation_bytes = zero_relation.written();
    try std.testing.expectEqual(@as(u16, 0), std.mem.readInt(u16, relation_bytes[relation_bytes.len - 2 ..][0..2], .big));
    var substituted_projection = generated_projection;
    substituted_projection[0].type_oid = 23;
    try std.testing.expectError(error.InvalidLogicalPublisher, Encoder.initProjectionLeaky(a, generated_description, &captured, &substituted_projection));
    try std.testing.expectError(error.InvalidLogicalPublisher, Encoder.initProjectionLeaky(a, generated_description, &captured, &.{ generated_columns[2], generated_columns[0] }));
    var wrong = description;
    wrong.columns = &.{.{ .attnum = 1, .name = "id", .type_oid = 23, .key = true }};
    try std.testing.expectError(error.RelationalIndexColumnTypeMismatch, Encoder.initLeaky(a, wrong, &captured));
    const Failures = struct {
        fn run(allocator: A, relation: wire.RelationDescription, input: []const outbox.Schema, change: outbox.Change) !void {
            var temporary = std.heap.ArenaAllocator.init(allocator);
            defer temporary.deinit();
            const scratch = temporary.allocator();
            const encoder = try Encoder.initLeaky(scratch, relation, input);
            var output = std.Io.Writer.Allocating.init(scratch);
            try encoder.write(scratch, &output.writer, change);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a0, Failures.run, .{ description, &captured, updated });
    // Genuine captured-byte stream framing. No metadata/physical ACK is
    // fabricated by this local codec fixture.
    updated.ordinal = 2;
    var changes_hash = std.crypto.hash.Blake3.init(.{});
    changes_hash.update("antfly/logical-outbox-changes/v1\x00");
    for ([_]outbox.Change{ changed, updated }) |item| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, try item.encodedLen(outbox.max_page_wire_bytes), .little);
        changes_hash.update(&length);
        try item.hashEncoded(&changes_hash, outbox.max_page_wire_bytes);
    }
    var changes_digest: [32]u8 = undefined;
    changes_hash.final(&changes_digest);
    const roster: wire.PublishedRelation = .{ .relation = description, .memberships = &.{.{ .publication = .{ .id = 40, .generation = 1, .digest = @splat(5) }, .parameters = .{} }} };
    const reference: wire.CaptureRef = .{ .owner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 }, .transaction_id = @splat(4), .participant = 0, .group_id = 9, .namespace = .{ .table_id = 200, .shard_id = 1, .range_id = 2 }, .physical_root_incarnation = 10, .position = .{ .raft = .{ .term = 1, .index = 2 } }, .roster_digest = try local.system_catalog_logical_replication_capture.canonicalDigest(a, "antfly/logical-capture-roster/v1\x00", roster), .guard_digest = @splat(6), .pregrant_body_digest = @splat(7), .changes_digest = changes_digest, .capacity_digest = @splat(10), .before_digest = @splat(11), .change_count = 2, .encoded_bytes = 20000 };
    const first_page: outbox.Page = .{ .header = .{ .reference = reference, .intent_revision = 1, .demand_digest = @splat(8), .rows_digest = changes_digest, .roster_bytes = 2048, .schema_count = 1, .schemas_digest = @splat(9) }, .roster = roster, .changes = &.{changed}, .schemas = &captured, .next_after = 1 };
    var second_page = first_page;
    second_page.changes = &.{updated};
    second_page.next_after = null;
    const request: outbox.PageRequest = .{ .reference = reference, .max_bytes = 32768 };
    var next_request = request;
    next_request.after = 1;
    var stream = try CaptureStream.init(reference);
    try stream.accept(a, request, first_page);
    try std.testing.expectError(error.LogicalCaptureReceiptMissing, stream.finish());
    try stream.accept(a, next_request, second_page);
    try stream.finish();
    var reordered = try CaptureStream.init(reference);
    try std.testing.expectError(error.InvalidLogicalOutbox, reordered.accept(a, request, second_page));
    try std.testing.expectError(error.InvalidLogicalOutbox, reordered.finish());
    var corrupted = try CaptureStream.init(reference);
    try corrupted.accept(a, request, first_page);
    var wrong_change = updated;
    wrong_change.key = "a different final mutation";
    var bad_page = second_page;
    bad_page.changes = &.{wrong_change};
    try corrupted.accept(a, next_request, bad_page);
    try std.testing.expectError(error.InvalidLogicalOutbox, corrupted.finish());
    // Drive the real binary page decoder, participant verifier and framed
    // transaction sender together. The local host supplies captured test
    // bytes only; it grants no native slot or physical owner authority.
    const delivery = @import("logical_replication_delivery.zig");
    const resources = local.storage_resource_manager;
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 * 1024 * 1024 } });
    defer manager.deinit(a0);
    const Transport = struct {
        manager: *resources.ResourceManager,
        backing: A = std.testing.allocator,
        first: outbox.Page,
        second: outbox.Page,
        calls: usize = 0,
        largest_page: usize = 0,
        response_reservation: u64 = 0,
        corrupt_last: bool = false,
        denied: bool = false,
        cancel_after_read: ?*std.atomic.Value(bool) = null,
        pub fn acquireLogicalPublicationMemory(host: *@This(), control: A) !local.sql_runtime.MemoryOwner {
            return @import("../api/sql/execution/memory_owner.zig").create(control, host.backing, host.manager);
        }
        pub fn routedPublicationOutboxPage(host: *@This(), allocator: A, input: @import("../api/logical_replication_capture_worker.zig").OutboxPageRequest, context: local.api_operation.RequestContext) ![]u8 {
            try context.ensureActive();
            const budget = resources.BudgetedAllocator.fromAllocator(allocator) orelse return error.MissingDeliveryMemoryAdmission;
            // Every previous Data page and its typed row scratch have been
            // released before the next native read, not at transaction end.
            try std.testing.expectEqual(@as(u64, 0), budget.live_bytes);
            if (host.denied) return error.Forbidden;
            try std.testing.expectEqualStrings("original-physical-name", input.access.table_name);
            const requested = input.page();
            host.calls += 1;
            if (host.cancel_after_read) |flag| flag.store(true, .release);
            var page = if (requested.after == 0) host.first else host.second;
            page.header.reference = requested.reference;
            if (host.corrupt_last and requested.reference.participant == 1 and requested.after == 1) {
                const changed_rows = try allocator.dupe(outbox.Change, page.changes);
                changed_rows[0].key = "substituted-last-participant-row";
                page.changes = changed_rows;
            }
            const encoded_page = try outbox.encodePage(page, allocator, requested.max_bytes);
            const used = host.manager.sliceStats(.sql_execution_working_set).used_bytes;
            try std.testing.expect(used >= encoded_page.len);
            host.largest_page = @max(host.largest_page, encoded_page.len);
            host.response_reservation = @max(host.response_reservation, used);
            return encoded_page;
        }
    };
    const realm: wire.Realm = .{ .group_id = 1, .incarnation = @splat('1'), .database_id = 1 };
    const lease: wire.Lease = .{ .slot = .{ .database_id = 1, .name = "subscriber", .birth = 1 }, .generation = 1, .connection = @splat(1), .nonce = @splat(2) };
    var sibling = reference;
    sibling.participant = 1;
    sibling.group_id = 10;
    sibling.namespace.shard_id = 3;
    sibling.namespace.range_id = 4;
    const entry: wire.JournalEntry = .{ .xid = 3, .begin_lsn = 1, .end_lsn = 20, .transaction = .{
        .owner = reference.owner,
        .transaction_id = reference.transaction_id,
        .realm = realm,
        .commit_version = 5,
        .commit_time_unix_us = 1_700_000_000_000_000,
        .captures = &.{ reference, sibling },
    }, .routes = &.{ .{ .participant = 0, .table_name = "original-physical-name" }, .{ .participant = 1, .table_name = "original-physical-name" } } };
    const offered: wire.FeedPage = .{ .publications = &.{roster.memberships[0].publication}, .realm = realm, .lease = lease, .head_lsn = 30, .offered_lsn = 20, .entries = &.{entry} };
    var host: Transport = .{ .manager = &manager, .first = first_page, .second = second_page };
    var sender = try delivery.Sender.init(realm, lease, 1);
    var framed = std.Io.Writer.Allocating.init(a);
    try sender.deliver(a, &host, &framed.writer, offered, .any, 1_700_000_000_000_000, .{});
    try std.testing.expectEqual(@as(usize, 4), host.calls);
    try std.testing.expectEqual(@as(u64, 20), sender.delivered);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.sql_execution_working_set).used_bytes);
    {
        // The real binary decoder/AROW encoder handles a capture far larger
        // than the connection control quota, while shared host admission owns
        // the response and preparation allocations. This is a byte fixture,
        // not a physical vote or native metadata lease.
        const large_json = try a.alloc(u8, 8 * 1024 * 1024 + 2);
        @memset(large_json, 'x');
        large_json[0] = '"';
        large_json[large_json.len - 1] = '"';
        var large_cells = cells;
        large_cells[1].value = .{ .bytes_val = large_json };
        const large_row = try rows.serializeOrdinal(a, table.version, &columns, &large_cells, @splat(0));
        var large_change = changed;
        large_change.after.?.row = large_row;
        const row_bytes = try large_change.encodedLen(outbox.max_page_wire_bytes);
        var large_hash = std.crypto.hash.Blake3.init(.{});
        large_hash.update("antfly/logical-outbox-changes/v1\x00");
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, row_bytes, .little);
        large_hash.update(&length);
        try large_change.hashEncoded(&large_hash, outbox.max_page_wire_bytes);
        var large_reference = reference;
        large_reference.change_count = 1;
        large_reference.encoded_bytes = row_bytes + encoded_schema.len + 2048;
        large_hash.final(&large_reference.changes_digest);
        var large_page = first_page;
        large_page.header.reference = large_reference;
        large_page.header.rows_digest = large_reference.changes_digest;
        large_page.changes = &.{large_change};
        large_page.next_after = null;
        var large_entry = entry;
        large_entry.transaction.captures = &.{large_reference};
        large_entry.routes = entry.routes[0..1];
        var large_offer = offered;
        large_offer.entries = &.{large_entry};
        var controls: @import("budget.zig").Budget = .{ .child = a0, .limit = 64 * 1024 };
        var large_host: Transport = .{ .manager = &manager, .first = large_page, .second = large_page };
        var large_sender = try delivery.Sender.init(realm, lease, 1);
        var output = std.Io.Writer.Discarding.init(&.{});
        try large_sender.deliver(controls.allocator(), &large_host, &output.writer, large_offer, .any, 1_700_000_000_000_000, .{});
        try std.testing.expectEqual(@as(u64, 20), large_sender.delivered);
        try std.testing.expect(large_host.largest_page > 8 * 1024 * 1024);
        try std.testing.expect(output.fullCount() > large_json.len);
        try std.testing.expectEqual(@as(usize, 0), controls.used);
        try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.sql_execution_working_set).used_bytes);
        std.debug.print("logical publisher shared admission frame_bytes={d} response_reserved_bytes={d} control_limit_bytes={d} remaining_reserved_bytes={d}\n", .{ large_host.largest_page, large_host.response_reservation, controls.limit, manager.sliceStats(.sql_execution_working_set).used_bytes });
    }
    var tags: std.ArrayList(u8) = .empty;
    var frame_bytes = framed.written();
    while (frame_bytes.len != 0) {
        try std.testing.expectEqual(@as(u8, 'd'), frame_bytes[0]);
        const length = std.mem.readInt(u32, frame_bytes[1..5], .big);
        try std.testing.expect(length >= 30 and length + 1 <= frame_bytes.len);
        try std.testing.expectEqual(@as(u8, 'w'), frame_bytes[5]);
        try std.testing.expectEqual(@as(u64, 30), std.mem.readInt(u64, frame_bytes[14..22], .big));
        try tags.append(a, frame_bytes[30]);
        frame_bytes = frame_bytes[1 + length ..];
    }
    try std.testing.expectEqualStrings("BRIURIUC", tags.items);
    {
        // Another real shared reservation leaves no processing headroom.
        // This denies the Data response allocation, not the immutable row's
        // validity. No wire bytes/COMMIT/ACK or delivered progress may escape.
        var competing = try manager.reserve(.sql_execution_working_set, manager.availableAdmissionBytes(.sql_execution_working_set));
        defer competing.release();
        host.calls = 0;
        sender = try delivery.Sender.init(realm, lease, 1);
        var blocked = std.Io.Writer.fixed(&.{});
        try std.testing.expectError(error.SqlWorkingMemoryLimitExceeded, sender.deliver(a, &host, &blocked, offered, .any, 1_700_000_000_000_000, .{}));
        try std.testing.expect(sender.failed);
        try std.testing.expectEqual(@as(u64, 1), sender.delivered);
        try std.testing.expectEqual(@as(usize, 0), blocked.buffered().len);
        try std.testing.expectEqual(competing.bytes, manager.sliceStats(.sql_execution_working_set).used_bytes);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.sql_execution_working_set).used_bytes);
    // A new stream with independently available credit can consume the same
    // immutable offer. The failed stream remains poisoned and is never retried.
    sender = try delivery.Sender.init(realm, lease, 1);
    var resumed = std.Io.Writer.Discarding.init(&.{});
    try sender.deliver(a, &host, &resumed.writer, offered, .any, 1_700_000_000_000_000, .{});
    try std.testing.expectEqual(@as(u64, 20), sender.delivered);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.sql_execution_working_set).used_bytes);
    // A late, correctly checksummed transport page with altered captured bytes
    // cannot produce COMMIT or advance the connection's delivered boundary.
    host = .{ .manager = &manager, .first = first_page, .second = second_page, .corrupt_last = true };
    sender = try delivery.Sender.init(realm, lease, 1);
    var incomplete = std.Io.Writer.Allocating.init(a);
    try std.testing.expectError(error.InvalidLogicalOutbox, sender.deliver(a, &host, &incomplete.writer, offered, .any, 1_700_000_000_000_000, .{}));
    try std.testing.expectEqual(@as(u64, 1), sender.delivered);
    try std.testing.expect(sender.failed);
    try std.testing.expectError(error.LogicalReplicationDeliveryFailed, sender.deliver(a, &host, &incomplete.writer, offered, .any, 1_700_000_000_000_000, .{}));
    frame_bytes = incomplete.written();
    while (frame_bytes.len != 0) {
        const length = std.mem.readInt(u32, frame_bytes[1..5], .big);
        try std.testing.expect(frame_bytes[30] != 'C');
        frame_bytes = frame_bytes[1 + length ..];
    }
    // An absent publication emits no rows but must verify every captured page.
    var filtered = offered;
    filtered.publications = &.{.{ .id = 41, .generation = 1, .digest = @splat(1) }};
    host = .{ .manager = &manager, .first = first_page, .second = second_page };
    sender = try delivery.Sender.init(realm, lease, 1);
    var hidden = std.Io.Writer.Allocating.init(a);
    try sender.deliver(a, &host, &hidden.writer, filtered, .any, 1_700_000_000_000_000, .{});
    try std.testing.expectEqual(@as(usize, 0), hidden.written().len);
    try std.testing.expectEqual(@as(usize, 4), host.calls);
    try std.testing.expectEqual(@as(u64, 20), sender.delivered);
    host = .{ .manager = &manager, .first = first_page, .second = second_page, .corrupt_last = true };
    sender = try delivery.Sender.init(realm, lease, 1);
    try std.testing.expectError(error.InvalidLogicalOutbox, sender.deliver(a, &host, &hidden.writer, filtered, .any, 1_700_000_000_000_000, .{}));
    host = .{ .manager = &manager, .first = first_page, .second = second_page, .denied = true };
    sender = try delivery.Sender.init(realm, lease, 1);
    try std.testing.expectError(error.Forbidden, sender.deliver(a, &host, &hidden.writer, offered, .any, 1_700_000_000_000_000, .{}));
    try std.testing.expectEqual(@as(usize, 0), hidden.written().len);
    var tiny_bytes: [8]u8 = undefined;
    var tiny = std.Io.Writer.fixed(&tiny_bytes);
    sender = try delivery.Sender.init(realm, lease, 1);
    host = .{ .manager = &manager, .first = first_page, .second = second_page };
    try std.testing.expectError(error.WriteFailed, sender.deliver(a, &host, &tiny, offered, .any, 1_700_000_000_000_000, .{}));
    try std.testing.expectEqual(@as(u64, 1), sender.delivered);
    // Envelope corruption is rejected before any transaction bytes or Data
    // reads. A native offer alone is not authority to switch this connection.
    var mismatched = offered;
    mismatched.lease.nonce[0] ^= 1;
    sender = try delivery.Sender.init(realm, lease, 1);
    hidden.clearRetainingCapacity();
    host = .{ .manager = &manager, .first = first_page, .second = second_page };
    try std.testing.expectError(error.InvalidLogicalPublisher, sender.deliver(a, &host, &hidden.writer, mismatched, .any, 1_700_000_000_000_000, .{}));
    try std.testing.expectEqual(@as(usize, 0), host.calls);
    try std.testing.expectEqual(@as(usize, 0), hidden.written().len);
    mismatched = offered;
    mismatched.publications = &.{ roster.memberships[0].publication, roster.memberships[0].publication };
    sender = try delivery.Sender.init(realm, lease, 1);
    try std.testing.expectError(error.InvalidLogicalPublisher, sender.deliver(a, &host, &hidden.writer, mismatched, .any, 1_700_000_000_000_000, .{}));
    var relayed_entry = entry;
    relayed_entry.transaction.origin = .{ .name = "original-provider", .position = 11 };
    mismatched = offered;
    mismatched.entries = &.{relayed_entry};
    sender = try delivery.Sender.init(realm, lease, 1);
    try sender.deliver(a, &host, &hidden.writer, mismatched, .none, 1_700_000_000_000_000, .{});
    try std.testing.expectEqual(@as(usize, 0), host.calls);
    try std.testing.expectEqual(@as(usize, 0), hidden.written().len);
    try std.testing.expectEqual(@as(u64, 20), sender.delivered);
    const DeliveryFailures = struct {
        fn run(allocator: A, first: outbox.Page, second: outbox.Page, offered_page: wire.FeedPage) !void {
            var owned = std.heap.ArenaAllocator.init(allocator);
            defer owned.deinit();
            var native_sender = try delivery.Sender.init(offered_page.realm, offered_page.lease, 1);
            var output = std.Io.Writer.Discarding.init(&.{});
            var inner_manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 64 * 1024 * 1024 } });
            defer inner_manager.deinit(allocator);
            var transport: Transport = .{ .manager = &inner_manager, .backing = allocator, .first = first, .second = second };
            native_sender.deliver(owned.allocator(), &transport, &output.writer, offered_page, .any, 1_700_000_000_000_000, .{}) catch |err| {
                try std.testing.expect(native_sender.failed);
                try std.testing.expectEqual(@as(u64, 1), native_sender.delivered);
                return err;
            };
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a0, DeliveryFailures.run, .{ first_page, second_page, offered });
    var canceled: std.atomic.Value(bool) = .init(false);
    sender = try delivery.Sender.init(realm, lease, 1);
    host = .{ .manager = &manager, .first = first_page, .second = second_page, .cancel_after_read = &canceled };
    hidden.clearRetainingCapacity();
    try std.testing.expectError(error.Canceled, sender.deliver(a, &host, &hidden.writer, offered, .any, 1_700_000_000_000_000, .{ .cancellation = local.api_operation.CancellationToken.fromAtomic(&canceled) }));
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.sql_execution_working_set).used_bytes);
    try std.testing.expectEqual(@as(usize, 0), hidden.written().len);
    try std.testing.expectEqual(@as(u64, 1), sender.delivered);
    try std.testing.expect(sender.failed);
}
