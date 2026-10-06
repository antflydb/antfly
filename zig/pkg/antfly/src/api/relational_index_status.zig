// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2
//! Authoritative all-owner readiness. The first ready shard is never a table
//! coverage proof. Every collection revalidates the exact metadata incarnation.
const std = @import("std");
const wire = @import("antfly_indexes_openapi").types;
const metadata = @import("../metadata/table_manager.zig");
const tables = @import("tables.zig");
const reads = @import("table_read_source.zig");
const operation = @import("operation.zig");
const native = @import("../storage/db/relational_index_status_contract.zig");

/// Initial FK publication accepts a parent-owner stage (or self-parent child
/// release) only after the exact schema-bound witness index is ready in that
/// owner's read-index snapshot. A stale or failed index is a retryable wait,
/// never a partially enforced public FK.
pub fn requireInitialFkSupportReady(status: native.Status, table_id: u64, schema_version: u32, start: []const u8, end: []const u8, comparison: [32]u8) !void {
    if (status.table_id != table_id or status.schema_version != schema_version or
        status.generation == 0 or status.state != .ready or status.failure != .none or
        !std.mem.eql(u8, &status.comparison, &comparison) or
        !std.mem.eql(u8, status.range_start, start) or
        !std.mem.eql(u8, status.range_end, end))
        return error.GenerationAdmissionPending;
}

test "relational index status initial FK owner readiness rejects building and stale witnesses before publication" {
    const comparison: [32]u8 = @splat(7);
    var status: native.Status = .{
        .table_id = 51,
        .schema_version = 3,
        .generation = 9,
        .slot = 0,
        .catalog = @splat(2),
        .comparison = comparison,
        .owner = @splat(4),
        .range_start = "a",
        .range_end = "z",
        .state = .building,
        .rows_scanned = 1,
        .failure = .none,
        .progress_digest = @splat(5),
        .maintenance_epoch = 0,
        .last_maintenance_request = @splat(0),
    };
    try std.testing.expectError(error.GenerationAdmissionPending, requireInitialFkSupportReady(status, 51, 3, "a", "z", comparison));
    status.state = .ready;
    try requireInitialFkSupportReady(status, 51, 3, "a", "z", comparison);
    status.generation = 0;
    try std.testing.expectError(error.GenerationAdmissionPending, requireInitialFkSupportReady(status, 51, 3, "a", "z", comparison));
    status.generation = 9;
    status.comparison = @splat(8);
    try std.testing.expectError(error.GenerationAdmissionPending, requireInitialFkSupportReady(status, 51, 3, "a", "z", comparison));
    status.comparison = comparison;
    status.failure = .invalid_row;
    try std.testing.expectError(error.GenerationAdmissionPending, requireInitialFkSupportReady(status, 51, 3, "a", "z", comparison));
    status.failure = .none;
    try std.testing.expectError(error.GenerationAdmissionPending, requireInitialFkSupportReady(status, 51, 4, "a", "z", comparison));
    try std.testing.expectError(error.GenerationAdmissionPending, requireInitialFkSupportReady(status, 51, 3, "a", "zz", comparison));
}

pub fn expectedComparison(alloc: std.mem.Allocator, parsed: @import("../schema/mod.zig").ParsedTableSchema, name: []const u8) ![32]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const temporary = arena.allocator();
    const declarations = (try parsed.relationalIndexDefinitions(temporary)) orelse return error.IndexNotFound;
    const definition = for (declarations) |definition| {
        if (std.mem.eql(u8, definition.name, name)) break definition;
    } else return error.IndexNotFound;
    const runtime_schema = try @import("../schema/mod.zig").deriveRelationalCheckLayout(temporary, parsed);
    var layout = try @import("../storage/db/algebraic/relational_row_codec.zig").PhysicalLayout.init(temporary, runtime_schema);
    defer layout.deinit();
    var tuple = try @import("../storage/db/relational_index_keys.zig").TuplePlan.init(temporary, runtime_schema, &layout, definition.keys);
    defer tuple.deinit();
    try bindCoverFingerprint(temporary, runtime_schema, &layout, definition, &tuple.fingerprint);
    return tuple.fingerprint;
}

fn bindCoverFingerprint(alloc: std.mem.Allocator, runtime_schema: @import("../storage/schema.zig").TableSchema, layout: *const @import("../storage/db/algebraic/relational_row_codec.zig").PhysicalLayout, definition: @import("../storage/relational_index.zig").RelationalIndexDefinition, fingerprint: *[32]u8) !void {
    if (definition.include_columns.len != 0) {
        var cover = try @import("../storage/db/relational_index_cover.zig").Plan.init(alloc, runtime_schema, layout, definition.keys, definition.include_columns);
        defer cover.deinit();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update(fingerprint);
        hash.update(&cover.fingerprint);
        hash.final(fingerprint);
    }
    if (definition.where.len != 0) {
        var condition = try @import("../storage/db/relational_index_predicate.zig").Plan.init(alloc, runtime_schema, layout, definition.where);
        defer condition.deinit();
        condition.bindFingerprint(fingerprint);
    }
}

pub fn collect(alloc: std.mem.Allocator, source: anytype, reader: reads.TableReadSource, name: []const u8, index_name: []const u8, expected_schema: []const u8, request: operation.RequestContext) ![]u8 {
    var result = try collectSelected(alloc, source, reader, name, index_name, expected_schema, request, null);
    defer result.deinit();
    return std.json.Stringify.valueAlloc(alloc, result.statuses[0], .{});
}

pub const Collection = struct {
    arena: std.heap.ArenaAllocator,
    statuses: []wire.RelationalIndexStatus,
    pub fn deinit(self: *Collection) void {
        self.arena.deinit();
    }
};

pub fn collectAll(alloc: std.mem.Allocator, source: anytype, reader: reads.TableReadSource, name: []const u8, expected_schema: []const u8, request: operation.RequestContext) !Collection {
    return collectSelected(alloc, source, reader, name, null, expected_schema, request, null);
}

/// Shared /indexes already parsed the exact expected schema for its configs.
/// Reuse that immutable request-owned value instead of parsing it a second time.
pub fn collectAllWithParsedSchema(alloc: std.mem.Allocator, source: anytype, reader: reads.TableReadSource, name: []const u8, expected_schema: []const u8, parsed: @import("../schema/mod.zig").ParsedTableSchema, request: operation.RequestContext) !Collection {
    return collectSelected(alloc, source, reader, name, null, expected_schema, request, parsed);
}

fn collectSelected(alloc: std.mem.Allocator, source: anytype, reader: reads.TableReadSource, name: []const u8, index_name: ?[]const u8, expected_schema: []const u8, request: operation.RequestContext, borrowed_schema: ?@import("../schema/mod.zig").ParsedTableSchema) !Collection {
    try request.ensureActive();
    var snapshot = (try source.adminSnapshot()) orelse return error.Unavailable;
    defer source.freeAdminSnapshot(&snapshot);
    const table = tables.findTableByName(&snapshot, name) orelse return error.TableNotFound;
    if (!std.mem.eql(u8, table.schema_json, expected_schema)) return error.PreparedGenerationChanged;
    var owned_schema: ?@import("../schema/mod.zig").ParsedTableSchema = null;
    defer if (owned_schema) |*value| value.deinit(alloc);
    const parsed = borrowed_schema orelse blk: {
        owned_schema = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, table.schema_json);
        break :blk owned_schema.?;
    };
    if (parsed.storage_mode != .relational) return error.RelationalTableRequired;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const temporary = arena.allocator();
    const definitions = (try parsed.relationalIndexDefinitions(temporary)) orelse return error.IndexNotFound;
    const runtime_schema = try @import("../schema/mod.zig").deriveRelationalCheckLayout(temporary, parsed);
    var layout = try @import("../storage/db/algebraic/relational_row_codec.zig").PhysicalLayout.init(temporary, runtime_schema);
    defer layout.deinit();
    var names: std.StringHashMapUnmanaged(usize) = .empty;
    var comparisons: std.ArrayList([32]u8) = .empty;
    for (definitions) |definition| {
        if (index_name) |selected| if (!std.mem.eql(u8, selected, definition.name)) continue;
        try request.ensureActive();
        var tuple = try @import("../storage/db/relational_index_keys.zig").TuplePlan.init(temporary, runtime_schema, &layout, definition.keys);
        defer tuple.deinit();
        try bindCoverFingerprint(temporary, runtime_schema, &layout, definition, &tuple.fingerprint);
        try names.put(temporary, definition.name, comparisons.items.len);
        try comparisons.append(temporary, tuple.fingerprint);
    }
    if (index_name != null and names.count() == 0) return error.IndexNotFound;
    var owners: std.ArrayList(*const metadata.RangeRecord) = .empty;
    var owner_by_start: std.StringHashMapUnmanaged(*const metadata.RangeRecord) = .empty;
    for (snapshot.ranges) |*owner| if (owner.table_id == table.table_id) {
        if (owners.items.len == 4096) return error.ResourceBudgetExceeded;
        const entry = try owner_by_start.getOrPut(temporary, owner.start_key);
        if (entry.found_existing) return error.TopologyChanged;
        entry.value_ptr.* = owner;
        try owners.append(temporary, owner);
    };
    if (owners.items.len == 0) return error.Unavailable;
    std.mem.sort(*const metadata.RangeRecord, owners.items, {}, struct {
        fn less(_: void, a: *const metadata.RangeRecord, b: *const metadata.RangeRecord) bool {
            return std.mem.lessThan(u8, a.start_key, b.start_key);
        }
    }.less);
    if (names.count() > native.max_status_cells / owners.items.len) return error.ResourceBudgetExceeded;
    const statuses = try temporary.alloc(wire.RelationalIndexStatus, names.count());
    var name_iter = names.iterator();
    while (name_iter.next()) |entry| statuses[entry.value_ptr.*] = .{
        .table_id = try std.fmt.allocPrint(temporary, "{d}", .{table.table_id}),
        .schema_version = parsed.version,
        .index_name = try temporary.dupe(u8, entry.key_ptr.*),
        .state = .ready,
        .ranges = try temporary.alloc(wire.RelationalIndexRangeStatus, owners.items.len),
    };
    const request_json = try std.json.Stringify.valueAlloc(temporary, native.Request{ .name = index_name, .schema_version = parsed.version }, .{});
    var expected_start: []const u8 = "";
    for (owners.items, 0..) |owner, i| {
        try request.ensureActive();
        if (owner.restore_backup_id.len != 0 or !std.mem.eql(u8, owner.start_key, expected_start) or ((i + 1 == owners.items.len) != (owner.end_key == null))) return error.TopologyChanged;
        var response = (try reader.lookup(alloc, name, owner.start_key, .{
            .relational_index_status_json = request_json,
            .execution_deadline_ns = request.deadline_ns,
            .execution_io = request.deadline_io,
            .cancellation = request.cancellation,
        }, .read_index)) orelse return error.Unavailable;
        defer response.deinit(alloc);
        try request.ensureActive();
        if (response.json.len > native.max_response_bytes) return error.ResourceBudgetExceeded;
        var response_arena = std.heap.ArenaAllocator.init(alloc);
        defer response_arena.deinit();
        const response_alloc = response_arena.allocator();
        const values = if (index_name) |selected| blk: {
            const value = try std.json.parseFromSliceLeaky(native.Status, response_alloc, response.json, .{});
            const values = try response_alloc.alloc(native.NamedStatus, 1);
            values[0] = .{ .name = selected, .status = value };
            break :blk values;
        } else (try std.json.parseFromSliceLeaky(native.Batch, response_alloc, response.json, .{})).statuses;
        if (values.len != statuses.len) return error.PreparedGenerationChanged;
        const seen = try response_alloc.alloc(bool, statuses.len);
        @memset(seen, false);
        var catalog: ?[32]u8 = null;
        var owner_digest: ?[32]u8 = null;
        for (values) |named| {
            try request.ensureActive();
            const position = names.get(named.name) orelse return error.PreparedGenerationChanged;
            if (seen[position]) return error.PreparedGenerationChanged;
            seen[position] = true;
            const value = named.status;
            if (catalog) |prior| {
                if (!std.mem.eql(u8, &prior, &value.catalog)) return error.PreparedGenerationChanged;
            } else catalog = value.catalog;
            if (owner_digest) |prior| {
                if (!std.mem.eql(u8, &prior, &value.owner)) return error.PreparedGenerationChanged;
            } else owner_digest = value.owner;
            if (value.table_id != table.table_id or value.schema_version != parsed.version or value.generation == 0 or
                !std.mem.eql(u8, value.range_start, owner.start_key) or !std.mem.eql(u8, value.range_end, owner.end_key orelse "")) return error.PreparedGenerationChanged;
            if (!std.mem.eql(u8, &comparisons.items[position], &value.comparison)) return error.PreparedGenerationChanged;
            const status = &statuses[position];
            const range = &@constCast(status.ranges)[i];
            range.* = .{
                .group_id = try std.fmt.allocPrint(temporary, "{d}", .{owner.group_id}),
                .generation = try std.fmt.allocPrint(temporary, "{d}", .{value.generation}),
                .slot = value.slot,
                .state = switch (value.state) {
                    .building => .building,
                    .ready => .ready,
                    .failed => .failed,
                },
                .rows_scanned = try std.fmt.allocPrint(temporary, "{d}", .{value.rows_scanned}),
                .owner = try temporary.dupe(u8, &std.fmt.bytesToHex(value.owner, .lower)),
                .comparison = try temporary.dupe(u8, &std.fmt.bytesToHex(value.comparison, .lower)),
                .progress_digest = try temporary.dupe(u8, &std.fmt.bytesToHex(value.progress_digest, .lower)),
                .maintenance_epoch = try std.fmt.allocPrint(temporary, "{d}", .{value.maintenance_epoch}),
                .last_maintenance_request = if (value.maintenance_epoch == 0) null else try temporary.dupe(u8, &std.fmt.bytesToHex(value.last_maintenance_request, .lower)),
                .failure = switch (value.failure) {
                    .none => null,
                    .incompatible_schema => .incompatible_schema,
                    .invalid_row => .invalid_row,
                    .key_too_large => .key_too_large,
                },
            };
            if (value.state == .failed) status.state = .failed else if (value.state == .building and status.state != .failed) status.state = .building;
        }
        expected_start = owner.end_key orelse "";
    }
    var current = (try source.adminSnapshot()) orelse return error.Unavailable;
    defer source.freeAdminSnapshot(&current);
    const current_table = tables.findTableByName(&current, name) orelse return error.TopologyChanged;
    if (!metadata.tableDefinitionsEqual(table.*, current_table.*)) return error.TopologyChanged;
    var found: usize = 0;
    for (current.ranges) |owner| if (owner.table_id == table.table_id) {
        found += 1;
        const prior = owner_by_start.get(owner.start_key) orelse return error.TopologyChanged;
        if (!metadata.rangeRecordsEqual(prior.*, owner)) return error.TopologyChanged;
    };
    if (found != owners.items.len) return error.TopologyChanged;
    try request.ensureActive();
    return .{ .arena = arena, .statuses = statuses };
}

/// Standard /indexes resource envelope, with one uniform stats shape for the
/// aggregate and individual shards. No artifact-index readiness is fabricated.
pub fn encodeResource(alloc: std.mem.Allocator, config_json: []const u8, status_json: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(wire.RelationalIndexStatus, alloc, status_json, .{});
    defer parsed.deinit();
    return encodeResourceValue(alloc, config_json, parsed.value);
}

pub fn encodeResourceValue(alloc: std.mem.Allocator, config_json: []const u8, status: wire.RelationalIndexStatus) ![]u8 {
    var out = std.Io.Writer.Allocating.init(alloc);
    defer out.deinit();
    try out.writer.writeAll("{\"config\":");
    try out.writer.writeAll(config_json);
    try out.writer.writeAll(",\"status\":");
    try writeStats(&out.writer, status);
    try out.writer.writeAll(",\"shard_status\":{");
    for (status.ranges, 0..) |range, i| {
        if (i != 0) try out.writer.writeByte(',');
        try std.json.Stringify.value(range.group_id, .{}, &out.writer);
        try out.writer.writeByte(':');
        var shard = status;
        shard.state = range.state;
        shard.ranges = @constCast((&range)[0..1]);
        try writeStats(&out.writer, shard);
    }
    try out.writer.writeAll("}}");
    return out.toOwnedSlice();
}

fn writeStats(writer: *std.Io.Writer, value: wire.RelationalIndexStatus) !void {
    const ready = value.state == .ready;
    const stats = wire.RelationalIndexStats{
        .index_type = .relational,
        .relational_index = value,
        .milestones = .{
            .queryable = .{ .reached = ready, .blockers = if (ready) &.{} else if (value.state == .failed) &.{"index_build_failed"} else &.{"index_build_coverage"} },
            .complete = .{ .reached = ready, .blockers = if (ready) &.{} else if (value.state == .failed) &.{"index_build_failed"} else &.{"index_build_coverage"} },
        },
    };
    try std.json.Stringify.value(stats, .{ .emit_null_optional_fields = false }, writer);
}

test "relational index status requires complete current owner coverage and standard index envelope" {
    const alloc = std.testing.allocator;
    const Fixture = struct {
        const schema_json =
            \\{"version":2,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"by_id","keys":[{"column":"id"}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        ;
        calls: usize = 0,
        snapshots: usize = 0,
        stale: bool = false,
        stale_definition: bool = false,
        changed_topology: bool = false,
        missing: bool = false,
        failed: bool = false,
        pub fn adminSnapshot(self: *@This()) !?@import("../metadata/api.zig").AdminSnapshot {
            self.snapshots += 1;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(&[_]metadata.TableRecord{.{ .table_id = 7, .name = "rows", .schema_json = schema_json }}),
                .ranges = if (self.changed_topology and self.snapshots > 1) @constCast(&[_]metadata.RangeRecord{
                    .{ .table_id = 7, .group_id = 11, .range_id = 999, .start_key = "", .end_key = "\x00\xff" },
                    .{ .table_id = 7, .group_id = 12, .start_key = "\x00\xff", .end_key = null },
                }) else @constCast(&[_]metadata.RangeRecord{
                    .{ .table_id = 7, .group_id = 11, .start_key = "", .end_key = "\x00\xff" },
                    .{ .table_id = 7, .group_id = 12, .start_key = "\x00\xff", .end_key = null },
                }),
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }
        pub fn freeAdminSnapshot(_: *@This(), _: *@import("../metadata/api.zig").AdminSnapshot) void {}
        fn lookup(ptr: *anyopaque, allocator: std.mem.Allocator, _: []const u8, key: []const u8, opts: @import("../storage/db/types.zig").LookupOptions, consistency: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(.read_index, consistency);
            var request = try std.json.parseFromSlice(native.Request, allocator, opts.relational_index_status_json, .{});
            defer request.deinit();
            try std.testing.expectEqualStrings("by_id", request.value.name.?);
            try std.testing.expectEqual(@as(u32, 2), request.value.schema_version);
            if (self.missing and key.len != 0) return null;
            var schema = try @import("../schema/mod.zig").parseValidatedTableSchema(allocator, schema_json);
            defer schema.deinit(allocator);
            return .{ .version = 0, .json = try std.json.Stringify.valueAlloc(allocator, native.Status{
                .table_id = 7,
                .schema_version = if (self.stale) 3 else 2,
                .generation = 9007199254740993,
                .slot = 1,
                .catalog = @splat(1),
                .comparison = if (self.stale_definition) @splat(0) else try expectedComparison(allocator, schema, "by_id"),
                .owner = @splat(if (key.len == 0) 3 else 4),
                .range_start = key,
                .range_end = if (key.len == 0) "\x00\xff" else "",
                .state = if (self.failed and key.len != 0) .failed else if (key.len == 0) .ready else .building,
                .rows_scanned = 9007199254740993,
                .progress_digest = @splat(5),
                .maintenance_epoch = 0,
                .last_maintenance_request = @splat(0),
                .failure = if (self.failed and key.len != 0) .invalid_row else .none,
            }, .{}) };
        }
    };
    var fixture = Fixture{};
    const reader = reads.TableReadSource{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = undefined, .query = undefined } };
    const body = try collect(alloc, &fixture, reader, "rows", "by_id", Fixture.schema_json, .{});
    defer alloc.free(body);
    var status = try std.json.parseFromSlice(wire.RelationalIndexStatus, alloc, body, .{});
    defer status.deinit();
    try std.testing.expectEqual(wire.RelationalIndexBuildState.building, status.value.state);
    try std.testing.expectEqualStrings("9007199254740993", status.value.ranges[0].generation);
    const resource = try encodeResource(alloc, "{\"name\":\"by_id\",\"type\":\"relational\",\"keys\":[{\"column\":\"id\"}]}", body);
    defer alloc.free(resource);
    var encoded = try std.json.parseFromSlice(std.json.Value, alloc, resource, .{});
    defer encoded.deinit();
    var public_resource = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.IndexStatus, alloc, resource, .{});
    defer public_resource.deinit();
    const aggregate = encoded.value.object.get("status").?.object;
    try std.testing.expectEqualStrings("relational", aggregate.get("index_type").?.string);
    try std.testing.expect(!aggregate.get("milestones").?.object.get("queryable").?.object.get("reached").?.bool);
    try std.testing.expectEqual(@as(usize, 2), encoded.value.object.get("shard_status").?.object.count());
    fixture.stale = true;
    try std.testing.expectError(error.PreparedGenerationChanged, collect(alloc, &fixture, reader, "rows", "by_id", Fixture.schema_json, .{}));
    fixture.stale = false;
    fixture.stale_definition = true;
    try std.testing.expectError(error.PreparedGenerationChanged, collect(alloc, &fixture, reader, "rows", "by_id", Fixture.schema_json, .{}));
    fixture.stale_definition = false;
    fixture.failed = true;
    const failed_body = try collect(alloc, &fixture, reader, "rows", "by_id", Fixture.schema_json, .{});
    defer alloc.free(failed_body);
    var failed_status = try std.json.parseFromSlice(wire.RelationalIndexStatus, alloc, failed_body, .{});
    defer failed_status.deinit();
    try std.testing.expectEqual(wire.RelationalIndexBuildState.failed, failed_status.value.state);
    try std.testing.expectEqual(wire.RelationalIndexBuildFailure.invalid_row, failed_status.value.ranges[1].failure.?);
    fixture.failed = false;
    fixture.missing = true;
    try std.testing.expectError(error.Unavailable, collect(alloc, &fixture, reader, "rows", "by_id", Fixture.schema_json, .{}));
    fixture.missing = false;
    fixture.changed_topology = true;
    fixture.snapshots = 0;
    try std.testing.expectError(error.TopologyChanged, collect(alloc, &fixture, reader, "rows", "by_id", Fixture.schema_json, .{}));
}

test "relational index status batches many indexes once per owner with complete proofs and cancellation" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const temporary = arena.allocator();
    var schema_out = std.Io.Writer.Allocating.init(temporary);
    try schema_out.writer.writeAll("{\"version\":2,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"relational_indexes\":[");
    for (0..32) |i| {
        if (i != 0) try schema_out.writer.writeByte(',');
        try schema_out.writer.print("{{\"name\":\"index_{d}\",\"keys\":[{{\"column\":\"id\"}}]}}", .{i});
    }
    try schema_out.writer.writeAll("],\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}");
    const schema_json = schema_out.written();
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(temporary, schema_json);
    defer parsed.deinit(temporary);
    const comparison = try expectedComparison(temporary, parsed, "index_0");
    const Fixture = struct {
        table: [1]metadata.TableRecord,
        ranges: [8]metadata.RangeRecord,
        comparison: [32]u8,
        calls: usize = 0,
        snapshots: usize = 0,
        mode: enum { valid, partial, duplicate, stale_catalog, stale_generation, malformed, cancel } = .valid,
        canceled: std.atomic.Value(bool) = .init(false),
        pub fn adminSnapshot(self: *@This()) !?@import("../metadata/api.zig").AdminSnapshot {
            self.snapshots += 1;
            return .{ .status = .{ .metadata_group_id = 1, .metrics = .{} }, .tables = &self.table, .ranges = &self.ranges, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        pub fn freeAdminSnapshot(_: *@This(), _: *@import("../metadata/api.zig").AdminSnapshot) void {}
        fn lookup(ptr: *anyopaque, allocator: std.mem.Allocator, _: []const u8, key: []const u8, opts: @import("../storage/db/types.zig").LookupOptions, consistency: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(.read_index, consistency);
            var request = try std.json.parseFromSlice(native.Request, allocator, opts.relational_index_status_json, .{});
            defer request.deinit();
            try std.testing.expectEqual(@as(?[]const u8, null), request.value.name);
            if (self.mode == .malformed) return .{ .version = 0, .json = try allocator.dupe(u8, "{\"statuses\":[") };
            if (self.mode == .cancel) self.canceled.store(true, .release);
            var response_arena = std.heap.ArenaAllocator.init(allocator);
            defer response_arena.deinit();
            const response_alloc = response_arena.allocator();
            const entries = try response_alloc.alloc(native.NamedStatus, if (self.mode == .partial) 31 else 32);
            const owner = for (self.ranges) |range| {
                if (std.mem.eql(u8, range.start_key, key)) break range;
            } else unreachable;
            for (entries, 0..) |*entry, i| entry.* = .{
                .name = try std.fmt.allocPrint(response_alloc, "index_{d}", .{if (self.mode == .duplicate and i == 31) 0 else i}),
                .status = .{ .table_id = 7, .schema_version = 2, .generation = if (self.mode == .stale_generation and key.len != 0) 2 else 1, .slot = @intCast(i), .catalog = @splat(if (self.mode == .stale_catalog and i == 31) 2 else 1), .comparison = self.comparison, .owner = @splat(3), .range_start = key, .range_end = owner.end_key orelse "", .state = .ready, .rows_scanned = 100, .failure = .none, .progress_digest = @splat(4), .maintenance_epoch = 0, .last_maintenance_request = @splat(0) },
            };
            return .{ .version = 0, .json = try std.json.Stringify.valueAlloc(allocator, native.Batch{ .statuses = entries }, .{}) };
        }
    };
    var fixture = Fixture{ .table = .{.{ .table_id = 7, .name = "rows", .schema_json = schema_json }}, .ranges = undefined, .comparison = comparison };
    for (&fixture.ranges, 0..) |*range, i| range.* = .{ .table_id = 7, .group_id = i + 10, .start_key = if (i == 0) "" else try std.fmt.allocPrint(temporary, "{d}", .{i}), .end_key = if (i == 7) null else try std.fmt.allocPrint(temporary, "{d}", .{i + 1}) };
    const reader = reads.TableReadSource{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = undefined, .query = undefined } };
    var result = try collectAll(alloc, &fixture, reader, "rows", schema_json, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 8), fixture.calls); // 256 before batching.
    try std.testing.expectEqual(@as(usize, 2), fixture.snapshots); // 64 before batching.
    try std.testing.expectEqual(@as(usize, 32), result.statuses.len);
    for (result.statuses) |status| {
        try std.testing.expectEqual(.ready, status.state);
        try std.testing.expectEqual(@as(usize, 8), status.ranges.len);
        try std.testing.expectEqualStrings("7", status.table_id);
    }
    fixture.mode = .stale_generation;
    var divergent = try collectAll(alloc, &fixture, reader, "rows", schema_json, .{});
    defer divergent.deinit();
    try std.testing.expectEqual(.ready, divergent.statuses[0].state);
    try std.testing.expectEqualStrings("1", divergent.statuses[0].ranges[0].generation);
    try std.testing.expectEqualStrings("2", divergent.statuses[0].ranges[1].generation);
    inline for (.{ .partial, .duplicate, .stale_catalog }) |mode| {
        fixture.mode = mode;
        try std.testing.expectError(error.PreparedGenerationChanged, collectAll(alloc, &fixture, reader, "rows", schema_json, .{}));
    }
    fixture.mode = .malformed;
    try std.testing.expectError(error.UnexpectedEndOfInput, collectAll(alloc, &fixture, reader, "rows", schema_json, .{}));
    fixture.mode = .cancel;
    fixture.calls = 0;
    try std.testing.expectError(error.Canceled, collectAll(alloc, &fixture, reader, "rows", schema_json, .{ .cancellation = .fromAtomic(&fixture.canceled) }));
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    fixture.calls = 0;
    try std.testing.expectError(error.DeadlineExceeded, collectAll(alloc, &fixture, reader, "rows", schema_json, .{ .deadline_ns = 0 }));
    try std.testing.expectEqual(@as(usize, 0), fixture.calls);
}
