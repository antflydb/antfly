// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Read-only distributed coverage collection. No readiness is synthesized
//! from a declaration or a partial set of owners.
const std = @import("std");
const wire = @import("antfly_metadata_openapi").types;
const tables = @import("tables.zig");
const metadata = @import("../metadata/table_manager.zig");
const schema = @import("../schema/mod.zig");
const reads = @import("table_read_source.zig");
const operation = @import("operation.zig");
const catalog_mod = @import("../storage/db/relational_integrity_catalog.zig");
const activation = @import("../storage/db/relational_integrity_activation.zig");
const types = @import("../storage/db/types.zig");

pub fn collect(alloc: std.mem.Allocator, source: anytype, reader: reads.TableReadSource, name: []const u8, request: operation.RequestContext) ![]u8 {
    try request.ensureActive();
    var snapshot = (try source.adminSnapshot()) orelse return error.ConstraintActivationPending;
    defer source.freeAdminSnapshot(&snapshot);
    const table = tables.findTableByName(&snapshot, name) orelse return error.TableNotFound;
    var parsed = try schema.parseValidatedTableSchema(alloc, table.schema_json);
    defer parsed.deinit(alloc);
    if (parsed.storage_mode != .relational) return error.RelationalTableRequired;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const temporary = arena.allocator();
    const has_constraints = (if (parsed.unique_constraints) |list| list.value.len else 0) != 0 or (if (parsed.foreign_keys) |list| list.value.len else 0) != 0;
    var result = wire.RelationalConstraintStatus{ .schema_version = parsed.version, .coverage_kind = "unique_and_foreign_key", .state = .enforced, .ranges = &.{} };
    if (!has_constraints) return std.json.Stringify.valueAlloc(alloc, result, .{});
    var opts: types.LookupOptions = .{ .relational_integrity_catalog = true, .execution_deadline_ns = request.deadline_ns, .execution_io = request.deadline_io, .cancellation = request.cancellation };
    var envelope = (try reader.lookup(temporary, name, "", opts, .read_index)) orelse return error.ConstraintActivationPending;
    defer envelope.deinit(temporary);
    var catalog_response = try std.json.parseFromSlice(struct { catalog: []const u8, schema_version: u32, table_id: []const u8 }, temporary, envelope.json, .{});
    defer catalog_response.deinit();
    const decoded_id = std.fmt.parseInt(u64, catalog_response.value.table_id, 10) catch return error.PreparedGenerationChanged;
    if (decoded_id != table.table_id or catalog_response.value.schema_version != parsed.version) return error.PreparedGenerationChanged;
    const encoded = catalog_response.value.catalog;
    const raw = try temporary.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(encoded));
    try std.base64.standard.Decoder.decode(raw, encoded);
    var catalog = try catalog_mod.decode(temporary, raw);
    defer catalog.deinit();
    var owners: std.ArrayList(*const metadata.RangeRecord) = .empty;
    for (snapshot.ranges) |*owner| if (owner.table_id == table.table_id) try owners.append(temporary, owner);
    if (owners.items.len == 0 or owners.items.len > 4096) return error.ConstraintActivationPending;
    std.mem.sort(*const metadata.RangeRecord, owners.items, {}, struct {
        fn less(_: void, left: *const metadata.RangeRecord, right: *const metadata.RangeRecord) bool {
            return std.mem.order(u8, left.start_key, right.start_key) == .lt;
        }
    }.less);
    const ranges = try temporary.alloc(wire.RelationalConstraintRangeStatus, owners.items.len);
    var expected_start: []const u8 = "";
    opts.relational_integrity_catalog = false;
    opts.relational_activation_json = "{\"mode\":\"status\"}";
    for (owners.items, ranges, 0..) |owner, *range, i| {
        try request.ensureActive();
        if (owner.restore_backup_id.len != 0 or !std.mem.eql(u8, owner.start_key, expected_start) or ((i + 1 == owners.items.len) != (owner.end_key == null))) return error.TopologyChanged;
        var response = (try reader.lookup(temporary, name, owner.start_key, opts, .read_index)) orelse return error.ConstraintActivationPending;
        defer response.deinit(temporary);
        const Status = struct {
            schema_version: u32,
            schema_digest: [32]u8,
            generation_set: [32]u8,
            owner: [32]u8,
            range_start: []const u8,
            range_end: []const u8,
            unique_covered: bool,
            state: activation.State,
            phase: activation.Phase,
            rows_scanned: u64,
            failure: []const u8,
        };
        var status = try std.json.parseFromSlice(Status, temporary, response.json, .{});
        defer status.deinit();
        const value = status.value;
        if (value.schema_version != parsed.version or !std.mem.eql(u8, &value.schema_digest, &catalog.schema_digest) or
            !std.mem.eql(u8, &value.generation_set, &activation.generationSet(catalog)) or
            !std.mem.eql(u8, value.range_start, owner.start_key) or !std.mem.eql(u8, value.range_end, owner.end_key orelse "")) return error.PreparedGenerationChanged;
        const owner_hex = std.fmt.bytesToHex(value.owner, .lower);
        range.* = .{
            .group_id = try std.fmt.allocPrint(temporary, "{d}", .{owner.group_id}),
            .state = switch (value.state) {
                .enforced => .enforced,
                .validating => .validating,
                .invalid => .invalid,
            },
            .phase = switch (value.phase) {
                .unique => .unique,
                .foreign_key => .foreign_key,
            },
            .rows_scanned = try std.fmt.allocPrint(temporary, "{d}", .{value.rows_scanned}),
            .owner = try temporary.dupe(u8, &owner_hex),
            .failure = if (value.failure.len != 0) try temporary.dupe(u8, value.failure) else null,
        };
        if (value.state == .invalid) result.state = .invalid else if (value.state == .validating and result.state != .invalid) result.state = .validating;
        expected_start = owner.end_key orelse "";
    }
    // Reject a topology/DDL change that raced the fanout, even if every old
    // owner individually reported internally consistent coverage.
    var current = (try source.adminSnapshot()) orelse return error.ConstraintActivationPending;
    defer source.freeAdminSnapshot(&current);
    const current_table = tables.findTableByName(&current, name) orelse return error.TopologyChanged;
    if (!metadata.tableDefinitionsEqual(table.*, current_table.*)) return error.TopologyChanged;
    var found: usize = 0;
    for (current.ranges) |owner| if (owner.table_id == table.table_id) {
        found += 1;
        const matches = for (owners.items) |prior| {
            if (prior.group_id == owner.group_id and std.mem.eql(u8, prior.start_key, owner.start_key) and std.mem.eql(u8, prior.end_key orelse "", owner.end_key orelse "")) break true;
        } else false;
        if (!matches) return error.TopologyChanged;
    };
    if (found != owners.items.len) return error.TopologyChanged;
    result.ranges = ranges;
    return std.json.Stringify.valueAlloc(alloc, result, .{});
}

test "relational declarations status requires complete matching owner coverage" {
    const alloc = std.testing.allocator;
    const Fixture = struct {
        update: *catalog_mod.Update,
        stale: bool = false,
        calls: usize = 0,
        const schema_json =
            \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        ;
        pub fn adminSnapshot(_: *@This()) !?@import("../metadata/api.zig").AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast(&[_]metadata.TableRecord{.{ .table_id = 7, .name = "rows", .schema_json = schema_json }}),
                .ranges = @constCast(&[_]metadata.RangeRecord{
                    .{ .table_id = 7, .group_id = 11, .start_key = "", .end_key = "\x00" },
                    .{ .table_id = 7, .group_id = 12, .start_key = "\x00", .end_key = null },
                }),
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }
        pub fn freeAdminSnapshot(_: *@This(), _: *@import("../metadata/api.zig").AdminSnapshot) void {}
        fn lookup(ptr: *anyopaque, allocator: std.mem.Allocator, _: []const u8, key: []const u8, opts: types.LookupOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (opts.relational_integrity_catalog) {
                try std.testing.expectEqualStrings("", key);
                const encoded = try allocator.alloc(u8, std.base64.standard.Encoder.calcSize(self.update.value.len));
                defer allocator.free(encoded);
                _ = std.base64.standard.Encoder.encode(encoded, self.update.value);
                return .{ .version = 0, .json = try std.json.Stringify.valueAlloc(allocator, .{ .catalog = encoded, .schema_version = @as(u32, 1), .table_id = "7" }, .{}) };
            }
            self.calls += 1;
            try std.testing.expectEqualStrings("{\"mode\":\"status\"}", opts.relational_activation_json);
            const first = key.len == 0;
            if (!first) try std.testing.expectEqualStrings("\x00", key);
            return .{ .version = 0, .json = try std.json.Stringify.valueAlloc(allocator, .{
                .schema_version = @as(u32, if (self.stale) 2 else 1),
                .schema_digest = self.update.catalog.schema_digest,
                .generation_set = activation.generationSet(self.update.catalog),
                .owner = [_]u8{1} ** 32,
                .range_start = key,
                .range_end = if (first) "\x00" else "",
                .unique_covered = true,
                .state = "enforced",
                .phase = "foreign_key",
                .rows_scanned = @as(u64, 9007199254740993),
                .failure = "",
            }, .{}) };
        }
    };
    var update = try catalog_mod.prepare(alloc, null, try catalog_mod.incarnationFromTableId(7), 1, @splat(9), &.{.{ .kind = .unique, .name = "pk", .fingerprint = @splat(1) }});
    defer update.deinit();
    var fixture = Fixture{ .update = &update };
    const reader = reads.TableReadSource{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = undefined, .query = undefined } };
    const body = try collect(alloc, &fixture, reader, "rows", .{});
    defer alloc.free(body);
    var result = try std.json.parseFromSlice(wire.RelationalConstraintStatus, alloc, body, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.value.ranges.len);
    try std.testing.expectEqualStrings("9007199254740993", result.value.ranges[0].rows_scanned);
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    fixture.stale = true;
    try std.testing.expectError(error.PreparedGenerationChanged, collect(alloc, &fixture, reader, "rows", .{}));
}
