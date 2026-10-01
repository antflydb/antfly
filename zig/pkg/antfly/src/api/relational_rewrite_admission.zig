// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Before-pin rewrite planning. Selection never reads rows; source admission
//! observations grant no mutation authority. The caller authorizes the entire
//! selected cohort before building and atomically persists plan plus job.
const std = @import("std");
const records = @import("../common/topology_records.zig");
const tables_api = @import("tables.zig");
const metadata = @import("../metadata/table_manager.zig");
const stages = @import("../metadata/restore_staging.zig");
const source = @import("../storage/db/online_source_contract.zig");
const wire = @import("../storage/db/online_merge_io_contract.zig");
const topology = @import("../storage/db/relational_integrity_topology_contract.zig");
const schema = @import("../schema/mod.zig");

pub fn requested(raw: []const u8) !bool {
    if (raw.len == 0) return false;
    var parts = std.mem.splitScalar(u8, raw, '&');
    var result: ?bool = null;
    while (parts.next()) |part| {
        if (result != null) return error.InvalidSchemaUpdateRequest;
        if (std.mem.eql(u8, part, "rewrite=true")) result = true else if (std.mem.eql(u8, part, "rewrite=false")) result = false else return error.InvalidSchemaUpdateRequest;
    }
    return result orelse false;
}

pub fn select(alloc: std.mem.Allocator, tables: []const records.TableRecord, table_name: []const u8, proposed: []const u8) ![]records.TableRecord {
    var names: std.StringHashMapUnmanaged(usize) = .empty;
    defer names.deinit(alloc);
    for (tables, 0..) |table, index| try names.put(alloc, table.name, index);
    const root = names.get(table_name) orelse return error.TableNotFound;
    const chosen = try alloc.alloc(bool, tables.len);
    defer alloc.free(chosen);
    @memset(chosen, false);
    chosen[root] = true;
    const Edge = struct { child: usize, parent: usize };
    var edges = std.ArrayList(Edge).empty;
    defer edges.deinit(alloc);
    var unbounded_graph_dependencies = false;
    for (tables, 0..) |table, child| {
        var indexes = try std.json.parseFromSlice(std.json.Value, alloc, table.indexes_json, .{});
        defer indexes.deinit();
        if (indexes.value != .object) return error.InvalidCreateTableRequest;
        for (indexes.value.object.values()) |index| {
            if (index == .object) if (index.object.get("type")) |kind| {
                if (kind == .string and std.mem.eql(u8, kind.string, "graph")) unbounded_graph_dependencies = true;
            };
        }
        for ([_][]const u8{ table.schema_json, table.read_schema_json, if (child == root) proposed else "" }) |json| {
            if (json.len == 0) continue;
            var parsed = try schema.parseValidatedTableSchema(alloc, json);
            defer parsed.deinit(alloc);
            if (parsed.foreign_keys) |keys| for (keys.value) |fk| {
                const parent = names.get(fk.parent_table) orelse return error.ForeignKeyParentTableNotFound;
                try edges.append(alloc, .{ .child = child, .parent = parent });
            };
        }
    }
    // Row-authored graph identities have no complete schema dependency index.
    // Include the catalog rather than pretending a narrower cut is proven.
    if (unbounded_graph_dependencies) @memset(chosen, true) else {
        var changed = true;
        while (changed) {
            changed = false;
            for (edges.items) |edge| {
                if (chosen[edge.child] == chosen[edge.parent]) continue;
                chosen[edge.child] = true;
                chosen[edge.parent] = true;
                changed = true;
            }
        }
    }
    var result = std.ArrayList(records.TableRecord).empty;
    errdefer result.deinit(alloc);
    for (tables, chosen) |table, included| if (included) {
        if (result.items.len == 128) return error.TransactionTooLarge;
        try result.append(alloc, table);
    };
    std.mem.sort(records.TableRecord, result.items, {}, struct {
        fn less(_: void, a: records.TableRecord, b: records.TableRecord) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.less);
    return result.toOwnedSlice(alloc);
}

fn identity(id: stages.Id, original: u64, ordinal: u64, domain: []const u8) u64 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly schema rewrite target v1");
    hash.update(domain);
    hash.update(&id);
    var numbers: [16]u8 = undefined;
    std.mem.writeInt(u64, numbers[0..8], original, .little);
    std.mem.writeInt(u64, numbers[8..16], ordinal, .little);
    hash.update(&numbers);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return @import("../common/group_ids.zig").dataGroupIdFromHash(std.mem.readInt(u64, digest[0..8], .little));
}

const SourceSchemas = struct {
    definitions: std.ArrayList([]const u8) = .empty,
    versions: std.ArrayList(u32) = .empty,
    bytes: usize = 0,

    fn add(self: *@This(), alloc: std.mem.Allocator, json: []const u8) !void {
        const limits = @import("../storage/db/relational_rewrite_contract.zig");
        if (json.len == 0 or json.len > limits.max_schema_bytes) return error.RelationalRewriteBudgetExceeded;
        // Owners commonly expose the same complete archive. Deduplicate bytes
        // before compiling/parsing so fanout does not retain a schema parse per
        // owner in the request arena.
        for (self.definitions.items) |existing| if (std.mem.eql(u8, existing, json)) return;
        var parsed = try schema.parseValidatedTableSchema(alloc, json);
        defer parsed.deinit(alloc);
        for (self.versions.items, self.definitions.items) |version, existing| if (version == parsed.version) {
            // One numeric epoch must denote exactly one immutable public
            // definition across every source owner. Never substitute a current
            // layout for an older physical row's interpretation.
            if (!std.mem.eql(u8, existing, json)) return error.RestoreStagingScopeChanged;
            return;
        };
        if (self.definitions.items.len == limits.max_source_schemas or self.bytes +| json.len > limits.max_schema_bytes) return error.RelationalRewriteBudgetExceeded;
        try self.definitions.append(alloc, try alloc.dupe(u8, json));
        try self.versions.append(alloc, parsed.version);
        self.bytes += json.len;
    }
};

/// `alloc` is a request arena. `observer.readFacts(table, request)` performs a
/// leader-fenced read only. No source pins, parent schema writes, or hidden
/// roots may be created until the returned plan is atomically admitted.
pub fn build(alloc: std.mem.Allocator, id: stages.Id, selected: []const records.TableRecord, all_ranges: []const records.RangeRecord, table_name: []const u8, proposed: []const u8, observer: anytype) !stages.Plan {
    // Source ownership must stay fixed throughout the rewrite cohort. Check
    // every member before issuing any source reads or admitting a durable job.
    for (selected) |table| if (table.storage_migration != null) return error.TableTransitionActive;
    const current = for (selected) |table| {
        if (std.mem.eql(u8, table.name, table_name)) break table;
    } else return error.TableNotFound;
    var owned_support = try @import("relational_witness_ddl.zig").prepare(alloc, selected, table_name, proposed, current.schema_json);
    defer owned_support.deinit();
    const targets = try alloc.alloc(stages.Target, selected.len);
    var range_count: usize = 0;
    var schema_bytes: usize = 0;
    for (selected, targets) |before, *target| {
        if (before.relational_retirement_json.len != 0 or before.restore_backup_id.len != 0) return error.TableTransitionActive;
        var table = if (std.mem.eql(u8, before.name, table_name))
            try tables_api.prepareSchemaRewriteRecord(alloc, &before, owned_support.schema_json)
        else copy: {
            for (owned_support.parents) |parent| if (parent.before.table_id == before.table_id) break :copy try metadata.cloneTable(alloc, parent.after);
            break :copy try metadata.cloneTable(alloc, before);
        };
        var parsed = try schema.parseValidatedTableSchema(alloc, tables_api.effectiveSchemaJson(table.schema_json));
        defer parsed.deinit(alloc);
        const document = parsed.storage_mode == .document;
        if (document and std.mem.eql(u8, before.name, table_name)) return error.InvalidSchemaUpdateRequest;
        const active = try alloc.dupe(u8, tables_api.effectiveSchemaJson(before.schema_json));
        const read = before.read_schema_json;
        var source_manifest: SourceSchemas = .{};
        try source_manifest.add(alloc, active);
        if (read.len != 0) try source_manifest.add(alloc, read);
        if (document) {
            table.schema_json = active;
        } else {
            table.read_schema_json = "";
        }
        const table_id = identity(id, before.table_id, 0, "table");
        var original_ranges = std.ArrayList(records.RangeRecord).empty;
        for (all_ranges) |range| if (range.table_id == before.table_id) try original_ranges.append(alloc, try metadata.cloneRange(alloc, range));
        metadata.sortKeyspaceRanges(records.RangeRecord, original_ranges.items);
        if (original_ranges.items.len == 0) return error.TableTransitionActive;
        range_count += original_ranges.items.len;
        if (range_count > 4096) return error.TransactionTooLarge;
        const ranges = try alloc.alloc(records.RangeRecord, original_ranges.items.len);
        const scopes = try alloc.alloc(source.Scope, ranges.len);
        const fences = try alloc.alloc(topology.Fence, ranges.len);
        for (original_ranges.items, ranges, scopes, fences, 0..) |original, *range, *scope, *fence, ordinal| {
            const group = identity(id, before.table_id, ordinal, "group");
            range.* = .{ .table_id = table_id, .group_id = group, .range_id = group, .doc_identity_shard_id = group, .doc_identity_range_id = group, .start_key = original.start_key, .end_key = original.end_key };
            scope.* = .{ .fence = .{ .role = .rewrite_source, .transition_id = std.mem.readInt(u64, id[0..8], .little), .attempt = 0, .admission_epoch = 0, .owner_group_id = original.group_id, .peer_group_id = group, .namespace = .{ .table_id = before.table_id, .shard_id = metadata.rangeDocIdentityShardId(original), .range_id = metadata.rangeDocIdentityRangeId(original) }, .catalog_digest = @splat(0) }, .receiver_namespace = .{ .table_id = table_id, .shard_id = group, .range_id = group }, .consumer_epoch = 0, .copy_attempt = .{} };
            const facts: wire.AdmissionFacts = try observer.readFacts(before.name, .{ .scope = scope.*, .operation = .{ .admission = .donor } });
            defer if (@hasDecl(@TypeOf(observer.*), "releaseFacts")) observer.releaseFacts(facts);
            if (!facts.eligible) return error.UnsupportedRestoreSource;
            if (!facts.namespace.eql(scope.fence.namespace)) return error.TableGenerationChanged;
            if (!document) {
                if (facts.source_schemas.len == 0) return error.UnknownSchemaVersion;
                for (facts.source_schemas) |definition| try source_manifest.add(alloc, definition);
            }
            scope.fence.attempt = std.mem.readInt(u64, id[8..16], .little);
            scope.fence.admission_epoch = facts.next_topology_epoch;
            scope.fence.catalog_digest = facts.catalog_digest;
            scope.authority = facts.authority;
            scope.consumer_epoch = facts.next_consumer_epoch;
            scope.copy_attempt = .{ .donor_term = facts.donor_term, .sequence = facts.next_copy_sequence };
            try scope.validate();
            fence.* = scope.fence;
        }
        const source_schemas = source_manifest.definitions.items;
        schema_bytes +|= source_manifest.bytes +| table.schema_json.len +| table.read_schema_json.len +| table.indexes_json.len;
        if (schema_bytes > @import("../storage/db/relational_rewrite_contract.zig").max_schema_bytes) return error.RelationalRewriteBudgetExceeded;
        var programs = if (document)
            try @import("../storage/db/relational_rewrite_program.zig").ProgramSet.initDocumentPreservationWithRead(alloc, active, read)
        else
            try @import("../storage/db/relational_rewrite_program.zig").ProgramSet.init(alloc, source_schemas, table.schema_json, .{});
        defer programs.deinit();
        table.table_id = table_id;
        table.min_ranges = @intCast(ranges.len);
        target.* = .{ .source_table_id = before.table_id, .table = table, .ranges = ranges, .rewrite = .{ .preserve_document = document, .source_schemas = source_schemas, .target_schema = table.schema_json, .target_read_schema = if (document) table.read_schema_json else "", .program_digest = programs.identity }, .rewrite_sources = scopes, .replace = .{ .table = try metadata.cloneTable(alloc, before), .ranges = original_ranges.items, .fences = fences } };
    }
    var result: stages.Plan = .{ .id = id, .cohort_digest = @splat(0), .targets = targets, .preparing_sources = true };
    result.cohort_digest = try result.rewriteIntentDigest(alloc);
    try result.validate(alloc);
    return result;
}

test "distributed txn rewrite admission closes current and historical dependencies before any pin" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const parent =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"x":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const child =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"fk","child_columns":["id"],"parent_table":"parents","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const grandchild = try std.mem.replaceOwned(u8, alloc, child, "\"parents\"", "\"children\"");
    const proposed = try std.mem.replaceOwned(u8, alloc, parent, "\"unique_constraints\":", "\"generated_columns\":[{\"column\":\"id\",\"expression\":{\"op\":\"column\",\"column\":\"x\"}}],\"unique_constraints\":");
    var tables = [_]records.TableRecord{
        .{ .table_id = 10, .name = "parents", .schema_json = parent },
        .{ .table_id = 11, .name = "children", .schema_json = child },
        .{ .table_id = 12, .name = "historical", .schema_json = parent, .read_schema_json = grandchild },
        .{ .table_id = 13, .name = "documents" },
    };
    const selected = try select(alloc, &tables, "parents", proposed);
    try std.testing.expectEqual(@as(usize, 3), selected.len);
    // Historical dependency expands closure even when absent from the active
    // schema. The build fixture below uses distinct schema versions as rows do.
    tables[2].read_schema_json = try std.mem.replaceOwned(u8, alloc, grandchild, "\"version\":1", "\"version\":0");
    const cohort = try select(alloc, &tables, "parents", proposed);
    const ranges = [_]records.RangeRecord{
        .{ .table_id = 10, .group_id = 301, .start_key = "" },
        .{ .table_id = 11, .group_id = 302, .start_key = "" },
        .{ .table_id = 12, .group_id = 303, .start_key = "" },
        .{ .table_id = 13, .group_id = 304, .start_key = "" },
    };
    const Observer = struct {
        calls: usize = 0,
        released: usize = 0,
        eligible: bool = true,
        tables: []const records.TableRecord,
        one_schema: [1][]const u8 = undefined,
        fn readFacts(self: *@This(), name: []const u8, request: wire.Request) !wire.AdmissionFacts {
            try request.validate();
            self.calls += 1;
            self.one_schema[0] = for (self.tables) |table| {
                if (std.mem.eql(u8, table.name, name)) break tables_api.effectiveSchemaJson(table.schema_json);
            } else return error.TableNotFound;
            return .{ .namespace = request.scope.fence.namespace, .eligible = self.eligible, .source_schemas = &self.one_schema, .catalog_digest = @splat(5), .next_topology_epoch = 1, .next_consumer_epoch = 1, .donor_term = 1, .next_copy_sequence = 1 };
        }
        fn releaseFacts(self: *@This(), _: wire.AdmissionFacts) void {
            self.released += 1;
        }
    };
    var observer = Observer{ .tables = &tables };
    const plan = try build(alloc, try stages.idForAttempt(19, 1), cohort, &ranges, "parents", proposed, &observer);
    try std.testing.expect(plan.preparing_sources);
    try std.testing.expectEqual(@as(usize, 3), observer.calls);
    try std.testing.expectEqual(observer.calls, observer.released);
    const migrating = try alloc.dupe(records.TableRecord, cohort);
    migrating[migrating.len - 1].storage_migration = .{ .request = .{ .job_id = "vectors", .mode = .online } };
    const calls_before_migration = observer.calls;
    try std.testing.expectError(error.TableTransitionActive, build(alloc, plan.id, migrating, &ranges, "parents", proposed, &observer));
    try std.testing.expectEqual(calls_before_migration, observer.calls);
    for (plan.targets) |target| {
        try std.testing.expect(target.replace != null);
        try std.testing.expectEqual(@as(usize, 0), target.source_artifacts.len);
        try std.testing.expectEqual(@as(usize, 1), target.rewrite_sources.len);
        try std.testing.expect(target.rewrite_sources[0].fence.eql(target.replace.?.fences[0]));
    }
    observer.eligible = false;
    try std.testing.expectError(error.UnsupportedRestoreSource, build(alloc, plan.id, cohort, &ranges, "parents", proposed, &observer));
    try std.testing.expectEqual(observer.calls, observer.released);
    tables[3].indexes_json = "{\"graph\":{\"type\":\"graph\"}}";
    try std.testing.expectEqual(@as(usize, 4), (try select(alloc, &tables, "parents", proposed)).len);
    try std.testing.expect(try requested("rewrite=true"));
    try std.testing.expect(!try requested("rewrite=false"));
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, requested("rewrite=true&rewrite=false"));
}

test "distributed txn rewrite admission binds bounded immutable historical schemas across owners" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const definition =
        \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const historical = try std.mem.replaceOwned(u8, alloc, definition, "\"version\":2", "\"version\":1");
    var unioned: SourceSchemas = .{};
    try unioned.add(alloc, definition);
    try unioned.add(alloc, historical);
    try unioned.add(alloc, historical);
    try std.testing.expectEqual(@as(usize, 2), unioned.definitions.items.len);
    try std.testing.expectEqualSlices(u32, &.{ 2, 1 }, unioned.versions.items);
    const conflicting = try std.mem.replaceOwned(u8, alloc, historical, "\"integer\"", "\"string\"");
    try std.testing.expectError(error.RestoreStagingScopeChanged, unioned.add(alloc, conflicting));
    const target = try std.mem.replaceOwned(u8, alloc, definition, "\"version\":2", "\"version\":3");
    var programs = try @import("../storage/db/relational_rewrite_program.zig").ProgramSet.init(alloc, unioned.definitions.items, target, .{});
    defer programs.deinit();
    try std.testing.expectEqual(@as(usize, 2), programs.programs.len);
    const oversized = try alloc.alloc(u8, @import("../storage/db/relational_rewrite_contract.zig").max_schema_bytes + 1);
    try std.testing.expectError(error.RelationalRewriteBudgetExceeded, unioned.add(alloc, oversized));
    unioned.bytes = @import("../storage/db/relational_rewrite_contract.zig").max_schema_bytes;
    try std.testing.expectError(error.RelationalRewriteBudgetExceeded, unioned.add(alloc, target));
}
