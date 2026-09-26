// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! TRUNCATE is a durable, fenced fresh-generation publication. No row scan,
//! delete batch, source artifact, or retained tail is involved.
const std = @import("std");
const builtin = @import("builtin");
const server_mod = @import("http_server.zig");
const catalog = @import("../sql/catalog.zig");
const ast = @import("../sql/ast.zig");
const domain = @import("../system_catalog/domain.zig");
const records = @import("../common/topology_records.zig");
const metadata = @import("../metadata/table_manager.zig");
const stages = @import("../metadata/restore_staging.zig");
const jobs = @import("restore_jobs.zig");
const operation = @import("operation.zig");
const integrity_catalog = @import("../storage/db/relational_integrity_catalog.zig");

// Test binaries can exercise the real SQL route while the public graph
// cutover gate remains closed. The permit is single-use and bound to one
// exact, already-authorized physical cohort; production cannot grant it.
const graph_test_busy = std.math.maxInt(u16);
var graph_truncate_test_count: std.atomic.Value(u16) = .init(0);
var graph_truncate_test_ids: [128]u64 = undefined;

pub fn grantGraphTruncateForTest(table_id: u64) !void {
    return grantGraphTruncateCohortForTest(&.{table_id});
}

/// IDs must be sorted and unique so a mounted CASCADE test cannot broaden a
/// permit merely by changing SQL name order or admitting another table.
pub fn grantGraphTruncateCohortForTest(table_ids: []const u64) !void {
    if (!builtin.is_test) return error.TestOnlyGraphTruncateUnavailable;
    if (table_ids.len == 0 or table_ids.len > graph_truncate_test_ids.len) return error.InvalidGraphTruncateTestPermit;
    for (table_ids, 0..) |id, index| {
        if (id == 0 or (index != 0 and id <= table_ids[index - 1])) return error.InvalidGraphTruncateTestPermit;
    }
    if (graph_truncate_test_count.cmpxchgStrong(0, graph_test_busy, .acq_rel, .acquire) != null)
        return error.GraphTruncateTestPermitAlreadyActive;
    @memcpy(graph_truncate_test_ids[0..table_ids.len], table_ids);
    graph_truncate_test_count.store(@intCast(table_ids.len), .release);
}

pub fn clearGraphTruncateForTest() void {
    if (builtin.is_test) graph_truncate_test_count.store(0, .release);
}

fn consumeGraphTruncateTestPermit(selected: []const records.TableRecord) bool {
    if (!builtin.is_test) return false;
    const count = graph_truncate_test_count.load(.acquire);
    if (count == 0 or count == graph_test_busy or selected.len != count) return false;
    if (graph_truncate_test_count.cmpxchgStrong(count, graph_test_busy, .acq_rel, .acquire) != null) return false;
    defer graph_truncate_test_count.store(0, .release);
    var ids: [128]u64 = undefined;
    for (selected, 0..) |table, index| ids[index] = table.table_id;
    std.mem.sort(u64, ids[0..selected.len], {}, std.sort.asc(u64));
    return std.mem.eql(u64, ids[0..selected.len], graph_truncate_test_ids[0..selected.len]);
}

test "graph TRUNCATE diagnostic permit is single-use and table-bound" {
    defer clearGraphTruncateForTest();
    const selected = [_]records.TableRecord{.{ .table_id = 71, .name = "docs" }};
    try std.testing.expect(!consumeGraphTruncateTestPermit(&selected));
    try grantGraphTruncateForTest(71);
    try std.testing.expectError(error.GraphTruncateTestPermitAlreadyActive, grantGraphTruncateForTest(72));
    try std.testing.expect(consumeGraphTruncateTestPermit(&selected));
    try std.testing.expect(!consumeGraphTruncateTestPermit(&selected));
    try grantGraphTruncateForTest(72);
    try std.testing.expect(!consumeGraphTruncateTestPermit(&selected));
    try std.testing.expectError(error.InvalidGraphTruncateTestPermit, grantGraphTruncateCohortForTest(&.{ 72, 71 }));
    const cohort = [_]records.TableRecord{ .{ .table_id = 72, .name = "child" }, .{ .table_id = 71, .name = "parent" } };
    try std.testing.expect(!consumeGraphTruncateTestPermit(&cohort));
    clearGraphTruncateForTest();
    try grantGraphTruncateCohortForTest(&.{ 71, 72 });
    try std.testing.expect(consumeGraphTruncateTestPermit(&cohort));
    try std.testing.expect(!consumeGraphTruncateTestPermit(&cohort));
}

/// Predict the fresh child identities with the exact owner compiler, before
/// the hidden generation exists. This is not a hash of a policy name: the
/// generation allocation order, typed declaration fingerprint, and table
/// incarnation must match the owner-side catalog byte-for-byte.
fn plannedIntegrityCatalog(alloc: std.mem.Allocator, table_id: u64, schema_json: []const u8) !integrity_catalog.Update {
    const schema_api = @import("../schema/mod.zig");
    const native_schema = @import("../storage/schema.zig");
    const declarations = @import("../schema/relational_declarations.zig");
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    if (parsed.storage_mode != .relational) return error.RelationalTableRequired;
    const runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer native_schema.freeSchema(alloc, runtime);
    const bytes = try native_schema.serializeSchema(alloc, runtime);
    defer alloc.free(bytes);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &digest, .{});
    const definitions = try declarations.definitionFingerprints(alloc, parsed, runtime);
    defer declarations.freeDefinitions(alloc, definitions);
    return integrity_catalog.prepare(alloc, null, try integrity_catalog.incarnationFromTableId(table_id), runtime.version, digest, definitions);
}

fn currentIntegrityCatalog(alloc: std.mem.Allocator, source: @import("table_read_source.zig").TableReadSource, table: records.TableRecord) !integrity_catalog.Catalog {
    var response = (try source.integrityCatalog(alloc, table.name)) orelse return error.IntegrityCatalogUnavailable;
    defer response.deinit(alloc);
    const Envelope = struct { catalog: []const u8, schema_version: u32, table_id: []const u8 };
    var envelope = try std.json.parseFromSlice(Envelope, alloc, response.json, .{});
    defer envelope.deinit();
    if ((std.fmt.parseInt(u64, envelope.value.table_id, 10) catch return error.InvalidIntegrityCatalog) != table.table_id) return error.CatalogGenerationChanged;
    const length = std.base64.standard.Decoder.calcSizeForSlice(envelope.value.catalog) catch return error.InvalidIntegrityCatalog;
    if (length > integrity_catalog.max_catalog_bytes) return error.InvalidIntegrityCatalog;
    const bytes = try alloc.alloc(u8, length);
    defer alloc.free(bytes);
    std.base64.standard.Decoder.decode(bytes, envelope.value.catalog) catch return error.InvalidIntegrityCatalog;
    var loaded_catalog = try integrity_catalog.decode(alloc, bytes);
    errdefer loaded_catalog.deinit();
    if (loaded_catalog.schema_version != envelope.value.schema_version or
        !std.mem.eql(u8, &loaded_catalog.incarnation, &(try integrity_catalog.incarnationFromTableId(table.table_id)))) return error.CatalogGenerationChanged;
    var expected = try plannedIntegrityCatalog(alloc, table.table_id, @import("tables.zig").effectiveSchemaJson(table.schema_json));
    defer expected.deinit();
    if (loaded_catalog.schema_version != expected.catalog.schema_version or !std.mem.eql(u8, &loaded_catalog.schema_digest, &expected.catalog.schema_digest)) return error.CatalogGenerationChanged;
    for (expected.catalog.bindings) |binding| {
        const actual = loaded_catalog.find(binding.definition.kind, binding.definition.name) orelse return error.CatalogGenerationChanged;
        if (!std.mem.eql(u8, &actual.definition.fingerprint, &binding.definition.fingerprint)) return error.CatalogGenerationChanged;
    }
    return loaded_catalog;
}

const ExternalForeign = std.meta.Child(@FieldType(stages.ExternalFkParent, "foreign_keys"));
const ExternalParentDraft = struct {
    table: records.TableRecord,
    foreign_keys: std.ArrayList(ExternalForeign) = .empty,
};

fn buildExternalParents(
    server: *server_mod.ApiHttpServer,
    identity: ?server_mod.AuthenticatedIdentity,
    context: operation.RequestContext,
    alloc: std.mem.Allocator,
    snapshot: @import("../metadata/api.zig").AdminSnapshot,
    selected: []const records.TableRecord,
    planned: []const stages.Target,
    id: stages.Id,
) ![]stages.ExternalFkParent {
    const reads = server.table_reads orelse return error.UnsupportedSqlExecution;
    var drafts: std.ArrayList(ExternalParentDraft) = .empty;
    for (selected, planned) |child, replacement| {
        var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, @import("tables.zig").effectiveSchemaJson(child.schema_json));
        defer parsed.deinit(alloc);
        if (parsed.storage_mode != .relational) continue;
        const definitions = try parsed.relationalForeignKeyDefinitions(alloc);
        if (definitions.len == 0) continue;
        const has_external = for (definitions) |fk| {
            const parent_selected = for (selected) |other| {
                if (std.mem.eql(u8, other.name, fk.parent_table)) break true;
            } else false;
            if (!parent_selected) break true;
        } else false;
        if (!has_external) continue;
        var old_catalog = try currentIntegrityCatalog(alloc, reads, child);
        defer old_catalog.deinit();
        var new_catalog = try plannedIntegrityCatalog(alloc, replacement.table.table_id, replacement.table.schema_json);
        defer new_catalog.deinit();
        for (definitions) |fk| {
            var parent_selected = false;
            for (selected) |other| if (std.mem.eql(u8, other.name, fk.parent_table)) {
                parent_selected = true;
                break;
            };
            if (parent_selected) continue;
            const parent = for (snapshot.tables) |candidate| {
                if (std.mem.eql(u8, candidate.name, fk.parent_table)) break candidate;
            } else return error.ForeignKeyParentTableNotFound;
            if (parent.storage_migration != null or parent.relational_retirement_json.len != 0 or parent.restore_backup_id.len != 0) return error.TableTransitionActive;
            const before = old_catalog.find(.foreign_key, fk.name) orelse return error.CatalogGenerationChanged;
            const after = new_catalog.catalog.find(.foreign_key, fk.name) orelse return error.CatalogGenerationChanged;
            var draft: ?*ExternalParentDraft = null;
            for (drafts.items) |*existing| if (existing.table.table_id == parent.table_id) {
                draft = existing;
                break;
            };
            if (draft == null) {
                if (drafts.items.len >= 128) return error.SqlProgramLimitExceeded;
                try drafts.append(alloc, .{ .table = parent });
                draft = &drafts.items[drafts.items.len - 1];
            }
            if (draft.?.foreign_keys.items.len >= 128) return error.SqlProgramLimitExceeded;
            try draft.?.foreign_keys.append(alloc, .{ .child_table_id = child.table_id, .child_table_name = child.name, .constraint_name = try alloc.dupe(u8, fk.name), .generation = before.generation, .next_generation = after.generation });
        }
    }
    const results = try alloc.alloc(stages.ExternalFkParent, drafts.items.len);
    var total_ranges: usize = 0;
    for (drafts.items, results) |draft, *result| {
        const names = try server.logicalTableNamesInArena(alloc, context, &.{draft.table.name});
        if (names.len != 1 or !try server_mod.tablePermissionCurrentlyAllowed(identity, names[0], .admin) or
            try server_mod.resolveEffectiveRowFilterJson(alloc, identity, names[0]) != null) return error.Forbidden;
        var ranges_list: std.ArrayList(records.RangeRecord) = .empty;
        for (snapshot.ranges) |range| if (range.table_id == draft.table.table_id) try ranges_list.append(alloc, range);
        total_ranges += ranges_list.items.len;
        if (ranges_list.items.len == 0 or total_ranges > 4096) return error.SqlProgramLimitExceeded;
        metadata.sortKeyspaceRanges(records.RangeRecord, ranges_list.items);
        const ranges = try ranges_list.toOwnedSlice(alloc);
        const fences = try alloc.alloc(@import("../storage/db/relational_integrity_topology_contract.zig").Fence, ranges.len);
        for (ranges, fences) |range, *fence| {
            try context.ensureActive();
            var response = (try reads.lookup(alloc, draft.table.name, range.start_key, .{ .relational_topology_json = "{\"mode\":\"identity\"}", .execution_deadline_ns = (try context.platformDeadline()).deadline_ns, .cancellation = context.cancellation }, .read_index)) orelse return error.TableNotFound;
            defer response.deinit(alloc);
            const Native = struct { namespace: @import("../storage/db/doc_identity.zig").Namespace, catalog_digest: [32]u8, next_epoch: u64 };
            const native = try std.json.parseFromSliceLeaky(Native, alloc, response.json, .{ .ignore_unknown_fields = true });
            if (native.namespace.table_id != draft.table.table_id or native.namespace.shard_id != metadata.rangeDocIdentityShardId(range) or native.namespace.range_id != metadata.rangeDocIdentityRangeId(range)) return error.TableGenerationChanged;
            fence.* = .{ .transition_id = std.mem.readInt(u64, id[0..8], .little), .attempt = 1, .admission_epoch = native.next_epoch, .owner_group_id = range.group_id, .peer_group_id = range.group_id, .role = .truncate_parent, .namespace = native.namespace, .catalog_digest = native.catalog_digest };
        }
        result.* = .{ .table = draft.table, .ranges = ranges, .fences = fences, .foreign_keys = draft.foreign_keys.items };
    }
    return results;
}

test {
    _ = @import("sql_truncate_test.zig");
}

/// Dependency adjacency is built once; closure is O(tables + FK edges).
/// CASCADE follows incoming references only, never silently empties parents.
pub fn select(alloc: std.mem.Allocator, tables: []const records.TableRecord, requested: []const []const u8, cascade: bool) ![]records.TableRecord {
    return selectInternal(alloc, tables, requested, cascade, false);
}

fn selectInternal(alloc: std.mem.Allocator, tables: []const records.TableRecord, requested: []const []const u8, cascade: bool, allow_external_parents: bool) ![]records.TableRecord {
    var names: std.StringHashMapUnmanaged(usize) = .empty;
    for (tables, 0..) |table, i| try names.put(alloc, table.name, i);
    const included = try alloc.alloc(bool, tables.len);
    @memset(included, false);
    var queue: std.ArrayList(usize) = .empty;
    // Resolve the caller's explicit targets before the O(all schemas + FK
    // edges) dependency walk. Bad names must not load unrelated schemas.
    for (requested) |name| {
        const index = names.get(name) orelse return error.TableNotFound;
        if (!included[index]) {
            included[index] = true;
            try queue.append(alloc, index);
        }
    }
    const incoming = try alloc.alloc(std.ArrayList(usize), tables.len);
    @memset(incoming, .empty);
    const Edge = struct { child: usize, parent: usize };
    var edges: std.ArrayList(Edge) = .empty;
    for (tables, 0..) |table, child| {
        for ([_][]const u8{ table.schema_json, table.read_schema_json }) |definition| {
            if (definition.len == 0) continue;
            var schema = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, definition);
            defer schema.deinit(alloc);
            if (schema.foreign_keys) |foreign_keys| for (foreign_keys.value) |fk| {
                const parent = names.get(fk.parent_table) orelse return error.ForeignKeyParentTableNotFound;
                try incoming[parent].append(alloc, child);
                try edges.append(alloc, .{ .child = child, .parent = parent });
            };
        }
    }
    var cursor: usize = 0;
    while (cursor < queue.items.len) : (cursor += 1) {
        if (queue.items.len > 128) return error.SqlProgramLimitExceeded;
        for (incoming[queue.items[cursor]].items) |child| if (!included[child]) {
            if (!cascade) return error.SqlTruncateReferenced;
            included[child] = true;
            try queue.append(alloc, child);
        };
    }
    // Until old-child inverse witness generations can be retired on untouched
    // parents, reject this boundary before admission. Never delete a parent
    // merely to make a cohort complete.
    if (!allow_external_parents) for (edges.items) |edge| if (included[edge.child] and !included[edge.parent]) return error.SqlTruncateExternalForeignKey;
    const result = try alloc.alloc(records.TableRecord, queue.items.len);
    for (queue.items, result) |index, *table| {
        table.* = tables[index];
        // Graph artifacts are owner-scoped and their exact declaration is
        // bound into the staged replacement below. Untouched graph tables do
        // not join this FK cohort merely because they have graph indexes.
    }
    std.mem.sort(records.TableRecord, result, {}, struct {
        fn less(_: void, a: records.TableRecord, b: records.TableRecord) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.less);
    return result;
}

test "SQL TRUNCATE planned child FK generation is table-incarnation bound" {
    const alloc = std.testing.allocator;
    const child =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["id"],"parent_table":"parents","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    var first = try plannedIntegrityCatalog(alloc, 700, child);
    defer first.deinit();
    var replay = try plannedIntegrityCatalog(alloc, 700, child);
    defer replay.deinit();
    var replacement = try plannedIntegrityCatalog(alloc, 701, child);
    defer replacement.deinit();
    const first_generation = first.catalog.find(.foreign_key, "fk").?.generation;
    try std.testing.expectEqualDeep(first_generation, replay.catalog.find(.foreign_key, "fk").?.generation);
    try std.testing.expect(!std.mem.eql(u8, &first_generation, &replacement.catalog.find(.foreign_key, "fk").?.generation));
}

fn generation(id: stages.Id, original: u64, ordinal: u64, label: []const u8) u64 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly truncate generation v1");
    hash.update(&id);
    hash.update(label);
    var numbers: [16]u8 = undefined;
    std.mem.writeInt(u64, numbers[0..8], original, .little);
    std.mem.writeInt(u64, numbers[8..16], ordinal, .little);
    hash.update(&numbers);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return @import("../common/group_ids.zig").dataGroupIdFromHash(std.mem.readInt(u64, digest[0..8], .little));
}

fn freeReceipt(alloc: std.mem.Allocator, receipt: catalog.DdlReceipt) void {
    alloc.free(receipt.database);
    alloc.free(receipt.namespace);
    alloc.free(receipt.table);
    alloc.free(receipt.table_id);
    if (receipt.restore_job_id) |id| alloc.free(id);
}

fn pendingReceipt(alloc: std.mem.Allocator, target: domain.Target, source_id: u64, schema_version: u32, job_id: u64) !catalog.DdlReceipt {
    var result: catalog.DdlReceipt = .{ .database = "", .namespace = "", .table = "", .table_id = "", .schema_version = schema_version, .state = .pending, .diagnostic = "TRUNCATE barrier admitted. Follow restore_job_id until publication; do not replay the statement." };
    errdefer freeReceipt(alloc, result);
    result.database = try alloc.dupe(u8, target.database);
    result.namespace = try alloc.dupe(u8, target.namespace);
    result.table = try alloc.dupe(u8, target.table);
    result.table_id = try std.fmt.allocPrint(alloc, "{d}", .{source_id});
    result.restore_job_id = try std.fmt.allocPrint(alloc, "{d}", .{job_id});
    return result;
}

pub fn execute(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, database: []const u8, namespace: []const u8, alloc: std.mem.Allocator, ddl: ast.CatalogDdl) !catalog.DdlOutcome {
    try context.ensureActive();
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    if (ddl.truncate_tables.len == 0 or ddl.truncate_tables.len > 128) return error.InvalidSqlSyntax;
    // SQL-owned sequences are not a catalog capability yet. Row identities
    // are supplied by the secure generator, not a table-local counter. Thus
    // RESTART IDENTITY has no sequence to reset in this catalog version; the
    // fresh table generation still resets all owner-local identity metadata.
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const targets = try a.alloc(domain.Target, ddl.truncate_tables.len);
    for (ddl.truncate_tables, targets) |name, *target| {
        target.* = .{ .database = name.database orelse database, .namespace = name.namespace orelse namespace, .table = name.table };
        try target.validate();
        const resource = try target.resourceNameAlloc(a);
        if (!try server_mod.tablePermissionCurrentlyAllowed(identity, resource, .admin)) return error.Forbidden;
        if (try server_mod.resolveEffectiveRowFilterJson(a, identity, resource) != null) return error.Forbidden;
    }
    const resolved = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, try server.source.systemCatalog(a, context, .{ .resolve_many = .{ .targets = targets } }), .{});
    if (resolved.tables.len != targets.len) return error.InvalidSqlBackendResponse;
    const logical_state = try std.json.parseFromSliceLeaky(domain.State, a, try server.source.systemCatalog(a, context, .snapshot), .{});
    var logical_index = try domain.StateIndex.init(a, logical_state);
    defer logical_index.deinit(a);
    for (resolved.tables, targets) |resolved_table, target| {
        const bound = resolved_table orelse return error.TableNotFound;
        const ns = try logical_index.namespaceFor(target.database, target.namespace);
        const current = logical_index.find(.table, ns.id, target.table) orelse return error.CatalogGenerationChanged;
        if (current.id != bound.table_id or !std.mem.eql(u8, current.storage_name, bound.name)) return error.CatalogGenerationChanged;
    }
    const requested = try a.alloc([]const u8, targets.len);
    for (resolved.tables, requested) |table, *name| name.* = (table orelse return error.TableNotFound).name;
    var snapshot = (try server.source.linearizableSnapshot(context)) orelse return error.MetadataCapabilityUnavailable;
    defer server.source.freeAdminSnapshot(&snapshot);
    for (resolved.tables) |resolved_table| {
        const bound = resolved_table.?;
        for (snapshot.tables) |table| {
            if (std.mem.eql(u8, bound.name, table.name) and bound.table_id == table.table_id) break;
        } else return error.CatalogGenerationChanged;
    }
    const selected = try selectInternal(a, snapshot.tables, requested, ddl.cascade, true);
    const names = try a.alloc([]const u8, selected.len);
    for (selected, names) |table, *name| {
        if (table.storage_migration != null or table.relational_retirement_json.len != 0 or table.restore_backup_id.len != 0) return error.TableTransitionActive;
        name.* = table.name;
    }
    for (try server.logicalTableNamesInArena(a, context, names)) |name| {
        if (!try server_mod.tablePermissionCurrentlyAllowed(identity, name, .admin)) return error.Forbidden;
        if (try server_mod.resolveEffectiveRowFilterJson(a, identity, name) != null) return error.Forbidden;
    }
    // The external-parent retirement protocol is staged, but its mounted
    // publication/ACK/restart proof is not complete. Check the exact same
    // selected cohort only after authorization, before owner reads or a
    // durable job ID. Do not disclose an FK dependency to another principal.
    _ = try selectInternal(a, snapshot.tables, requested, ddl.cascade, false);
    // Graph-derived state and metrics do not yet have a cutover proof
    // spanning the old and fresh owner generations. A pristine target alone
    // cannot prove dependent graph readers and workers are fenced. Check
    // only after authorization of the entire FK-closed cohort, but before
    // source reads or durable job admission.
    var has_graph = false;
    for (selected) |table| {
        const graph = try stages.hasGraphIndex(a, table.indexes_json);
        has_graph = has_graph or graph;
    }
    const principal = server_mod.storedDestinationPrincipal(identity);
    try server.requireEmptyGenerationAuthority(principal, names);
    if (has_graph and !consumeGraphTruncateTestPermit(selected)) return error.UnsupportedSqlExecution;
    var random: [16]u8 = undefined;
    try (server.restore_job_store.io orelse return error.AsyncRestoreUnavailable).randomSecure(&random);
    const key = std.fmt.bytesToHex(random, .lower);
    const idempotency_namespace = try std.fmt.allocPrint(a, "sql-truncate:{s}", .{principal});
    const job_id = try jobs.jobIdForIdempotency(a, idempotency_namespace, &key);
    const id = try stages.idForAttempt(job_id, 1);
    const planned = try a.alloc(stages.Target, selected.len);
    var total_ranges: usize = 0;
    for (selected, planned) |before, *target| {
        var old_ranges: std.ArrayList(records.RangeRecord) = .empty;
        for (snapshot.ranges) |range| if (range.table_id == before.table_id) try old_ranges.append(a, range);
        total_ranges += old_ranges.items.len;
        if (old_ranges.items.len == 0 or total_ranges > 4096) return error.SqlProgramLimitExceeded;
        metadata.sortKeyspaceRanges(records.RangeRecord, old_ranges.items);
        const ranges = try a.alloc(records.RangeRecord, old_ranges.items.len);
        const fences = try a.alloc(@import("../storage/db/relational_integrity_topology_contract.zig").Fence, ranges.len);
        const handoffs = try a.alloc(stages.GenerationHandoffRange, ranges.len);
        var table = before;
        table.table_id = generation(id, before.table_id, 0, "table");
        table.min_ranges = @intCast(ranges.len);
        for (old_ranges.items, ranges, fences, handoffs, 0..) |old, *range, *fence, *handoff, ordinal| {
            try context.ensureActive();
            const group = generation(id, before.table_id, ordinal, "group");
            range.* = .{ .table_id = table.table_id, .group_id = group, .range_id = group, .doc_identity_shard_id = group, .doc_identity_range_id = group, .start_key = old.start_key, .end_key = old.end_key };
            var response = (try (server.table_reads orelse return error.UnsupportedSqlExecution).lookup(a, before.name, old.start_key, .{ .relational_topology_json = "{\"mode\":\"identity\"}", .execution_deadline_ns = (try context.platformDeadline()).deadline_ns, .cancellation = context.cancellation }, .read_index)) orelse return error.TableNotFound;
            defer response.deinit(a);
            const Native = struct { namespace: @import("../storage/db/doc_identity.zig").Namespace, catalog_digest: [32]u8, next_epoch: u64 };
            const native = try std.json.parseFromSliceLeaky(Native, a, response.json, .{ .ignore_unknown_fields = true });
            if (native.namespace.table_id != before.table_id or native.namespace.shard_id != metadata.rangeDocIdentityShardId(old) or native.namespace.range_id != metadata.rangeDocIdentityRangeId(old)) return error.TableGenerationChanged;
            fence.* = .{ .transition_id = std.mem.readInt(u64, id[0..8], .little), .attempt = 1, .admission_epoch = native.next_epoch, .owner_group_id = old.group_id, .peer_group_id = group, .role = .rewrite_source, .namespace = native.namespace, .catalog_digest = native.catalog_digest };
            // A linearizable preview becomes an immutable Plan assertion, not
            // authority by itself. The coordinator will fence this source and
            // compare a second read-indexed summary before irreversible
            // cutover, catching any intervening FK admission or retirement.
            var summary_response = (try (server.table_reads orelse return error.UnsupportedSqlExecution).topologyStatus(a, before.name, old.start_key, "{\"mode\":\"generation_handoff_summary\"}")) orelse return error.TableGenerationChanged;
            defer summary_response.deinit(a);
            const summary = try std.json.parseFromSliceLeaky(@import("../storage/db/empty_generation_handoff.zig").Summary, a, summary_response.json, .{ .allocate = .alloc_always });
            if (!summary.namespace.eql(native.namespace) or summary.intent != null or summary.seal != null) return error.TableGenerationChanged;
            handoff.* = .{
                .source_group_id = old.group_id,
                .target_group_id = group,
                .source_namespace = summary.namespace,
                .admissions = summary.admissions,
                .admissions_digest = summary.admissions_digest,
                .retired_digest = summary.retired_digest,
                .retired_count = summary.retired_count,
            };
        }
        var binding = logical_index.byId(.table, before.table_id) orelse return error.CatalogGenerationChanged;
        if (!std.mem.eql(u8, binding.storage_name, before.name)) return error.CatalogGenerationChanged;
        binding.id = table.table_id;
        target.* = .{ .source_table_id = before.table_id, .table = table, .catalog_binding = binding, .ranges = ranges, .empty_generation = true, .graph_retirement_digest = try stages.graphRetirementDigest(a, before.table_id, table.table_id, table.indexes_json), .generation_handoffs = handoffs, .replace = .{ .table = before, .ranges = old_ranges.items, .fences = fences } };
    }
    try stages.prepareEmptyGenerationHandoffMappingsAlloc(a, planned);
    const external_parents = try buildExternalParents(server, identity, context, a, snapshot, selected, planned, id);
    if (external_parents.len != 0) {
        const parent_names = try a.alloc([]const u8, external_parents.len);
        for (external_parents, parent_names) |parent, *name| name.* = parent.table.name;
        try server.requireEmptyGenerationAuthority(principal, parent_names);
    }
    var digest: [32]u8 = undefined;
    const cohort_manifest = try std.json.Stringify.valueAlloc(a, .{ .targets = planned, .external_fk_parents = external_parents }, .{});
    std.crypto.hash.Blake3.hash(cohort_manifest, &digest, .{});
    const plan: stages.Plan = .{ .id = id, .cohort_digest = digest, .targets = planned, .external_fk_parents = external_parents };
    try plan.validate(a);
    const primary_target = for (planned) |target| {
        if (target.source_table_id == resolved.tables[0].?.table_id) break target.table;
    } else return error.InvalidRestoreStaging;
    var primary_schema = try @import("../schema/mod.zig").parseValidatedTableSchema(a, @import("tables.zig").effectiveSchemaJson(primary_target.schema_json));
    defer primary_schema.deinit(a);
    const receipt = try pendingReceipt(alloc, targets[0], resolved.tables[0].?.table_id, primary_schema.version, job_id);
    errdefer freeReceipt(alloc, receipt);
    // Allocate every receipt field before durable admission. There must be no
    // fallible result construction after an accepted (or unknown) proposal.
    try context.ensureActive();
    const accepted = try server.restore_job_store.startRecoverable(a, .{ .scope = .cluster, .source_kind = .empty_generation, .backup_id = &std.fmt.bytesToHex(digest, .lower), .location = try std.fmt.allocPrint(a, "metadata://empty-generation/{s}", .{std.fmt.bytesToHex(id, .lower)}), .connection = "internal", .restore_mode = "overwrite", .table_names = names, .idempotency_namespace = idempotency_namespace, .idempotency_key = &key, .destination_authorization_principal = principal, .generation_plan_json = try std.json.Stringify.valueAlloc(a, plan, .{}) });
    server.schedulePendingRestoreJobs() catch {};
    var result = receipt;
    if (accepted == .unknown) {
        result.state = .admission_unknown;
        result.diagnostic = "TRUNCATE admission is unresolved. Reconcile restore_job_id; do not replay the statement.";
    }
    return .{ .mutation_outcome = if (accepted == .unknown) null else .committed_pending, .receipt = result };
}

test "SQL TRUNCATE closure follows incoming FKs without emptying an untouched parent" {
    // sql-0162 RESTRICT and sql-0165 CASCADE admission semantics.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const parent =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const child =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["id"],"parent_table":"parents","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const tables = [_]records.TableRecord{
        .{ .table_id = 10, .name = "parents", .schema_json = parent },
        .{ .table_id = 11, .name = "children", .schema_json = child },
        .{ .table_id = 12, .name = "unrelated" },
    };
    try std.testing.expectError(error.SqlTruncateReferenced, select(a, &tables, &.{"parents"}, false));
    try std.testing.expectError(error.SqlTruncateExternalForeignKey, select(a, &tables, &.{"children"}, true));
    const staged_child = try selectInternal(a, &tables, &.{"children"}, true, true);
    try std.testing.expectEqual(@as(usize, 1), staged_child.len);
    try std.testing.expectEqualStrings("children", staged_child[0].name);
    const cascade = try select(a, &tables, &.{"parents"}, true);
    try std.testing.expectEqual(@as(usize, 2), cascade.len);
    try std.testing.expectEqualStrings("children", cascade[0].name);
    try std.testing.expectEqualStrings("parents", cascade[1].name);
    try std.testing.expectEqual(@as(usize, 2), (try select(a, &tables, &.{ "children", "parents", "parents" }, false)).len);
    const document = try select(a, &tables, &.{"unrelated"}, false);
    try std.testing.expectEqual(@as(usize, 1), document.len);
}

test "SQL TRUNCATE graph declarations stay scoped to the selected FK cohort" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const tables = [_]records.TableRecord{
        .{ .table_id = 10, .name = "plain" },
        .{ .table_id = 11, .name = "graph", .indexes_json = "{\"graph_idx\":{\"type\":\"graph\"}}" },
    };
    const plain = try select(a, &tables, &.{"plain"}, false);
    try std.testing.expectEqual(@as(usize, 1), plain.len);
    try std.testing.expectEqualStrings("plain", plain[0].name);
    const graph = try select(a, &tables, &.{"graph"}, false);
    try std.testing.expectEqual(@as(usize, 1), graph.len);
    try std.testing.expectEqualStrings("graph", graph[0].name);

    const relational = [_]records.TableRecord{
        .{ .table_id = 20, .name = "parent" },
        .{ .table_id = 21, .name = "child", .indexes_json = "{\"graph_idx\":{\"type\":\"graph\"}}", .schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["id"],"parent_table":"parent","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        },
    };
    const cascade = try select(a, &relational, &.{"parent"}, true);
    try std.testing.expectEqual(@as(usize, 2), cascade.len);
}

test "SQL TRUNCATE unknown target fails before unrelated schema parsing" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const tables = [_]records.TableRecord{.{ .table_id = 10, .name = "unrelated", .schema_json = "not json" }};
    try std.testing.expectError(error.TableNotFound, select(arena.allocator(), &tables, &.{"missing"}, false));
}
