// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! TRUNCATE is a durable, fenced fresh-generation publication. No row scan,
//! delete batch, source artifact, or retained tail is involved.
const std = @import("std");
const server_mod = @import("http_server.zig");
const catalog = @import("../sql/catalog.zig");
const ast = @import("../sql/ast.zig");
const domain = @import("../system_catalog/domain.zig");
const records = @import("../common/topology_records.zig");
const metadata = @import("../metadata/table_manager.zig");
const stages = @import("../metadata/restore_staging.zig");
const jobs = @import("restore_jobs.zig");
const operation = @import("operation.zig");

test {
    _ = @import("sql_truncate_test.zig");
}

/// Dependency adjacency is built once; closure is O(tables + FK edges).
/// CASCADE follows incoming references only, never silently empties parents.
pub fn select(alloc: std.mem.Allocator, tables: []const records.TableRecord, requested: []const []const u8, cascade: bool) ![]records.TableRecord {
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
    for (edges.items) |edge| if (included[edge.child] and !included[edge.parent]) return error.SqlTruncateExternalForeignKey;
    const result = try alloc.alloc(records.TableRecord, queue.items.len);
    for (queue.items, result) |index, *table| {
        table.* = tables[index];
        // A graph artifact on another table cannot block this cohort. Parse
        // index declarations only for actual participants after FK closure.
        const indexes = try std.json.parseFromSliceLeaky(std.json.Value, alloc, table.indexes_json, .{});
        if (indexes != .object) return error.InvalidRestoreStaging;
        for (indexes.object.values()) |decl| if (decl == .object) {
            if (decl.object.get("type")) |kind| {
                if (kind == .string and std.mem.eql(u8, kind.string, "graph")) return error.SqlTruncateGraphDependency;
            }
        };
    }
    std.mem.sort(records.TableRecord, result, {}, struct {
        fn less(_: void, a: records.TableRecord, b: records.TableRecord) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.less);
    return result;
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
    const selected = try select(a, snapshot.tables, requested, ddl.cascade);
    const names = try a.alloc([]const u8, selected.len);
    for (selected, names) |table, *name| {
        if (table.storage_migration != null or table.relational_retirement_json.len != 0 or table.restore_backup_id.len != 0) return error.TableTransitionActive;
        name.* = table.name;
    }
    for (try server.logicalTableNamesInArena(a, context, names)) |name| {
        if (!try server_mod.tablePermissionCurrentlyAllowed(identity, name, .admin)) return error.Forbidden;
        if (try server_mod.resolveEffectiveRowFilterJson(a, identity, name) != null) return error.Forbidden;
    }
    const principal = server_mod.storedDestinationPrincipal(identity);
    try server.requireEmptyGenerationAuthority(principal, names);
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
        var table = before;
        table.table_id = generation(id, before.table_id, 0, "table");
        table.min_ranges = @intCast(ranges.len);
        for (old_ranges.items, ranges, fences, 0..) |old, *range, *fence, ordinal| {
            try context.ensureActive();
            const group = generation(id, before.table_id, ordinal, "group");
            range.* = .{ .table_id = table.table_id, .group_id = group, .range_id = group, .doc_identity_shard_id = group, .doc_identity_range_id = group, .start_key = old.start_key, .end_key = old.end_key };
            var response = (try (server.table_reads orelse return error.UnsupportedSqlExecution).lookup(a, before.name, old.start_key, .{ .relational_topology_json = "{\"mode\":\"identity\"}", .execution_deadline_ns = (try context.platformDeadline()).deadline_ns, .cancellation = context.cancellation }, .read_index)) orelse return error.TableNotFound;
            defer response.deinit(a);
            const Native = struct { namespace: @import("../storage/db/doc_identity.zig").Namespace, catalog_digest: [32]u8, next_epoch: u64 };
            const native = try std.json.parseFromSliceLeaky(Native, a, response.json, .{ .ignore_unknown_fields = true });
            if (native.namespace.table_id != before.table_id or native.namespace.shard_id != metadata.rangeDocIdentityShardId(old) or native.namespace.range_id != metadata.rangeDocIdentityRangeId(old)) return error.TableGenerationChanged;
            fence.* = .{ .transition_id = std.mem.readInt(u64, id[0..8], .little), .attempt = 1, .admission_epoch = native.next_epoch, .owner_group_id = old.group_id, .peer_group_id = group, .role = .rewrite_source, .namespace = native.namespace, .catalog_digest = native.catalog_digest };
        }
        var binding = logical_index.byId(.table, before.table_id) orelse return error.CatalogGenerationChanged;
        if (!std.mem.eql(u8, binding.storage_name, before.name)) return error.CatalogGenerationChanged;
        binding.id = table.table_id;
        target.* = .{ .source_table_id = before.table_id, .table = table, .catalog_binding = binding, .ranges = ranges, .empty_generation = true, .replace = .{ .table = before, .ranges = old_ranges.items, .fences = fences } };
    }
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(try std.json.Stringify.valueAlloc(a, planned, .{}), &digest, .{});
    const plan: stages.Plan = .{ .id = id, .cohort_digest = digest, .targets = planned };
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
    const cascade = try select(a, &tables, &.{"parents"}, true);
    try std.testing.expectEqual(@as(usize, 2), cascade.len);
    try std.testing.expectEqualStrings("children", cascade[0].name);
    try std.testing.expectEqualStrings("parents", cascade[1].name);
    try std.testing.expectEqual(@as(usize, 2), (try select(a, &tables, &.{ "children", "parents", "parents" }, false)).len);
    const document = try select(a, &tables, &.{"unrelated"}, false);
    try std.testing.expectEqual(@as(usize, 1), document.len);
}

test "SQL TRUNCATE graph guard applies only to the selected FK cohort" {
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
    try std.testing.expectError(error.SqlTruncateGraphDependency, select(a, &tables, &.{"graph"}, false));

    const relational = [_]records.TableRecord{
        .{ .table_id = 20, .name = "parent" },
        .{ .table_id = 21, .name = "child", .indexes_json = "{\"graph_idx\":{\"type\":\"graph\"}}", .schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["id"],"parent_table":"parent","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        },
    };
    try std.testing.expectError(error.SqlTruncateGraphDependency, select(a, &relational, &.{"parent"}, true));
}

test "SQL TRUNCATE unknown target fails before unrelated schema parsing" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const tables = [_]records.TableRecord{.{ .table_id = 10, .name = "unrelated", .schema_json = "not json" }};
    try std.testing.expectError(error.TableNotFound, select(arena.allocator(), &tables, &.{"missing"}, false));
}
