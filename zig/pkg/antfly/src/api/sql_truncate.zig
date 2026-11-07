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

//! TRUNCATE is a durable, fenced fresh-generation publication. No row scan,
//! delete batch, source artifact, or retained tail is involved.
const std = @import("std");
const server_mod = @import("http_server.zig");
const catalog = @import("antfly_local_sources").sql_catalog;
const ast = @import("antfly_local_sources").sql_ast;
const domain = @import("antfly_local_sources").system_catalog_domain;
const records = @import("antfly_local_sources").common_topology_records;
const metadata = @import("../metadata/table_manager.zig");
const stages = @import("../metadata/restore_staging.zig");
const jobs = @import("restore_jobs.zig");
const operation = @import("antfly_local_sources").api_operation;
const integrity_catalog = @import("antfly_local_sources").storage_db_relational_integrity_catalog;

/// Predict the fresh child identities with the exact owner compiler, before
/// the hidden generation exists. This is not a hash of a policy name: the
/// generation allocation order, typed declaration fingerprint, and table
/// incarnation must match the owner-side catalog byte-for-byte.
fn plannedIntegrityCatalog(alloc: std.mem.Allocator, table_id: u64, schema_json: []const u8) !integrity_catalog.Update {
    const schema_api = @import("antfly_local_sources").schema_mod;
    const native_schema = @import("antfly_local_sources").storage_schema;
    const declarations = @import("antfly_local_sources").schema_relational_declarations;
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

fn currentIntegrityCatalog(alloc: std.mem.Allocator, source: @import("antfly_local_sources").api_table_read_source.TableReadSource, table: records.TableRecord) !integrity_catalog.Catalog {
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
        var parsed = try @import("antfly_local_sources").schema_mod.parseValidatedTableSchema(alloc, @import("tables.zig").effectiveSchemaJson(child.schema_json));
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
        const fences = try alloc.alloc(@import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Fence, ranges.len);
        for (ranges, fences) |range, *fence| {
            try context.ensureActive();
            var response = (try reads.lookup(alloc, draft.table.name, range.start_key, .{ .relational_topology_json = "{\"mode\":\"identity\"}", .execution_deadline_ns = (try context.platformDeadline()).deadline_ns, .cancellation = context.cancellation }, .read_index)) orelse return error.TableNotFound;
            defer response.deinit(alloc);
            const Native = @import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Identity;
            const native = try std.json.parseFromSliceLeaky(Native, alloc, response.json, .{ .ignore_unknown_fields = true });
            if (native.namespace.table_id != draft.table.table_id or native.namespace.shard_id != metadata.rangeDocIdentityShardId(range) or native.namespace.range_id != metadata.rangeDocIdentityRangeId(range)) return error.TableGenerationChanged;
            try native.requireGenerationHandoffReceipts();
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
    return selectInternal(alloc, tables, requested, cascade);
}

fn selectInternal(alloc: std.mem.Allocator, tables: []const records.TableRecord, requested: []const []const u8, cascade: bool) ![]records.TableRecord {
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
    var schema_scratch = std.heap.ArenaAllocator.init(alloc);
    defer schema_scratch.deinit();
    const incoming = try alloc.alloc(std.ArrayList(usize), tables.len);
    @memset(incoming, .empty);
    for (tables, 0..) |table, child| {
        for ([_][]const u8{ table.schema_json, table.read_schema_json }) |definition| {
            if (definition.len == 0) continue;
            _ = schema_scratch.reset(.retain_capacity);
            const temporary = schema_scratch.allocator();
            var schema = try @import("antfly_local_sources").schema_mod.parseValidatedTableSchema(temporary, definition);
            defer schema.deinit(temporary);
            if (schema.foreign_keys) |foreign_keys| for (foreign_keys.value) |fk| {
                const parent = names.get(fk.parent_table) orelse return error.ForeignKeyParentTableNotFound;
                try incoming[parent].append(alloc, child);
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
    // Outgoing parents remain untouched. Their accepted child generations
    // are switched by the separately fenced retirement/ACK portion of Plan.
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
    return @import("antfly_local_sources").common_group_ids.dataGroupIdFromHash(std.mem.readInt(u64, digest[0..8], .little));
}

fn freeReceipt(alloc: std.mem.Allocator, receipt: catalog.DdlReceipt) void {
    alloc.free(receipt.database);
    alloc.free(receipt.namespace);
    alloc.free(receipt.table);
    alloc.free(receipt.table_id);
    if (receipt.restore_job_id) |id| alloc.free(id);
    if (receipt.idempotency_key) |key| alloc.free(key);
}

fn pendingReceipt(alloc: std.mem.Allocator, target: domain.Target, source_id: u64, schema_version: u32, job_id: u64, key: []const u8) !catalog.DdlReceipt {
    var result: catalog.DdlReceipt = .{ .database = "", .namespace = "", .table = "", .table_id = "", .schema_version = schema_version, .state = .pending, .diagnostic = "TRUNCATE barrier admitted. Follow restore_job_id until publication; do not replay the statement." };
    errdefer freeReceipt(alloc, result);
    result.database = try alloc.dupe(u8, target.database);
    result.namespace = try alloc.dupe(u8, target.namespace);
    result.table = try alloc.dupe(u8, target.table);
    result.table_id = try std.fmt.allocPrint(alloc, "{d}", .{source_id});
    result.restore_job_id = try std.fmt.allocPrint(alloc, "{d}", .{job_id});
    result.idempotency_key = try alloc.dupe(u8, key);
    return result;
}

pub fn execute(server: *server_mod.ApiHttpServer, identity: ?server_mod.AuthenticatedIdentity, context: operation.RequestContext, database: []const u8, namespace: []const u8, alloc: std.mem.Allocator, ddl: ast.CatalogDdl) !catalog.DdlOutcome {
    var owned_identity = identity;
    var root: @import("sql_execution.zig").Adapter = .{ .server = server, .identity = &owned_identity, .context = context, .database = database, .namespace = namespace };
    defer root.releaseExecutionOwners();
    defer freeCapturedNotices(alloc, root.result_ddl_notices);
    return executeWithAdapter(&root, alloc, ddl);
}

pub fn executeWithAdapter(root: *@import("sql_execution.zig").Adapter, alloc: std.mem.Allocator, ddl: ast.CatalogDdl) !catalog.DdlOutcome {
    const server = root.server;
    const identity = root.identity.*;
    const context = root.context;
    const database = root.database;
    const namespace = root.namespace;
    try context.ensureActive();
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    if (ddl.truncate_tables.len == 0 or ddl.truncate_tables.len > 128) return error.InvalidSqlSyntax;
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
        try bound.requireRequestAccess(context.native_service, context.sql_relation_owner);
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
        const binding = logical_index.byId(.table, table.table_id) orelse return error.CatalogGenerationChanged;
        const access: domain.ResolvedTable = .{ .table_id = table.table_id, .name = table.name, .native_service = binding.native_service, .sql_persistence = table.sql_persistence };
        try access.requireRequestAccess(context.native_service, context.sql_relation_owner);
        name.* = table.name;
    }
    for (try server.logicalTableNamesInArena(a, context, names)) |name| {
        if (!try server_mod.tablePermissionCurrentlyAllowed(identity, name, .admin)) return error.Forbidden;
        if (try server_mod.resolveEffectiveRowFilterJson(a, identity, name) != null) return error.Forbidden;
    }
    const principal = server_mod.storedDestinationPrincipal(identity);
    try server.requireEmptyGenerationAuthority(principal, names);
    var random: [16]u8 = undefined;
    try (server.sharedApiIo() orelse return error.AsyncRestoreUnavailable).randomSecure(&random);
    const key = std.fmt.bytesToHex(random, .lower);
    const idempotency_namespace = try std.fmt.allocPrint(a, "sql-truncate:{s}", .{principal});
    const job_id = try jobs.jobIdForIdempotency(a, idempotency_namespace, &key);
    const id = try stages.idForAttempt(job_id, 1);
    var planned: []const stages.Target = try a.alloc(stages.Target, selected.len);
    var next_relation_oid = logical_state.next_id;
    var total_ranges: usize = 0;
    for (selected, @constCast(planned)) |before, *target| {
        var old_ranges: std.ArrayList(records.RangeRecord) = .empty;
        for (snapshot.ranges) |range| if (range.table_id == before.table_id) try old_ranges.append(a, range);
        total_ranges += old_ranges.items.len;
        if (old_ranges.items.len == 0 or total_ranges > 4096) return error.SqlProgramLimitExceeded;
        metadata.sortKeyspaceRanges(records.RangeRecord, old_ranges.items);
        const ranges = try a.alloc(records.RangeRecord, old_ranges.items.len);
        const fences = try a.alloc(@import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Fence, ranges.len);
        const handoffs = try a.alloc(stages.GenerationHandoffRange, ranges.len);
        var table = before;
        table.table_id = generation(id, before.table_id, 0, "table");
        if (before.sql_persistence) |previous| {
            var lifecycle_context = context;
            lifecycle_context.setting_admin = true;
            const bytes = try server.source.systemCatalog(a, lifecycle_context, .{ .sql_relation_boot_read = .{ .node = previous.owner_node_id } });
            const observed = try std.json.parseFromSliceLeaky(@import("../metadata/sql_relation_boots.zig").Observation, a, bytes, .{});
            const current = try observed.requireReady();
            if (current.identity.store_id != previous.owner_store_id or
                (previous.kind == .temporary and !std.mem.eql(u8, &previous.generation_instance, &current.instance))) return error.SqlRelationOwnerChanged;
            var successor = previous;
            successor.generation_instance = current.instance;
            successor.data_generation = std.math.add(u64, previous.data_generation, 1) catch return error.CatalogGenerationExhausted;
            table.sql_persistence = successor;
        }
        table.min_ranges = @intCast(ranges.len);
        for (old_ranges.items, ranges, fences, handoffs, 0..) |old, *range, *fence, *handoff, ordinal| {
            try context.ensureActive();
            const group = generation(id, before.table_id, ordinal, "group");
            range.* = .{ .table_id = table.table_id, .group_id = group, .range_id = group, .doc_identity_shard_id = group, .doc_identity_range_id = group, .start_key = old.start_key, .end_key = old.end_key };
            var response = (try (server.table_reads orelse return error.UnsupportedSqlExecution).lookup(a, before.name, old.start_key, .{ .relational_topology_json = "{\"mode\":\"identity\"}", .execution_deadline_ns = (try context.platformDeadline()).deadline_ns, .cancellation = context.cancellation }, .read_index)) orelse return error.TableNotFound;
            defer response.deinit(a);
            const Native = @import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Identity;
            const native = try std.json.parseFromSliceLeaky(Native, a, response.json, .{ .ignore_unknown_fields = true });
            if (native.namespace.table_id != before.table_id or native.namespace.shard_id != metadata.rangeDocIdentityShardId(old) or native.namespace.range_id != metadata.rangeDocIdentityRangeId(old)) return error.TableGenerationChanged;
            try native.requireGenerationHandoffReceipts();
            fence.* = .{ .transition_id = std.mem.readInt(u64, id[0..8], .little), .attempt = 1, .admission_epoch = native.next_epoch, .owner_group_id = old.group_id, .peer_group_id = group, .role = .rewrite_source, .namespace = native.namespace, .catalog_digest = native.catalog_digest };
            // A linearizable preview becomes an immutable Plan assertion, not
            // authority by itself. The coordinator will fence this source and
            // compare a second read-indexed summary before irreversible
            // cutover, catching any intervening FK admission or retirement.
            var summary_response = (try (server.table_reads orelse return error.UnsupportedSqlExecution).lookup(a, before.name, old.start_key, .{ .relational_topology_json = "{\"mode\":\"generation_handoff_summary\"}", .execution_deadline_ns = (try context.platformDeadline()).deadline_ns, .cancellation = context.cancellation }, .read_index)) orelse return error.TableGenerationChanged;
            defer summary_response.deinit(a);
            const summary = try std.json.parseFromSliceLeaky(@import("antfly_local_sources").storage_db_empty_generation_handoff.Summary, a, summary_response.json, .{ .allocate = .alloc_always });
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
        if (binding.sql_relation_id == null) {
            try domain.requireRelationOid(next_relation_oid);
            if (logical_index.tableByLogicalRelationId(next_relation_oid) != null) return error.InvalidCatalogRecord;
            binding.sql_relation_id = next_relation_oid;
            next_relation_oid += 1;
        }
        binding = try binding.withPhysicalRelationId(table.table_id);
        target.* = .{ .source_table_id = before.table_id, .table = table, .catalog_binding = binding, .ranges = ranges, .empty_generation = true, .graph_retirement_digest = try stages.graphRetirementDigest(a, before.table_id, table.table_id, table.indexes_json), .generation_handoffs = handoffs, .replace = .{ .table = before, .ranges = old_ranges.items, .fences = fences } };
    }
    var sequence_context = context;
    sequence_context.setting_admin = true;
    const sequence_bytes = try server.source.systemCatalog(a, sequence_context, .{ .sql_column_sequence_handoffs = .{ .targets = planned, .restart = ddl.restart_identity } });
    planned = try std.json.parseFromSliceLeaky([]const stages.Target, a, sequence_bytes, .{});
    try stages.prepareEmptyGenerationHandoffMappingsAlloc(a, @constCast(planned));
    const external_parents = try buildExternalParents(server, identity, context, a, snapshot, selected, planned, id);
    if (external_parents.len != 0) {
        const parent_names = try a.alloc([]const u8, external_parents.len);
        for (external_parents, parent_names) |parent, *name| name.* = parent.table.name;
        try server.requireEmptyGenerationAuthority(principal, parent_names);
    }
    var digest: [32]u8 = undefined;
    const cohort_manifest = try std.json.Stringify.valueAlloc(a, .{ .targets = planned, .external_fk_parents = external_parents }, .{});
    std.crypto.hash.Blake3.hash(cohort_manifest, &digest, .{});
    // Both phases run in the closed captured VM, before admission. Its eager
    // notice callback is private: errors and unknown proposals expose nothing.
    var private_notices: catalog.notices.Buffer = .{};
    const previous_sink = root.notice_sink;
    root.notice_sink = .{ .ptr = &private_notices, .emit = captureNotice, .reserve = reserveNotices };
    defer root.notice_sink = previous_sink;
    var capture: TruncateCapture = .{ .a = a, .root = root };
    defer if (capture.broker) |broker| broker.deinit();
    try prepareTruncateStatements(a, selected, &capture);
    const before_notice_count = private_notices.count;
    if (capture.broker) |broker| try broker.finish();
    const receipts: []const catalog.trigger_handoff.Receipt = if (capture.broker) |broker| try broker.receipts(a) else &.{};
    // Only BEFORE notices need a result owner now. The complete private
    // buffer is serialized into the Plan before this stack frame ends.
    const canonical_notices = try copyCapturedNotices(alloc, &private_notices, before_notice_count);
    var notices_transferred = false;
    defer if (!notices_transferred) freeCapturedNotices(alloc, canonical_notices);
    const durable_notices = try a.alloc(stages.TruncateNotice, private_notices.count);
    for (durable_notices, 0..) |*out, ordinal| {
        const notice = private_notices.get(ordinal);
        out.* = .{ .code = notice.code, .message = notice.message, .severity = notice.severity };
    }
    const plan: stages.Plan = .{ .id = id, .cohort_digest = digest, .targets = planned, .external_fk_parents = external_parents, .truncate_preparation = if (receipts.len == 0) null else .{
        .format_version = 1,
        .vm_contract_version = catalog.trigger_runtime.predecision_contract_version,
        .plan_id = id,
        .receipts = receipts,
        .before_notice_count = before_notice_count,
        .notices = durable_notices,
    } };
    try plan.validate(a);
    const primary_target = for (planned) |target| {
        if (target.source_table_id == resolved.tables[0].?.table_id) break target.table;
    } else return error.InvalidRestoreStaging;
    var primary_schema = try @import("antfly_local_sources").schema_mod.parseValidatedTableSchema(a, @import("tables.zig").effectiveSchemaJson(primary_target.schema_json));
    defer primary_schema.deinit(a);
    const receipt = try pendingReceipt(alloc, targets[0], resolved.tables[0].?.table_id, primary_schema.version, job_id, &key);
    errdefer freeReceipt(alloc, receipt);
    // Allocate every receipt field before durable admission. There must be no
    // fallible result construction after an accepted (or unknown) proposal.
    try context.ensureActive();
    const location = try std.fmt.allocPrint(a, "metadata://empty-generation/{s}", .{std.fmt.bytesToHex(id, .lower)});
    const generation_plan = try std.json.Stringify.valueAlloc(a, plan, .{});
    const job_control = @import("../metadata/query_population_job.zig");
    const start_request: jobs.StartRequest = .{ .scope = .cluster, .source_kind = .empty_generation, .routine_actor = if (try @import("sql_trigger_restore.zig").needsActorForPlan(a, plan)) try @import("sql_trigger_restore.zig").actorForAdmission(server, identity) else null, .backup_id = &std.fmt.bytesToHex(digest, .lower), .location = location, .connection = "internal", .restore_mode = "overwrite", .table_names = names, .idempotency_namespace = idempotency_namespace, .idempotency_key = &key, .destination_authorization_principal = principal, .generation_plan_json = generation_plan };
    const reference: job_control.Reference = .{ .job_id = job_id, .namespace = idempotency_namespace, .key = &key, .fingerprint = try jobs.requestFingerprintAlloc(a, start_request), .plan_digest = try plan.digest(a) };
    const completion_owner: operation.SQLCompletionOwner = .{ .id = job_id, .schema_version = primary_schema.version, .kind = .restore };
    try @import("sql_completion.zig").prepareMetadataWithContext(context, completion_owner);
    context.ensureActive() catch |err| {
        try @import("sql_completion.zig").rejectMetadataBeforeSubmission(context, completion_owner);
        return err;
    };
    var submitted = false;
    defer if (submitted) context.markSQLSubmission();
    var admission_context = context;
    admission_context.setting_admin = true;
    admission_context.sql_completion_submission = &submitted;
    const admission = job_control.admit(server, a, admission_context, start_request) catch |err| {
        if (err == error.MetadataMutationOutcomeUnknown) {
            submitted = true;
            var unknown = receipt;
            unknown.state = .admission_unknown;
            unknown.diagnostic = "TRUNCATE admission is unresolved. Reconcile restore_job_id; do not replay the statement.";
            return .{ .mutation_outcome = null, .receipt = unknown };
        }
        if (!submitted) try @import("sql_completion.zig").rejectMetadataBeforeSubmission(context, completion_owner);
        try @import("sql_completion.zig").rejectMetadataWithContext(context, completion_owner, err);
        return err;
    };
    const accepted = admission.result;
    const state_bytes = switch (accepted) {
        inline else => |bytes| bytes,
    };
    defer a.free(state_bytes);
    var unknown_admission = accepted == .unknown;
    var known_admission = false;
    if (!unknown_admission) {
        const state = std.json.parseFromSliceLeaky(jobs.JobState, a, state_bytes, .{}) catch null;
        if (state) |actual| {
            reference.requireState(a, actual) catch {
                unknown_admission = true;
            };
            if (actual.phase != .failed and (admission.plan_digest == null or !std.mem.eql(u8, &admission.plan_digest.?, &reference.plan_digest))) unknown_admission = true;
            known_admission = actual.phase != .failed and !unknown_admission;
        } else unknown_admission = true;
    }
    if (server.restore_job_store.replicated != null) server.schedulePendingRestoreJobs() catch {};
    var result = receipt;
    if (unknown_admission) {
        submitted = true;
        result.state = .admission_unknown;
        result.diagnostic = "TRUNCATE admission is unresolved. Reconcile restore_job_id; do not replay the statement.";
    }
    if (known_admission) {
        root.result_ddl_notices = canonical_notices;
        notices_transferred = true;
    }
    return .{ .mutation_outcome = if (unknown_admission) null else .committed_pending, .receipt = result };
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
    const staged_child = try select(a, &tables, &.{"children"}, false);
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

const NativeResetDiagnosticStage = enum { planning, existing_job, admitted };
var native_reset_job_observations: std.atomic.Value(u16) = .init(0);
fn diagnoseNativeResetJob(a: std.mem.Allocator, bytes: []const u8) void {
    if (comptime @import("builtin").mode == .debug) {
        var parsed = std.json.parseFromSlice(jobs.JobState, a, bytes, .{}) catch return;
        defer parsed.deinit();
        const job = parsed.value;
        const has_reason = job.last_error != null or job.staging_failure.len != 0;
        const bit = @as(u16, 1) << @as(u4, @intCast(@as(u16, @backingInt(job.phase)) * 2 + @as(u16, @intFromBool(has_reason))));
        if (native_reset_job_observations.fetchOr(bit, .monotonic) & bit != 0) return;
        const errors = @import("antfly_runtime_abi").error_abi;
        const last = if (job.last_error) |reason| errors.errorFromStableName(reason) else null;
        const failure = errors.errorFromStableName(job.staging_failure);
        // Only registered error symbols are printable. Arbitrary persisted
        // repository/user messages never enter this diagnostic.
        std.debug.print("native reset job phase={s} attempt={d} staged={any} resolution={s} owner_phase={d} owner_cursor={d} validation_phase={d} validation_owner={d} last_error={s} last_error_bytes={d} failure={s} failure_bytes={d}\n", .{ @tagName(job.phase), job.attempt_id, job.staging_attempt_id != 0, @tagName(job.staging_resolution), job.staging_owner_phase, job.staging_owner_cursor, job.staging_validation_phase, job.staging_validation_owner, if (last) |err| @errorName(err) else "none_or_unregistered", if (job.last_error) |reason| reason.len else 0, if (failure) |err| @errorName(err) else "none_or_unregistered", job.staging_failure.len });
    }
}

var native_reset_trace_seen: [3]std.atomic.Value(bool) = .{ .init(false), .init(false), .init(false) };
fn diagnoseNativeReset(stage: NativeResetDiagnosticStage, err: anyerror, _: ?*std.builtin.StackTrace) void {
    if (comptime @import("builtin").mode == .debug) {
        if (native_reset_trace_seen[@backingInt(stage)].swap(true, .monotonic)) return;
        std.debug.print("native reset catch stage={s} err={s}\n", .{ @tagName(stage), @errorName(err) });
    }
}

/// Native restart origin. No user trigger, RLS bypass masquerading as a SQL
/// principal, source JSON scan, or source-row replay participates in this path.
/// Every irreversible loss is independently rejoined by native metadata admit.
pub fn recoverNativeLoss(server: *server_mod.ApiHttpServer, backing: std.mem.Allocator, boot: @import("../metadata/sql_relation_boots.zig").Record, entry: @import("../metadata/sql_relation_lifetimes.zig").Entry, input: operation.RequestContext) !void {
    var diagnostic_stage: NativeResetDiagnosticStage = .planning;
    return recoverNativeLossObserved(server, backing, boot, entry, input, &diagnostic_stage) catch |err| {
        diagnoseNativeReset(diagnostic_stage, err, @errorReturnTrace());
        return err;
    };
}

fn recoverNativeLossObserved(server: *server_mod.ApiHttpServer, backing: std.mem.Allocator, boot: @import("../metadata/sql_relation_boots.zig").Record, entry: @import("../metadata/sql_relation_lifetimes.zig").Entry, input: operation.RequestContext, diagnostic_stage: *NativeResetDiagnosticStage) !void {
    const persistence = @import("antfly_local_sources").common_topology_records.RelationPersistence;
    const recovery = @import("../metadata/sql_relation_recovery.zig");
    try input.ensureActive();
    try entry.validate();
    const original = entry.initialized orelse return error.SqlRelationRecoveryIncomplete;
    if (entry.descriptor.kind != .unlogged or boot.phase != .recovering or !boot.unclean_restart) return error.InvalidSqlRelationPersistence;
    const old_pin = try persistence.PhysicalPin.fromBinding(original.binding);
    // This operation's immutable identity is independent of catalog revision;
    // a lost admission reply must discover the admitted job before replanning.
    var key_hash = std.crypto.hash.Blake3.init(.{});
    key_hash.update("antfly/native-unlogged-reset-job/v1\x00");
    key_hash.update(&try old_pin.encode());
    key_hash.update(&boot.instance);
    var generation_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &generation_bytes, boot.generation, .little);
    key_hash.update(&generation_bytes);
    var operation_digest: [32]u8 = undefined;
    key_hash.final(&operation_digest);
    const key = std.fmt.bytesToHex(operation_digest, .lower);
    const idempotency_namespace = "native-unlogged-recovery";
    const job_id = try jobs.jobIdForIdempotency(backing, idempotency_namespace, &key);
    if (try server.restore_job_store.load(backing, job_id)) |existing| {
        diagnoseNativeResetJob(backing, existing);
        backing.free(existing);
        diagnostic_stage.* = .existing_job;
        try server.schedulePendingRestoreJobs();
        return error.SqlRelationRecoveryIncomplete;
    }
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    var context = input;
    context.setting_admin = true;
    var captures: std.ArrayList(RecoveryCapture) = .empty;
    const root = try loadRecoveryCapture(server, a, context, &captures, .{ .table_id = entry.table_id });
    const revision = root.relation.revision;
    if (root.relation.record.sql_persistence == null or !root.relation.record.sql_persistence.?.eql(entry.descriptor)) return error.SqlRelationOwnerChanged;
    var selected_tables: std.ArrayList(records.TableRecord) = .empty;
    try selectRecoveryTable(a, &selected_tables, root.relation.record);
    var position: usize = 0;
    while (position < selected_tables.items.len) : (position += 1) {
        const table = selected_tables.items[position];
        const capture = try loadRecoveryCapture(server, a, context, &captures, .{ .table_id = table.table_id, .expected_revision = revision });
        for (capture.relation.incoming_tables) |child_id| {
            const child = try loadRecoveryCapture(server, a, context, &captures, .{ .table_id = child_id, .expected_revision = revision });
            try selectRecoveryTable(a, &selected_tables, child.relation.record);
        }
        var schema = try @import("antfly_local_sources").schema_mod.parseValidatedTableSchema(a, @import("tables.zig").effectiveSchemaJson(table.schema_json));
        defer schema.deinit(a);
        if (schema.foreign_keys) |foreign_keys| for (foreign_keys.value) |foreign| {
            const parent = try loadRecoveryCapture(server, a, context, &captures, .{ .storage_name = foreign.parent_table, .expected_revision = revision });
            if (parent.boot) |observed| {
                const current = observed.record orelse return error.SqlRelationOwnerUnavailable;
                const initialized = observed.initialized orelse return error.SqlRelationRecoveryIncomplete;
                if (!observed.registration_current) return error.SqlRelationOwnerChanged;
                if (current.phase == .recovering and current.unclean_restart and !std.mem.eql(u8, &initialized.binding.native_instance, &current.instance)) try selectRecoveryTable(a, &selected_tables, parent.relation.record);
            }
        };
    }
    const selected = selected_tables.items;
    std.mem.sort(records.TableRecord, selected, {}, struct {
        fn less(_: void, left: records.TableRecord, right: records.TableRecord) bool {
            return left.table_id < right.table_id;
        }
    }.less);
    const all_tables = try a.alloc(records.TableRecord, captures.items.len);
    var all_ranges: std.ArrayList(records.RangeRecord) = .empty;
    for (captures.items, all_tables) |captured, *table| {
        table.* = captured.relation.record;
        try all_ranges.appendSlice(a, captured.ranges);
    }
    const snapshot: RecoveryCatalog = .{ .tables = all_tables, .ranges = all_ranges.items };
    const names = try a.alloc([]const u8, selected.len);
    const id = try stages.idForAttempt(job_id, 1);
    const planned = try a.alloc(stages.Target, selected.len);
    for (selected, planned, names) |before, *target, *name| {
        if (before.storage_migration != null or before.relational_retirement_json.len != 0 or before.restore_backup_id.len != 0) return error.TableTransitionActive;
        name.* = before.name;
        const prior = before.sql_persistence orelse return error.SqlRelationPersistenceForeignKeyMismatch;
        if (prior.kind != .unlogged) return error.SqlRelationPersistenceForeignKeyMismatch;
        const captured = try loadRecoveryCapture(server, a, context, &captures, .{ .table_id = before.table_id, .expected_revision = revision });
        const observed = captured.boot orelse return error.InvalidSqlRelationPersistence;
        const current = observed.record orelse return error.SqlRelationOwnerUnavailable;
        if (!observed.registration_current) return error.SqlRelationOwnerChanged;
        const initialized = observed.initialized orelse return error.SqlRelationRecoveryIncomplete;
        if (!initialized.binding.descriptor.eql(prior)) return error.SqlRelationOwnerChanged;
        const lost = current.phase == .recovering and current.unclean_restart and !std.mem.eql(u8, &current.instance, &initialized.binding.native_instance);
        if (!lost and current.phase != .ready) return error.SqlRelationRecoveryIncomplete;
        var old_ranges: std.ArrayList(records.RangeRecord) = .empty;
        for (snapshot.ranges) |range| if (range.table_id == before.table_id) try old_ranges.append(a, range);
        if (old_ranges.items.len != 1) return error.InvalidSqlRelationPersistence;
        const ranges = try a.alloc(records.RangeRecord, 1);
        const group = generation(id, before.table_id, 0, "group");
        const table_id = generation(id, before.table_id, 0, "table");
        ranges[0] = .{ .table_id = table_id, .group_id = group, .range_id = group, .doc_identity_shard_id = group, .doc_identity_range_id = group, .start_key = old_ranges.items[0].start_key, .end_key = old_ranges.items[0].end_key };
        var next = prior;
        next.generation_instance = current.instance;
        next.data_generation = std.math.add(u64, prior.data_generation, 1) catch return error.CatalogGenerationExhausted;
        var table = before;
        table.table_id = table_id;
        table.sql_persistence = next;
        table.min_ranges = 1;
        var binding = captured.resource;
        if (!std.mem.eql(u8, binding.storage_name, before.name)) return error.CatalogGenerationChanged;
        binding = try binding.withPhysicalRelationId(table_id);
        const graph_digest = try stages.graphRetirementDigest(a, before.table_id, table_id, table.indexes_json);
        var guard: ?recovery.Guard = null;
        const fences = try a.alloc(@import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Fence, if (lost) 0 else 1);
        const handoffs = try a.alloc(stages.GenerationHandoffRange, if (lost) 0 else 1);
        if (lost) {
            guard = .{ .before = initialized, .source_group_id = old_ranges.items[0].group_id, .lost_boot_generation = current.generation, .lost_native_instance = current.instance, .catalog_revision = revision };
            try guard.?.requireLostBoot(current);
            try guard.?.requireSuccessor(next, current);
        } else {
            const old = old_ranges.items[0];
            const reads = server.table_reads orelse return error.UnsupportedSqlExecution;
            var identity_response = (try reads.lookup(a, before.name, old.start_key, .{ .relational_topology_json = "{\"mode\":\"identity\"}", .execution_deadline_ns = (try context.platformDeadline()).deadline_ns, .cancellation = context.cancellation }, .read_index)) orelse return error.TableGenerationChanged;
            defer identity_response.deinit(a);
            const Native = @import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Identity;
            const native = try std.json.parseFromSliceLeaky(Native, a, identity_response.json, .{ .ignore_unknown_fields = true });
            if (native.namespace.table_id != before.table_id or native.namespace.shard_id != metadata.rangeDocIdentityShardId(old) or native.namespace.range_id != metadata.rangeDocIdentityRangeId(old)) return error.TableGenerationChanged;
            try native.requireGenerationHandoffReceipts();
            fences[0] = .{ .transition_id = std.mem.readInt(u64, id[0..8], .little), .attempt = 1, .admission_epoch = native.next_epoch, .owner_group_id = old.group_id, .peer_group_id = group, .role = .rewrite_source, .namespace = native.namespace, .catalog_digest = native.catalog_digest };
            var response = (try reads.lookup(a, before.name, old.start_key, .{ .relational_topology_json = "{\"mode\":\"generation_handoff_summary\"}", .execution_deadline_ns = (try context.platformDeadline()).deadline_ns, .cancellation = context.cancellation }, .read_index)) orelse return error.TableGenerationChanged;
            defer response.deinit(a);
            const summary = try std.json.parseFromSliceLeaky(@import("antfly_local_sources").storage_db_empty_generation_handoff.Summary, a, response.json, .{ .allocate = .alloc_always });
            if (!summary.namespace.eql(native.namespace) or summary.intent != null or summary.seal != null) return error.TableGenerationChanged;
            handoffs[0] = .{ .source_group_id = old.group_id, .target_group_id = group, .source_namespace = summary.namespace, .admissions = summary.admissions, .admissions_digest = summary.admissions_digest, .retired_digest = summary.retired_digest, .retired_count = summary.retired_count };
        }
        target.* = .{ .source_table_id = before.table_id, .table = table, .catalog_binding = binding, .ranges = ranges, .empty_generation = true, .graph_retirement_digest = graph_digest, .generation_handoffs = handoffs, .native_reset = guard, .replace = .{ .table = before, .ranges = old_ranges.items, .fences = fences } };
    }
    const sequence_bytes = try server.source.systemCatalog(a, context, .{ .sql_column_sequence_handoffs = .{ .targets = planned, .restart = false } });
    const with_sequences = try std.json.parseFromSliceLeaky([]stages.Target, a, sequence_bytes, .{});
    // Trigger provenance shares the actual native source directory; neither
    // recovery nor an ordinal-derived reconstruction invents old column IDs.
    var incarnation: [16]u8 = undefined;
    _ = std.fmt.hexToBytes(&incarnation, &boot.identity.metadata_incarnation) catch return error.MetadataIncarnationMismatch;
    for (selected, with_sequences) |before, *target| {
        if (!try stages.needsTriggerHandoff(a, before.schema_json)) continue;
        const captured = try loadRecoveryCapture(server, a, context, &captures, .{ .table_id = before.table_id, .expected_revision = revision });
        if (captured.relation.source.value != .table) return error.CatalogGenerationChanged;
        const columns = try std.json.parseFromSliceLeaky(@import("antfly_local_sources").system_catalog_column_identities.State, a, captured.relation.source.value.table.column_identities_json orelse return error.CatalogGenerationChanged, .{});
        try stages.prepareTargetTriggersAlloc(a, target, .{ .source_schema_json = before.schema_json, .source_columns = columns, .destination_incarnation = incarnation });
    }
    try stages.prepareEmptyGenerationHandoffMappingsAlloc(a, with_sequences);
    try stages.prepareNativeLossGenerationsAlloc(a, with_sequences);
    const parents = try buildRecoveryExternalParents(server, context, a, snapshot, selected, with_sequences, id);
    const cohort = try std.json.Stringify.valueAlloc(a, .{ .targets = with_sequences, .external_fk_parents = parents }, .{});
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(cohort, &digest, .{});
    const plan: stages.Plan = .{ .id = id, .cohort_digest = digest, .targets = with_sequences, .external_fk_parents = parents };
    try plan.validate(a);
    const location = try std.fmt.allocPrint(a, "metadata://native-unlogged-recovery/{s}", .{std.fmt.bytesToHex(id, .lower)});
    // No native image has been opened and no user SQL effect has occurred.
    // The existing metadata job admission rechecks all lost epochs, closure,
    // catalog revision and native capabilities before loss fence publication.
    const admission = try server.restore_job_store.startRecoverableObserved(a, .{ .scope = .cluster, .source_kind = .empty_generation, .backup_id = &std.fmt.bytesToHex(digest, .lower), .location = location, .connection = "internal", .restore_mode = "overwrite", .table_names = names, .idempotency_namespace = idempotency_namespace, .idempotency_key = &key, .destination_authorization_principal = "", .generation_plan_json = try std.json.Stringify.valueAlloc(a, plan, .{}) }, true, null);
    if (comptime @import("builtin").mode == .debug) {
        const Once = struct {
            var outcomes: std.atomic.Value(u8) = .init(0);
        };
        const bit: u8 = if (admission == .accepted) 1 else 2;
        if (Once.outcomes.fetchOr(bit, .monotonic) & bit == 0)
            std.debug.print("native reset submission outcome={s}\n", .{@tagName(admission)});
    }
    diagnostic_stage.* = .admitted;
    try server.schedulePendingRestoreJobs();
    return error.SqlRelationRecoveryIncomplete;
}

/// The healthy parent's own accepted-generation summary is the authority for
/// retiring an old child generation. A lost child's missing ICatalog cannot
/// be reconstructed from a schema declaration or guessed generation counter.
fn buildRecoveryExternalParents(server: *server_mod.ApiHttpServer, context: operation.RequestContext, a: std.mem.Allocator, snapshot: RecoveryCatalog, selected: []const records.TableRecord, planned: []const stages.Target, id: stages.Id) ![]stages.ExternalFkParent {
    const reads = server.table_reads orelse return error.UnsupportedSqlExecution;
    var drafts: std.ArrayList(ExternalParentDraft) = .empty;
    for (selected, planned) |child, replacement| {
        var schema = try @import("antfly_local_sources").schema_mod.parseValidatedTableSchema(a, @import("tables.zig").effectiveSchemaJson(child.schema_json));
        defer schema.deinit(a);
        if (schema.storage_mode != .relational) continue;
        var next_catalog = try plannedIntegrityCatalog(a, replacement.table.table_id, replacement.table.schema_json);
        defer next_catalog.deinit();
        for (try schema.relationalForeignKeyDefinitions(a)) |fk| {
            if (for (selected) |other| {
                if (std.mem.eql(u8, other.name, fk.parent_table)) break true;
            } else false) continue;
            const parent = for (snapshot.tables) |candidate| {
                if (std.mem.eql(u8, candidate.name, fk.parent_table)) break candidate;
            } else return error.ForeignKeyParentTableNotFound;
            if (parent.storage_migration != null or parent.relational_retirement_json.len != 0 or parent.restore_backup_id.len != 0) return error.TableTransitionActive;
            var old_generation: ?[16]u8 = null;
            var parent_ranges: usize = 0;
            for (snapshot.ranges) |range| if (range.table_id == parent.table_id) {
                parent_ranges += 1;
                if (parent_ranges > 4096) return error.SqlProgramLimitExceeded;
                var response = (try reads.lookup(a, parent.name, range.start_key, .{ .relational_topology_json = "{\"mode\":\"generation_handoff_summary\"}", .execution_deadline_ns = (try context.platformDeadline()).deadline_ns, .cancellation = context.cancellation }, .read_index)) orelse return error.TableGenerationChanged;
                defer response.deinit(a);
                const summary = try std.json.parseFromSliceLeaky(@import("antfly_local_sources").storage_db_empty_generation_handoff.Summary, a, response.json, .{ .allocate = .alloc_always });
                if (summary.namespace.table_id != parent.table_id or summary.namespace.shard_id != metadata.rangeDocIdentityShardId(range) or summary.namespace.range_id != metadata.rangeDocIdentityRangeId(range) or summary.intent != null or summary.seal != null) return error.TableGenerationChanged;
                const accepted = for (summary.admissions) |scope| {
                    if (scope.child_table_id == child.table_id and std.mem.eql(u8, scope.child_table_name, child.name) and std.mem.eql(u8, scope.constraint_name, fk.name)) break scope.active_generation orelse return error.GenerationAdmissionChanged;
                } else return error.GenerationAdmissionChanged;
                if (old_generation) |prior| {
                    if (!std.mem.eql(u8, &prior, &accepted)) return error.GenerationAdmissionChanged;
                } else old_generation = accepted;
            };
            if (parent_ranges == 0) return error.TableGenerationChanged;
            const next = next_catalog.catalog.find(.foreign_key, fk.name) orelse return error.CatalogGenerationChanged;
            var draft: ?*ExternalParentDraft = null;
            for (drafts.items) |*existing| if (existing.table.table_id == parent.table_id) {
                draft = existing;
                break;
            };
            if (draft == null) {
                if (drafts.items.len >= 128) return error.SqlProgramLimitExceeded;
                try drafts.append(a, .{ .table = parent });
                draft = &drafts.items[drafts.items.len - 1];
            }
            if (draft.?.foreign_keys.items.len >= 128) return error.SqlProgramLimitExceeded;
            try draft.?.foreign_keys.append(a, .{ .child_table_id = child.table_id, .child_table_name = child.name, .constraint_name = fk.name, .generation = old_generation.?, .next_generation = next.generation });
        }
    }
    const result = try a.alloc(stages.ExternalFkParent, drafts.items.len);
    var range_count: usize = 0;
    for (drafts.items, result) |draft, *parent| {
        var ranges: std.ArrayList(records.RangeRecord) = .empty;
        for (snapshot.ranges) |range| if (range.table_id == draft.table.table_id) try ranges.append(a, range);
        range_count += ranges.items.len;
        if (ranges.items.len == 0 or range_count > 4096) return error.SqlProgramLimitExceeded;
        metadata.sortKeyspaceRanges(records.RangeRecord, ranges.items);
        const fences = try a.alloc(@import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Fence, ranges.items.len);
        for (ranges.items, fences) |range, *fence| {
            var response = (try reads.lookup(a, draft.table.name, range.start_key, .{ .relational_topology_json = "{\"mode\":\"identity\"}", .execution_deadline_ns = (try context.platformDeadline()).deadline_ns, .cancellation = context.cancellation }, .read_index)) orelse return error.TableGenerationChanged;
            defer response.deinit(a);
            const Native = @import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Identity;
            const native = try std.json.parseFromSliceLeaky(Native, a, response.json, .{ .ignore_unknown_fields = true });
            if (native.namespace.table_id != draft.table.table_id or native.namespace.shard_id != metadata.rangeDocIdentityShardId(range) or native.namespace.range_id != metadata.rangeDocIdentityRangeId(range)) return error.TableGenerationChanged;
            try native.requireGenerationHandoffReceipts();
            fence.* = .{ .transition_id = std.mem.readInt(u64, id[0..8], .little), .attempt = 1, .admission_epoch = native.next_epoch, .owner_group_id = range.group_id, .peer_group_id = range.group_id, .role = .truncate_parent, .namespace = native.namespace, .catalog_digest = native.catalog_digest };
        }
        parent.* = .{ .table = draft.table, .ranges = ranges.items, .fences = fences, .foreign_keys = draft.foreign_keys.items };
    }
    return result;
}

const RecoveryCapture = @import("../metadata/sql_relation_recovery_capture.zig").Capture;
const RecoveryCatalog = struct { tables: []const records.TableRecord, ranges: []const records.RangeRecord };
fn loadRecoveryCapture(server: *server_mod.ApiHttpServer, a: std.mem.Allocator, context: operation.RequestContext, captures: *std.ArrayList(RecoveryCapture), request: @import("../metadata/sql_relation_recovery_capture.zig").Request) !RecoveryCapture {
    for (captures.items) |value| {
        if ((request.table_id != null and request.table_id.? == value.relation.record.table_id) or
            (request.storage_name != null and std.mem.eql(u8, request.storage_name.?, value.relation.record.name))) return value;
    }
    if (captures.items.len == 256) return error.SqlProgramLimitExceeded;
    const bytes = try server.source.systemCatalog(a, context, .{ .sql_relation_boot_read = .{ .recovery_capture = request } });
    const value = try std.json.parseFromSliceLeaky(RecoveryCapture, a, bytes, .{});
    if (captures.items.len != 0 and (value.relation.revision != captures.items[0].relation.revision or !std.meta.eql(value.relation.epoch, captures.items[0].relation.epoch))) return error.CatalogGenerationChanged;
    try captures.append(a, value);
    return value;
}
fn selectRecoveryTable(a: std.mem.Allocator, selected: *std.ArrayList(records.TableRecord), table: records.TableRecord) !void {
    for (selected.items) |before| if (before.table_id == table.table_id) return;
    if (selected.items.len == 128) return error.SqlProgramLimitExceeded;
    const descriptor = table.sql_persistence orelse return error.SqlRelationPersistenceForeignKeyMismatch;
    if (descriptor.kind != .unlogged) return error.SqlRelationPersistenceForeignKeyMismatch;
    try selected.append(a, table);
}

fn captureNotice(raw: *anyopaque, notice: catalog.notices.Notice) !void {
    const buffer: *catalog.notices.Buffer = @ptrCast(@alignCast(raw));
    try buffer.append(notice);
}
fn reserveNotices(raw: *anyopaque, count: usize, bytes: usize) !void {
    const buffer: *catalog.notices.Buffer = @ptrCast(@alignCast(raw));
    try buffer.reserve(count, bytes);
}

fn copyCapturedNotices(a: std.mem.Allocator, buffer: *const catalog.notices.Buffer, count: usize) ![]const catalog.notices.Notice {
    const result = try a.alloc(catalog.notices.Notice, count);
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |notice| {
            a.free(notice.code);
            a.free(notice.message);
        }
        a.free(result);
    }
    for (result, 0..) |*out, ordinal| {
        const notice = buffer.get(ordinal);
        const code = try a.dupe(u8, notice.code);
        errdefer a.free(code);
        out.* = .{ .code = code, .message = try a.dupe(u8, notice.message), .severity = notice.severity };
        initialized += 1;
    }
    return result;
}
fn freeCapturedNotices(a: std.mem.Allocator, notices: []const catalog.notices.Notice) void {
    for (notices) |notice| {
        a.free(notice.code);
        a.free(notice.message);
    }
    a.free(notices);
}

const TruncateCapture = struct {
    a: std.mem.Allocator,
    root: *@import("sql_execution.zig").Adapter,
    broker: ?*@import("sql_trigger_commit.zig").Broker = null,
    fn begin(self: *@This(), table: records.TableRecord) !void {
        if (self.broker == null) self.broker = try @import("sql_trigger_commit.zig").Broker.create(self.a, self.root);
        try self.broker.?.beginTruncate(table);
    }
};

/// Preflight the actual source declarations, not a global active-trigger flag.
/// No matching definition means no Broker, metadata snapshot, column identity
/// or routine capture. Definitions are included without an enabled filter so
/// the writer still receives any required disabled-attachment witness.
fn prepareTruncateStatements(a: std.mem.Allocator, selected: []const records.TableRecord, capture: anytype) !void {
    for (selected) |table| {
        if (try stages.hasTruncateAttachments(a, table.schema_json)) try capture.begin(table);
    }
}

test "SQL TRUNCATE preparation skips absent and other-event definitions without opening captures or unrelated FK tables" {
    var region: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    const Fixture = struct {
        fn schema(alloc: std.mem.Allocator, id: u64, events: @FieldType(catalog.triggers.Declaration, "events")) ![]u8 {
            const initial: catalog.triggers.State = .{ .metadata_incarnation = @splat(1), .table_id = id, .generation = 1 };
            const state = try catalog.triggers.prepare(alloc, initial, .{ .name = "capture", .timing = .before, .events = events, .level = .statement, .function = .{ .database = "antfly", .namespace = "public", .namespace_id = 2, .name = "capture", .id = 15 } }, false);
            const foreign_keys = if (id == 9)
                "[{\"name\":\"fk\",\"child_columns\":[\"id\"],\"parent_table\":\"unselected_fk_parent\",\"parent_columns\":[\"id\"]}]"
            else
                "[]";
            return std.fmt.allocPrint(alloc, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"sql_triggers\":{s},\"unique_constraints\":[{{\"name\":\"pk\",\"columns\":[\"id\"]}}],\"foreign_keys\":{s},\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"id\":{{\"type\":\"integer\"}}}},\"additionalProperties\":false}}}}}}}}", .{ try state.encode(alloc), foreign_keys });
        }
    };
    const sources = [_]records.TableRecord{
        .{ .table_id = 7, .name = "schema_free" },
        .{ .table_id = 8, .name = "insert_only", .schema_json = try Fixture.schema(a, 8, .{ .insert = true }) },
        .{ .table_id = 9, .name = "selected_child", .schema_json = try Fixture.schema(a, 9, .{ .truncate = true }) },
        .{ .table_id = 11, .name = "unselected_fk_parent", .schema_json = try Fixture.schema(a, 11, .{ .truncate = true }) },
    };
    var child_schema = try @import("antfly_local_sources").schema_mod.parseValidatedTableSchema(a, sources[2].schema_json);
    defer child_schema.deinit(a);
    try std.testing.expectEqualStrings("unselected_fk_parent", child_schema.foreign_keys.?.value[0].parent_table);
    const Capture = struct {
        calls: usize = 0,
        fn begin(self: *@This(), table: records.TableRecord) !void {
            try std.testing.expectEqual(@as(u64, 9), table.table_id);
            self.calls += 1;
        }
    };
    var capture: Capture = .{};
    try prepareTruncateStatements(a, sources[0..2], &capture);
    try std.testing.expectEqual(@as(usize, 0), capture.calls);
    // The selected child can depend on an untouched parent. TRUNCATE's FK
    // cohort selection owns that distinction; trigger capture never expands it.
    try prepareTruncateStatements(a, sources[0..3], &capture);
    try std.testing.expectEqual(@as(usize, 1), capture.calls);
}
