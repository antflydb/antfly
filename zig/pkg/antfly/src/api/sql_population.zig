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

//! Public query-produced tables use the shared hidden-generation job lane.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const domain = local.system_catalog_domain;
const operation = local.api_operation;
const http = @import("http_server.zig");
const create = @import("../metadata/relation_create.zig");
const stages = @import("../metadata/restore_staging.zig");
const jobs = @import("restore_jobs.zig");
const delivery = @import("sql_population_delivery.zig");
const Path = @import("../pgwire/search_path.zig").Path;
const time = @import("antfly_platform").time;
const A = std.mem.Allocator;

fn namespaceId(server: *http.ApiHttpServer, a: A, context: operation.RequestContext, target: domain.Target) !u64 {
    if (target.isDefault()) return domain.default_namespace_id;
    const bytes = try server.source.systemCatalog(a, context, .{ .read = .{ .kind = .namespace, .database = target.database, .name = target.namespace } });
    const value = try std.json.parseFromSliceLeaky(domain.State, a, bytes, .{});
    for (value.resources) |resource| if (resource.kind == .namespace and std.mem.eql(u8, resource.name, target.namespace)) return resource.id;
    return error.NamespaceNotFound;
}
fn freeReceipt(a: A, receipt: catalog.DdlReceipt) void {
    a.free(receipt.database);
    a.free(receipt.namespace);
    a.free(receipt.table);
    a.free(receipt.table_id);
    if (receipt.restore_job_id) |id| a.free(id);
    if (receipt.idempotency_key) |key| a.free(key);
}
fn receiptAlloc(a: A, target: domain.Target, table_id: u64, job_id: u64, key: []const u8) !catalog.DdlReceipt {
    const database = try a.dupe(u8, target.database);
    errdefer a.free(database);
    const namespace = try a.dupe(u8, target.namespace);
    errdefer a.free(namespace);
    const table = try a.dupe(u8, target.table);
    errdefer a.free(table);
    const id = try std.fmt.allocPrint(a, "{d}", .{table_id});
    errdefer a.free(id);
    const job = try std.fmt.allocPrint(a, "{d}", .{job_id});
    errdefer a.free(job);
    return .{ .database = database, .namespace = namespace, .table = table, .table_id = id, .schema_version = 1, .state = .pending, .restore_job_id = job, .idempotency_key = try a.dupe(u8, key), .diagnostic = "Table population was admitted. Retain this job receipt and do not replay the DDL." };
}
fn pending(receipt: catalog.DdlReceipt, rows: u64) catalog.DdlOutcome {
    return .{ .rows_affected = rows, .mutation_outcome = .committed_pending, .receipt = receipt };
}
fn owner(raw: *anyopaque, a: A, table: []const u8, group: u64, request: @import("restore_owner.zig").Request, context: operation.RequestContext) !@import("restore_owner.zig").Response {
    const server: *http.ApiHttpServer = @ptrCast(@alignCast(raw));
    return server.executeRestoreOwner(a, table, group, request, context);
}
fn requestCleanup(server: *http.ApiHttpServer, job_id: u64) void {
    if (server.restore_job_store.cancel(server.alloc, job_id)) |record| {
        if (record) |bytes| server.alloc.free(bytes);
    } else |_| {}
    server.schedulePendingRestoreJobs() catch {};
}

const Refresh = struct { target: domain.Target, record: @import("../system_catalog/views.zig").Record };

pub fn executeRefresh(root: *@import("sql_execution.zig").Adapter, allocators: catalog.DdlAllocators, input: @FieldType(catalog.Ddl, "populate")) !catalog.DdlOutcome {
    var region: std.heap.ArenaAllocator = .init(allocators.scratch);
    defer region.deinit();
    const a = region.allocator();
    const intent = input.intent;
    var target: domain.Target = .{ .database = intent.name.database orelse root.database, .namespace = intent.name.namespace orelse root.namespace, .table = intent.name.table };
    var source: ?domain.SqlSource = null;
    if (intent.name.namespace == null and intent.name.database == null and root.ddl_search_path != null) {
        for (root.ddl_search_path.?.entries[0..root.ddl_search_path.?.len]) |*entry| {
            target.namespace = entry.slice();
            const snapshot = try @import("sql_view_execution.zig").lookup(root, a, target);
            if (snapshot.sql_sources[0]) |found| {
                source = found;
                break;
            }
        }
    } else {
        const snapshot = try @import("sql_view_execution.zig").lookup(root, a, target);
        source = snapshot.sql_sources[0];
    }
    const found = source orelse return error.TableNotFound;
    if (found.value != .view) return error.SqlWrongObjectType;
    const parsed = try @import("../system_catalog/views.zig").Record.decode(a, found.value.view.source_json);
    const backing = parsed.value.materialization orelse return error.SqlWrongObjectType;
    const logical = try target.resourceNameAlloc(a);
    if (!try http.tablePermissionCurrentlyAllowed(root.identity.*, logical, .admin)) return error.Forbidden;
    try root.server.requireGenerationAuthority(http.storedDestinationPrincipal(root.identity.*), &.{logical}, true);
    if (intent.concurrently) {
        if (!backing.populated) return error.SqlConcurrentRefreshUnpopulated;
        if (!intent.with_data) return error.SqlRefreshOptionsConflict;
        return error.SqlConcurrentRefreshIndexRequired;
    }
    const preparation = input.preparation orelse return error.InvalidSqlBackendResponse;
    const previous = root.refresh_definition_view;
    root.refresh_definition_view = parsed.value.id;
    defer root.refresh_definition_view = previous;
    defer preparation.release(preparation.ptr);
    const query = try a.create(local.sql_ast.Select);
    query.* = .{ .table = .{ .database = target.database, .namespace = target.namespace, .table = target.table } };
    var bound = intent;
    bound.name = query.table.?;
    bound.query = query;
    bound.columns = &.{};
    const producer = try preparation.prepare(preparation.ptr, a, bound);
    return executeInner(root.server, root.identity.*, root.context, root.database, root.namespace, allocators, .{ .intent = bound, .source = producer }, null, .{ .target = target, .record = parsed.value });
}

pub fn execute(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, request_context: operation.RequestContext, database: []const u8, namespace: []const u8, allocators: catalog.DdlAllocators, input: @FieldType(catalog.Ddl, "populate"), path: ?*const Path) !catalog.DdlOutcome {
    return executeInner(server, identity, request_context, database, namespace, allocators, input, path, null);
}
fn executeInner(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, request_context: operation.RequestContext, database: []const u8, namespace: []const u8, allocators: catalog.DdlAllocators, input: @FieldType(catalog.Ddl, "populate"), path: ?*const Path, refresh: ?Refresh) !catalog.DdlOutcome {
    const intent = input.intent;
    if (intent.persistence != .permanent or (intent.operation != .create_table_as and intent.operation != .select_into and intent.operation != .create_materialized_view and !(intent.operation == .refresh_materialized_view and refresh != null))) return error.UnsupportedSqlExecution;
    const source = input.source orelse return error.InvalidSqlBackendResponse;
    const io = server.restore_job_store.io orelse return error.AsyncRestoreUnavailable;
    var context = try request_context.platformDeadline();
    if (context.deadline_ns == null) context.deadline_ns = time.monotonicNs() +| 30 * std.time.ns_per_s;
    try context.ensureActive();
    var region: std.heap.ArenaAllocator = .init(allocators.scratch);
    defer region.deinit();
    const a = region.allocator();
    var target: domain.Target = .{ .database = intent.name.database orelse database, .namespace = intent.name.namespace orelse namespace, .table = intent.name.table };
    try target.validate();
    // Unqualified CREATE selects the first extant user namespace in search_path.
    if (intent.name.namespace == null and intent.name.database == null) if (path) |lookup| {
        var found = false;
        for (lookup.entries[0..lookup.len]) |*entry| {
            if (std.mem.eql(u8, entry.slice(), "pg_catalog")) continue;
            var candidate = target;
            candidate.namespace = entry.slice();
            _ = namespaceId(server, a, context, candidate) catch |err| switch (err) {
                error.NamespaceNotFound, error.CatalogNotFound => continue,
                else => return err,
            };
            target = candidate;
            found = true;
            break;
        }
        if (!found) return error.NamespaceNotFound;
    };
    try @import("sql_catalog_auth.zig").authorizeTarget(server, identity, a, target);
    if (!try @import("sql_catalog_auth.zig").namespaceUsage(identity, a, target)) return error.Forbidden;
    if (intent.tablespace) |space| if (identity) |actor| if (!http.permissionsAllow(actor.permissions, .tablespace, space, .read)) return error.Forbidden;
    const principal = http.storedDestinationPrincipal(identity);
    const logical_name = try target.resourceNameAlloc(a);
    try server.requireGenerationAuthority(principal, &.{logical_name}, true);
    var trusted = context;
    trusted.setting_admin = true;
    const ns = try namespaceId(server, a, trusted, target);
    const physical = try server.catalogStorageNameAlloc(a);
    var prepared: create.Prepared = undefined;
    while (true) {
        try context.ensureActive();
        var attempt: std.heap.ArenaAllocator = .init(allocators.scratch);
        defer attempt.deinit();
        const bytes = server.source.systemCatalog(attempt.allocator(), trusted, .{ .query_population_prepare = .{
            .namespace_id = ns,
            .materialized_view = intent.operation == .create_materialized_view or refresh != null,
            .expected_view = if (refresh) |value| .{ .id = value.record.id, .generation = value.record.generation } else null,
            .logical_name = target.table,
            .physical_name = physical,
            .tablespace_name = intent.tablespace,
            .candidate = .{ .table_id = 0, .name = "", .schema_json = source.schema_json, .indexes_json = "{}" },
        } }) catch |err| switch (err) {
            error.CatalogAlreadyExists, error.TableAlreadyExists => if (intent.if_not_exists) return .{ .population_skipped = true } else return error.CatalogAlreadyExists,
            error.CatalogOccupancyNotReady, error.CatalogPublicationProofPending, error.TableTransitionActive => {
                if (server.cfg.deployment_mode == .standalone) _ = try server.source.runRound();
                try io.sleep(.fromMilliseconds(10), .awake);
                continue;
            },
            else => return err,
        };
        prepared = try std.json.parseFromSliceLeaky(create.Prepared, a, bytes, .{ .allocate = .alloc_always });
        break;
    }
    const guard = prepared.creation_guard orelse return error.InvalidSqlBackendResponse;
    var nonce: [16]u8 = undefined;
    try io.randomSecure(&nonce);
    const key = try std.fmt.allocPrint(a, "auto:{s}", .{std.fmt.bytesToHex(nonce, .lower)});
    const job_namespace = try std.fmt.allocPrint(a, "query-population:{s}:{s}", .{ principal, logical_name });
    const job_id = try jobs.jobIdForIdempotency(a, job_namespace, key);
    const id = try stages.idForAttempt(job_id, 1);
    const instance = if (identity) |actor| actor.user_instance_id orelse return error.Forbidden else nonce;
    const capture: local.storage_db_relational_population_contract.Capture = .{
        .materialized_view_id = prepared.child.sql_materialized_view_id,
        .principal_name = if (identity) |actor| actor.username else "",
        .capture_id = nonce,
        .definition = source.definition,
        .source_definition = source.source_definition,
        .target_schema = try stages.runtimeSchemaDigestForTable(a, prepared.child.schema_json),
        .principal_instance = instance,
        .catalog_revision = prepared.expected_catalog_revision,
        .write_version = @max(1, time.realtimeNs()),
        .expires_at_ms = time.realtimeNs() / std.time.ns_per_ms +| (try context.remainingTimeoutMs()).?,
    };
    var view_definition: ?local.sql_view_definition.Owned = null;
    defer if (view_definition) |*value| value.deinit();
    if (intent.operation == .create_materialized_view)
        view_definition = try @import("sql_view_ddl.zig").preparePopulation(server, identity, context, database, namespace, a, intent, prepared.expected_catalog_revision, path);
    const fences = if (prepared.previous) |prior| try replacementFences(server, a, context, id, prior, prepared.child_ranges) else &.{};
    const destination: stages.Target = .{
        .source_table_id = 0,
        .population = capture,
        .materialized_view = if (refresh) |value| .{ .id = value.record.id, .definition = value.record.definition, .populated = intent.with_data, .refresh_identity = .{ .id = value.record.id, .generation = value.record.generation } } else if (view_definition) |definition| .{ .id = prepared.child.sql_materialized_view_id orelse return error.InvalidSqlBackendResponse, .definition = definition.value, .populated = intent.with_data } else null,
        .replace = if (prepared.previous) |prior| .{ .table = prior.table, .ranges = prior.ranges, .fences = fences } else null,
        .creation_guard = guard,
        .table = prepared.child,
        .ranges = prepared.child_ranges,
        .catalog_binding = .{ .kind = if (view_definition != null or refresh != null) .view else .table, .id = prepared.child.sql_materialized_view_id orelse prepared.child.table_id, .parent_id = ns, .tablespace_id = prepared.tablespace_id, .name = target.table, .storage_name = prepared.child.name },
    };
    const plan: stages.Plan = .{ .id = id, .cohort_digest = source.definition, .targets = &.{destination} };
    try plan.validate(a);
    var receipt = try receiptAlloc(allocators.result, target, prepared.child.sql_materialized_view_id orelse prepared.child.table_id, job_id, key);
    var transferred = false;
    defer if (!transferred) freeReceipt(allocators.result, receipt);
    const result = try server.restore_job_store.startRecoverable(allocators.scratch, .{
        .scope = .cluster,
        .source_kind = .query_population,
        .restore_mode = if (refresh != null) "overwrite" else "fail_if_exists",
        .backup_id = try a.dupe(u8, &std.fmt.bytesToHex(source.definition, .lower)),
        .location = "metadata://query-population",
        .connection = "internal",
        .table_names = &.{logical_name},
        .idempotency_namespace = job_namespace,
        .idempotency_key = key,
        .destination_authorization_principal = principal,
        .generation_plan_json = try std.json.Stringify.valueAlloc(a, plan, .{}),
    });
    const admission_bytes = switch (result) {
        inline else => |bytes| bytes,
    };
    defer allocators.scratch.free(admission_bytes);
    if (result == .unknown) {
        receipt.state = .admission_unknown;
        receipt.diagnostic = "Population admission is unresolved. Retain this receipt, poll the job and do not replay the DDL.";
        transferred = true;
        return .{ .mutation_outcome = null, .receipt = receipt };
    }
    const admitted = try std.json.parseFromSlice(jobs.JobState, a, admission_bytes, .{});
    defer admitted.deinit();
    if (admitted.value.phase == .failed) {
        if (std.mem.eql(u8, admitted.value.staging_failure, "CatalogAlreadyExists")) {
            if (intent.if_not_exists) return .{ .population_skipped = true };
            return error.CatalogAlreadyExists;
        }
        if (std.mem.eql(u8, admitted.value.staging_failure, "RelationalTopologyProtocolUpgradeRequired")) return error.RelationalTopologyProtocolUpgradeRequired;
        return error.CatalogGenerationChanged;
    }
    errdefer requestCleanup(server, job_id);
    // The producer nonce, not just source/type equality, proves this request
    // owns the one evaluation admitted by the durable first row.
    const persisted = (try server.source.getRestoreStaging(a, id, context)) orelse return error.SqlMutationOutcomeUnknown;
    const bound = try std.json.parseFromSliceLeaky(stages.Job, a, persisted, .{});
    if (!std.mem.eql(u8, &bound.plan_digest, &try plan.digest(a))) return error.SqlMutationOutcomeUnknown;
    server.schedulePendingRestoreJobs() catch {};
    const destinations = try a.alloc(delivery.Destination, destination.ranges.len);
    for (destinations, destination.ranges) |*out, range| out.* = .{ .port = .{ .ptr = server, .execute_fn = owner }, .table = destination.table.name, .group = range.group_id, .scope = try stages.ownerScope(a, plan, bound.plan_digest, destination, range), .range = .{ .start = range.start_key, .end = range.end_key orelse "" } };
    // Wait for all hidden owners before any evaluation or begin RPC. A routing
    // gap during preparation cannot consume half a producer admission.
    for (destinations) |out| while (true) {
        try context.ensureActive();
        _ = owner(server, allocators.scratch, out.table, out.group, .{ .scope = out.scope, .action = .status }, context) catch |err| switch (err) {
            error.GroupLeaderUnavailable, error.RestoreValidationPending, error.StorageBusy, error.NotFound => {
                try io.sleep(.fromMilliseconds(10), .awake);
                continue;
            },
            else => {
                requestCleanup(server, job_id);
                return err;
            },
        };
        break;
    };
    var stats: delivery.Stats = .{};
    const cursors = delivery.deliverCohort(allocators.scratch, destinations, source, context, .{ .stats = &stats }) catch |err| {
        requestCleanup(server, job_id);
        if (!stats.seals_started) return err;
        // Sealed output may already have published. Its opaque receipt is the
        // recovery boundary; an error here must not invite query reevaluation.
        transferred = true;
        return pending(receipt, stats.source_rows);
    };
    defer allocators.scratch.free(cursors);
    var rows: u64 = 0;
    for (cursors) |cursor| rows = std.math.add(u64, rows, cursor.rows) catch unreachable;
    server.schedulePendingRestoreJobs() catch {};
    const observe_until = if (allocators.completion == .until_deadline) context.deadline_ns.? else @min(context.deadline_ns.?, time.monotonicNs() +| 250 * std.time.ns_per_ms);
    while (time.monotonicNs() < observe_until) {
        const bytes = server.restore_job_store.load(allocators.scratch, job_id) catch break;
        if (bytes) |encoded| {
            defer allocators.scratch.free(encoded);
            const state = std.json.parseFromSlice(jobs.JobState, allocators.scratch, encoded, .{}) catch break;
            defer state.deinit();
            if (state.value.phase == .succeeded) {
                receipt.state = .ready;
                receipt.diagnostic = null;
                transferred = true;
                return .{ .rows_affected = rows, .receipt = receipt };
            }
            if (state.value.phase == .failed or state.value.phase == .cancelled) {
                receipt.state = .invalid;
                receipt.diagnostic = "Population did not publish. Inspect the retained job for the failure and cleanup result.";
                transferred = true;
                return pending(receipt, rows);
            }
        }
        io.sleep(.fromMilliseconds(10), .awake) catch break;
    }
    transferred = true;
    return pending(receipt, rows);
}

fn replacementFences(server: *http.ApiHttpServer, a: A, context: operation.RequestContext, id: stages.Id, prior: create.Previous, destinations: []const local.common_topology_records.RangeRecord) ![]const local.storage_db_relational_integrity_topology_contract.Fence {
    const reads = server.table_reads orelse return error.UnsupportedSqlExecution;
    const topology = local.storage_db_relational_integrity_topology_contract;
    const result = try a.alloc(topology.Fence, prior.ranges.len);
    for (prior.ranges, result, 0..) |range, *fence, i| {
        try context.ensureActive();
        var response = (try reads.lookup(a, prior.table.name, range.start_key, .{ .relational_topology_json = "{\"mode\":\"identity\"}", .execution_deadline_ns = (try context.platformDeadline()).deadline_ns, .cancellation = context.cancellation }, .read_index)) orelse return error.TableNotFound;
        defer response.deinit(a);
        const native = try std.json.parseFromSliceLeaky(topology.Identity, a, response.json, .{ .ignore_unknown_fields = true });
        if (native.namespace.table_id != prior.table.table_id or native.namespace.shard_id != @import("../metadata/table_manager.zig").rangeDocIdentityShardId(range) or native.namespace.range_id != @import("../metadata/table_manager.zig").rangeDocIdentityRangeId(range)) return error.TableGenerationChanged;
        fence.* = .{ .transition_id = @max(1, std.mem.readInt(u64, id[0..8], .little)), .attempt = 1, .admission_epoch = native.next_epoch, .owner_group_id = range.group_id, .peer_group_id = destinations[i % destinations.len].group_id, .role = .rewrite_source, .namespace = native.namespace, .catalog_digest = native.catalog_digest };
    }
    return result;
}
