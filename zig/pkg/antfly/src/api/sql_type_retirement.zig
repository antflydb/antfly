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

//! Dependent type retirement is one ordinary staged schema rewrite. Preparation
//! grants no authority; the accepting writer independently derives postimages.
const std = @import("std");
const local = @import("antfly_local_sources");
const execution = @import("sql_execution.zig");
const ddl = @import("sql_type_ddl.zig");
const types = @import("../system_catalog/named_types.zig");
const recovery = @import("sql_type_recovery.zig");
const admission = @import("relational_rewrite_admission.zig");
const stages = @import("../metadata/restore_staging.zig");
const jobs = @import("restore_jobs.zig");
const http = @import("http_server.zig");

pub fn execute(root: *execution.Adapter, allocators: local.sql_catalog.DdlAllocators, cut: types.Snapshot, request: types.Mutation) !local.sql_catalog.DdlOutcome {
    const server = root.server;
    if (server.restore_job_store.replicated == null) return error.SqlSchemaRewriteRequiresMetadataOwner;
    if (server.sharedApiIo() == null) return error.UnsupportedSqlExecution;
    var observation = try types.observation(allocators.scratch, request);
    // The receipt owns its identity before the first durable proposal. It is
    // moved out of this scope only after preparation has released all frames.
    var receipt: local.sql_catalog.CatalogOperationReceipt = undefined;
    var owns_receipt = false;
    errdefer if (owns_receipt) receipt.deinit(allocators.result);
    {
        var region = std.heap.ArenaAllocator.init(allocators.scratch);
        defer region.deinit();
        const a = region.allocator();
        const before = (cut.found orelse return error.SqlNamedTypeNotFound).record;
        const captured = try ddl.snapshot(root, a, .{ .targets = &.{}, .reference = .{ .id = before.identity.id, .namespace_id = before.identity.namespace_id }, .include_dependents = true, .prepare_retirement = true });
        const current = (captured.found orelse return error.SqlNamedTypeNotFound).record;
        if (current.identity.id != before.identity.id or current.identity.generation != before.identity.generation or !std.mem.eql(u8, &captured.incarnation, &request.incarnation)) return error.CatalogGenerationChanged;
        try recovery.authorize(server, root.identity.*, a, request.target);
        if (captured.dependents.len == 0) return error.CatalogGenerationChanged;
        var topology = (try server.source.linearizableSnapshot(root.context)) orelse return error.MetadataCapabilityUnavailable;
        defer server.source.freeAdminSnapshot(&topology);
        const roots = try a.alloc(admission.SelectionRoot, captured.dependents.len);
        for (captured.dependents, roots) |table, *entry| entry.* = .{ .name = table.name };
        const cohort = try admission.selectMany(a, topology.tables, roots);
        const names = try a.alloc([]const u8, cohort.len);
        const inputs = try a.alloc(local.sql_catalog.type_retirement.Input, cohort.len);
        for (cohort, names, inputs) |table, *name, *input| {
            name.* = table.name;
            input.* = .{ .name = table.name, .schema_json = @import("tables.zig").effectiveSchemaJson(table.schema_json) };
        }
        for (try server.logicalTableNamesInArena(a, root.context, names)) |name|
            if (!try http.tablePermissionCurrentlyAllowed(root.identity.*, name, .admin)) return error.Forbidden;
        const principal = http.storedDestinationPrincipal(root.identity.*);
        try server.requireTransformRewriteAuthority(principal, names);
        var prepared = try local.sql_catalog.type_retirement.Prepared.init(a, inputs, before.identity.id);
        defer prepared.deinit();
        var cascade_count: usize = 0;
        for (prepared.tables) |table| cascade_count += table.cascade_objects.len;
        const notice: ?local.sql_catalog.notices.Notice = if (cascade_count == 0) null else .{ .message = if (cascade_count > 1)
            try std.fmt.allocPrint(a, "drop cascades to {d} other objects", .{cascade_count})
        else single: {
            for (prepared.tables, try server.logicalTableNamesInArena(a, root.context, names)) |table, logical| {
                if (table.cascade_objects.len == 0) continue;
                const object = table.cascade_objects[0];
                const target = try local.system_catalog_domain.Target.parse(logical);
                const display = try target.displayNameAlloc(a);
                break :single switch (object.kind) {
                    .column => try std.fmt.allocPrint(a, "drop cascades to column {s} of table {s}", .{ object.name, display }),
                    .default => try std.fmt.allocPrint(a, "drop cascades to default value for column {s} of table {s}", .{ object.name, display }),
                    .constraint => try std.fmt.allocPrint(a, "drop cascades to constraint {s} on table {s}", .{ object.name, display }),
                    .index => try std.fmt.allocPrint(a, "drop cascades to index {s}", .{object.name}),
                };
            }
            unreachable;
        } };
        // Notice allocation is strictly before durable admission too.
        if (notice) |value| if (root.notice_sink) |sink| try sink.emit(sink.ptr, value);
        var replacements = std.ArrayList(admission.Replacement).empty;
        for (cohort, prepared.tables) |table, candidate| {
            for (candidate.dropped_columns) |column| try local.sql_schema_ddl.requireIndependentRenameIndexes(a, table.indexes_json, column);
            if (candidate.changed) try replacements.append(a, .{ .table_id = table.table_id, .schema_json = candidate.schema_json, .policies = .{ .dropped_columns = if (candidate.dropped_columns.len != 0) .allow else .reject } });
        }
        const key = std.fmt.bytesToHex(request.admission.nonce, .lower);
        const namespace = try std.fmt.allocPrint(a, "type-retirement:{s}:{d}", .{ principal, before.identity.id });
        const job_id = try jobs.jobIdForIdempotency(a, namespace, &key);
        const id = try stages.idForAttempt(job_id, 1);
        var observer: admission.SourceObserver = .{ .server = server, .context = root.context };
        defer observer.deinit();
        var plan = try admission.buildReplacements(a, id, cohort, topology.ranges, replacements.items, &observer, null);
        plan.type_retirement = .{ .request = request, .before = before };
        try admission.bindSequenceHandoffs(server, a, root.context, &plan);
        observation.retirement = .{ .group = (try server.source.status()).metadata_group_id, .id = id, .intent_digest = plan.cohort_digest, .incarnation = request.incarnation, .identity = .{ .id = before.identity.id, .generation = before.identity.generation } };
        const start: jobs.StartRequest = .{ .scope = .cluster, .source_kind = .schema_rewrite, .backup_id = &std.fmt.bytesToHex(plan.cohort_digest, .lower), .location = try std.fmt.allocPrint(a, "metadata://schema-rewrite/{s}", .{std.fmt.bytesToHex(id, .lower)}), .connection = "internal", .restore_mode = "overwrite", .table_names = names, .idempotency_namespace = namespace, .idempotency_key = &key, .destination_authorization_principal = principal, .rewrite_plan_json = try std.json.Stringify.valueAlloc(a, plan, .{}) };
        receipt = try recovery.newReceipt(allocators.result, a, observation, cut.revision);
        owns_receipt = true;
        try root.context.ensureActive();
        const accepted = try server.restore_job_store.startRecoverable(a, start);
        switch (accepted) {
            .accepted => |bytes| {
                a.free(bytes);
                // Durable job admission is known even if the request expires
                // before its first read-only observation. No type generation
                // or publication revision is predicted here.
                receipt.state = .pending;
                receipt.object_id = before.identity.id;
                receipt.diagnostic = null;
            },
            .unknown => |bytes| a.free(bytes),
        }
        server.schedulePendingRestoreJobs() catch {};
    }
    return ddl.awaitOutcome(root, allocators, observation, cut.revision, receipt);
}
