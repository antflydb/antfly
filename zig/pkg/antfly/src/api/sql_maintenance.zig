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

//! SQL maintenance coordinates bounded native work across the exact owner
//! cohort. Completion is observation of finished work, never mere admission.
const std = @import("std");
const local = @import("antfly_local_sources");
const contract = local.api_sql_maintenance_contract;
const catalog = local.sql_catalog;
const ast = local.sql_ast;
const execution = @import("sql_execution.zig");
const metadata = @import("../metadata/api.zig");
const domain = local.system_catalog_domain;
const wire = @import("antfly_indexes_openapi").types;
const A = std.mem.Allocator;

fn wait(root: *execution.Adapter) !void {
    try root.context.ensureActive();
    const io = root.server.sharedApiIo() orelse return error.UnsupportedSqlExecution;
    try io.sleep(.fromMilliseconds(10), .awake);
    try root.context.ensureActive();
}

fn tableForIndex(root: *execution.Adapter, a: A, name: ast.Name) !catalog.Table {
    const path = root.ddl_search_path;
    const use_path = name.namespace == null and name.database == null and path != null;
    const count = if (use_path) path.?.len else 1;
    const targets = try a.alloc(domain.RelationTarget, count);
    for (targets, 0..) |*target, i| target.* = .{ .database = name.database orelse root.database, .namespace = if (use_path) path.?.entries[i].slice() else name.namespace orelse root.namespace, .name = name.table };
    const request: domain.ResolveMany = .{ .relations = targets, .include_query_definitions = true, .expected_revision = root.revision };
    const bytes = try root.server.source.systemCatalog(a, root.context, .{ .resolve_many = request });
    const cut = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
    try cut.validateRelations(request);
    for (cut.relations, targets) |found, target| if (found) |owner| {
        root.revision = cut.revision;
        const backend = root.backend();
        const table = try backend.vtable.resolve(backend.ptr, a, .{ .database = target.database, .namespace = target.namespace, .table = owner.logical_table }, .admin);
        if (owner.owner.kind != .index) return error.SqlWrongObjectType;
        if (table.id != owner.owner.table_id or table.schema_version != owner.owner.schema_version) return error.CatalogGenerationChanged;
        return table;
    };
    return error.SqlIndexNotFound;
}

fn validateCut(table: catalog.Table, cut: metadata.AdminSnapshot) !void {
    const record = @import("tables.zig").findTableByName(&cut, table.physical_name) orelse return error.TableNotFound;
    if (record.table_id != table.id or !std.mem.eql(u8, record.schema_json, table.schema_json)) return error.PreparedGenerationChanged;
    if (record.restore_backup_id.len != 0 or record.relational_retirement_json.len != 0) return error.TableTransitionActive;
    var count: usize = 0;
    for (cut.ranges) |range| if (range.table_id == table.id) {
        if (range.restore_backup_id.len != 0) return error.TableTransitionActive;
        count += 1;
    };
    if (count == 0) return error.LeaderUnavailable;
}

pub fn execute(root: *execution.Adapter, allocators: catalog.DdlAllocators, ddl: ast.CatalogDdl) !catalog.DdlOutcome {
    const intent = ddl.maintenance orelse return error.InvalidSqlSyntax;
    if (root.active_transaction != null) return error.SqlMaintenanceInTransaction;
    if (root.server.cfg.auth_enabled and root.identity.* == null) return error.Forbidden;
    try root.context.ensureActive();
    var region = std.heap.ArenaAllocator.init(allocators.scratch);
    defer region.deinit();
    const a = region.allocator();
    const previous_privileges = root.table_privileges;
    root.table_privileges = local.system_catalog_privileges.Access.bit(.maintain);
    defer root.table_privileges = previous_privileges;
    const backend = root.backend();
    const table = if (ddl.kind == .index) try tableForIndex(root, a, ddl.name) else try backend.vtable.resolve(backend.ptr, a, ddl.name, .admin);
    try @import("sql_authorization_runtime.zig").authorize(root, a, table);
    if (table.external_base_source != null) return error.ExternalLakeReadOnly;
    for (intent.columns) |name| _ = try table.column(name);
    const verbose_message = if (intent.verbose) try std.fmt.allocPrint(a, "{s} completed for table \"{s}\"", .{ if (intent.operation == .vacuum) @as([]const u8, "VACUUM") else "ANALYZE", ddl.name.table }) else null;
    if (verbose_message) |message| if (root.notice_sink) |sink| if (sink.reserve) |reserve| try reserve(sink.ptr, 1, message.len);
    const writer = root.server.table_writes orelse return error.UnsupportedSqlExecution;
    var initial = (try root.server.source.adminSnapshot()) orelse return error.Unavailable;
    defer root.server.source.freeAdminSnapshot(&initial);
    try validateCut(table, initial);
    const topology = metadata.catalogTableTopology(table.id, initial.ranges);
    if (intent.operation == .reindex) {
        try reindex(root, a, allocators.scratch, table, if (ddl.kind == .index) ddl.name.table else null);
    } else {
        var nonce: [16]u8 = undefined;
        try root.server.sqlPlanCacheIo().randomSecure(&nonce);
        const body = try std.json.Stringify.valueAlloc(a, contract.Request{ .table_id = table.id, .schema_version = table.schema_version, .request_id = nonce, .operation = if (intent.operation == .vacuum) .vacuum else .analyze, .full = intent.full, .analyze = intent.analyze, .columns = intent.columns }, .{});
        const control: @import("index_maintenance_control.zig").Control = .{ .request = root.context };
        const done = try a.alloc(bool, initial.ranges.len);
        @memset(done, false);
        while (true) {
            var pending = false;
            for (initial.ranges, 0..) |range, i| {
                if (range.table_id != table.id or done[i]) continue;
                try root.context.ensureActive();
                var quantum = std.heap.ArenaAllocator.init(allocators.scratch);
                defer quantum.deinit();
                const qa = quantum.allocator();
                const reply = writer.sqlMaintenanceGroup(qa, range.group_id, table.physical_name, body, control.cancellation()) catch |err| switch (err) {
                    error.MaintenanceBusy, error.LeaderUnavailable, error.Unavailable => {
                        pending = true;
                        continue;
                    },
                    else => return err,
                } orelse return error.UnsupportedSqlExecution;
                defer qa.free(reply);
                if (reply.len > contract.max_bytes) return error.InvalidSqlBackendResponse;
                var response = try std.json.parseFromSlice(contract.Response, allocators.scratch, reply, .{});
                defer response.deinit();
                if (response.value.table_id != table.id or response.value.schema_version != table.schema_version) return error.PreparedGenerationChanged;
                done[i] = response.value.done;
                pending = pending or !done[i];
            }
            if (!pending) break;
            try wait(root);
        }
    }
    var current = (try root.server.source.adminSnapshot()) orelse return error.Unavailable;
    defer root.server.source.freeAdminSnapshot(&current);
    try validateCut(table, current);
    const after = metadata.catalogTableTopology(table.id, current.ranges);
    if (topology.range_count != after.range_count or !std.mem.eql(u8, &topology.digest, &after.digest)) return error.TopologyChanged;
    if (intent.operation == .analyze or intent.analyze) {
        // Replace a prior negative cache entry immediately after explicit work.
        if (statistics(root, a, table) catch null) |sample| {
            const raw = try std.json.Stringify.valueAlloc(a, sample, .{});
            root.server.sql_schema_cache.statistics.put(root.server.sqlPlanCacheIo(), root.server.sql_schema_cache.allocator, table.id, table.schema_version, raw) catch {};
        }
    }
    try root.context.ensureActive();
    if (verbose_message) |message| if (root.notice_sink) |sink| try sink.emit(sink.ptr, .{ .message = message });
    return .{};
}

/// Collect only completed same-schema observations from every current owner.
/// Missing shards invalidate the whole estimate rather than treating them as
/// empty. No primary rows are read by this path.
fn statistics(root: *execution.Adapter, a: A, table: catalog.Table) !?contract.Aggregate {
    const writer = root.server.table_writes orelse return null;
    if (writer.vtable.sql_maintenance_group == null) return null;
    var cut = (try root.server.source.adminSnapshot()) orelse return null;
    defer root.server.source.freeAdminSnapshot(&cut);
    try validateCut(table, cut);
    const topology = metadata.catalogTableTopology(table.id, cut.ranges);
    if (topology.range_count > 128) return null;
    var request = try root.context.platformDeadline();
    request.deadline_ns = @min(request.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 100 * std.time.ns_per_ms);
    const control: @import("index_maintenance_control.zig").Control = .{ .request = request };
    const body = try std.json.Stringify.valueAlloc(a, contract.Request{ .table_id = table.id, .schema_version = table.schema_version, .request_id = @splat(1), .operation = .statistics }, .{});
    var budget: local.sql_memory_budget = .{ .backing = root.server.alloc, .limit = 32 * 1024 * 1024 };
    var scratch = std.heap.ArenaAllocator.init(budget.allocator());
    defer scratch.deinit();
    var result: contract.Aggregate = .{};
    for (cut.ranges) |range| {
        if (range.table_id != table.id) continue;
        try request.ensureActive();
        _ = scratch.reset(.free_all);
        const qa = scratch.allocator();
        const raw = (try writer.sqlMaintenanceGroup(qa, range.group_id, table.physical_name, body, control.cancellation())) orelse return null;
        if (raw.len > contract.max_bytes) return error.InvalidSqlBackendResponse;
        const response = try std.json.parseFromSliceLeaky(contract.Response, qa, raw, .{ .allocate = .alloc_always });
        if (!response.done or !response.available) return null;
        const sample = response.statistics orelse return error.InvalidSqlBackendResponse;
        try sample.validate();
        const manager = @import("../metadata/table_manager.zig");
        if (response.table_id != table.id or response.schema_version != table.schema_version or sample.schema_version != table.schema_version or !sample.identity.eql(.{ .table_id = table.id, .shard_id = manager.rangeDocIdentityShardId(range), .range_id = manager.rangeDocIdentityRangeId(range) })) return error.PreparedGenerationChanged;
        if (result.fields.len == 0) {
            result.fields = try a.dupe(contract.Field, sample.fields);
            for (result.fields) |*field| field.name = try a.dupe(u8, field.name);
        } else {
            if (sample.fields.len != result.fields.len) return null;
            for (result.fields) |*field| {
                const matching = for (sample.fields) |candidate| {
                    if (std.mem.eql(u8, candidate.name, field.name)) break candidate;
                } else return null;
                field.sketch.merge(matching.sketch);
            }
        }
        result.rows +|= sample.rows;
    }
    return result;
}

/// Warm during binding, before this statement retains any native read view.
/// Later correlated/dynamic bindings use cached estimates or no estimate.
/// Missing advisory data must never force a second owner admission from a
/// cursor that already retains the same physical owner's snapshot.
pub fn prepareStatistics(root: *execution.Adapter, table: catalog.Table) !void {
    if (table.external_base_source != null or root.statistics_reads_started) return;
    try root.context.ensureActive();
    const cache = &root.server.sql_schema_cache.statistics;
    const io = root.server.sqlPlanCacheIo();
    const a = root.server.sql_schema_cache.allocator;
    const token = (cache.claim(io, a, table.id, table.schema_version) catch return) orelse return;
    var budget: local.sql_memory_budget = .{ .backing = root.server.alloc, .limit = 32 * 1024 * 1024 };
    var region = std.heap.ArenaAllocator.init(budget.allocator());
    defer region.deinit();
    const scratch = region.allocator();
    const sample = statistics(root, scratch, table) catch {
        cache.complete(io, a, table.id, table.schema_version, token, "null") catch {};
        try root.context.ensureActive();
        return;
    };
    const raw = std.json.Stringify.valueAlloc(scratch, sample, .{}) catch return;
    cache.complete(io, a, table.id, table.schema_version, token, raw) catch {};
    try root.context.ensureActive();
}

pub fn attachStatistics(root: *execution.Adapter, backing: A, table: catalog.Table, cursor: *catalog.Cursor) !?std.heap.ArenaAllocator {
    if (table.external_base_source != null) return null;
    var owner = std.heap.ArenaAllocator.init(backing);
    var retained = false;
    defer if (!retained) owner.deinit();
    const a = owner.allocator();
    const cache = &root.server.sql_schema_cache.statistics;
    const io = root.server.sqlPlanCacheIo();
    // Cursor acquisition must never start owner/catalog work. A retained
    // snapshot can itself delay the owner operation needed by a cache miss.
    const cached = (try cache.get(io, a, table.id, table.schema_version)) orelse return null;
    const aggregate = try std.json.parseFromSliceLeaky(?contract.Aggregate, a, cached, .{});
    const sample = aggregate orelse return null;
    const columns = try a.alloc(catalog.ColumnStatistics, sample.fields.len);
    var width: u64 = 0;
    for (sample.fields, columns) |field, *column| {
        _ = table.column(field.name) catch return null;
        column.* = try field.sketch.estimate(a, field.name);
        width +|= column.average_bytes orelse 0;
    }
    cursor.column_statistics = columns;
    cursor.estimated_rows = sample.rows;
    cursor.estimated_bytes = if (width != 0) sample.rows *| width else null;
    retained = true;
    return owner;
}

fn observeIndexes(root: *execution.Adapter, a: A, table: catalog.Table) !@import("relational_index_status.zig").Collection {
    const reader = root.server.table_reads orelse return error.UnsupportedSqlExecution;
    while (true) {
        try root.context.ensureActive();
        // Status performs a before/after catalog check. Retry an unstable
        // observation without changing or readmitting the repair owner ticket.
        return @import("relational_index_status.zig").collectAll(a, root.server.source, reader, table.physical_name, table.schema_json, root.context) catch |err| switch (err) {
            error.TopologyChanged, error.LeaderUnavailable, error.GroupLeaderUnavailable, error.Unavailable => {
                try wait(root);
                continue;
            },
            else => return err,
        };
    }
}

fn readyIndexes(root: *execution.Adapter, a: A, table: catalog.Table, selected: ?[]const u8) !@import("relational_index_status.zig").Collection {
    while (true) {
        var cut = try observeIndexes(root, a, table);
        var retained = false;
        defer if (!retained) cut.deinit();
        var ready = true;
        var found = selected == null;
        for (cut.statuses) |status| {
            if (selected) |name| if (!std.mem.eql(u8, name, status.index_name)) continue;
            found = true;
            ready = ready and (status.state == .ready or status.state == .failed);
        }
        if (!found) return error.SqlIndexNotFound;
        if (ready) {
            retained = true;
            return cut;
        }
        try wait(root);
    }
}

fn reindex(root: *execution.Adapter, a: A, scratch_backing: A, table: catalog.Table, selected: ?[]const u8) !void {
    // Reuse the /indexes replicated ticket protocol and wait on every exact
    // owner. UNIQUE/FK support keeps its ordinary write-side enforcement.
    const reader = root.server.table_reads orelse return error.UnsupportedSqlExecution;
    const writer = root.server.table_writes orelse return error.UnsupportedSqlExecution;
    if (table.storage_mode != .relational) return error.UnsupportedSqlExecution;
    var before = try readyIndexes(root, scratch_backing, table, selected);
    defer before.deinit();
    var found = selected == null;
    for (before.statuses) |status| {
        if (selected) |name| if (!std.mem.eql(u8, name, status.index_name)) continue;
        found = true;
        const owners = try a.alloc(wire.IndexMaintenanceOwnerProof, status.ranges.len);
        for (status.ranges, owners) |range, *proof| proof.* = .{ .group_id = range.group_id, .generation = range.generation, .slot = range.slot, .owner = range.owner, .comparison = range.comparison, .progress_digest = range.progress_digest, .maintenance_epoch = range.maintenance_epoch };
        var offset: usize = 0;
        while (offset < owners.len) {
            var chunk = std.heap.ArenaAllocator.init(scratch_backing);
            defer chunk.deinit();
            const ca = chunk.allocator();
            const end = @min(owners.len, offset + 128);
            const body = try std.json.Stringify.valueAlloc(ca, wire.IndexMaintenanceRequest{ .table_id = status.table_id, .schema_version = status.schema_version, .owners = owners[offset..end] }, .{});
            // An ambiguous reply is retried with exactly the same owner proofs.
            while (true) {
                const admitted = @import("relational_index_maintenance.zig").execute(scratch_backing, root.server.source, reader, writer, table.physical_name, status.index_name, .repair, body, root.context) catch |err| switch (err) {
                    error.TopologyChanged, error.LeaderUnavailable, error.Unavailable => {
                        try wait(root);
                        continue;
                    },
                    else => return err,
                };
                scratch_backing.free(admitted);
                break;
            }
            offset = end;
        }
        while (true) {
            var current = try observeIndexes(root, scratch_backing, table);
            defer current.deinit();
            const latest = for (current.statuses) |candidate| {
                if (std.mem.eql(u8, candidate.index_name, status.index_name)) break candidate;
            } else return error.PreparedGenerationChanged;
            if (latest.ranges.len != owners.len) return error.TopologyChanged;
            var ready = latest.state == .ready;
            for (owners) |owner| {
                const observation = for (latest.ranges) |candidate| {
                    if (std.mem.eql(u8, candidate.group_id, owner.group_id)) break candidate;
                } else return error.TopologyChanged;
                if (!std.mem.eql(u8, observation.generation, owner.generation) or observation.slot != owner.slot or !std.mem.eql(u8, observation.owner, owner.owner) or !std.mem.eql(u8, observation.comparison, owner.comparison)) return error.PreparedGenerationChanged;
                const epoch = try std.fmt.parseInt(u64, observation.maintenance_epoch, 10);
                const predecessor = try std.fmt.parseInt(u64, owner.maintenance_epoch, 10);
                if (epoch != predecessor + 1) return error.PreparedGenerationChanged;
                if (observation.state == .failed) return error.IndexBuildFailed;
                ready = ready and observation.state == .ready;
            }
            if (ready) break;
            try wait(root);
        }
    }
    if (!found) return error.SqlIndexNotFound;
}

test "SQL statistics cache cannot start owner work after native read admission" {
    var root: execution.Adapter = .{ .server = undefined, .identity = undefined, .context = .{}, .statistics_reads_started = true };
    try prepareStatistics(&root, .{ .id = 1, .physical_name = "retained", .schema_version = 1, .columns = &.{} });
}
