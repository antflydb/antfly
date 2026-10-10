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

//! Embedded retirement uses the native owner proof and durable transaction
//! coordinator. The target declaration survives interruption; recovery resumes
//! checkpoints rather than replaying DDL or releasing claims without a fence.
const h = @import("handles.zig");
const std = h.std;
const d = h.antfly.capi_dependencies;
const sql = @import("sql.zig");
const catalog = d.storage_db_relational_integrity_catalog;
const activation = d.storage_db_relational_integrity_activation_contract;
const retirement = d.storage_db_relational_integrity_retirement_contract;
const key = "\x00antfly/embedded/sql-ddl";
const Job = struct { table_id: u64, version: u32, target: []const u8, generations: []const [16]u8, digest: [32]u8 };
const Status = struct { catalog: []const u8, progress: ?[]const u8, owner: [32]u8, range_start: []const u8, range_end: []const u8 };

const DrainBudget = struct {
    rows: u32 = 128,

    fn shrink(self: *DrainBudget, observed: usize) bool {
        if (observed <= 1 or self.rows <= 1) return false;
        self.rows = @intCast(@max(@as(usize, 1), @min(self.rows / 2, observed / 2)));
        return true;
    }
};

fn status(adapter: anytype, alloc: std.mem.Allocator) !Status {
    const response = (try adapter.db.lookup(alloc, "", .{ .relational_integrity_jobs_json = "{\"kind\":\"retirement\"}" })) orelse return error.IntegrityCatalogUnavailable;
    return std.json.parseFromSliceLeaky(Status, alloc, response.json, .{});
}

fn store(db: *h.db_mod.DB, alloc: std.mem.Allocator, job: ?Job) !void {
    const bytes = if (job) |value| try std.json.Stringify.valueAlloc(alloc, value, .{}) else null;
    var write = try db.core.store.beginWriteTxn();
    errdefer write.abort();
    if (bytes) |value| try write.put(key, value) else try write.delete(key);
    write.commit() catch return error.SqlMutationOutcomeUnknown;
}

/// Returns false when ordinary schema CAS is sufficient. A true result has
/// durably retired the selected UNIQUE generations and published the target.
pub fn publish(adapter: anytype, alloc: std.mem.Allocator, target: []const u8, version: u32) !bool {
    const state = try status(adapter, alloc);
    if (state.progress != null) return error.ConstraintRetirementInProgress;
    var current = try catalog.decode(alloc, state.catalog);
    defer current.deinit();
    if (current.schema_version != version) return error.PreparedGenerationChanged;
    const parsed = try h.tables_api.parseValidatedTableSchema(alloc, target);
    const runtime = try h.tables_api.deriveRuntimeTableSchema(alloc, parsed);
    const definitions = try d.schema_relational_declarations.definitionFingerprints(alloc, parsed, runtime);
    var removed: std.ArrayList([16]u8) = .empty;
    for (current.bindings) |binding| {
        if (binding.retired) continue;
        const retained = for (definitions) |definition| {
            if (definition.kind == binding.definition.kind and std.mem.eql(u8, definition.name, binding.definition.name) and std.mem.eql(u8, &definition.fingerprint, &binding.definition.fingerprint)) break true;
        } else false;
        if (!retained) {
            // Child FK changes require parent-owner generation publication;
            // native storage intentionally disallows an ordinary schema CAS.
            if (binding.definition.kind == .foreign_key) return error.ForeignKeyGenerationPublicationRequired;
            try removed.append(alloc, binding.generation);
        }
    }
    if (removed.items.len == 0) return false;
    const metadata = try adapter.localCatalog(alloc, version);
    const previous_bytes = (try adapter.db.getSchemaJson(alloc)) orelse return error.IntegrityCatalogUnavailable;
    const previous = try h.tables_api.parseValidatedTableSchema(alloc, previous_bytes);
    const uniques = try previous.relationalUniqueDefinitions(alloc);
    // Existing references own one UNIQUE generation. An equivalent retained
    // UNIQUE does not transfer references away from the retiring generation.
    for (metadata.table) |record| {
        const child = try h.tables_api.parseValidatedTableSchema(alloc, if (record.table_id == adapter.db.core.identity_namespace.table_id) target else record.schema_json);
        for (try child.relationalForeignKeyDefinitions(alloc)) |fk| {
            if (!std.mem.eql(u8, fk.parent_table, adapter.table_name)) continue;
            const unique = try d.schema_relational_foreign_key_target.resolveUnique(uniques, fk.parent_columns);
            const binding = current.find(.unique, unique.name) orelse return error.IntegrityCatalogChanged;
            for (removed.items) |generation| if (std.mem.eql(u8, &generation, &binding.generation)) return error.SqlDependentConstraint;
        }
    }
    const serialized = try d.storage_schema.serializeSchema(alloc, runtime);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(serialized, &digest, .{});
    const job: Job = .{ .table_id = adapter.db.core.identity_namespace.table_id, .version = version, .target = target, .generations = removed.items, .digest = digest };
    try store(adapter.db, alloc, job);
    finishJob(adapter, job) catch return error.SqlDdlPending;
    return true;
}

fn commit(adapter: anytype, alloc: std.mem.Allocator, requests: []const d.api_distributed_txn_contract.TableCommitRequest) !void {
    const outcome = try @import("sql_commit.zig").commit(adapter.handle.?, requests, &adapter.outcome_transaction_id);
    if (outcome != .committed) return error.SqlDdlPending;
    _ = alloc;
}

fn finishJob(adapter: anytype, job: Job) !void {
    if (job.table_id != adapter.db.core.identity_namespace.table_id) return error.PreparedGenerationChanged;
    const backing = adapter.db.alloc;
    const current_bytes = (try adapter.db.getSchemaJson(backing)) orelse return error.IntegrityCatalogUnavailable;
    defer backing.free(current_bytes);
    if (std.mem.eql(u8, current_bytes, job.target)) {
        try store(adapter.db, backing, null);
        return;
    }
    var setup = std.heap.ArenaAllocator.init(backing);
    defer setup.deinit();
    const a = setup.allocator();
    const metadata = try adapter.localCatalog(a, job.version);
    var budget: DrainBudget = .{};
    // One owner contains the complete embedded table. Native admission still
    // checks its incarnation, generation set, transaction guards and range.
    for (0..4096) |_| {
        var page_arena = std.heap.ArenaAllocator.init(backing);
        defer page_arena.deinit();
        const p = page_arena.allocator();
        const state = try status(adapter, p);
        if (state.range_start.len != 0 or state.range_end.len != 0) return error.UnsupportedSqlExecution;
        var bindings = try catalog.decode(p, state.catalog);
        defer bindings.deinit();
        if (bindings.schema_version != job.version) return error.PreparedGenerationChanged;
        var progress: retirement.Progress = if (state.progress) |bytes| try retirement.Progress.decode(bytes) else .{
            .job_id = job.digest[0..16].*,
            .generation_set = activation.generationSet(bindings),
            .owner = state.owner,
            .target_schema_digest = job.digest,
            .schema_version = job.version,
            .generations = job.generations,
        };
        if (!std.mem.eql(u8, &progress.target_schema_digest, &job.digest)) return error.ConstraintRetirementChanged;
        if (state.progress == null or progress.phase == .fenced) {
            if (state.progress != null) progress.phase = .foreign_keys;
            const command: retirement.Command = .{ .routing_key = "", .expected = state.progress, .next = try progress.encode(p) };
            try commit(adapter, p, &.{.{ .table_name = adapter.table_name, .relational_schema_version = job.version, .relational_integrity_generation_set = progress.generation_set, .relational_retirement = command }});
            continue;
        }
        if (progress.phase == .ready) {
            try adapter.db.compareAndSetSchemaJson(p, job.target, job.version);
            try store(adapter.db, p, null);
            return;
        }
        const page_request = try std.fmt.allocPrint(p, "{{\"kind\":\"retirement\",\"mode\":\"page\",\"max_rows\":{d}}}", .{budget.rows});
        const response = (try adapter.db.lookup(p, "", .{ .relational_integrity_jobs_json = page_request })) orelse return error.ConstraintRetirementChanged;
        const page = try std.json.parseFromSliceLeaky(struct { rows: []const d.api_relational_integrity_commit.BackfillRow, command: retirement.Command, phase: retirement.Phase }, p, response.json, .{});
        if (page.phase != progress.phase or !std.mem.eql(u8, page.command.expected orelse return error.ConstraintRetirementChanged, state.progress.?) or page.command.routing_key.len != 0) return error.ConstraintRetirementChanged;
        var prepared = d.api_relational_integrity_commit.prepareRetirementPage(p, adapter.localSource(), metadata.table, adapter.table_name, page.rows, progress, .{}) catch |err| {
            if (err == error.TransactionTooLarge and budget.shrink(page.rows.len)) continue;
            return err;
        };
        defer prepared.deinit();
        const requests = try p.dupe(d.api_distributed_txn_contract.TableCommitRequest, prepared.tables);
        const source = for (requests) |*request| {
            if (std.mem.eql(u8, request.table_name, adapter.table_name)) break request;
        } else return error.InvalidConstraintRetirement;
        source.relational_retirement = page.command;
        commit(adapter, p, requests) catch |err| {
            if (err == error.TransactionTooLarge and budget.shrink(page.rows.len)) continue;
            return err;
        };
        budget = .{};
    }
    return error.SqlDdlPending;
}

pub fn recover(handle: *h.Handle) !void {
    try @import("sql_fk.zig").recover(handle);
    try recoverTable(handle, &handle.db, "default");
    var iterator = handle.embedded_tables.valueIterator();
    while (iterator.next()) |table| try recoverTable(handle, &table.*.db, table.*.name);
}

/// Native schema publication creates durable activation progress. Reuse the
/// server's bounded worker so row observations, claims and coverage publish
/// in the same transaction, including invalid-generation diagnostics.
fn activationWriter(adapter: anytype) d.api_table_write_source.TableWriteSource {
    const A = @TypeOf(adapter.*);
    const Writer = struct {
        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: h.db_mod.types.BatchRequest) anyerror!?void {
            return error.UnsupportedSqlExecution;
        }
        fn commitPage(ptr: *anyopaque, _: std.mem.Allocator, requests: []const d.api_distributed_txn_contract.TableCommitRequest, _: h.db_mod.types.SyncLevel, cancellation: h.db_mod.types.CancellationToken) anyerror!?d.api_distributed_txn_contract.CommitOutcome {
            const owner: *A = @ptrCast(@alignCast(ptr));
            if (cancellation.isCancelled()) return error.Canceled;
            const outcome = try @import("sql_commit.zig").commit(owner.handle.?, requests, &owner.outcome_transaction_id);
            if (outcome != .committed) return error.SqlDdlPending;
            return .{ .committed = .{ .participant_count = requests.len } };
        }
    };
    return .{ .ptr = adapter, .vtable = &.{ .batch = Writer.batch, .commit_batch_with_cancellation = Writer.commitPage } };
}

/// Validation retries the exact failed coverage checkpoint. It does not edit
/// declarations, allocate a new generation or advance the schema epoch.
pub fn retry(adapter: anytype, version: u32) !void {
    var arena = std.heap.ArenaAllocator.init(adapter.db.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const metadata = try adapter.localCatalog(a, version);
    try d.api_relational_constraint_recovery.retry(a, adapter.localSource(), activationWriter(adapter), metadata.table, metadata.range, .{ .table_name = adapter.table_name, .relational_schema_version = version }, .{
        .deadline_ns = h.antfly.platform_time.monotonicNs() +| 5 * std.time.ns_per_s,
    });
}

pub fn activate(adapter: anytype, version: u32) !void {
    var arena = std.heap.ArenaAllocator.init(adapter.db.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const schema = (try adapter.db.getSchemaJson(a)) orelse return;
    if (!try d.api_relational_integrity_commit.requiresActivation(a, schema)) return;
    const metadata = try adapter.localCatalog(a, version);
    const writer = activationWriter(adapter);
    for (0..4096) |_| {
        if (!try d.api_relational_activation_worker.runPage(adapter.db.alloc, adapter.localSource(), writer, metadata.table, metadata.range, metadata.range[0])) return;
    }
    return error.SqlDdlPending;
}

fn recoverTable(handle: *h.Handle, db: *h.db_mod.DB, name: []const u8) !void {
    var adapter = sql.Adapter(h.antfly){ .handle = handle, .db = db, .table_name = name };
    if (try db.core.getStoreValue(handle.alloc, key)) |bytes| {
        defer handle.alloc.free(bytes);
        if (!h.liteOpenModeCanWrite(handle.open_mode)) return error.SqlStatementReadUnavailable;
        const job = try std.json.parseFromSlice(Job, handle.alloc, bytes, .{});
        defer job.deinit();
        try finishJob(&adapter, job.value);
    }
    if (!h.liteOpenModeCanWrite(handle.open_mode)) return;
    const schema = (try db.getSchemaJson(handle.alloc)) orelse return;
    defer handle.alloc.free(schema);
    if (!try d.api_relational_integrity_commit.requiresActivation(handle.alloc, schema)) return;
    if (try db.lookup(handle.alloc, "", .{ .relational_activation_json = "{}" })) |response| {
        defer handle.alloc.free(response.json);
        var state = try std.json.parseFromSlice(struct { state: activation.State }, handle.alloc, response.json, .{ .ignore_unknown_fields = true });
        defer state.deinit();
        if (state.value.state != .validating) return;
    }
    var view = db.core.acquireSchemaView() orelse return error.PreparedGenerationChanged;
    defer view.release();
    activate(&adapter, view.version()) catch |err| switch (err) {
        error.SqlDdlPending => return,
        else => return err,
    };
}
