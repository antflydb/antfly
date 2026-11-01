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
    const root = adapter.handle orelse return error.UnsupportedSqlExecution;
    try requireCatalogRecovered(root);
    const was_recovering = root.sql_ddl_recovering;
    root.sql_ddl_recovering = true;
    defer root.sql_ddl_recovering = was_recovering;
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
    invalidate(adapter.handle.?);
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

/// Call before admission, including attempts whose durable outcome is unknown.
/// Table handles share their owning database's recovery state.
pub fn invalidate(handle: *h.Handle) void {
    const root = handle.parent_handle orelse handle;
    root.sql_ddl_recovery_needed = true;
    root.sql_ddl_agenda_scan_started = false;
}

pub fn recover(handle: *h.Handle) !void {
    if (handle.sql_ddl_recovering) return;
    handle.sql_ddl_recovering = true;
    defer handle.sql_ddl_recovering = false;
    if (!handle.sql_ddl_recovery_needed) {
        if (handle.owned_lite_backend) |*backend| {
            if (backend.native_docstore) |storage| {
                if (!storage.file.header.indexed_reclamation or std.mem.eql(u8, &handle.sql_ddl_recovery_generation, &storage.file.activeCheckpoint().logical.metadata)) return;
            } else return;
        } else return;
    }
    if (h.builtin.is_test) handle.sql_ddl_recovery_scans +%= 1;
    if (handle.owned_lite_backend) |*backend| {
        if (backend.native_docstore) |storage| if (storage.file.header.indexed_reclamation) {
            try recoverAgenda(handle, storage);
            return;
        };
    }
    try @import("tables.zig").loadAvailable(handle);
    try @import("sql_fk.zig").recover(handle);
    var complete = try recoverTable(handle, &handle.db, "default", canRecoverWrite(handle));
    var iterator = handle.embedded_tables.valueIterator();
    while (iterator.next()) |table| {
        if (!try recoverTable(handle, &table.*.db, table.*.name, canRecoverWrite(handle))) complete = false;
    }
    handle.sql_ddl_recovery_needed = !complete or handle.embedded_table_errors.count() != 0;
}

/// Read only the compact agenda before opening any namespace/index runtime.
/// The path lease encloses recovery and acknowledgement. A crash leaves a
/// conservative marker; a peer's later mutation atomically recreates it.
fn recoverAgenda(handle: *h.Handle, storage: anytype) !void {
    const agenda = @import("../storage/lite/recovery_agenda.zig");
    const native = @import("../storage/lite/native.zig");
    // This coordinator affects every table. It must settle even when this
    // agenda page wraps past a root marker earlier than the cursor.
    try @import("sql_fk.zig").recover(handle);
    const read_only = !canRecoverWrite(handle);
    // Cursor progress is retained for fairness, including on failed namespace
    // opens. An interrupted sweep can never certify the unchecked remainder;
    // reaching its end must wrap and successfully inspect a complete sweep.
    errdefer if (read_only) {
        handle.sql_ddl_agenda_scan_complete = false;
        handle.sql_ddl_recovery_needed = true;
    };
    const generation = storage.file.activeCheckpoint().logical.metadata;
    if (read_only and (!handle.sql_ddl_agenda_scan_started or
        !std.mem.eql(u8, &handle.sql_ddl_agenda_scan_generation, &generation)))
    {
        handle.sql_ddl_agenda_after_len = 0;
        handle.sql_ddl_agenda_scan_generation = generation;
        handle.sql_ddl_agenda_scan_started = true;
        handle.sql_ddl_agenda_scan_complete = true;
    }
    var cursor = try storage.file.metadataCatalogCursor(storage.file.activeCheckpoint(), agenda.prefix);
    defer cursor.deinit();
    if (handle.sql_ddl_agenda_after_len != 0)
        cursor.start_after = handle.sql_ddl_agenda_after[0..handle.sql_ddl_agenda_after_len];
    var keys: std.ArrayList([]u8) = .empty;
    defer {
        for (keys.items) |key_bytes| handle.alloc.free(key_bytes);
        keys.deinit(handle.alloc);
    }
    // Capture keys before recovery writes can retire their catalog pages.
    const max_namespaces_per_turn = 8;
    while (keys.items.len < max_namespaces_per_turn) {
        const entry = (try cursor.next()) orelse break;
        defer storage.file.allocator.free(entry.key);
        const owned = try handle.alloc.dupe(u8, entry.key);
        errdefer handle.alloc.free(owned);
        try keys.append(handle.alloc, owned);
    }
    const more = keys.items.len == max_namespaces_per_turn;
    if (keys.items.len == 0) {
        // Wrap on the next admission; a failed earlier namespace must not
        // starve siblings, and a completed sweep cannot hide earlier debt.
        const wrapped = handle.sql_ddl_agenda_after_len != 0;
        handle.sql_ddl_agenda_after_len = 0;
        const scanned_clean = read_only and handle.sql_ddl_agenda_scan_complete;
        handle.sql_ddl_recovery_needed = wrapped and !scanned_clean;
        handle.sql_ddl_agenda_scan_started = false;
        if (!handle.sql_ddl_recovery_needed) handle.sql_ddl_recovery_generation = generation;
        return;
    }
    var complete = if (read_only) handle.sql_ddl_agenda_scan_complete else true;
    for (keys.items) |marker| {
        if (marker.len > handle.sql_ddl_agenda_after.len) return error.InvalidRecoveryAgenda;
        @memcpy(handle.sql_ddl_agenda_after[0..marker.len], marker);
        handle.sql_ddl_agenda_after_len = @intCast(marker.len);
        const id = try agenda.tableId(marker[agenda.prefix.len..]);
        var database = &handle.db;
        var name: []const u8 = "default";
        if (id) |table_id| {
            try @import("tables.zig").loadCatalog(handle);
            var records = handle.embedded_table_records.iterator();
            const found = while (records.next()) |record| {
                if (record.value_ptr.* == table_id) {
                    name = record.key_ptr.*;
                    break true;
                }
            } else false;
            if (found) {
                database = try @import("tables.zig").get(handle, name);
            } else {
                // IDs never repeat. Dropping a table retires its recovery work.
                if (canRecoverWrite(handle)) try acknowledgeAgenda(storage, marker, native);
                continue;
            }
        } else {
            try @import("sql_fk.zig").recover(handle);
        }
        const recovered = recoverTable(handle, database, name, canRecoverWrite(handle)) catch |err| switch (err) {
            // A bounded background sweep may encounter another namespace's
            // pending job. Its operation-specific admission fence below still
            // refuses every access to that namespace, independent of this cursor.
            error.SqlStatementReadUnavailable => false,
            else => return err,
        };
        if (!recovered) {
            complete = false;
            continue;
        }
        if (canRecoverWrite(handle)) try acknowledgeAgenda(storage, marker, native);
    }
    // Check the current queue after acknowledgements: recovery itself can
    // create work in a sibling namespace. An empty durable queue is the clean
    // witness; a rotated page alone never proves earlier work completed.
    var remaining = try storage.file.metadataCatalogCursor(storage.file.activeCheckpoint(), agenda.prefix);
    defer remaining.deinit();
    const first = try remaining.next();
    defer if (first) |entry| storage.file.allocator.free(entry.key);
    if (read_only) handle.sql_ddl_agenda_scan_complete = complete;
    const read_only_clean = read_only and complete and !more and
        std.mem.eql(u8, &generation, &storage.file.activeCheckpoint().logical.metadata);
    handle.sql_ddl_recovery_needed = first != null and !read_only_clean;
    if (read_only and !more) {
        handle.sql_ddl_agenda_after_len = 0;
        handle.sql_ddl_agenda_scan_started = false;
    }
    if (!handle.sql_ddl_recovery_needed) {
        handle.sql_ddl_agenda_after_len = 0;
        handle.sql_ddl_recovery_generation = storage.file.activeCheckpoint().logical.metadata;
    }
}

/// Bounded discovery is scheduling, never admission authority. Resolve each
/// namespace actually used by an operation even when it lies outside that page
/// or an earlier recovery attempt yielded/failed. Table resolution calls this
/// after releasing its open mutex, so recovery can safely resolve dependencies.
pub fn requireRecovered(handle: *h.Handle, database: *h.db_mod.DB, name: []const u8) anyerror!void {
    const root = handle.parent_handle orelse handle;
    const backend = if (root.owned_lite_backend) |*value| value else return;
    const storage = backend.native_docstore orelse return;
    if (root.sql_ddl_recovering) return;
    if (!storage.file.header.indexed_reclamation) return; // legacy admission drains globally
    if (!root.sql_ddl_recovery_needed and std.mem.eql(u8, &root.sql_ddl_recovery_generation, &storage.file.activeCheckpoint().logical.metadata)) return;
    root.sql_ddl_recovering = true;
    defer root.sql_ddl_recovering = false;
    if (root.lease_snapshot) {
        // Captured generations may inspect recovery state, never finish a
        // decision against their old primary cut or publish a schema update.
        for ([_][]const u8{ "\x00antfly/embedded/sql-commit/v1", "\x00antfly/embedded/sql-fk-publication" }) |global_key| {
            if (try root.db.core.getStoreValue(root.alloc, global_key)) |bytes| {
                root.alloc.free(bytes);
                return error.SqlStatementReadUnavailable;
            }
        }
    }
    // Global decisions are reconciled by admission before namespace lookup.
    // Never re-enter their coordinator from a lookup inside a statement.
    const agenda = @import("../storage/lite/recovery_agenda.zig");
    var buffer: [128]u8 = undefined;
    const marker = if (std.mem.eql(u8, name, "default")) agenda.prefix else try std.fmt.bufPrint(&buffer, "{s}{s}{d}\x00", .{ agenda.prefix, agenda.table_prefix, database.core.identity_namespace.table_id });
    const pending = (try storage.file.getCatalogRecordAlloc(root.alloc, marker)) orelse return;
    defer root.alloc.free(pending);
    var drive = canRecoverWrite(root);
    if (drive) {
        // SQL preparation may already hold this coordinator. Inspect terminal
        // state there, but leave pending work to the pre-statement recovery turn.
        drive = root.sql_commit_mutex.tryLock();
        if (drive) root.sql_commit_mutex.unlock(h.handleLockIo());
    }
    if (!try recoverTable(root, database, name, drive)) return error.SqlDdlPending;
    if (canRecoverWrite(root))
        try acknowledgeAgenda(storage, marker, @import("../storage/lite/native.zig"));
}

/// Integrity/DDL consumes the complete dependency catalog, including inbound
/// references. Its declared participants must be ready before publication;
/// healthy namespaces remain unopened when they have no recovery marker.
pub fn requireCatalogRecovered(handle: *h.Handle) !void {
    const root = handle.parent_handle orelse handle;
    const backend = if (root.owned_lite_backend) |*value| value else return;
    const storage = backend.native_docstore orelse return;
    if (root.sql_ddl_recovering) return;
    if (!storage.file.header.indexed_reclamation) return;
    if (!root.sql_ddl_recovery_needed and std.mem.eql(u8, &root.sql_ddl_recovery_generation, &storage.file.activeCheckpoint().logical.metadata)) return;
    try requireRecovered(root, &root.db, "default");
    const tables = @import("tables.zig");
    try tables.loadCatalog(root);
    var entries = root.embedded_table_records.iterator();
    while (entries.next()) |entry| {
        var buffer: [128]u8 = undefined;
        const agenda = @import("../storage/lite/recovery_agenda.zig");
        const marker = try std.fmt.bufPrint(&buffer, "{s}{s}{d}\x00", .{ agenda.prefix, agenda.table_prefix, entry.value_ptr.* });
        const value = (try storage.file.getCatalogRecordAlloc(root.alloc, marker)) orelse continue;
        root.alloc.free(value);
        _ = try tables.get(root, entry.key_ptr.*);
    }
}

fn acknowledgeAgenda(storage: anytype, marker: []const u8, comptime native: type) !void {
    const Ack = struct {
        key_bytes: []const u8,
        fn apply(ptr: *anyopaque, file: *native.NativeFile) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try file.deleteCatalogRecord(self.key_bytes);
        }
    };
    var ack = Ack{ .key_bytes = marker };
    try storage.submitIndexMutation(&ack, Ack.apply, true);
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
    const root = adapter.handle orelse return error.UnsupportedSqlExecution;
    const was_recovering = root.sql_ddl_recovering;
    root.sql_ddl_recovering = true;
    defer root.sql_ddl_recovering = was_recovering;
    var arena = std.heap.ArenaAllocator.init(adapter.db.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const metadata = try adapter.localCatalog(a, version);
    try d.api_relational_constraint_recovery.retry(a, adapter.localSource(), activationWriter(adapter), metadata.table, metadata.range, .{ .table_name = adapter.table_name, .relational_schema_version = version }, .{
        .deadline_ns = h.antfly.platform_time.monotonicNs() +| 5 * std.time.ns_per_s,
    });
}

pub fn activate(adapter: anytype, version: u32) !void {
    const root = adapter.handle orelse return error.UnsupportedSqlExecution;
    const was_recovering = root.sql_ddl_recovering;
    root.sql_ddl_recovering = true;
    defer root.sql_ddl_recovering = was_recovering;
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

fn canRecoverWrite(handle: *h.Handle) bool {
    return h.liteOpenModeCanWrite(handle.open_mode) and !handle.lease_snapshot;
}

fn recoverTable(handle: *h.Handle, db: *h.db_mod.DB, name: []const u8, drive: bool) !bool {
    var adapter = sql.Adapter(h.antfly){ .handle = handle, .db = db, .table_name = name };
    if (try db.core.getStoreValue(handle.alloc, key)) |bytes| {
        defer handle.alloc.free(bytes);
        if (!drive) return if (canRecoverWrite(handle)) error.SqlDdlPending else error.SqlStatementReadUnavailable;
        const job = try std.json.parseFromSlice(Job, handle.alloc, bytes, .{});
        defer job.deinit();
        try finishJob(&adapter, job.value);
    }
    const schema = (try db.getSchemaJson(handle.alloc)) orelse return true;
    defer handle.alloc.free(schema);
    if (!try d.api_relational_integrity_commit.requiresActivation(handle.alloc, schema)) return true;
    if (try db.lookup(handle.alloc, "", .{ .relational_activation_json = "{}" })) |response| {
        defer handle.alloc.free(response.json);
        var state = try std.json.parseFromSlice(struct { state: activation.State }, handle.alloc, response.json, .{ .ignore_unknown_fields = true });
        defer state.deinit();
        if (state.value.state != .validating) return true;
    }
    if (!drive) return if (canRecoverWrite(handle)) error.SqlDdlPending else error.SqlStatementReadUnavailable;
    var view = db.core.acquireSchemaView() orelse return error.PreparedGenerationChanged;
    defer view.release();
    activate(&adapter, view.version()) catch |err| switch (err) {
        error.SqlDdlPending => return false,
        else => return err,
    };
    // The worker can yield without error on a parent dependency or a guarded
    // repair. Only durable terminal state makes this generation clean.
    if (try db.lookup(handle.alloc, "", .{ .relational_activation_json = "{}" })) |response| {
        defer handle.alloc.free(response.json);
        var state = try std.json.parseFromSlice(struct { state: activation.State }, handle.alloc, response.json, .{ .ignore_unknown_fields = true });
        defer state.deinit();
        return state.value.state != .validating;
    }
    return false;
}
