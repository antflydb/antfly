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

//! Native physical maintenance uses existing manifest reclamation and primary
//! snapshot readers. Each invocation is bounded; a durable cursor/sketch set
//! resumes after cancellation or restart without retaining request arenas.
const std = @import("std");
const contract = @import("../../api/sql_maintenance_contract.zig");
const catalog = @import("../../sql/catalog.zig");
const platform = @import("antfly_platform");
const A = std.mem.Allocator;

fn load(comptime T: type, db: anytype, a: A, key: []const u8) !?T {
    const raw = db.core.store.get(a, key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len > contract.max_bytes) return error.InvalidSqlBackendResponse;
    return std.json.parseFromSliceLeaky(T, a, raw, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidSqlBackendResponse,
    };
}
/// The shared owner mutex serializes quanta; primary transactions durably
/// publish their cursor and sketches together. A stale
/// quantum can finish harmless physical work but cannot overwrite a new owner.
fn publish(db: anytype, a: A, state: *contract.Progress, expected: ?u64, publish_statistics: bool) !void {
    db.core.lockApplyShared();
    defer db.core.unlockApplyShared();
    try db.enforceReplicationWriteGate();
    try matches(db, state.request);
    if (!state.identity.eql(db.core.identity_namespace)) return error.CatalogGenerationChanged;
    const next_revision = std.math.add(u64, expected orelse 0, 1) catch return error.SqlProgramLimitExceeded;
    state.revision = next_revision;
    state.lease_until_ms = @as(u64, @intCast(platform.time.realtimeNs() / std.time.ns_per_ms)) +| 1000;
    const bytes = try std.json.Stringify.valueAlloc(a, state.*, .{});
    if (bytes.len > contract.max_bytes) return error.SqlProgramLimitExceeded;
    var txn = try db.core.store.beginWriteTxn();
    var committed = false;
    defer if (!committed) txn.abort();
    const previous = txn.get(contract.progress_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (previous) |raw| {
        const current = try std.json.parseFromSliceLeaky(contract.Progress, a, raw, .{});
        if (expected == null or current.revision != expected.?) return error.MaintenanceBusy;
    } else if (expected != null) return error.MaintenanceBusy;
    if (state.cluster) |placement| {
        const indexes = @import("relational/indexes/catalog.zig");
        try @import("../pessimistic_lock/participant.zig").requireTableGrant(a, &txn, state.identity, state.request.cluster_admission orelse return error.InvalidSqlParameters, .access_exclusive);
        if (!std.mem.eql(u8, &(try indexes.Head.decode(try txn.get(indexes.head_key))).encode(), &placement.catalog_head)) return error.PreparedGenerationChanged;
        if (state.phase == .done) {
            const preference = try std.json.Stringify.valueAlloc(a, placement, .{});
            try txn.put(contract.cluster_preferred_key, preference);
            try txn.delete(contract.cluster_cursor_key);
        }
    }
    if (publish_statistics) {
        // Column-targeted ANALYZE replaces only its selected observations.
        // Preserve other columns from the same schema/owner, never another
        // incarnation's estimates after split/restore/name reuse.
        if (state.request.columns.len != 0) {
            if (try load(contract.Statistics, db, a, contract.stats_key)) |stored| {
                if (stored.identity.eql(state.identity) and stored.schema_version == state.request.schema_version) {
                    var fields: std.ArrayList(contract.Field) = .empty;
                    try fields.appendSlice(a, state.statistics.fields);
                    for (stored.fields) |old| {
                        const replaced = for (state.statistics.fields) |field| {
                            if (std.mem.eql(u8, field.name, old.name)) break true;
                        } else false;
                        if (!replaced) try fields.append(a, old);
                    }
                    state.statistics.fields = fields.items;
                }
            }
        }
        const encoded = try std.json.Stringify.valueAlloc(a, state.statistics, .{});
        if (encoded.len > contract.max_bytes) return error.SqlProgramLimitExceeded;
        try txn.put(contract.stats_key, encoded);
    }
    try txn.put(contract.progress_key, bytes);
    try txn.commit();
    committed = true;
    try db.core.store.sync(true);
}

fn matches(db: anytype, request: contract.Request) !void {
    if (db.core.identity_namespace.table_id != request.table_id) return error.CatalogGenerationChanged;
    var schema = db.core.acquireSchemaView();
    defer if (schema) |*view| view.release();
    const active = if (schema) |view| view.version() else @as(u32, 0);
    // Installation follows the durable metadata decision. A lagging owner is
    // a bounded wait, while a successor means this request lost its binding.
    if (active < request.schema_version) return error.MaintenanceBusy;
    if (active != request.schema_version) return error.PreparedGenerationChanged;
}
fn columns(a: A, request: contract.Request, view: ?@import("schema_registry.zig").SchemaView) ![]const catalog.Column {
    if (view == null) {
        if (request.columns.len != 0) return error.UnknownSqlColumn;
        return &.{};
    }
    const borrowed: []const catalog.Column = if (view.?.storageMode() == .relational)
        try catalog.relationalColumnsLeaky(a, view.?.tableSchema().*)
    else if (view.?.validator()) |validator| if (validator.document_layout) |layout| layout.columns else &.{} else &.{};
    const all = try a.dupe(catalog.Column, borrowed);
    for (all) |*column| {
        column.name = try a.dupe(u8, column.name);
        column.path = try a.dupe(u8, column.path);
        column.document_path = try catalog.cloneDocumentPath(a, column.document_path);
    }
    if (request.columns.len == 0) return all;
    const selected = try a.alloc(catalog.Column, request.columns.len);
    for (request.columns, selected) |name, *out| {
        out.* = for (all) |column| {
            if (std.mem.eql(u8, name, column.name)) break column;
        } else return error.UnknownSqlColumn;
    }
    return selected;
}

/// The coordinator holds its existing native ACCESS EXCLUSIVE grants across
/// these quanta. Never call this from an unfenced background producer. Each
/// owner additionally binds the selected immutable index/schema/range image;
/// its primary transaction publishes bodies, locations and the resume key.
fn cluster(db: anytype, alloc: A, a: A, request: contract.Request) ![]u8 {
    const indexes = @import("relational/indexes/catalog.zig");
    const records = @import("relational/indexes/records.zig");
    const internal = @import("../internal_keys.zig");
    const exclusive = request.operation == .cluster;
    if (exclusive) db.core.lockApplyExclusive() else db.core.lockApplyShared();
    var locked = true;
    defer if (locked) {
        if (exclusive) db.core.unlockApplyExclusive() else db.core.unlockApplyShared();
    };
    try matches(db, request);
    _ = db.core.primary_store_owner.lsmBackend() orelse return error.UnsupportedSqlExecution;
    var snapshot = db.core.relational_indexes.acquire() orelse return error.SqlIndexNotFound;
    defer snapshot.deinit();
    const preferred = if (request.cluster_index == null) try load(contract.Cluster, db, a, contract.cluster_preferred_key) else null;
    const index = for (snapshot.plan.boundIndexes()) |index| {
        if (request.cluster_index) |name| {
            if (std.mem.eql(u8, name, index.name)) break index;
        } else if (preferred) |prior| {
            if (prior.index_generation == index.generation and prior.index_slot == index.slot) break index;
        }
    } else return error.SqlIndexNotFound;
    // A partial index cannot describe every heap row. Reject it before
    // publishing the format fence, cursor, preferred index or any location.
    if (index.predicate != null) return error.InvalidRelationalIndexDefinition;
    if (snapshot.head.schema_version != request.schema_version) return error.PreparedGenerationChanged;
    if (request.operation == .cluster_check) {
        // Read-only observation of the last completed rewrite, not a source
        // permit or an authority to resume. This also lets operators verify
        // that successful SQL actually traversed the physical owner path.
        var relocated: u64 = 0;
        if (try load(contract.Progress, db, a, contract.progress_key)) |history| if (history.phase == .done and history.identity.eql(db.core.identity_namespace) and history.request.schema_version == request.schema_version) {
            if (history.cluster) |placement| if (placement.index_generation == index.generation and placement.index_slot == index.slot and std.mem.eql(u8, &placement.catalog_head, &snapshot.head.encode())) {
                if (placement.next_ordinal != history.statistics.rows or !std.mem.eql(u8, &(try history.request.fingerprint(a)), &history.fingerprint)) return error.InvalidSqlBackendResponse;
                relocated = placement.next_ordinal;
            };
        };
        return std.json.Stringify.valueAlloc(alloc, contract.Response{ .table_id = request.table_id, .schema_version = request.schema_version, .done = true, .available = true, .rows_scanned = relocated, .row_heap_format = @import("table_catalog.zig").maximum_row_heap_format }, .{});
    }
    const head = snapshot.head.encode();
    const identity = db.core.identity_namespace;
    const fingerprint = try request.fingerprint(a);
    try db.enforceReplicationWriteGate();
    _ = try db.core.store.collectRowHeap(2048);
    var writer = try db.core.store.beginWriteTxn();
    var committed = false;
    defer if (!committed) writer.abort();
    try @import("../pessimistic_lock/participant.zig").requireTableGrant(a, &writer, identity, request.cluster_admission.?, .access_exclusive);
    const actual_head = try indexes.Head.decode(try writer.get(indexes.head_key));
    if (!actual_head.eql(snapshot.head)) return error.PreparedGenerationChanged;
    const previous = writer.get(contract.progress_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    const now_ms: u64 = @intCast(platform.time.realtimeNs() / std.time.ns_per_ms);
    var prior_revision: u64 = 0;
    var state: contract.Progress = .{
        .request = request,
        .identity = identity,
        .fingerprint = fingerprint,
        .phase = .cluster,
        .statistics = .{ .identity = identity, .schema_version = request.schema_version },
        .cluster = .{ .index_name = index.name, .index_generation = index.generation, .index_slot = index.slot, .catalog_head = head, .generation = request.request_id },
    };
    if (previous) |raw| {
        if (raw.len > contract.max_bytes) return error.InvalidSqlBackendResponse;
        const old = try std.json.parseFromSliceLeaky(contract.Progress, a, raw, .{ .allocate = .alloc_always });
        try old.request.validate();
        try old.statistics.validate();
        if (old.revision == 0 or !old.statistics.identity.eql(old.identity) or old.statistics.schema_version != old.request.schema_version or !std.mem.eql(u8, &(try old.request.fingerprint(a)), &old.fingerprint)) return error.InvalidSqlBackendResponse;
        prior_revision = old.revision;
        if (old.identity.eql(identity) and old.request.schema_version == request.schema_version) {
            if (std.mem.eql(u8, &old.request.request_id, &request.request_id)) {
                if (!std.mem.eql(u8, &old.fingerprint, &fingerprint)) return error.IdempotencyConflict;
                const bound = old.cluster orelse return error.InvalidSqlBackendResponse;
                if (!std.mem.eql(u8, &bound.catalog_head, &head) or bound.index_generation != index.generation or bound.index_slot != index.slot or !std.mem.eql(u8, &bound.generation, &request.request_id)) return error.PreparedGenerationChanged;
                if (old.phase != .cluster and old.phase != .cluster_flush and old.phase != .done) return error.InvalidSqlBackendResponse;
                state = old;
            } else if (old.phase != .done and old.lease_until_ms > now_ms and old.lease_until_ms <= now_ms +| 1000) return error.MaintenanceBusy;
            // A new producer restarts the physical walk. Writes may have run
            // after its predecessor released ACCESS EXCLUSIVE; adopting that
            // old key would skip moved tuples. Existing locations remain valid.
        }
    }
    if (state.phase == .done) {
        writer.abort();
        committed = true;
        return std.json.Stringify.valueAlloc(alloc, contract.Response{ .table_id = request.table_id, .schema_version = request.schema_version, .done = true, .rows_scanned = state.statistics.rows }, .{});
    }
    if (state.after != null) return error.InvalidSqlBackendResponse;
    var checkpoint: ?[]const u8 = null;
    if (state.cluster.?.cursor_digest) |expected| {
        const raw = writer.get(contract.cluster_cursor_key) catch |err| switch (err) {
            error.NotFound => return error.InvalidSqlBackendResponse,
            else => return err,
        };
        if (!std.mem.eql(u8, &expected, &(try state.cluster.?.cursorDigest(raw)))) return error.InvalidSqlBackendResponse;
        _ = try records.parseForward(raw, index);
        checkpoint = try a.dupe(u8, raw);
    }
    if (state.phase == .cluster_flush) {
        // Do not hold the primary writer across a flush/compaction wait.
        writer.abort();
        committed = true;
        const primary = db.core.primary_store_owner.lsmBackend() orelse return error.UnsupportedSqlExecution;
        db.core.unlockApplyExclusive();
        locked = false;
        try primary.flushBufferedWritesWithOptions(.{ .flush = true, .compact = true });
        state.phase = .done;
        state.cluster.?.cursor_digest = null;
        // The identical progress revision still owns publication. A successor
        // cannot receive a stale completion or preferred-index assignment.
        try publish(db, a, &state, state.revision, false);
        return std.json.Stringify.valueAlloc(alloc, contract.Response{ .table_id = request.table_id, .schema_version = request.schema_version, .done = true, .rows_scanned = state.statistics.rows }, .{});
    }
    const prefix = try records.forwardPrefix(index.id());
    const Selected = struct { row_key: []const u8, forward_key: []const u8 };
    var selected: std.ArrayList(Selected) = .empty;
    var after: ?[]const u8 = checkpoint;
    var exhausted = false;
    {
        var cursor = try writer.openCursor();
        defer cursor.close();
        cursor.setKeysOnly();
        var current = try cursor.seekAtOrAfter(checkpoint orelse &prefix);
        if (checkpoint) |key| if (current) |entry| if (std.mem.eql(u8, key, entry.key)) {
            current = try cursor.next();
        };
        var key_bytes: usize = 0;
        while (current) |entry| {
            if (!std.mem.startsWith(u8, entry.key, &prefix)) break;
            if (selected.items.len >= request.max_rows or (selected.items.len != 0 and key_bytes +| entry.key.len > 1024 * 1024)) break;
            const owner = try records.parseForward(entry.key, index);
            const document = try internal.decodeBodyAlloc(a, owner.document_component[0 .. owner.document_component.len - 2]);
            try selected.append(a, .{ .row_key = try internal.relationalRowKeyAlloc(a, document), .forward_key = try a.dupe(u8, entry.key) });
            key_bytes += entry.key.len;
            current = try cursor.next();
        }
        exhausted = current == null or !std.mem.startsWith(u8, current.?.key, &prefix);
    }
    var bytes: u64 = 0;
    const started = platform.time.monotonicNs();
    // Catalog writers copy these facts under the same apply fence. Raise the
    // in-memory requirement before attempting a location so even an ambiguous
    // commit cannot let a later schema/count write downgrade its durable ATBL8
    // fence. A failed/aborted attempt may conservatively require the decoder
    // before any heap row exists; no row-presence or schema fact is inferred.
    if (selected.items.len != 0) db.core.table_catalog.row_heap_format = @import("table_catalog.zig").maximum_row_heap_format;
    // A cursor remains unpublished until every selected row is placed. This
    // loop is row/key bounded; large bodies are additionally capped by the
    // request arena. No JSON parsing, index rebuilding or row-version bump.
    for (selected.items, 0..) |row, ordinal| {
        bytes += writer.relocatePhysicalRow(row.row_key, state.cluster.?.generation, state.cluster.?.next_ordinal) catch |err| switch (err) {
            error.RowHeapCollectionBusy => return error.MaintenanceBusy,
            else => return err,
        };
        state.cluster.?.next_ordinal = std.math.add(u64, state.cluster.?.next_ordinal, 1) catch return error.SqlProgramLimitExceeded;
        after = row.forward_key;
        if (ordinal + 1 != selected.items.len and (bytes >= 16 * 1024 * 1024 or platform.time.monotonicNs() -| started >= 2 * std.time.ns_per_ms)) {
            exhausted = false;
            break;
        }
    }
    state.statistics.rows = state.cluster.?.next_ordinal;
    state.after = null;
    if (after) |key| {
        state.cluster.?.cursor_digest = try state.cluster.?.cursorDigest(key);
        if (selected.items.len != 0) try writer.put(contract.cluster_cursor_key, key);
    } else {
        state.cluster.?.cursor_digest = null;
        try writer.delete(contract.cluster_cursor_key);
    }
    if (exhausted) state.phase = .cluster_flush;
    state.revision = std.math.add(u64, prior_revision, 1) catch return error.SqlProgramLimitExceeded;
    state.lease_until_ms = now_ms +| 1000;
    const encoded = try std.json.Stringify.valueAlloc(a, state, .{});
    if (encoded.len > contract.max_bytes) return error.SqlProgramLimitExceeded;
    try writer.put(contract.progress_key, encoded);
    try writer.commit();
    committed = true;
    try db.core.store.sync(true);
    return std.json.Stringify.valueAlloc(alloc, contract.Response{ .table_id = request.table_id, .schema_version = request.schema_version, .done = false, .rows_scanned = state.statistics.rows }, .{});
}

/// Only an authenticated table-admin operation or an internal planning read
/// can reach this owner. Durable observations are always bound to its namespace.
pub fn execute(db: anytype, alloc: A, bytes: []const u8) ![]u8 {
    if (bytes.len > 128 * 1024) return error.SqlProgramLimitExceeded;
    var budget: @import("../../sql/memory_budget.zig") = .{ .backing = alloc, .limit = 64 * 1024 * 1024 };
    var region = std.heap.ArenaAllocator.init(budget.allocator());
    defer region.deinit();
    const a = region.allocator();
    const request = std.json.parseFromSliceLeaky(contract.Request, a, bytes, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidSqlParameters,
    };
    try request.validate();
    if (request.operation == .statistics) {
        // Estimates are advisory; never queue behind primary apply or a
        // waiting schema writer merely to load a bounded metadata record.
        if (!db.core.tryLockApplyShared()) return error.MaintenanceBusy;
    } else db.core.lockApplyShared();
    var locked = true;
    defer if (locked) db.core.unlockApplyShared();
    try db.enforcePortableRuntimeGate();
    try matches(db, request);
    if (request.operation == .cluster or request.operation == .cluster_check) {
        db.core.unlockApplyShared();
        locked = false;
        return cluster(db, alloc, a, request);
    }
    var pinned = db.core.acquireSchemaView();
    defer if (pinned) |*view| view.release();
    const identity = db.core.identity_namespace;
    if (request.operation == .statistics) {
        const stored = try load(contract.Statistics, db, a, contract.stats_key);
        if (stored) |sample| try sample.validate();
        const valid = if (stored) |value| value.identity.eql(identity) and value.schema_version == request.schema_version else false;
        return std.json.Stringify.valueAlloc(alloc, contract.Response{ .table_id = request.table_id, .schema_version = request.schema_version, .done = true, .available = valid, .statistics = if (valid) stored else null }, .{});
    }
    const selected: []const catalog.Column = if (request.operation == .analyze or request.analyze) try columns(a, request, pinned) else &.{};
    if (selected.len > 256) return error.SqlProgramLimitExceeded;
    db.core.unlockApplyShared();
    locked = false;
    const fingerprint = try request.fingerprint(a);
    var progress = try load(contract.Progress, db, a, contract.progress_key);
    const predecessor = if (progress) |state| state.revision else null;
    var claimed = false;
    if (progress) |state| {
        try state.request.validate();
        try state.statistics.validate();
        if (!std.mem.eql(u8, &(try state.request.fingerprint(a)), &state.fingerprint) or state.revision == 0 or !state.statistics.identity.eql(state.identity) or state.statistics.schema_version != state.request.schema_version) return error.InvalidSqlBackendResponse;
        if (!state.identity.eql(identity) or state.request.schema_version != request.schema_version) progress = null else if (!std.mem.eql(u8, &state.request.request_id, &request.request_id)) {
            // An abandoned vacuum still drains through ordinary maintenance.
            // A new statement can replace a completed objective, never a live
            // different request's scan state.
            const now_ms: u64 = @intCast(platform.time.realtimeNs() / std.time.ns_per_ms);
            if (state.phase != .done and state.lease_until_ms > now_ms and state.lease_until_ms <= now_ms +| 1000) return error.MaintenanceBusy;
            if (state.phase != .done and state.request.sameObjective(request)) {
                // Producer restarts/cancellation may lose a nonce. Adopt only
                // the identical unfinished schema/owner objective, retaining
                // its cursor and sketches; completed ANALYZE starts afresh.
                progress.?.request = request;
                progress.?.fingerprint = fingerprint;
                claimed = true;
            } else progress = null;
        } else if (!std.mem.eql(u8, &state.fingerprint, &fingerprint)) return error.IdempotencyConflict;
    }
    if (progress == null) {
        claimed = true;
        const fields = try a.alloc(contract.Field, selected.len);
        for (fields, selected) |*out, column| out.* = .{ .name = column.name };
        progress = .{ .request = request, .identity = identity, .fingerprint = fingerprint, .phase = if (request.operation == .vacuum) .vacuum_start else .analyze, .statistics = .{ .identity = identity, .schema_version = request.schema_version, .fields = fields } };
    }
    const state = &progress.?;
    if (claimed) try publish(db, a, state, predecessor, false);
    if (state.phase == .done) return std.json.Stringify.valueAlloc(alloc, contract.Response{ .table_id = request.table_id, .schema_version = request.schema_version, .done = true, .rows_scanned = state.statistics.rows }, .{});
    if (state.phase == .vacuum_start) {
        const primary = db.core.primary_store_owner.lsmBackend() orelse return error.UnsupportedSqlExecution;
        try primary.flushBufferedWritesWithOptions(.{ .flush = true, .compact = false });
        try primary.requestValueReclamation();
        state.phase = .vacuum;
        try publish(db, a, state, state.revision, false);
    }
    if (state.phase == .vacuum) {
        const primary = db.core.primary_store_owner.lsmBackend() orelse return error.UnsupportedSqlExecution;
        const started = platform.time.monotonicNs();
        var turns: usize = 0;
        while (try primary.hasValueReclamationRequests()) : (turns += 1) {
            if (turns >= (if (request.full) @as(usize, 64) else 16) or platform.time.monotonicNs() -| started >= 50 * std.time.ns_per_ms) break;
            if (!try primary.runValueReclamationStep()) break;
        }
        if (!try primary.hasValueReclamationRequests()) state.phase = if (request.analyze) .analyze else .done;
    }
    if (state.phase == .analyze) {
        if (state.statistics.fields.len != selected.len) return error.InvalidSqlBackendResponse;
        for (state.statistics.fields, selected) |field, column| if (!std.mem.eql(u8, field.name, column.name)) return error.InvalidSqlBackendResponse;
        try analyze(db, a, budget.allocator(), state, selected);
    }
    try matches(db, request);
    // The manifest already owns GC progress. Avoid an extra WAL/fsync for
    // every empty poll while old readers retain runs; renew only the lease.
    const now_ms: u64 = @intCast(platform.time.realtimeNs() / std.time.ns_per_ms);
    if (state.phase == .vacuum and state.lease_until_ms > now_ms +| 500) return std.json.Stringify.valueAlloc(alloc, contract.Response{ .table_id = request.table_id, .schema_version = request.schema_version, .done = false }, .{});
    try publish(db, a, state, state.revision, state.phase == .done and (request.operation == .analyze or request.analyze));
    return std.json.Stringify.valueAlloc(alloc, contract.Response{ .table_id = request.table_id, .schema_version = request.schema_version, .done = state.phase == .done, .rows_scanned = state.statistics.rows }, .{});
}

fn analyze(db: anytype, a: A, scratch_backing: A, state: *contract.Progress, selected: []const catalog.Column) !void {
    db.core.lockApplyShared();
    var locked = true;
    defer if (locked) db.core.unlockApplyShared();
    try db.enforcePortableRuntimeGate();
    try matches(db, state.request);
    // Each cut is a bounded advisory sample. Native rows stay typed; JSON null,
    // SQL NULL, enum identity and exact numeric payloads retain their domains.
    var schema = db.core.acquireSchemaView();
    defer if (schema) |*view| view.release();
    const names = try a.alloc([]const u8, selected.len);
    for (names, selected) |*name, column| name.* = column.name;
    const started = platform.time.monotonicNs();
    var pages: usize = 0;
    var rows: usize = 0;
    if (schema != null and schema.?.storageMode() == .relational) {
        var reader = try @import("read_session_backend").RelationalRows.Reader.open(a, db.core.store, schema.?, null, .{ .fields = names, .expected_schema_version = state.request.schema_version, .primary_lower = if (state.after) |after| .{ .key = after, .inclusive = false } else null }, @intCast(platform.time.realtimeNs()));
        defer reader.deinit();
        db.core.unlockApplyShared();
        locked = false;
        while (pages < 16 and rows < state.request.max_rows and platform.time.monotonicNs() -| started < 50 * std.time.ns_per_ms) : (pages += 1) {
            var scratch = std.heap.ArenaAllocator.init(scratch_backing);
            defer scratch.deinit();
            const page_alloc = scratch.allocator();
            var page = try reader.nextTypedPage(page_alloc, null, .{ .rows = @min(128, state.request.max_rows - rows), .output_bytes = 16 * 1024 * 1024, .target_bytes = 1024 * 1024 });
            defer page.deinit();
            rows += page.rows.len;
            for (page.rows) |row| {
                const source: catalog.Row = .{ .id = row.key, .version = row.version, .value = row.typed orelse .null, .typed_cells = row.typed_cells, .sql_nulls = row.sql_nulls };
                try observe(page_alloc, state, selected, source);
            }
            if (!page.more) {
                state.phase = .done;
                break;
            }
            if (reader.after.items.len != 0) state.after = (try @import("../internal_keys.zig").decodeDocumentComponentAlloc(a, reader.after.items)) orelse return error.InvalidSqlBackendResponse;
        }
    } else {
        var read = try db.core.store.beginReadTxn();
        var owns_read = true;
        defer if (owns_read) read.abort();
        const ranges = @import("range_state.zig");
        const raw_range = read.get(ranges.range_key) catch |err| switch (err) {
            error.NotFound => &(@as([8]u8, @splat(0))),
            else => return err,
        };
        const owned_range = try ranges.decodeRangeAlloc(a, raw_range);
        owns_read = false; // Session consumes the snapshot on every outcome.
        const session = try @import("read_session_backend").Document.openSnapshot(a, db.core.store, read, schema, .{ .start = owned_range.start, .end = owned_range.end }, state.after orelse owned_range.start, owned_range.end, .{ .inclusive_from = state.after == null, .relational_query = .{ .fields = names, .schema_version = state.request.schema_version } }, @intCast(platform.time.realtimeNs()));
        defer session.deinit();
        db.core.unlockApplyShared();
        locked = false;
        while (pages < 16 and rows < state.request.max_rows and platform.time.monotonicNs() -| started < 50 * std.time.ns_per_ms) : (pages += 1) {
            var scratch = std.heap.ArenaAllocator.init(scratch_backing);
            defer scratch.deinit();
            const page_alloc = scratch.allocator();
            var page = try session.next(page_alloc, @intCast(@min(128, state.request.max_rows - rows)));
            defer page.deinit();
            rows += page.rows.len;
            for (page.rows) |row| try observe(page_alloc, state, selected, row.sqlRow());
            if (page.after == null) {
                state.phase = .done;
                break;
            }
            if (session.after.items.len != 0) state.after = try a.dupe(u8, session.after.items);
        }
    }
}
fn observe(a: A, state: *contract.Progress, selected: []const catalog.Column, source: catalog.Row) !void {
    for (selected, state.statistics.fields) |column, *field| {
        const value = try @import("../../sql/document_row.zig").declaredCell(a, column, try source.cell(column.name));
        try field.sketch.add(value, try @import("../../sql/operators.zig").datumBytes(value));
    }
    state.statistics.rows +|= 1;
}
