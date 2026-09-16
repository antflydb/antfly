// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Bounded source-vector backfill and verification. The DB owns admission;
//! candidate mappings and progress use the same primary transaction. Online
//! mutation capture maintains one latest reference per artifact in that same
//! transaction, so replay cannot lag or retain a second unbounded payload log.
const std = @import("std");
pub const contract = @import("../common/vector_migration.zig");
const payload = @import("artifact_payload.zig");
const docstore = @import("docstore.zig");
const erased = @import("backend_erased.zig");
const internal_keys = @import("internal_keys.zig");
const codec = @import("db/enrichment/artifact_codec.zig");
const Allocator = std.mem.Allocator;

pub const Boundary = enum { before_prepare, after_prepare, after_commit, after_sync, publication_commit, publication_sync, reclamation_request, reclamation_receipt };
pub var test_boundary: ?*const fn (Boundary) anyerror!void = null;

pub fn boundary(point: Boundary) !void {
    if (@import("builtin").is_test) if (test_boundary) |hook| try hook(point);
}

pub fn load(alloc: Allocator, primary: *docstore.DocStore) !?std.json.Parsed(contract.Job) {
    const raw = primary.get(alloc, contract.job_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    var parsed = try std.json.parseFromSlice(contract.Job, alloc, raw, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}

pub fn save(alloc: Allocator, txn: anytype, job: contract.Job) !void {
    try job.validate();
    const raw = try std.json.Stringify.valueAlloc(alloc, job, .{});
    defer alloc.free(raw);
    try txn.put(contract.job_key, raw);
}

const Rows = struct {
    const Row = struct {
        key: []const u8,
        value: []const u8,
        dense: bool,
        inline_payload: bool,
    };
    arena: std.heap.ArenaAllocator,
    items: []const Row,
    exhausted: bool,
    fn deinit(self: *Rows) void {
        self.arena.deinit();
    }
};

fn readRows(alloc: Allocator, primary: *docstore.DocStore, job: contract.Job) !Rows {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    var rows = std.ArrayListUnmanaged(Rows.Row).empty;
    var read = try primary.runtime_store.beginReadWithBlockCacheAdmission(.transient);
    defer read.abort();
    var cursor = try read.openCursor();
    defer cursor.close();
    const cleanup = job.phase == .cleanup or job.phase == .cancelling;
    const decoded_cursor = try scratch.alloc(u8, job.cursor.len / 2);
    if (job.cursor.len % 2 != 0) return error.InvalidVectorMigrationState;
    _ = std.fmt.hexToBytes(decoded_cursor, job.cursor) catch return error.InvalidVectorMigrationState;
    const lower = if (decoded_cursor.len != 0) decoded_cursor else if (cleanup) contract.candidate_prefix else "";
    var entry = try cursor.seekAtOrAfter(lower);
    if (entry) |row| if (decoded_cursor.len != 0 and std.mem.eql(u8, row.key, decoded_cursor)) {
        entry = try cursor.next();
    };
    var bytes: u64 = 0;
    while (entry) |row| {
        if (cleanup and !std.mem.startsWith(u8, row.key, contract.candidate_prefix)) {
            entry = null;
            break;
        }
        // Unrelated primary values only contribute a cursor key. In draining,
        // hash the borrowed inline value into its compact reference before
        // advancing the cursor; a concurrent capture after verification may
        // have introduced an embedding larger than the original page budget.
        const dense = !cleanup and try denseArtifact(.{ .key = row.key, .value = row.value });
        const inline_payload = dense and !payload.isReference(row.value);
        const work_bytes = try std.math.add(u64, row.key.len, if (dense) row.value.len else 0);
        if (rows.items.len == job.budget.batch_rows or
            (rows.items.len != 0 and bytes + work_bytes > job.budget.batch_bytes)) break;
        var reference: [payload.reference_len]u8 = undefined;
        const value = if (!dense) "" else if (job.phase == .draining and inline_payload) blk: {
            reference = (try payload.Reference.forArtifact(row.key, row.value)).encode();
            break :blk &reference;
        } else row.value;
        if (job.phase == .final_verification and inline_payload) return error.VectorMigrationInlinePayloadRemains;
        const size = try std.math.add(u64, row.key.len, value.len);
        if (size > job.budget.batch_bytes) return error.VectorMigrationRowExceedsBudget;
        try rows.append(scratch, .{ .key = try scratch.dupe(u8, row.key), .value = try scratch.dupe(u8, value), .dense = dense, .inline_payload = inline_payload });
        // An oversized published inline vector consumes a page by itself. Its
        // bytes are borrowed from the cursor; only the reference is retained.
        bytes += work_bytes;
        entry = try cursor.next();
    }
    return .{ .arena = arena, .items = rows.items, .exhausted = entry == null };
}

fn denseArtifact(row: docstore.KVPair) !bool {
    if (!payload.isEmbeddingKey(row.key)) return false;
    if (payload.isReference(row.value)) {
        _ = try payload.Reference.decode(row.value);
        return true;
    }
    const header = try codec.decodeHeader(row.value);
    return header.kind == .dense_embedding;
}

fn sameCurrent(txn: *erased.WriteTxn, row: docstore.KVPair) !bool {
    const current = txn.get(row.key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    return std.mem.eql(u8, current, row.value);
}

/// Caller serializes steps with document mutations. Each call retains at most
/// one budgeted page, prepares source bytes before committing any references,
/// and commits its exclusive cursor together with those references.
pub fn advance(alloc: Allocator, primary: *docstore.DocStore, source: payload.Store, job: contract.Job) !void {
    if (job.phase == .ready or job.phase == .serving or !job.active()) return;
    var rows = try readRows(alloc, primary, job);
    defer rows.deinit();
    var next = job;
    next.last_error = null;
    const session = try payload.Session.create(alloc, source);
    defer session.release();
    session.migration_allowance = job.budget.temporary_bytes;
    var txn = try primary.runtime_store.beginWrite();
    var committed = false;
    defer if (!committed) txn.abort();
    for (rows.items) |row| {
        // Primary identities contain arbitrary bytes. Hex keeps both the
        // durable record and HTTP progress valid UTF-8 JSON.
        next.cursor = try std.fmt.allocPrint(rows.arena.allocator(), "{x}", .{row.key});
        if (job.phase == .cleanup or job.phase == .cancelling) {
            try txn.delete(row.key);
            continue;
        }
        next.scanned_rows +|= 1;
        if (!row.dense) continue;
        // The DB holds apply-exclusive across page capture and commit. The
        // published phases therefore use the reference certified above without
        // retaining or hashing another copy of the inline payload.
        if (!job.published() and !try sameCurrent(&txn, .{ .key = row.key, .value = row.value })) continue;
        const candidate_key = try contract.candidateKeyAlloc(alloc, row.key);
        defer alloc.free(candidate_key);
        switch (job.phase) {
            .backfill => {
                if (payload.isReference(row.value)) return error.VectorMigrationCoverageMismatch;
                const reference = try session.put(row.key, row.value);
                try txn.put(candidate_key, reference);
                next.prepared_artifacts +|= 1;
                next.prepared_bytes = try std.math.add(u64, next.prepared_bytes, row.value.len);
                if (next.prepared_bytes > job.budget.temporary_bytes) return error.VectorMigrationTemporaryBudgetExceeded;
            },
            .verifying => {
                if (payload.isReference(row.value)) return error.VectorMigrationCoverageMismatch;
                const candidate = txn.get(candidate_key) catch |err| switch (err) {
                    error.NotFound => return error.VectorMigrationCoverageMismatch,
                    else => return err,
                };
                const expected = try payload.Reference.forArtifact(row.key, row.value);
                if (!std.mem.eql(u8, candidate, &expected.encode())) return error.VectorMigrationCoverageMismatch;
                const restored = try session.getAlloc(alloc, row.key, candidate);
                defer alloc.free(restored);
                if (!std.mem.eql(u8, restored, row.value)) return error.VectorMigrationCoverageMismatch;
                next.verified_artifacts +|= 1;
            },
            .draining => {
                if (!row.inline_payload) continue;
                const reference = row.value;
                const candidate = txn.get(candidate_key) catch |err| switch (err) {
                    error.NotFound => return error.VectorMigrationCoverageMismatch,
                    else => return err,
                };
                if (!std.mem.eql(u8, candidate, reference)) return error.VectorMigrationCoverageMismatch;
                // Preparation already committed with the candidate mapping.
                // Reuse that proof instead of appending/charging the payload a
                // second time while draining the old primary representation.
                try txn.put(row.key, reference);
                try session.recordOwnership(&txn, row.key, reference);
                try txn.delete(candidate_key);
                next.rewritten_artifacts +|= 1;
            },
            .final_verification => {
                if (!payload.isReference(row.value)) return error.VectorMigrationInlinePayloadRemains;
                const restored = try session.getAlloc(alloc, row.key, row.value);
                defer alloc.free(restored);
                const expected = try payload.Reference.forArtifact(row.key, restored);
                if (!std.mem.eql(u8, row.value, &expected.encode())) return error.VectorMigrationCoverageMismatch;
            },
            else => unreachable,
        }
    }
    if (rows.exhausted) {
        next.cursor = "";
        next.phase = switch (job.phase) {
            .backfill => .verifying,
            .verifying => .ready,
            .draining => .final_verification,
            .final_verification => .serving,
            .cleanup => .reclaiming,
            .cancelling => .cancelled,
            else => unreachable,
        };
    }
    try session.stageReferenceEpoch(&txn);
    if (txn.get(contract.accounting_key)) |raw| {
        if (raw.len != 8) return error.InvalidVectorMigrationState;
        next.charged_temporary_bytes = std.mem.readInt(u64, raw[0..8], .little);
    } else |err| if (err != error.NotFound) return err;
    const epoch = txn.get(payload.reference_epoch_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (epoch) |bytes| {
        if (bytes.len != 8) return error.InvalidVectorReferenceEpoch;
        next.replay_cursor = std.mem.readInt(u64, bytes[0..8], .little);
    }
    try save(alloc, &txn, next);
    try boundary(.before_prepare);
    try session.prepareCommit();
    try boundary(.after_prepare);
    session.primary_commit_attempted = true;
    try txn.commit();
    committed = true;
    try boundary(.after_commit);
    // The candidate references and cursor share the committed primary WAL
    // record. Make that record durable without forcing an SSTable for every
    // bounded page (verification pages often change only the job receipt).
    // Publication retains the full storage barrier below.
    try primary.runtime_store.syncReplayState();
    try boundary(.after_sync);
    session.committed = true;
}

/// Ownership and the publication decision are one primary commit. A stale
/// catalog may reconcile this decision, but cannot cause a second conversion.
pub fn publish(alloc: Allocator, primary: *docstore.DocStore, job: contract.Job) !void {
    if (job.published()) return;
    if (job.phase != .ready) return error.VectorMigrationNotReady;
    var next = job;
    next.phase = .draining;
    next.cursor = "";
    var txn = try primary.runtime_store.beginWrite();
    var committed = false;
    defer if (!committed) txn.abort();
    const epoch = txn.get(payload.reference_epoch_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (epoch) |bytes| {
        if (bytes.len != 8) return error.InvalidVectorReferenceEpoch;
        next.replay_cursor = std.mem.readInt(u64, bytes[0..8], .little);
    }
    next.publication_fence = next.replay_cursor;
    try txn.put(&internal_keys.table_storage_settings_key, "{\"dense_embeddings\":\"vector_store\"}");
    try save(alloc, &txn, next);
    try txn.commit();
    committed = true;
    try boundary(.publication_commit);
    try primary.runtime_store.sync(true);
    try boundary(.publication_sync);
}
