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

const std = @import("std");
const Crc32 = @import("antfly_hash").Crc32;
const Allocator = std.mem.Allocator;
const backend_erased = @import("../../backend_erased.zig");
const docstore_mod = @import("../../docstore.zig");
const lsm_backend = @import("../../lsm_backend.zig");
const mem_backend = @import("../../mem_backend.zig");
const projection_checkpoint_mod = @import("../derived/apply_state.zig");

const applied_seq_prefix = "\x00\x00__metadata__:enrichment_applied:";
const runtime_status_prefix = "\x00\x00__metadata__:enrichment_status:";
const runtime_retry_count_prefix = "\x00\x00__metadata__:enrichment_retry_count:";
const replay_cursor_prefix = "\x00\x00__metadata__:enrichment_replay_cursor:";
const checkpoint_magic = "AFENRCP1";
const checkpoint_format_version: u32 = 1;
const replay_cursor_magic = "AFENRC1";
const replay_cursor_format_version: u32 = 1;

pub const ProjectionStatus = projection_checkpoint_mod.ProjectionStatus;
pub const ProjectionCheckpoint = projection_checkpoint_mod.ProjectionCheckpoint;

pub fn projectionStatusName(status: ProjectionStatus) []const u8 {
    return switch (status) {
        .clean => "clean",
        .rebuilding => "rebuilding",
        .degraded => "degraded",
        .repair_required => "repair_required",
    };
}

pub const RuntimeStatus = struct {
    output_counts: @import("media_output_counts.zig").Counts = .{},
    last_error_code: @import("../types.zig").RuntimeErrorName = .{},
    target_sequence: u64 = 0,
    error_count: u64 = 0,
    retryable_error_count: u64 = 0,
    fatal_error_count: u64 = 0,
    skipped_source_count: u64 = 0,
    /// All retryable worker-boundary failures since the last durable or
    /// terminally parked replay progress. This is the supervisor liveness
    /// ceiling and deliberately does not reset when only failure identity or
    /// pipeline phase changes.
    consecutive_retry_count: u32 = 0,
    next_retry_at_ms: u64 = 0,
    /// Stable identity of the request or compatible batch that owns the
    /// consecutive retry budget. Zero is reserved for pipeline/infrastructure
    /// failures that cannot safely be attributed to one source request.
    retry_failure_fingerprint: u64 = 0,
    /// Failures attributed to retry_failure_fingerprint. Kept separate from
    /// the no-progress supervisor count so per-request isolation remains fair.
    retry_failure_count: u32 = 0,
    /// Admission waits for the same fingerprint survive worker/process restart
    /// without spending the provider-failure budget.
    admission_wait_count: u32 = 0,
    /// Inclusive source-sequence interval containing terminal request debt
    /// observed since the last applied boundary. Synchronous barriers use it
    /// to return committed_repair_required for the affected write range.
    terminal_failure_min_sequence: u64 = 0,
    terminal_failure_max_sequence: u64 = 0,
    retrying: bool = false,
    worker_failed: bool = false,
};

/// Durable progress within one source replay interval. The ordinary applied
/// sequence advances only after every document through that sequence settles;
/// this cursor prevents a restart from republishing already-checkpointed
/// preparation windows from a large batch. `base_applied_sequence` fences the
/// cursor to the exact replay epoch that created it.
pub const ReplayCursor = struct {
    base_applied_sequence: u64,
    sequence: u64,
    doc_key: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.doc_key);
        self.* = undefined;
    }
};

// Keep the primary record byte-for-byte readable by pre-sidecar binaries so a
// production rollback never turns enrichment telemetry into a DB-open error.
const runtime_status_len = 62;
const runtime_status_extended_len = 66;
const retry_count_magic_v2 = "AFR2";
const retry_count_sidecar_v2_len = retry_count_magic_v2.len + 4 + 8;
const runtime_aux_magic = "AFR3";
const runtime_aux_sidecar_len = runtime_aux_magic.len + 4 + 8 + 8 + 8;

const RuntimeAux = struct {
    retry_failure_count: u32,
    terminal_failure_min_sequence: u64 = 0,
    terminal_failure_max_sequence: u64 = 0,
};

fn appliedSequenceKey(alloc: Allocator, scope: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ applied_seq_prefix, scope });
}

fn runtimeStatusKey(alloc: Allocator, scope: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ runtime_status_prefix, scope });
}

fn runtimeRetryCountKey(alloc: Allocator, scope: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ runtime_retry_count_prefix, scope });
}

fn replayCursorKey(alloc: Allocator, scope: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ replay_cursor_prefix, scope });
}

pub fn loadReplayCursor(alloc: Allocator, store: anytype, scope: []const u8) !?ReplayCursor {
    const key = try replayCursorKey(alloc, scope);
    defer alloc.free(key);
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const borrowed = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const raw = try alloc.dupe(u8, borrowed);
    defer alloc.free(raw);
    return try decodeReplayCursor(alloc, raw);
}

/// A pass may use a cursor only when both records belong to its cut.
/// Leave cleanup to forward publication, which owns the current write view.
pub fn loadReplayCursorAtCut(alloc: Allocator, store: anytype, scope: []const u8, cursor_scope: []const u8, applied: u64) !?ReplayCursor {
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();

    const checkpoint = try loadProjectionCheckpointTxn(alloc, &txn, scope);
    if (checkpoint.applied_sequence != applied) return null;

    const cursor_key = try replayCursorKey(alloc, cursor_scope);
    defer alloc.free(cursor_key);
    const bytes = txn.get(cursor_key) catch |err| if (err == error.NotFound) return null else return err;
    var cursor = decodeReplayCursor(alloc, bytes) catch |err| switch (err) {
        error.InvalidEnrichmentState => return null,
        else => return err,
    };
    if (cursor.base_applied_sequence == applied and cursor.sequence > applied) return cursor;
    cursor.deinit(alloc);
    return null;
}

pub fn saveReplayCursor(store: anytype, scope: []const u8, cursor: ReplayCursor) !void {
    try saveReplayCursorAtCut(std.heap.page_allocator, store, null, scope, cursor);
}

pub fn saveReplayCursorAtCut(alloc: Allocator, store: anytype, scope: ?[]const u8, cursor_scope: []const u8, cursor: ReplayCursor) !void {
    const key = try replayCursorKey(alloc, cursor_scope);
    defer alloc.free(key);
    const raw = try encodeReplayCursor(alloc, cursor);
    defer alloc.free(raw);
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    if (scope) |checkpoint_scope| {
        const checkpoint = try loadProjectionCheckpointTxn(alloc, &txn, checkpoint_scope);
        if (checkpoint.applied_sequence != cursor.base_applied_sequence or cursor.sequence <= checkpoint.applied_sequence) {
            txn.abort();
            return;
        }
    }
    try txn.put(key, raw);
    try txn.commit();
}

pub fn clearReplayCursor(store: anytype, scope: []const u8) !void {
    const key = try replayCursorKey(std.heap.page_allocator, scope);
    defer std.heap.page_allocator.free(key);
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    txn.delete(key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    try txn.commit();
}

fn encodeReplayCursor(alloc: Allocator, cursor: ReplayCursor) ![]u8 {
    if (cursor.doc_key.len > std.math.maxInt(u32))
        return error.InvalidEnrichmentState;
    const body_len = replay_cursor_magic.len + 4 + 8 + 8 + 4 + cursor.doc_key.len;
    const raw = try alloc.alloc(u8, body_len + 4);
    @memcpy(raw[0..replay_cursor_magic.len], replay_cursor_magic);
    var pos: usize = replay_cursor_magic.len;
    writeCheckpointInt(raw, &pos, u32, replay_cursor_format_version);
    writeCheckpointInt(raw, &pos, u64, cursor.base_applied_sequence);
    writeCheckpointInt(raw, &pos, u64, cursor.sequence);
    writeCheckpointInt(raw, &pos, u32, @intCast(cursor.doc_key.len));
    @memcpy(raw[pos .. pos + cursor.doc_key.len], cursor.doc_key);
    pos += cursor.doc_key.len;
    writeCheckpointInt(raw, &pos, u32, Crc32.hash(raw[0..body_len]));
    return raw;
}

fn decodeReplayCursor(alloc: Allocator, raw: []const u8) !ReplayCursor {
    const fixed_len = replay_cursor_magic.len + 4 + 8 + 8 + 4 + 4;
    if (raw.len < fixed_len or !std.mem.eql(u8, raw[0..replay_cursor_magic.len], replay_cursor_magic))
        return error.InvalidEnrichmentState;
    const body_len = raw.len - 4;
    const expected_crc = std.mem.readInt(u32, raw[body_len..][0..4], .little);
    if (Crc32.hash(raw[0..body_len]) != expected_crc) return error.InvalidEnrichmentState;
    var pos: usize = replay_cursor_magic.len;
    if (readCheckpointInt(raw, &pos, u32) != replay_cursor_format_version)
        return error.InvalidEnrichmentState;
    const base_applied_sequence = readCheckpointInt(raw, &pos, u64);
    const sequence = readCheckpointInt(raw, &pos, u64);
    const key_len: usize = readCheckpointInt(raw, &pos, u32);
    if (key_len != body_len - pos) return error.InvalidEnrichmentState;
    return .{
        .base_applied_sequence = base_applied_sequence,
        .sequence = sequence,
        .doc_key = try alloc.dupe(u8, raw[pos..body_len]),
    };
}

pub fn loadAppliedSequence(alloc: Allocator, store: anytype, scope: []const u8) !u64 {
    return (try loadProjectionCheckpoint(alloc, store, scope)).applied_sequence;
}

pub fn loadProjectionCheckpoint(alloc: Allocator, store: anytype, scope: []const u8) !ProjectionCheckpoint {
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    return loadProjectionCheckpointTxn(alloc, &txn, scope);
}

fn loadProjectionCheckpointTxn(alloc: Allocator, txn: anytype, scope: []const u8) !ProjectionCheckpoint {
    const key = try appliedSequenceKey(alloc, scope);
    defer alloc.free(key);
    const raw = txn.get(key) catch |err| if (err == error.NotFound) return .{} else return err;
    return decodeProjectionCheckpoint(raw);
}

pub fn saveAppliedSequence(store: anytype, scope: []const u8, sequence: u64) !void {
    return try saveProjectionCheckpoint(store, scope, .{ .applied_sequence = sequence });
}

pub fn saveProjectionCheckpoint(store: anytype, scope: []const u8, checkpoint: ProjectionCheckpoint) !void {
    const key = try appliedSequenceKey(std.heap.page_allocator, scope);
    defer std.heap.page_allocator.free(key);
    var buf = encodeProjectionCheckpoint(checkpoint);
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try txn.put(key, &buf);
    try txn.commit();
}

/// Commit a precomputed visibility cut as one publication. A failed write
/// leaves progress, cursors and worker status together at the previous cut.
/// Late acknowledgements cannot retire a newer cursor or regress progress.
pub fn publishAppliedProgress(alloc: Allocator, store: anytype, scope: []const u8, sequence: u64, config_hash: u64, status: RuntimeStatus) !u64 {
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    var committed = false;
    defer if (!committed) txn.abort();
    const key = try appliedSequenceKey(alloc, scope);
    defer alloc.free(key);
    const raw = txn.get(key) catch |err| if (err == error.NotFound) null else return err;
    var checkpoint = if (raw) |bytes| try decodeProjectionCheckpoint(bytes) else ProjectionCheckpoint{};
    if (checkpoint.applied_sequence > sequence) return checkpoint.applied_sequence;
    const previous = try loadRuntimeStatusTxn(alloc, &txn, scope);
    // A worker may already have parked a later request while this caller was
    // acquiring publication authority. Preserve that durable status as well.
    const current_status = if (previous.target_sequence > sequence and previous.target_sequence >= status.target_sequence) previous else status;
    const previous_applied = checkpoint.applied_sequence;
    checkpoint.applied_sequence = sequence;
    checkpoint.config_hash = config_hash;
    checkpoint.status = if (current_status.worker_failed) .repair_required else if (current_status.retrying) .degraded else .clean;
    for ([_][]const u8{ "assets", "dense" }) |stream| {
        const cursor_scope = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ scope, stream });
        defer alloc.free(cursor_scope);
        const cursor_key = try replayCursorKey(alloc, cursor_scope);
        defer alloc.free(cursor_key);
        const cursor_raw = txn.get(cursor_key) catch |err| if (err == error.NotFound) null else return err;
        if (cursor_raw) |bytes| {
            var cursor = decodeReplayCursor(alloc, bytes) catch |err| switch (err) {
                // Cursors only avoid replaying already prepared work. Match
                // worker recovery: corrupt optimization state cannot block an
                // authoritative applied cut. Delete it in this same transaction
                // so a storage failure still rolls back the whole publication.
                error.InvalidEnrichmentState => {
                    std.log.warn("discarding corrupt enrichment replay cursor scope={s}", .{cursor_scope});
                    try txn.delete(cursor_key);
                    continue;
                },
                else => return err,
            };
            defer cursor.deinit(alloc);
            if (cursor.sequence <= sequence or cursor.base_applied_sequence != previous_applied) {
                try txn.delete(cursor_key);
            } else if (previous_applied < sequence) {
                // Preserve partial work beyond this cut, keeping its epoch
                // fence valid without resurrecting an already stale cursor.
                cursor.base_applied_sequence = sequence;
                const rebased = try encodeReplayCursor(alloc, cursor);
                defer alloc.free(rebased);
                try txn.put(cursor_key, rebased);
            }
        }
    }
    var encoded = encodeProjectionCheckpoint(checkpoint);
    try txn.put(key, &encoded);
    try putRuntimeStatusTxn(&txn, scope, current_status);
    try txn.commit();
    committed = true;
    return sequence;
}

fn decodeProjectionCheckpoint(raw: []const u8) !ProjectionCheckpoint {
    if (raw.len != checkpoint_magic.len + 4 + 8 + 1 + 8 + 8) return error.InvalidEnrichmentState;
    if (!std.mem.eql(u8, raw[0..checkpoint_magic.len], checkpoint_magic)) return error.InvalidEnrichmentState;
    var pos: usize = checkpoint_magic.len;
    const format_version = readCheckpointInt(raw, &pos, u32);
    if (format_version != checkpoint_format_version) return error.InvalidEnrichmentState;
    const applied_sequence = readCheckpointInt(raw, &pos, u64);
    const status_raw = readCheckpointInt(raw, &pos, u8);
    const generation = readCheckpointInt(raw, &pos, u64);
    const config_hash = readCheckpointInt(raw, &pos, u64);
    const status: ProjectionStatus = switch (status_raw) {
        @backingInt(ProjectionStatus.clean) => .clean,
        @backingInt(ProjectionStatus.rebuilding) => .rebuilding,
        @backingInt(ProjectionStatus.degraded) => .degraded,
        @backingInt(ProjectionStatus.repair_required) => .repair_required,
        else => return error.InvalidEnrichmentState,
    };
    return .{
        .applied_sequence = applied_sequence,
        .status = status,
        .generation = generation,
        .config_hash = config_hash,
    };
}

fn encodeProjectionCheckpoint(checkpoint: ProjectionCheckpoint) [37]u8 {
    var out: [checkpoint_magic.len + 4 + 8 + 1 + 8 + 8]u8 = undefined;
    @memcpy(out[0..checkpoint_magic.len], checkpoint_magic);
    var pos: usize = checkpoint_magic.len;
    writeCheckpointInt(&out, &pos, u32, checkpoint_format_version);
    writeCheckpointInt(&out, &pos, u64, checkpoint.applied_sequence);
    writeCheckpointInt(&out, &pos, u8, @backingInt(checkpoint.status));
    writeCheckpointInt(&out, &pos, u64, checkpoint.generation);
    writeCheckpointInt(&out, &pos, u64, checkpoint.config_hash);
    return out;
}

fn readCheckpointInt(raw: []const u8, pos: *usize, comptime T: type) T {
    const size = @sizeOf(T);
    const out = std.mem.readInt(T, raw[pos.* .. pos.* + size][0..size], .little);
    pos.* += size;
    return out;
}

fn writeCheckpointInt(out: []u8, pos: *usize, comptime T: type, value: T) void {
    const size = @sizeOf(T);
    std.mem.writeInt(T, out[pos.* .. pos.* + size][0..size], value, .little);
    pos.* += size;
}

pub fn loadRuntimeStatus(alloc: Allocator, store: anytype, scope: []const u8) !RuntimeStatus {
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    return loadRuntimeStatusTxn(alloc, &txn, scope);
}

fn loadRuntimeStatusTxn(alloc: Allocator, txn: anytype, scope: []const u8) !RuntimeStatus {
    const key = try runtimeStatusKey(alloc, scope);
    defer alloc.free(key);
    const retry_count_key = try runtimeRetryCountKey(alloc, scope);
    defer alloc.free(retry_count_key);

    const counts_key = try @import("media_output_counts.zig").key(alloc, scope);
    defer alloc.free(counts_key);
    const counts_raw = txn.get(counts_key) catch |err| if (err == error.NotFound) null else return err;
    const output_counts = if (counts_raw) |bytes| try @import("media_output_counts.zig").decode(bytes) else @import("media_output_counts.zig").Counts{};
    const borrowed = txn.get(key) catch |err| switch (err) {
        error.NotFound => return .{ .output_counts = output_counts },
        else => return err,
    };
    const raw = try alloc.dupe(u8, borrowed);
    defer alloc.free(raw);
    const retry_count_raw = if (raw.len == runtime_status_len)
        txn.get(retry_count_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        }
    else
        null;
    var status = try decodeRuntimeStatus(raw, retry_count_raw);
    status.output_counts = output_counts;
    // Separate, hash-bound sidecar keeps older status/aux records readable on
    // rollback. An older writer changes the primary hash, invalidating it.
    const admission_key = try std.mem.concat(alloc, u8, &.{ runtime_status_prefix, scope, ":admission" });
    defer alloc.free(admission_key);
    const admission = txn.get(admission_key) catch |err| if (err == error.NotFound) null else return err;
    if (admission) |bytes| {
        if (bytes.len == 12 and std.mem.readInt(u64, bytes[0..8], .little) == std.hash.Wyhash.hash(0x454e525354415455, raw))
            status.admission_wait_count = std.mem.readInt(u32, bytes[8..12], .little);
    }
    const detail_key = try std.mem.concat(alloc, u8, &.{ runtime_status_prefix, scope, ":error" });
    defer alloc.free(detail_key);
    const detail = txn.get(detail_key) catch |err| switch (err) {
        error.NotFound => return status,
        else => return err,
    };
    if (detail.len >= 8 and detail.len <= 264 and std.mem.readInt(u64, detail[0..8], .little) == std.hash.Wyhash.hash(0x454e525354415455, raw))
        status.last_error_code = @import("../types.zig").RuntimeErrorName.init(detail[8..]);
    return status;
}

fn decodeRuntimeStatus(raw: []const u8, retry_count_raw: ?[]const u8) !RuntimeStatus {
    if (raw.len != runtime_status_len and raw.len != runtime_status_extended_len) return error.InvalidEnrichmentState;
    const aux = decodeRuntimeAuxSidecar(raw, retry_count_raw);
    return .{
        .target_sequence = std.mem.readInt(u64, raw[0..8], .little),
        .error_count = std.mem.readInt(u64, raw[8..16], .little),
        .retryable_error_count = std.mem.readInt(u64, raw[16..24], .little),
        .fatal_error_count = std.mem.readInt(u64, raw[24..32], .little),
        .skipped_source_count = std.mem.readInt(u64, raw[32..40], .little),
        .consecutive_retry_count = std.mem.readInt(u32, raw[40..44], .little),
        .next_retry_at_ms = std.mem.readInt(u64, raw[44..52], .little),
        .retry_failure_fingerprint = std.mem.readInt(u64, raw[52..60], .little),
        .retrying = raw[60] != 0,
        .worker_failed = raw[61] != 0,
        // v1 used consecutive_retry_count for both meanings. Preserve that
        // request budget on upgrade while beginning independent accounting.
        .retry_failure_count = if (raw.len == runtime_status_extended_len)
            std.mem.readInt(u32, raw[62..66], .little)
        else if (aux) |value| value.retry_failure_count else std.mem.readInt(u32, raw[40..44], .little),
        .terminal_failure_min_sequence = if (aux) |value| value.terminal_failure_min_sequence else 0,
        .terminal_failure_max_sequence = if (aux) |value| value.terminal_failure_max_sequence else 0,
    };
}

fn decodeRuntimeAuxSidecar(runtime_status: []const u8, raw: ?[]const u8) ?RuntimeAux {
    const sidecar = raw orelse return null;
    if (sidecar.len == retry_count_sidecar_v2_len and std.mem.eql(u8, sidecar[0..retry_count_magic_v2.len], retry_count_magic_v2)) {
        const expected_hash = std.mem.readInt(u64, sidecar[8..16], .little);
        if (expected_hash != std.hash.Wyhash.hash(0x454e525354415455, runtime_status)) return null;
        return .{ .retry_failure_count = std.mem.readInt(u32, sidecar[4..8], .little) };
    }
    if (sidecar.len != runtime_aux_sidecar_len) return null;
    if (!std.mem.eql(u8, sidecar[0..runtime_aux_magic.len], runtime_aux_magic)) return null;
    const expected_hash = std.mem.readInt(u64, sidecar[24..32], .little);
    if (expected_hash != std.hash.Wyhash.hash(0x454e525354415455, runtime_status)) return null;
    return .{
        .retry_failure_count = std.mem.readInt(u32, sidecar[4..8], .little),
        .terminal_failure_min_sequence = std.mem.readInt(u64, sidecar[8..16], .little),
        .terminal_failure_max_sequence = std.mem.readInt(u64, sidecar[16..24], .little),
    };
}

fn encodeRuntimeStatus(status: RuntimeStatus) [runtime_status_len]u8 {
    var buf: [runtime_status_len]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], status.target_sequence, .little);
    std.mem.writeInt(u64, buf[8..16], status.error_count, .little);
    std.mem.writeInt(u64, buf[16..24], status.retryable_error_count, .little);
    std.mem.writeInt(u64, buf[24..32], status.fatal_error_count, .little);
    std.mem.writeInt(u64, buf[32..40], status.skipped_source_count, .little);
    std.mem.writeInt(u32, buf[40..44], status.consecutive_retry_count, .little);
    std.mem.writeInt(u64, buf[44..52], status.next_retry_at_ms, .little);
    std.mem.writeInt(u64, buf[52..60], status.retry_failure_fingerprint, .little);
    buf[60] = if (status.retrying) 1 else 0;
    buf[61] = if (status.worker_failed) 1 else 0;
    return buf;
}

fn encodeRuntimeAuxSidecar(runtime_status: []const u8, status: RuntimeStatus) [runtime_aux_sidecar_len]u8 {
    var out: [runtime_aux_sidecar_len]u8 = undefined;
    @memcpy(out[0..runtime_aux_magic.len], runtime_aux_magic);
    std.mem.writeInt(u32, out[4..8], status.retry_failure_count, .little);
    std.mem.writeInt(u64, out[8..16], status.terminal_failure_min_sequence, .little);
    std.mem.writeInt(u64, out[16..24], status.terminal_failure_max_sequence, .little);
    std.mem.writeInt(u64, out[24..32], std.hash.Wyhash.hash(0x454e525354415455, runtime_status), .little);
    return out;
}

pub fn saveRuntimeStatus(store: anytype, scope: []const u8, status: RuntimeStatus) !void {
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try putRuntimeStatusTxn(&txn, scope, status);
    try txn.commit();
}

/// Publish worker status and its projection metadata without replacing progress
/// from a checkpoint read before write admission.
pub fn saveRuntimeStatusAndProjection(alloc: Allocator, store: anytype, scope: []const u8, observed_applied: u64, status: RuntimeStatus, projection_status: ProjectionStatus, config_hash: u64) !void {
    const key = try appliedSequenceKey(alloc, scope);
    defer alloc.free(key);
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    const raw = txn.get(key) catch |err| if (err == error.NotFound) null else return err;
    var checkpoint = if (raw) |bytes| try decodeProjectionCheckpoint(bytes) else ProjectionCheckpoint{};
    if (checkpoint.applied_sequence > observed_applied and status.target_sequence <= checkpoint.applied_sequence) {
        txn.abort();
        return;
    }
    checkpoint.status = projection_status;
    checkpoint.config_hash = config_hash;
    const encoded = encodeProjectionCheckpoint(checkpoint);
    try txn.put(key, &encoded);
    try putRuntimeStatusTxn(&txn, scope, status);
    try txn.commit();
}

fn putRuntimeStatusTxn(txn: anytype, scope: []const u8, status: RuntimeStatus) !void {
    // Status is saved on the indexing hot path. Keep the four ordinary keys
    // on the stack; unusually long scopes share a single arena lifetime.
    var key_buffer: [1024]u8 = undefined;
    var fixed_keys = std.heap.FixedBufferAllocator.init(&key_buffer);
    var heap_keys = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer heap_keys.deinit();
    const key_overhead = 3 * runtime_status_prefix.len + runtime_retry_count_prefix.len + ":admission".len + ":error".len;
    const key_alloc = if (scope.len <= (key_buffer.len - key_overhead) / 4) fixed_keys.allocator() else heap_keys.allocator();
    const key = try runtimeStatusKey(key_alloc, scope);
    defer key_alloc.free(key);
    const retry_count_key = try runtimeRetryCountKey(key_alloc, scope);
    defer key_alloc.free(retry_count_key);
    var buf = encodeRuntimeStatus(status);
    var retry_count_buf = encodeRuntimeAuxSidecar(&buf, status);
    try txn.put(key, &buf);
    try txn.put(retry_count_key, &retry_count_buf);
    const admission_key = try std.mem.concat(key_alloc, u8, &.{ runtime_status_prefix, scope, ":admission" });
    defer key_alloc.free(admission_key);
    var admission: [12]u8 = undefined;
    std.mem.writeInt(u64, admission[0..8], std.hash.Wyhash.hash(0x454e525354415455, &buf), .little);
    std.mem.writeInt(u32, admission[8..12], status.admission_wait_count, .little);
    try txn.put(admission_key, &admission);
    const detail_key = try std.mem.concat(key_alloc, u8, &.{ runtime_status_prefix, scope, ":error" });
    defer key_alloc.free(detail_key);
    var detail: [264]u8 = undefined;
    std.mem.writeInt(u64, detail[0..8], std.hash.Wyhash.hash(0x454e525354415455, &buf), .little);
    const code = status.last_error_code.slice();
    @memcpy(detail[8..][0..code.len], code);
    try txn.put(detail_key, detail[0 .. 8 + code.len]);
}

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    pub fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = false };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

test "enrichment apply state works with memory backend store" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "chunks"));
    try saveAppliedSequence(runtime, "chunks", 19);
    try std.testing.expectEqual(@as(u64, 19), try loadAppliedSequence(std.testing.allocator, runtime, "chunks"));
}

test "enrichment apply state works with lsm backend store" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "chunks"));
    try saveAppliedSequence(runtime, "chunks", 23);
    try std.testing.expectEqual(@as(u64, 23), try loadAppliedSequence(std.testing.allocator, runtime, "chunks"));
}

test "enrichment replay cursor round trips and clears independently" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expect((try loadReplayCursor(alloc, runtime, "generated")) == null);
    try saveReplayCursor(runtime, "generated", .{
        .base_applied_sequence = 11,
        .sequence = 12,
        .doc_key = @constCast("doc:064"),
    });
    var cursor = (try loadReplayCursor(alloc, runtime, "generated")).?;
    defer cursor.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 11), cursor.base_applied_sequence);
    try std.testing.expectEqual(@as(u64, 12), cursor.sequence);
    try std.testing.expectEqualStrings("doc:064", cursor.doc_key);
    try clearReplayCursor(runtime, "generated");
    try std.testing.expect((try loadReplayCursor(alloc, runtime, "generated")) == null);
}

test "enrichment replay cursor rejects corruption" {
    const alloc = std.testing.allocator;
    const encoded = try encodeReplayCursor(alloc, .{
        .base_applied_sequence = 1,
        .sequence = 2,
        .doc_key = @constCast("doc:a"),
    });
    defer alloc.free(encoded);
    encoded[encoded.len - 1] ^= 0xff;
    try std.testing.expectError(error.InvalidEnrichmentState, decodeReplayCursor(alloc, encoded));
}

test "enrichment projection checkpoint persists status and identity fields" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveProjectionCheckpoint(runtime, "generated", .{
        .applied_sequence = 41,
        .status = .degraded,
        .generation = 5,
        .config_hash = 0x309,
    });

    const checkpoint = try loadProjectionCheckpoint(std.testing.allocator, runtime, "generated");
    try std.testing.expectEqual(@as(u64, 41), checkpoint.applied_sequence);
    try std.testing.expectEqual(ProjectionStatus.degraded, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 5), checkpoint.generation);
    try std.testing.expectEqual(@as(u64, 0x309), checkpoint.config_hash);
    try std.testing.expectEqual(@as(u64, 41), try loadAppliedSequence(std.testing.allocator, runtime, "generated"));
}

test "enrichment status publication keeps progress advanced before write admission" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer store.deinit();

    try saveProjectionCheckpoint(store, "generated", .{ .applied_sequence = 4, .generation = 7 });
    const Admission = struct {
        var original: backend_erased.Store = undefined;
        var writes: usize = 0;

        fn begin(a: Allocator, ptr: *anyopaque) anyerror!backend_erased.WriteTxn {
            writes += 1;
            // Publish after the worker captured its status and immediately
            // before its write transaction obtains a view of the checkpoint.
            if (writes == 1) _ = try publishAppliedProgress(a, original, "generated", 6, 11, .{ .target_sequence = 6 });
            return original.vtable.begin_write(a, ptr);
        }
    };
    var erased = try initRuntimeStore(alloc, store);
    defer erased.deinit();
    Admission.original = erased.store;
    Admission.writes = 0;
    var vtable = erased.store.vtable.*;
    vtable.begin_write = Admission.begin;
    var instrumented = erased.store;
    instrumented.vtable = &vtable;
    instrumented.write_gate = null;

    const worker_status: RuntimeStatus = .{ .target_sequence = 8, .retrying = true };
    try saveRuntimeStatusAndProjection(alloc, instrumented, "generated", 4, worker_status, .degraded, 12);
    try std.testing.expectEqual(@as(usize, 1), Admission.writes);

    const checkpoint = try loadProjectionCheckpoint(alloc, store, "generated");
    try std.testing.expectEqual(@as(u64, 6), checkpoint.applied_sequence);
    try std.testing.expectEqual(@as(u64, 7), checkpoint.generation);
    try std.testing.expectEqual(@as(u64, 12), checkpoint.config_hash);
    try std.testing.expectEqual(ProjectionStatus.degraded, checkpoint.status);
    const persisted = try loadRuntimeStatus(alloc, store, "generated");
    try std.testing.expect(persisted.retrying);
    try std.testing.expectEqual(@as(u64, 8), persisted.target_sequence);

    // A worker proposes cut 5 while the other publisher commits cut 6
    // and rebases a cursor for unfinished work at cut 9.
    try saveProjectionCheckpoint(store, "generated", .{ .applied_sequence = 4, .generation = 7 });
    var doc_key = [_]u8{'a'};
    try saveReplayCursor(store, "generated.dense", .{
        .base_applied_sequence = 4,
        .sequence = 9,
        .doc_key = &doc_key,
    });
    Admission.writes = 0;
    try std.testing.expectEqual(@as(u64, 6), try publishAppliedProgress(alloc, instrumented, "generated", 5, 13, .{ .target_sequence = 5 }));
    try std.testing.expectEqual(@as(usize, 1), Admission.writes);
    const durable = try loadProjectionCheckpoint(alloc, store, "generated");
    try std.testing.expectEqual(@as(u64, 6), durable.applied_sequence);
    try std.testing.expectEqual(@as(u64, 11), durable.config_hash);
    try std.testing.expectEqual(ProjectionStatus.degraded, durable.status);
    var cursor = (try loadReplayCursor(alloc, store, "generated.dense")).?;
    defer cursor.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 6), cursor.base_applied_sequence);
    try std.testing.expectEqual(@as(u64, 9), cursor.sequence);
    try std.testing.expectEqualStrings("a", cursor.doc_key);

    // An old failure for an acknowledged cut must not be restored after
    // restart, even when it only attempts to publish status.
    try saveRuntimeStatusAndProjection(alloc, store, "generated", 4, .{
        .target_sequence = 5,
        .worker_failed = true,
    }, .repair_required, 99);
    try std.testing.expectEqual(durable, try loadProjectionCheckpoint(alloc, store, "generated"));
    try std.testing.expectEqual(persisted, try loadRuntimeStatus(alloc, store, "generated"));

    try std.testing.expect((try loadReplayCursorAtCut(alloc, store, "generated", "generated.dense", 4)) == null);
    try saveReplayCursorAtCut(alloc, store, "generated", "generated.dense", .{
        .base_applied_sequence = 4,
        .sequence = 7,
        .doc_key = &doc_key,
    });
    var current = (try loadReplayCursorAtCut(alloc, store, "generated", "generated.dense", 6)).?;
    defer current.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 9), current.sequence);
    try std.testing.expectEqualStrings("a", current.doc_key);
}

test "enrichment projection checkpoint rejects malformed structured state" {
    try std.testing.expectError(error.InvalidEnrichmentState, decodeProjectionCheckpoint("short"));

    var encoded = encodeProjectionCheckpoint(.{ .applied_sequence = 7 });
    encoded[0] = 'X';
    try std.testing.expectError(error.InvalidEnrichmentState, decodeProjectionCheckpoint(&encoded));

    encoded = encodeProjectionCheckpoint(.{ .applied_sequence = 7 });
    encoded[checkpoint_magic.len] = 2;
    try std.testing.expectError(error.InvalidEnrichmentState, decodeProjectionCheckpoint(&encoded));
}

test "enrichment state lsm point loads do not clone mutable snapshot" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 1024 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveAppliedSequence(runtime, "chunks", 29);
    try saveRuntimeStatus(runtime, "generated", .{
        .target_sequence = 31,
        .error_count = 1,
        .retryable_error_count = 2,
        .fatal_error_count = 3,
        .skipped_source_count = 4,
        .consecutive_retry_count = 5,
        .next_retry_at_ms = 12_345,
        .retry_failure_fingerprint = 98_765,
        .retry_failure_count = 3,
        .terminal_failure_min_sequence = 6,
        .terminal_failure_max_sequence = 7,
        .retrying = true,
    });

    const before = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 29), try loadAppliedSequence(std.testing.allocator, runtime, "chunks"));
    const status = try loadRuntimeStatus(std.testing.allocator, runtime, "generated");
    try std.testing.expectEqual(@as(u64, 31), status.target_sequence);
    try std.testing.expectEqual(@as(u64, 4), status.skipped_source_count);
    try std.testing.expectEqual(@as(u64, 98_765), status.retry_failure_fingerprint);
    try std.testing.expectEqual(@as(u32, 3), status.retry_failure_count);
    try std.testing.expect(status.retrying);
    const after = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(before.mutable_snapshot_clone_calls, after.mutable_snapshot_clone_calls);
}

test "enrichment runtime status persists source target sequence" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveRuntimeStatus(runtime, "generated", .{
        .target_sequence = 7,
        .last_error_code = @import("../types.zig").RuntimeErrorName.init("GlinerBoundaryPaddedSequenceLimitExceeded"),
        .error_count = 1,
        .retryable_error_count = 2,
        .fatal_error_count = 3,
        .skipped_source_count = 4,
        .consecutive_retry_count = 5,
        .next_retry_at_ms = 12_345,
        .retry_failure_fingerprint = 98_765,
        .retry_failure_count = 3,
        .terminal_failure_min_sequence = 6,
        .terminal_failure_max_sequence = 7,
        .retrying = true,
    });
    const loaded = try loadRuntimeStatus(std.testing.allocator, runtime, "generated");
    try std.testing.expectEqual(@as(u64, 7), loaded.target_sequence);
    try std.testing.expectEqualStrings("GlinerBoundaryPaddedSequenceLimitExceeded", loaded.last_error_code.slice());
    try std.testing.expectEqual(@as(u64, 1), loaded.error_count);
    try std.testing.expectEqual(@as(u64, 2), loaded.retryable_error_count);
    try std.testing.expectEqual(@as(u64, 3), loaded.fatal_error_count);
    try std.testing.expectEqual(@as(u64, 4), loaded.skipped_source_count);
    try std.testing.expectEqual(@as(u32, 5), loaded.consecutive_retry_count);
    try std.testing.expectEqual(@as(u64, 12_345), loaded.next_retry_at_ms);
    try std.testing.expectEqual(@as(u64, 98_765), loaded.retry_failure_fingerprint);
    try std.testing.expectEqual(@as(u32, 3), loaded.retry_failure_count);
    try std.testing.expectEqual(@as(u64, 6), loaded.terminal_failure_min_sequence);
    try std.testing.expectEqual(@as(u64, 7), loaded.terminal_failure_max_sequence);
    try std.testing.expect(loaded.retrying);
    try std.testing.expect(!loaded.worker_failed);
}

test "enrichment runtime status upgrades legacy shared retry count" {
    var raw = @as([runtime_status_len]u8, @splat(0));
    std.mem.writeInt(u32, raw[40..44], 4, .little);
    std.mem.writeInt(u64, raw[52..60], 91, .little);
    raw[60] = 1;

    const loaded = try decodeRuntimeStatus(&raw, null);
    try std.testing.expectEqual(@as(u32, 4), loaded.consecutive_retry_count);
    try std.testing.expectEqual(@as(u32, 4), loaded.retry_failure_count);
    try std.testing.expectEqual(@as(u64, 91), loaded.retry_failure_fingerprint);
    try std.testing.expect(loaded.retrying);
}

test "enrichment runtime status reads legacy retry-count sidecar" {
    const status: RuntimeStatus = .{
        .consecutive_retry_count = 5,
        .retry_failure_fingerprint = 77,
        .retry_failure_count = 2,
        .retrying = true,
    };
    const primary = encodeRuntimeStatus(status);
    var sidecar: [retry_count_sidecar_v2_len]u8 = undefined;
    @memcpy(sidecar[0..retry_count_magic_v2.len], retry_count_magic_v2);
    std.mem.writeInt(u32, sidecar[4..8], status.retry_failure_count, .little);
    std.mem.writeInt(u64, sidecar[8..16], std.hash.Wyhash.hash(0x454e525354415455, &primary), .little);

    const loaded = try decodeRuntimeStatus(&primary, &sidecar);
    try std.testing.expectEqual(@as(u32, 5), loaded.consecutive_retry_count);
    try std.testing.expectEqual(@as(u32, 2), loaded.retry_failure_count);
    try std.testing.expectEqual(@as(u64, 0), loaded.terminal_failure_min_sequence);
    try std.testing.expectEqual(@as(u64, 0), loaded.terminal_failure_max_sequence);
}

test "enrichment runtime status sidecar preserves rollback-readable primary" {
    const status: RuntimeStatus = .{
        .target_sequence = 9,
        .consecutive_retry_count = 5,
        .retry_failure_fingerprint = 77,
        .retry_failure_count = 2,
        .terminal_failure_min_sequence = 8,
        .terminal_failure_max_sequence = 9,
        .retrying = true,
    };
    var primary = encodeRuntimeStatus(status);
    var sidecar = encodeRuntimeAuxSidecar(&primary, status);

    try std.testing.expectEqual(@as(usize, 62), primary.len);
    const loaded = try decodeRuntimeStatus(&primary, &sidecar);
    try std.testing.expectEqual(@as(u32, 5), loaded.consecutive_retry_count);
    try std.testing.expectEqual(@as(u32, 2), loaded.retry_failure_count);
    try std.testing.expectEqual(@as(u64, 8), loaded.terminal_failure_min_sequence);
    try std.testing.expectEqual(@as(u64, 9), loaded.terminal_failure_max_sequence);

    // A rolled-back binary may rewrite the legacy primary while leaving an
    // unknown sidecar behind. Its checksum must prevent stale attribution.
    primary[40] = 3;
    const after_rollback_write = try decodeRuntimeStatus(&primary, &sidecar);
    try std.testing.expectEqual(@as(u32, 3), after_rollback_write.retry_failure_count);
}

test "enrichment runtime status reads transient extended development record" {
    var raw = @as([runtime_status_extended_len]u8, @splat(0));
    std.mem.writeInt(u32, raw[40..44], 5, .little);
    std.mem.writeInt(u64, raw[52..60], 91, .little);
    std.mem.writeInt(u32, raw[62..66], 2, .little);

    const loaded = try decodeRuntimeStatus(&raw, null);
    try std.testing.expectEqual(@as(u32, 5), loaded.consecutive_retry_count);
    try std.testing.expectEqual(@as(u32, 2), loaded.retry_failure_count);
}

test "admission wait budget survives reopen and ignores stale rollback sidecars" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();
    var store = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer store.deinit();
    const status: RuntimeStatus = .{
        .target_sequence = 10,
        .retry_failure_fingerprint = 42,
        .retry_failure_count = 0,
        .admission_wait_count = 15,
        .retrying = true,
    };
    try saveRuntimeStatus(store, "admission-test", status);
    const loaded = try loadRuntimeStatus(std.testing.allocator, store, "admission-test");
    try std.testing.expectEqual(@as(u32, 15), loaded.admission_wait_count);
    try std.testing.expectEqual(@as(u32, 0), loaded.retry_failure_count);
    // Simulate an older writer replacing only the legacy primary record.
    const key = try runtimeStatusKey(std.testing.allocator, "admission-test");
    defer std.testing.allocator.free(key);
    var txn = try store.beginWrite();
    errdefer txn.abort();
    var newer = status;
    newer.retry_failure_fingerprint = 43;
    const raw = encodeRuntimeStatus(newer);
    try txn.put(key, &raw);
    try txn.commit();
    const rolled_back = try loadRuntimeStatus(std.testing.allocator, store, "admission-test");
    try std.testing.expectEqual(@as(u32, 0), rolled_back.admission_wait_count);
    // Long scopes use the heap fallback without truncating any sidecar key.
    const long_scope = @as([1024]u8, @splat('x'));
    try saveRuntimeStatus(store, &long_scope, status);
    const long_loaded = try loadRuntimeStatus(std.testing.allocator, store, &long_scope);
    try std.testing.expectEqual(@as(u32, 15), long_loaded.admission_wait_count);
}

test "admission wait budget survives disk close and WAL replay" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    // Reconstruct both backend and runtime store between every attempt. This
    // exercises durable sidecars rather than reopening an in-memory view.
    for (0..17) |attempt| {
        var backend = try lsm_backend.Backend.open(alloc, path, .{});
        defer backend.close();
        var store = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer store.deinit();
        var status = try loadRuntimeStatus(alloc, store, "admission-test");
        try std.testing.expectEqual(@as(u32, @intCast(attempt)), status.admission_wait_count);
        try std.testing.expectEqual(@as(u32, 0), status.retry_failure_count);
        status.target_sequence = 10;
        status.retry_failure_fingerprint = 42;
        status.admission_wait_count += 1;
        status.retrying = true;
        try saveRuntimeStatus(store, "admission-test", status);
    }
}

pub const scoped_retries = @import("scoped_retries.zig");
const scoped_retry_key = "\x00\x00__metadata__:enrichment_scoped_retries:v1";

pub fn loadScopedRetries(alloc: Allocator, store: anytype) !scoped_retries.State {
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const raw = txn.get(scoped_retry_key) catch |err| if (err == error.NotFound) return .{} else return err;
    if (!scoped_retries.State.isHeader(raw)) return scoped_retries.State.decode(alloc, raw);
    var header = try scoped_retries.State.decodeHeader(alloc, raw);
    errdefer header.state.deinit(alloc);
    for (header.slots[0..header.count]) |slot| {
        const bytes = try txn.get(&scopedRetryTicketKey(slot));
        var ticket = try scoped_retries.State.decodeTicket(alloc, bytes, slot);
        errdefer ticket.deinit(alloc);
        try header.state.tickets.append(alloc, ticket);
    }
    header.state.markPersisted();
    return header.state;
}

fn saveScopedRetries(store: anytype, raw: []const u8) !void {
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try txn.put(scoped_retry_key, raw);
    try txn.commit();
}

const scoped_retry_ticket_prefix = "\x00\x00__metadata__:enrichment_scoped_retry_ticket:v2:";
fn scopedRetryTicketKey(slot: u8) [scoped_retry_ticket_prefix.len + 1]u8 {
    var key: [scoped_retry_ticket_prefix.len + 1]u8 = undefined;
    @memcpy(key[0..scoped_retry_ticket_prefix.len], scoped_retry_ticket_prefix);
    key[scoped_retry_ticket_prefix.len] = slot;
    return key;
}
const scoped_retry_document_prefix = "\x00\x00__metadata__:enrichment_scoped_retry_document:v1:";
fn scopedRetryDocumentKey(digest: [32]u8) [scoped_retry_document_prefix.len + 32]u8 {
    return scoped_retry_document_prefix.* ++ digest;
}
pub fn loadScopedRetryDocument(a: Allocator, store: anytype, digest: [32]u8) ![]u8 {
    var runtime = try initRuntimeStore(a, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const raw = txn.get(&scopedRetryDocumentKey(digest)) catch |err|
        return if (err == error.NotFound) error.InvalidEnrichmentState else err;
    return a.dupe(u8, raw);
}

/// One ownership-fenced transaction publishes the manifest, changed rows and
/// retirements. A failed transaction leaves every dirty bit/old slot intact.
pub fn saveScopedRetryState(a: Allocator, store: anytype, state: *scoped_retries.State, guard: anytype) !void {
    const prepared = try state.preparePersistence(a, false);
    defer prepared.deinit(a);
    var runtime = try initRuntimeStore(a, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try guard.validate(&txn);
    for (prepared.documents) |document| try txn.put(&scopedRetryDocumentKey(document.digest), document.bytes);
    for (prepared.rows) |row| try txn.put(&scopedRetryTicketKey(row.slot), row.bytes);
    for (prepared.removed[0..prepared.removed_len]) |slot| txn.delete(&scopedRetryTicketKey(slot)) catch |err| if (err != error.NotFound) {
        return err;
    };
    for (prepared.removed_documents[0..prepared.removed_document_count]) |digest|
        txn.delete(&scopedRetryDocumentKey(digest)) catch |err| if (err != error.NotFound) return err;
    try txn.put(scoped_retry_key, prepared.header);
    try txn.commit();
    state.markPersisted();
    state.releasePersistedDocuments(a);
}

test "durable enrichment scoped retry references oversized IDs atomically and reclaims unreferenced keys" {
    const a = std.testing.allocator;
    const backend_mod = @import("../../mem_backend.zig");
    var backend = backend_mod.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "retry-key-references" });
    defer store.deinit();
    const Guard = struct {
        canceled: bool = false,
        pub fn validate(self: @This(), _: anytype) !void {
            if (self.canceled) return error.Canceled;
        }
    };
    const key = try a.alloc(u8, scoped_retries.max_document_key_bytes + 1);
    defer a.free(key);
    @memset(key, 'x');
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(key, &digest, .{});
    var state: scoped_retries.State = .{};
    defer state.deinit(a);
    try state.park(a, .{ .doc_key = key, .sequence = 7 }, 10, .{ .fingerprint = 1 }, null);
    try state.setScanDocument(a, .{ .doc_key = key, .sequence = 7 });
    {
        const plan = try state.preparePersistence(a, false);
        defer plan.deinit(a);
        try std.testing.expectEqual(@as(usize, 1), plan.documents.len);
        try std.testing.expect(plan.header.len + plan.rows[0].bytes.len < 512);
    }
    try std.testing.expectError(error.Canceled, saveScopedRetryState(a, &store, &state, Guard{ .canceled = true }));
    try std.testing.expectEqual(@as(usize, 1), state.pending_documents.items.len);
    try std.testing.expectError(error.InvalidEnrichmentState, loadScopedRetryDocument(a, &store, digest));
    try saveScopedRetryState(a, &store, &state, Guard{});
    try std.testing.expectEqual(@as(usize, 0), state.pending_documents.items.len);
    var restored = try loadScopedRetries(a, &store);
    defer restored.deinit(a);
    const Reader = struct {
        store: *@TypeOf(store),
        reads: usize = 0,
        fn get(ptr: *anyopaque, allocator: Allocator, id: [32]u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reads += 1;
            return loadScopedRetryDocument(allocator, self.store, id);
        }
    };
    var reader = Reader{ .store = &store };
    const resolver: scoped_retries.DocumentResolver = .{ .ptr = &reader, .get = Reader.get };
    try std.testing.expectEqual(@as(usize, 0), restored.pending_documents.items.len);
    const cursor = try restored.scanDocumentAllocWithReferences(a, resolver);
    defer a.free(cursor);
    try std.testing.expectEqualSlices(u8, key, cursor);
    const due = try restored.dueWithReferences(a, 10, resolver, 1);
    defer due.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), due.groups.len);
    try std.testing.expectEqualSlices(u8, key, due.groups[0].doc_key);
    try std.testing.expectEqual(@as(usize, 2), reader.reads);
    restored.retire(a, due.tokens);
    try saveScopedRetryState(a, &store, &restored, Guard{});
    // The cursor still protects the same deduplicated key after ticket retirement.
    const retained = try loadScopedRetryDocument(a, &store, digest);
    defer a.free(retained);
    try std.testing.expectEqualSlices(u8, key, retained);
    restored.clearScanDocument(a);
    try saveScopedRetryState(a, &store, &restored, Guard{});
    try std.testing.expectError(error.InvalidEnrichmentState, loadScopedRetryDocument(a, &store, digest));
}

test "durable enrichment scoped retry flushes large identities before exceeding the staging budget" {
    const a = std.testing.allocator;
    const backend_mod = @import("../../mem_backend.zig");
    var backend = backend_mod.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "retry-key-bounded-flush" });
    defer store.deinit();
    const Guard = struct {
        canceled: bool = false,
        pub fn validate(self: @This(), _: anytype) !void {
            if (self.canceled) return error.Canceled;
        }
    };
    var state: scoped_retries.State = .{};
    defer state.deinit(a);
    const Publisher = struct {
        state: *scoped_retries.State,
        store: *@TypeOf(store),
        canceled: bool = false,
        calls: usize = 0,
        pub fn save(self: *@This()) !void {
            self.calls += 1;
            try saveScopedRetryState(std.testing.allocator, self.store, self.state, Guard{ .canceled = self.canceled });
        }
    };
    var publisher = Publisher{ .state = &state, .store = &store };
    const first = try a.alloc(u8, scoped_retries.max_document_key_bytes + 1);
    defer a.free(first);
    @memset(first, 'a');
    const second = try a.dupe(u8, first);
    defer a.free(second);
    second[0] = 'b';
    const limit = first.len * 2 - 1;
    try state.parkBounded(a, .{ .doc_key = first, .sequence = 1 }, 10, .{ .fingerprint = 1 }, null, null, null, limit, &publisher);
    // Repeated identities do not force another publication or retain a copy.
    try state.parkBounded(a, .{ .doc_key = first, .sequence = 1 }, 10, .{ .fingerprint = 1 }, null, null, null, limit, &publisher);
    try std.testing.expectEqual(@as(usize, 0), publisher.calls);
    publisher.canceled = true;
    try std.testing.expectError(error.Canceled, state.parkBounded(a, .{ .doc_key = second, .sequence = 2 }, 10, .{ .fingerprint = 2 }, null, null, null, limit, &publisher));
    try std.testing.expectEqual(@as(usize, 1), state.tickets.items.len);
    try std.testing.expectEqual(@as(usize, 1), state.pending_documents.items.len);
    publisher.canceled = false;
    try state.parkBounded(a, .{ .doc_key = second, .sequence = 2 }, 10, .{ .fingerprint = 2 }, null, null, null, limit, &publisher);
    try std.testing.expectEqual(@as(usize, 2), publisher.calls);
    try std.testing.expectEqual(@as(usize, 1), state.pending_documents.items.len);
    try std.testing.expectEqualSlices(u8, second, state.pending_documents.items[0].bytes);
    var restored = try loadScopedRetries(a, &store);
    defer restored.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), restored.tickets.items.len);
    try publisher.save();
    var final = try loadScopedRetries(a, &store);
    defer final.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), final.tickets.items.len);
}

test "durable enrichment scoped retry references cannot alias inline IDs and reject corrupted keys" {
    const a = std.testing.allocator;
    const backend_mod = @import("../../mem_backend.zig");
    var backend = backend_mod.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "retry-key-identity" });
    defer store.deinit();
    const Guard = struct {
        pub fn validate(_: @This(), _: anytype) !void {}
    };
    const key = try a.alloc(u8, scoped_retries.max_document_key_bytes + 1);
    defer a.free(key);
    @memset(key, 'z');
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(key, &digest, .{});
    var state: scoped_retries.State = .{};
    defer state.deinit(a);
    try state.park(a, .{ .doc_key = key, .sequence = 7 }, 10, .{ .fingerprint = 1 }, null);
    try state.park(a, .{ .doc_key = &digest, .sequence = 7 }, 10, .{ .fingerprint = 2 }, null);
    try saveScopedRetryState(a, &store, &state, Guard{});
    const Reader = struct {
        store: *@TypeOf(store),
        reads: usize = 0,
        fn get(ptr: *anyopaque, allocator: Allocator, id: [32]u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reads += 1;
            return loadScopedRetryDocument(allocator, self.store, id);
        }
    };
    var reader = Reader{ .store = &store };
    const resolver: scoped_retries.DocumentResolver = .{ .ptr = &reader, .get = Reader.get };
    {
        const due = try state.dueWithReferences(a, 10, resolver, 1);
        defer due.deinit(a);
        try std.testing.expectEqual(@as(usize, 1), due.groups.len);
        try std.testing.expectEqualSlices(u8, key, due.groups[0].doc_key);
        try std.testing.expectEqual(@as(usize, 1), reader.reads);
        state.retire(a, due.tokens);
    }
    try saveScopedRetryState(a, &store, &state, Guard{});
    try std.testing.expectError(error.InvalidEnrichmentState, loadScopedRetryDocument(a, &store, digest));
    {
        const due = try state.dueWithReferences(a, 10, resolver, 1);
        defer due.deinit(a);
        try std.testing.expectEqualSlices(u8, &digest, due.groups[0].doc_key);
        try std.testing.expectEqual(@as(usize, 1), reader.reads);
    }
    try state.park(a, .{ .doc_key = key, .sequence = 8 }, 10, .{ .fingerprint = 3 }, null);
    try saveScopedRetryState(a, &store, &state, Guard{});
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        try txn.put(&scopedRetryDocumentKey(digest), "corrupt");
        try txn.commit();
    }
    try std.testing.expectError(error.InvalidEnrichmentState, state.dueWithReferences(a, 10, resolver, 0));
}

test "durable enrichment scoped retry persists incremental records and migrates legacy clocks" {
    const a = std.testing.allocator;
    const backend_mod = @import("../../mem_backend.zig");
    var backend = backend_mod.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "retry-migration" });
    defer store.deinit();
    const Guard = struct {
        pub fn validate(_: @This(), _: anytype) !void {}
    };
    const old =
        \\{"version":1,"scanned_sequence":100,"tickets":[{"document":"ZG9j","sequence":2,"source_digest":null,"deadline_ms":3601000,"revision":1,"budgets":[{"fingerprint":17,"logical_fingerprint":7,"failures":2,"waits":3,"deadline_ms":3601000,"revision":1}]}]}
    ;
    try saveScopedRetries(store, old);
    var migrated = try loadScopedRetries(a, store);
    defer migrated.deinit(a);
    try saveScopedRetryState(a, store, &migrated, Guard{});
    var restored = try loadScopedRetries(a, store);
    defer restored.deinit(a);
    try std.testing.expectEqual(@as(u64, 3_601_000), restored.deadline());
    try std.testing.expectEqual(@as(u32, 2), restored.prior(17).failures);
    try std.testing.expectEqual(@as(u32, 3), restored.prior(17).waits);
    try std.testing.expectEqual(@as(u64, 1), restored.appliedThrough(100));
    const unchanged = try restored.preparePersistence(a, false);
    defer unchanged.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), unchanged.rows.len);
    const due = try restored.due(a, 3_601_000);
    defer due.deinit(a);
    const old_slot = restored.tickets.items[0].slot;
    restored.retire(a, due.tokens);
    try saveScopedRetryState(a, store, &restored, Guard{});
    var handle = try initRuntimeStore(a, store);
    defer handle.deinit();
    {
        var read = try handle.store.beginProbe();
        defer read.abort();
        try std.testing.expectError(error.NotFound, read.get(&scopedRetryTicketKey(old_slot)));
    }
    // Reuse a retired slot. A rejected transaction cannot clear dirty state
    // or expose a manifest that references an unwritten ticket.
    try restored.park(a, .{ .doc_key = "replacement", .sequence = 101 }, 9_000_000, .{ .fingerprint = 19, .failures = 1 }, null);
    try std.testing.expectEqual(old_slot, restored.tickets.items[0].slot);
    const Reject = struct {
        pub fn validate(_: @This(), _: anytype) !void {
            return error.FenceLost;
        }
    };
    try std.testing.expectError(error.FenceLost, saveScopedRetryState(a, store, &restored, Reject{}));
    try std.testing.expect(restored.tickets.items[0].dirty);
    var still_empty = try loadScopedRetries(a, store);
    defer still_empty.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), still_empty.tickets.items.len);
    try saveScopedRetryState(a, store, &restored, Guard{});
    var reused = try loadScopedRetries(a, store);
    defer reused.deinit(a);
    try std.testing.expectEqual(@as(u64, 9_000_000), reused.deadline());
    try std.testing.expectEqual(@as(u32, 1), reused.prior(19).failures);
}

test "privacy replay cursor preserves an empty document identifier" {
    const a = std.testing.allocator;
    const raw = try encodeReplayCursor(a, .{ .base_applied_sequence = 1, .sequence = 2, .doc_key = @constCast("") });
    defer a.free(raw);
    var decoded = try decodeReplayCursor(a, raw);
    defer decoded.deinit(a);
    try std.testing.expectEqualStrings("", decoded.doc_key);
    try std.testing.expectEqual(@as(u64, 2), decoded.sequence);
}

test "durable enrichment scoped retry drains configuration replans across bounded pages and restart" {
    const a = std.testing.allocator;
    const backend_mod = @import("../../mem_backend.zig");
    var backend = backend_mod.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "retry-replan-pages" });
    defer store.deinit();
    const Guard = struct {
        canceled: bool = false,
        pub fn validate(self: @This(), _: anytype) !void {
            if (self.canceled) return error.Canceled;
        }
    };
    const Reader = struct {
        fn get(_: *anyopaque, _: Allocator, _: [32]u8) ![]u8 {
            return error.TestUnexpectedResult;
        }
    };
    var marker: u8 = 0;
    const resolver: scoped_retries.DocumentResolver = .{ .ptr = &marker, .get = Reader.get };
    var state: scoped_retries.State = .{};
    defer state.deinit(a);
    // A full queue prevents fresh discovery from masking missed replans.
    const future = 3_600_000;
    for (0..scoped_retries.max_documents) |i| {
        var buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&buf, "doc-{d}", .{i});
        try state.park(a, .{ .doc_key = key, .sequence = i + 1 }, future, .{ .fingerprint = i + 1, .logical_fingerprint = i + 1, .failures = 2, .waits = 3 }, null);
    }
    state.config_hash = 2;
    state.beginReplan();
    try saveScopedRetryState(a, &store, &state, Guard{});
    var visited: std.StaticBitSet(scoped_retries.max_documents) = .empty;
    for (0..scoped_retries.max_documents) |page| {
        // Reload every page: the new configuration hash and pending work must
        // survive independently of the original config_changed stack frame.
        var restored = try loadScopedRetries(a, &store);
        defer restored.deinit(a);
        try std.testing.expectEqual(@as(u64, 2), restored.config_hash);
        try std.testing.expectEqual(@as(u64, 0), restored.deadline());
        const due = try restored.dueWithReferences(a, 0, resolver, 1);
        defer due.deinit(a);
        try std.testing.expectEqual(@as(usize, 1), due.groups.len);
        try std.testing.expectEqual(@as(usize, 0), due.tokens.len);
        const index: usize = @intCast(due.groups[0].sequence - 1);
        try std.testing.expect(!visited.isSet(index));
        visited.set(index);
        restored.completeReplan(due);
        // Even an immediately due producer in a completed page cannot starve
        // the rest of the finite replan inventory.
        restored.tickets.items[index].deadline_ms = 0;
        restored.tickets.items[index].budgets.items[0].deadline_ms = 0;
        restored.tickets.items[index].dirty = true;
        if (page == 0) {
            try std.testing.expectError(error.Canceled, saveScopedRetryState(a, &store, &restored, Guard{ .canceled = true }));
            var unchanged = try loadScopedRetries(a, &store);
            defer unchanged.deinit(a);
            try std.testing.expectEqual(@as(usize, scoped_retries.max_documents), unchanged.pending_replan.count());
        }
        try saveScopedRetryState(a, &store, &restored, Guard{});
    }
    try std.testing.expectEqual(@as(usize, scoped_retries.max_documents), visited.count());
    var final = try loadScopedRetries(a, &store);
    defer final.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), final.pending_replan.count());
    for (final.tickets.items) |*ticket| {
        ticket.deadline_ms = future;
        ticket.budgets.items[0].deadline_ms = future;
    }
    try std.testing.expectEqual(@as(u64, future), final.deadline());
    for (final.tickets.items) |ticket| {
        try std.testing.expectEqual(@as(u32, 2), ticket.budgets.items[0].failures);
        try std.testing.expectEqual(@as(u32, 3), ticket.budgets.items[0].waits);
    }
    const waiting = try final.dueWithReferences(a, 0, resolver, 1);
    defer waiting.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), waiting.groups.len);
    // A later change must rearm all slots, including completed earlier pages.
    final.beginReplan();
    final.removeTicket(a, 0);
    try std.testing.expectEqual(@as(usize, scoped_retries.max_documents - 1), final.pending_replan.count());
    try final.park(a, .{ .doc_key = "new", .sequence = 1000 }, future, .{ .fingerprint = 1000 }, null);
    try std.testing.expect(!final.pending_replan.isSet(final.tickets.items[final.tickets.items.len - 1].slot));
}
