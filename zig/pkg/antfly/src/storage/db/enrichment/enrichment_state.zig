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

const std = @import("std");
const Allocator = std.mem.Allocator;
const backend_erased = @import("../../backend_erased.zig");
const docstore_mod = @import("../../docstore.zig");
const lsm_backend = @import("../../lsm_backend.zig");
const mem_backend = @import("../../mem_backend.zig");
const projection_checkpoint_mod = @import("../derived/apply_state.zig");

const applied_seq_prefix = "\x00\x00__metadata__:enrichment_applied:";
const runtime_status_prefix = "\x00\x00__metadata__:enrichment_status:";
const runtime_retry_count_prefix = "\x00\x00__metadata__:enrichment_retry_count:";
const checkpoint_magic = "AFENRCP1";
const checkpoint_format_version: u32 = 1;

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
    /// Inclusive source-sequence interval containing terminal request debt
    /// observed since the last applied boundary. Synchronous barriers use it
    /// to return committed_repair_required for the affected write range.
    terminal_failure_min_sequence: u64 = 0,
    terminal_failure_max_sequence: u64 = 0,
    retrying: bool = false,
    worker_failed: bool = false,
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

pub fn loadAppliedSequence(alloc: Allocator, store: anytype, scope: []const u8) !u64 {
    return (try loadProjectionCheckpoint(alloc, store, scope)).applied_sequence;
}

pub fn loadProjectionCheckpoint(alloc: Allocator, store: anytype, scope: []const u8) !ProjectionCheckpoint {
    const key = try appliedSequenceKey(alloc, scope);
    defer alloc.free(key);

    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const borrowed = txn.get(key) catch |err| switch (err) {
        error.NotFound => return .{},
        else => return err,
    };
    const raw = try alloc.dupe(u8, borrowed);
    defer alloc.free(raw);
    return try decodeProjectionCheckpoint(raw);
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
        @intFromEnum(ProjectionStatus.clean) => .clean,
        @intFromEnum(ProjectionStatus.rebuilding) => .rebuilding,
        @intFromEnum(ProjectionStatus.degraded) => .degraded,
        @intFromEnum(ProjectionStatus.repair_required) => .repair_required,
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
    writeCheckpointInt(&out, &pos, u8, @intFromEnum(checkpoint.status));
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
    const key = try runtimeStatusKey(alloc, scope);
    defer alloc.free(key);
    const retry_count_key = try runtimeRetryCountKey(alloc, scope);
    defer alloc.free(retry_count_key);

    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const borrowed = txn.get(key) catch |err| switch (err) {
        error.NotFound => return .{},
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
    return decodeRuntimeStatus(raw, retry_count_raw);
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
    const key = try runtimeStatusKey(std.heap.page_allocator, scope);
    defer std.heap.page_allocator.free(key);
    const retry_count_key = try runtimeRetryCountKey(std.heap.page_allocator, scope);
    defer std.heap.page_allocator.free(retry_count_key);
    var buf = encodeRuntimeStatus(status);
    var retry_count_buf = encodeRuntimeAuxSidecar(&buf, status);
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try txn.put(key, &buf);
    try txn.put(retry_count_key, &retry_count_buf);
    try txn.commit();
}

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    fn deinit(self: *@This()) void {
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
    var raw = [_]u8{0} ** runtime_status_len;
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
    var raw = [_]u8{0} ** runtime_status_extended_len;
    std.mem.writeInt(u32, raw[40..44], 5, .little);
    std.mem.writeInt(u64, raw[52..60], 91, .little);
    std.mem.writeInt(u32, raw[62..66], 2, .little);

    const loaded = try decodeRuntimeStatus(&raw, null);
    try std.testing.expectEqual(@as(u32, 5), loaded.consecutive_retry_count);
    try std.testing.expectEqual(@as(u32, 2), loaded.retry_failure_count);
}
