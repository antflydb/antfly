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
const builtin = @import("builtin");
const resource_manager_mod = @import("../../resource_manager.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const derived_worker = @import("derived_worker.zig");

const dense_replay_coalesce_min_records: u64 = 256;
const dense_replay_coalesce_delay_ns: u64 = 50 * std.time.ns_per_ms;
const dense_replay_coalesce_max_wait_ns: u64 = 2_000 * std.time.ns_per_ms;
const dense_catch_up_session_idle_ns: u64 = 5 * std.time.ns_per_s;
const replay_cursor_refresh_records: u64 = 8 * 1024;
const catch_up_max_windows_per_publish: usize = 1;
const dense_replay_max_items_per_window: usize = 25_000;
const replay_default_max_items_per_window: usize = 8 * 1024;
const replay_default_window_bytes: u64 = 16 * 1024 * 1024;
const dense_replay_default_window_bytes: u64 = 64 * 1024 * 1024;
const dense_replay_max_window_bytes: u64 = 256 * 1024 * 1024;
const recoverable_retry_min_delay_ns: u64 = 10 * std.time.ns_per_ms;
const recoverable_retry_max_delay_ns: u64 = 250 * std.time.ns_per_ms;

/// Bounded backoff for transient derived-worker failures. The worker owns this
/// state so a persistently unavailable dependency yields without poisoning the
/// whole runtime or spinning until the dependency becomes available.
pub const RecoverableRetryBackoff = struct {
    failures: u8 = 0,

    pub fn reset(self: *@This()) void {
        self.failures = 0;
    }

    pub fn nextDelayNs(self: *@This()) u64 {
        const shift: u6 = @intCast(@min(self.failures, 5));
        self.failures +|= 1;
        return @min(recoverable_retry_min_delay_ns << shift, recoverable_retry_max_delay_ns);
    }

    pub fn shouldLog(self: @This()) bool {
        return self.failures == 1 or std.math.isPowerOfTwo(self.failures);
    }
};

pub const RecoverableRetryStats = struct {
    total: u64 = 0,
    writer_locked: u64 = 0,
    resource_budget: u64 = 0,
    replay_document_not_visible: u64 = 0,
    artifact_repair_required: u64 = 0,
    not_found: u64 = 0,
};

pub const RecoverableRetryCounters = struct {
    total: std.atomic.Value(u64) = .init(0),
    writer_locked: std.atomic.Value(u64) = .init(0),
    resource_budget: std.atomic.Value(u64) = .init(0),
    replay_document_not_visible: std.atomic.Value(u64) = .init(0),
    artifact_repair_required: std.atomic.Value(u64) = .init(0),
    not_found: std.atomic.Value(u64) = .init(0),

    pub fn record(self: *@This(), err: anyerror) void {
        _ = self.total.fetchAdd(1, .monotonic);
        switch (err) {
            error.WriterLocked => _ = self.writer_locked.fetchAdd(1, .monotonic),
            error.ResourceBudgetExceeded => _ = self.resource_budget.fetchAdd(1, .monotonic),
            error.ReplayDocumentNotVisible => _ = self.replay_document_not_visible.fetchAdd(1, .monotonic),
            error.ArtifactRepairRequired => _ = self.artifact_repair_required.fetchAdd(1, .monotonic),
            error.NotFound => _ = self.not_found.fetchAdd(1, .monotonic),
            else => {},
        }
    }

    pub fn snapshot(self: *const @This()) RecoverableRetryStats {
        return .{
            .total = self.total.load(.monotonic),
            .writer_locked = self.writer_locked.load(.monotonic),
            .resource_budget = self.resource_budget.load(.monotonic),
            .replay_document_not_visible = self.replay_document_not_visible.load(.monotonic),
            .artifact_repair_required = self.artifact_repair_required.load(.monotonic),
            .not_found = self.not_found.load(.monotonic),
        };
    }
};

pub fn recordRecoverableRetry(
    counters: *RecoverableRetryCounters,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backoff: *RecoverableRetryBackoff,
    err: anyerror,
) u64 {
    counters.record(err);
    if (resource_manager) |manager| manager.recordDerivedRecoverableRetry(err);
    return backoff.nextDelayNs();
}

fn getenv(name: [*:0]const u8) ?[*:0]u8 {
    if (!builtin.link_libc) return null;
    return std.c.getenv(name);
}

pub const Policy = struct {
    coalesce_min_records: u64 = 1,
    coalesce_delay_ns: u64 = 0,
    coalesce_max_wait_ns: u64 = 0,
    session_idle_ns: u64 = 0,
    cursor_refresh_records: u64 = replay_cursor_refresh_records,
    max_windows_per_publish: usize = catch_up_max_windows_per_publish,
    max_items_per_window: usize = 0,
    max_chunk_bytes: u64 = replay_default_window_bytes,
    estimated_dense_vector_bytes: u64 = 0,
    force_persist_applied_sequence: bool = false,
    not_found_is_recoverable: bool = false,
};

fn scaleCeilNs(max_ns: u64, numerator: u64, denominator: u64) u64 {
    if (max_ns == 0 or numerator == 0) return 0;
    if (denominator == 0) return max_ns;
    const product = @as(u128, max_ns) * @as(u128, numerator);
    const divisor = @as(u128, denominator);
    var scaled = product / divisor;
    if (product % divisor != 0) scaled += 1;
    return @intCast(@min(scaled, @as(u128, std.math.maxInt(u64))));
}

pub fn replayWindowMaxWaitNs(policy: Policy, pending_records: u64) u64 {
    if (pending_records == 0) return 0;
    if (policy.coalesce_min_records == 0 or policy.coalesce_delay_ns == 0 or policy.coalesce_max_wait_ns == 0) return 0;
    if (pending_records >= policy.coalesce_min_records) return 0;
    return scaleCeilNs(policy.coalesce_max_wait_ns, pending_records, policy.coalesce_min_records);
}

pub fn sessionIdleMaxWaitNs(policy: Policy, recent_tail_records: u64) u64 {
    if (recent_tail_records == 0) return 0;
    if (policy.session_idle_ns == 0) return 0;
    if (policy.coalesce_min_records == 0) return policy.session_idle_ns;
    if (recent_tail_records >= policy.coalesce_min_records) return policy.session_idle_ns;
    return scaleCeilNs(policy.session_idle_ns, recent_tail_records, policy.coalesce_min_records);
}

pub fn forIndex(index_ref: index_manager_mod.ManagedIndexRef, resource_manager: ?*resource_manager_mod.ResourceManager) Policy {
    return switch (index_ref.kind) {
        .dense_vector => .{
            .coalesce_min_records = denseReplayCoalesceMinRecords(),
            .coalesce_delay_ns = denseReplayCoalesceDelayNs(),
            .coalesce_max_wait_ns = denseReplayCoalesceMaxWaitNs(),
            .session_idle_ns = denseCatchUpSessionIdleNs(),
            .cursor_refresh_records = replayCursorRefreshRecords(),
            .max_windows_per_publish = replayMaxWindowsPerPublish(),
            .max_items_per_window = denseReplayMaxItemsPerWindow(),
            .max_chunk_bytes = denseReplayMaxWindowBytes(resource_manager),
            .estimated_dense_vector_bytes = denseReplayEstimatedVectorBytes(),
            .force_persist_applied_sequence = true,
            .not_found_is_recoverable = true,
        },
        .full_text, .sparse_vector, .graph, .algebraic => .{
            .cursor_refresh_records = replayCursorRefreshRecords(),
            .max_windows_per_publish = replayMaxWindowsPerPublish(),
            .max_items_per_window = replayMaxItemsPerWindow(),
            .max_chunk_bytes = replayMaxWindowBytes(resource_manager),
        },
    };
}

fn envU64(name: [:0]const u8, default: u64) u64 {
    const raw_z = getenv(name) orelse return default;
    const raw = std.mem.span(raw_z);
    if (raw.len == 0) return default;
    return std.fmt.parseUnsigned(u64, raw, 10) catch default;
}

fn envUsize(name: [:0]const u8, default: usize) usize {
    const raw_z = getenv(name) orelse return default;
    const raw = std.mem.span(raw_z);
    if (raw.len == 0) return default;
    return std.fmt.parseUnsigned(usize, raw, 10) catch default;
}

fn denseReplayCoalesceMinRecords() u64 {
    return envU64("ANTFLY_DENSE_REPLAY_COALESCE_MIN_RECORDS", dense_replay_coalesce_min_records);
}

fn denseReplayCoalesceDelayNs() u64 {
    return envU64("ANTFLY_DENSE_REPLAY_COALESCE_DELAY_MS", dense_replay_coalesce_delay_ns / std.time.ns_per_ms) * std.time.ns_per_ms;
}

fn denseReplayCoalesceMaxWaitNs() u64 {
    return envU64("ANTFLY_DENSE_REPLAY_COALESCE_MAX_WAIT_MS", dense_replay_coalesce_max_wait_ns / std.time.ns_per_ms) * std.time.ns_per_ms;
}

fn denseCatchUpSessionIdleNs() u64 {
    return envU64("ANTFLY_DENSE_CATCH_UP_SESSION_IDLE_MS", dense_catch_up_session_idle_ns / std.time.ns_per_ms) * std.time.ns_per_ms;
}

fn replayCursorRefreshRecords() u64 {
    return envU64("ANTFLY_DERIVED_REPLAY_CURSOR_REFRESH_RECORDS", replay_cursor_refresh_records);
}

fn replayMaxWindowsPerPublish() usize {
    return envUsize("ANTFLY_DERIVED_REPLAY_MAX_WINDOWS_PER_PUBLISH", catch_up_max_windows_per_publish);
}

fn denseReplayMaxItemsPerWindow() usize {
    return envUsize("ANTFLY_DENSE_REPLAY_MAX_ITEMS_PER_WINDOW", dense_replay_max_items_per_window);
}

fn replayMaxItemsPerWindow() usize {
    return envUsize("ANTFLY_DERIVED_REPLAY_MAX_ITEMS_PER_WINDOW", replay_default_max_items_per_window);
}

fn denseReplayEstimatedVectorBytes() u64 {
    return envU64("ANTFLY_DENSE_REPLAY_ESTIMATED_VECTOR_BYTES", derived_worker.dense_replay_estimated_vector_bytes_default);
}

fn replayMaxWindowBytes(resource_manager: ?*resource_manager_mod.ResourceManager) u64 {
    if (getenv("ANTFLY_DERIVED_REPLAY_MAX_WINDOW_BYTES")) |raw_z| {
        const raw = std.mem.span(raw_z);
        if (raw.len > 0) return std.fmt.parseUnsigned(u64, raw, 10) catch replay_default_window_bytes;
    }
    const manager = resource_manager orelse return replay_default_window_bytes;
    var budget = replay_default_window_bytes;
    const replay = manager.sliceStats(.derived_replay_window);
    if (replay.hard_limit_bytes > 0) budget = @min(budget, replay.hard_limit_bytes);
    return budget;
}

fn denseReplayMaxWindowBytes(resource_manager: ?*resource_manager_mod.ResourceManager) u64 {
    if (getenv("ANTFLY_DENSE_REPLAY_MAX_WINDOW_BYTES")) |raw_z| {
        const raw = std.mem.span(raw_z);
        if (raw.len > 0) return std.fmt.parseUnsigned(u64, raw, 10) catch dense_replay_default_window_bytes;
    }
    const manager = resource_manager orelse return dense_replay_default_window_bytes;
    return manager.denseReplayWindowBudget(.{
        .default_bytes = dense_replay_default_window_bytes,
        .max_bytes = dense_replay_max_window_bytes,
    });
}

test "recoverable retry backoff is bounded and resets" {
    var backoff = RecoverableRetryBackoff{};
    try std.testing.expectEqual(@as(u64, 10 * std.time.ns_per_ms), backoff.nextDelayNs());
    try std.testing.expectEqual(@as(u64, 20 * std.time.ns_per_ms), backoff.nextDelayNs());
    try std.testing.expectEqual(@as(u64, 40 * std.time.ns_per_ms), backoff.nextDelayNs());
    try std.testing.expectEqual(@as(u64, 80 * std.time.ns_per_ms), backoff.nextDelayNs());
    try std.testing.expectEqual(@as(u64, 160 * std.time.ns_per_ms), backoff.nextDelayNs());
    try std.testing.expectEqual(@as(u64, 250 * std.time.ns_per_ms), backoff.nextDelayNs());
    try std.testing.expectEqual(@as(u64, 250 * std.time.ns_per_ms), backoff.nextDelayNs());
    backoff.reset();
    try std.testing.expectEqual(@as(u64, 10 * std.time.ns_per_ms), backoff.nextDelayNs());
}

test "writer-locked derived-worker retries are counted and backoffed" {
    var counters = RecoverableRetryCounters{};
    var manager = resource_manager_mod.ResourceManager.init(.{});
    defer manager.deinit(std.testing.allocator);
    var backoff = RecoverableRetryBackoff{};
    try std.testing.expectEqual(
        @as(u64, 10 * std.time.ns_per_ms),
        recordRecoverableRetry(&counters, &manager, &backoff, error.WriterLocked),
    );
    const stats = counters.snapshot();
    try std.testing.expectEqual(@as(u64, 1), stats.total);
    try std.testing.expectEqual(@as(u64, 1), stats.writer_locked);
    try std.testing.expectEqual(@as(u64, 1), manager.derivedRecoverableRetryStats().writer_locked);
}

test "recoverable retry counters preserve failure reasons" {
    var counters = RecoverableRetryCounters{};
    counters.record(error.WriterLocked);
    counters.record(error.ResourceBudgetExceeded);
    counters.record(error.ReplayDocumentNotVisible);
    counters.record(error.ArtifactRepairRequired);
    counters.record(error.NotFound);

    const stats = counters.snapshot();
    try std.testing.expectEqual(@as(u64, 5), stats.total);
    try std.testing.expectEqual(@as(u64, 1), stats.writer_locked);
    try std.testing.expectEqual(@as(u64, 1), stats.resource_budget);
    try std.testing.expectEqual(@as(u64, 1), stats.replay_document_not_visible);
    try std.testing.expectEqual(@as(u64, 1), stats.artifact_repair_required);
    try std.testing.expectEqual(@as(u64, 1), stats.not_found);
}

test "full text replay policy bounds work by item count as well as bytes" {
    const policy = forIndex(.{ .name = "text", .kind = .full_text }, null);
    try std.testing.expectEqual(replay_default_max_items_per_window, policy.max_items_per_window);
    try std.testing.expectEqual(replay_default_window_bytes, policy.max_chunk_bytes);
}
