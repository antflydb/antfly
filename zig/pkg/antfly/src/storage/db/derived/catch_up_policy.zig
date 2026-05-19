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
const replay_default_window_bytes: u64 = 16 * 1024 * 1024;
const dense_replay_default_window_bytes: u64 = 64 * 1024 * 1024;
const dense_replay_max_window_bytes: u64 = 256 * 1024 * 1024;

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
