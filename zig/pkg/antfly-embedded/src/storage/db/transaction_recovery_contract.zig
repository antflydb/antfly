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

//! Local maintenance configuration and an optional owned runtime factory.
//! Server coordination is supplied through opaque lifecycle operations.
const std = @import("std");
const transactions = @import("../transactions.zig");
const backend = @import("../backend_erased.zig");
const background = @import("../background_runtime.zig");
const Stats = @import("types.zig").TransactionRecoveryStats;

pub const LocalResolutionFn = *const fn (*anyopaque, transactions.TxnId, transactions.TxnStatus, u64) anyerror!void;
pub const CreateContext = struct {
    resolution_extra_hooks: transactions.TxnManager.RecoveryExtraBatchHooks,
    local_resolution_ctx: ?*anyopaque,
    resolve_local_fn: ?LocalResolutionFn,
};
pub const OwnedRuntime = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        deinit: *const fn (*anyopaque) void,
        start: *const fn (*anyopaque) anyerror!void,
        stop: *const fn (*anyopaque) bool,
        pause: *const fn (*anyopaque) bool,
        resume_after_pause: *const fn (*anyopaque) anyerror!void,
        ensure_running: *const fn (*anyopaque) anyerror!bool,
        is_started: *const fn (*anyopaque) bool,
        teardown: *const fn (*anyopaque) void,
        stats: *const fn (*anyopaque) Stats,
        run_once: *const fn (*anyopaque) anyerror!void,
    };
};
pub const Factory = struct {
    // Borrowed through initialization; the created runtime owns its lifetime.
    ptr: *anyopaque,
    // Store is borrowed until OwnedRuntime.deinit returns; do not deinit it.
    create: *const fn (*anyopaque, std.mem.Allocator, backend.Store, *background.BackendRuntime, CreateContext) anyerror!OwnedRuntime,
};
pub const Config = struct {
    enabled: bool = false,
    lease_owned: bool = false,
    owner_id: []const u8 = "local",
    lease_ttl_ms: u64 = 30_000,
    interval_ms: u64 = 30_000,
    cutoff_ns: u64 = 5 * std.time.ns_per_min,
    retained_terminal_ns: u64 = 8 * std.time.ns_per_day,
    max_records_per_run: usize = 16_384,
    clock: @import("antfly_platform").clock.Clock = @import("antfly_platform").clock.Clock.real(),
    factory: ?Factory = null,
    local_resolution_ctx: ?*anyopaque = null,
    resolve_local_fn: ?LocalResolutionFn = null,
    resolution_extra_hooks: transactions.TxnManager.RecoveryExtraBatchHooks = .{},
};
