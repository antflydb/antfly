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

//! Optional snapshots. Disabled execution performs no counter collection,
//! provider sampling or locking; snapshots never expose mutable backend owners.
const std = @import("std");
const ops = @import("../ops/ops.zig");
pub const ExecutionStats = @import("stats.zig").ExecutionStats;
pub const Specializations = @import("specialization_cache.zig").Stats;
pub const Snapshot = struct { specializations: ?Specializations = null, execution: ExecutionStats = .{}, provider: ops.NativeQuantTimingStats = .{}, runtime: ops.RuntimeTelemetry = .{} };
pub const Diagnostics = struct {
    enabled: std.atomic.Value(bool) = .init(false),
    mutex: std.atomic.Mutex = .unlocked,
    last: ?Snapshot = null,

    pub fn configure(self: *Diagnostics, enabled: bool) void {
        const previous = self.enabled.swap(enabled, .acq_rel);
        if (previous and !enabled) {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            self.last = null;
        }
    }
    pub fn isEnabled(self: *const Diagnostics) bool {
        return self.enabled.load(.acquire);
    }
    pub fn snapshot(self: *Diagnostics) ?Snapshot {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        return self.last;
    }
    pub fn publish(self: *Diagnostics, value: Snapshot) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.isEnabled()) self.last = value;
    }
    pub fn recordSpecializations(self: *Diagnostics, stats: Specializations) void {
        if (!self.isEnabled()) return;
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.isEnabled()) if (self.last) |*last| {
            last.specializations = stats;
        };
    }
    pub fn begin(self: *const Diagnostics, backend: *const ops.ComputeBackend) ?ops.RuntimeTelemetry {
        if (!self.isEnabled()) return null;
        return backend.debugTimingSnapshot().runtime;
    }
    pub fn record(self: *Diagnostics, stats: ExecutionStats, backend: *const ops.ComputeBackend, before: ?ops.RuntimeTelemetry) void {
        if (!self.isEnabled()) return;
        const after = backend.debugTimingSnapshot();
        self.publish(.{ .execution = stats, .provider = after.provider, .runtime = if (before) |start| after.runtime.since(start) else .{} });
    }
};
