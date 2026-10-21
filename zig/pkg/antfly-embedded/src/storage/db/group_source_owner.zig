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
const runtime = @import("../background_runtime.zig");
const work = @import("group_source_worker.zig");
/// Resident-only lifecycle. Stop joins the slice before releasing cursor state.
pub const Owner = struct {
    pub const Port = struct { ptr: *anyopaque, runtime: *runtime.BackendRuntime, run: *const fn (*anyopaque) anyerror!bool };
    port: Port = undefined,
    registration: ?runtime.MaintenanceScheduler.Handle = null,
    mutex: std.atomic.Mutex = .unlocked,
    slice_mutex: std.atomic.Mutex = .unlocked,
    stopping: std.atomic.Value(bool) = .init(false),
    worker: ?work.Worker = null,
    next_definition: usize = 0,
    submitted: u64 = 0,
    pub fn start(self: *Owner, port: Port) void {
        while (!self.mutex.tryLock()) @import("antfly_platform").time.yieldNow();
        defer self.mutex.unlock();
        if (self.stopping.load(.acquire) or self.registration != null) return;
        self.port = port;
        const scheduler = port.runtime.maintenanceScheduler() catch return;
        self.registration = scheduler.registerClass(.producer, self, step) catch return;
    }
    pub fn stop(self: *Owner, io: ?std.Io) void {
        while (!self.mutex.tryLock()) @import("antfly_platform").time.yieldNow();
        self.stopping.store(true, .release);
        var task = self.registration;
        self.registration = null;
        self.mutex.unlock();
        if (task) |*handle| handle.await(io.?);
        while (!self.slice_mutex.tryLock()) @import("antfly_platform").time.yieldNow();
        defer self.slice_mutex.unlock();
        if (self.worker) |*worker| worker.deinit();
        self.worker = null;
    }
    fn step(self: *Owner) ?u64 {
        if (self.stopping.load(.acquire)) return null;
        const changed = self.port.run(self.port.ptr) catch |err| {
            std.log.warn("group source processing deferred: {}", .{err});
            return 250;
        };
        return if (changed) 1 else 100;
    }
};
