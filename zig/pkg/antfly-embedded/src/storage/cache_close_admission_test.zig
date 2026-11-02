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

//! Exercise real cache callback ownership through controlled close preflight.
const std = @import("std");
const resources = @import("resource_manager.zig");
const admission = @import("../common/read_admission.zig");

test "maintenance runtime cache close lends execution and retains rejected callback ownership" {
    const Mode = enum { success, begin_failure, end_failure, expired, registry_contention };
    inline for (.{ @import("restore_decoder_cache.zig").Cache, @import("rewrite_program_cache.zig").Cache, @import("rewrite_tail_spool.zig").Cache }) |Cache| {
        for (std.enums.values(Mode)) |mode| {
            var runtime = std.Io.Threaded.init(std.testing.allocator, .{});
            defer runtime.deinit();
            const io = runtime.io();
            var manager = try resources.metadata_fixture.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 } });
            defer manager.deinit(std.testing.allocator);
            var usage: u64 = 0;
            manager.observeUsage(.hbc_node_metadata_cache, &usage, 100);
            defer manager.observeUsage(.hbc_node_metadata_cache, &usage, 0);
            const F = struct {
                io: std.Io,
                manager: *resources.ResourceManager,
                mode: Mode,
                reached: std.Io.Event = .unset,
                release: std.Io.Event = .unset,
                begins: usize = 0,
                ends: usize = 0,
                parked: bool = false,
                registry_held: bool = false,
                cache_mutex: *std.Io.Mutex,
                fn reclaim(raw: *anyopaque, target: u64) u64 {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.reached.set(self.io);
                    self.release.waitUncancelable(self.io);
                    return target;
                }
                fn run(self: *@This()) void {
                    _ = self.manager.reclaimForAllocation(.dense_apply_working_set, 1);
                }
                fn check(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    if (self.mode == .expired and self.begins != 0) return error.DeadlineExceeded;
                }
                fn deadline(_: *anyopaque) !?u64 {
                    return 1000;
                }
                fn begin(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.begins += 1;
                    if (self.mode == .begin_failure) return error.NotFound;
                    if (self.registry_held) {
                        self.manager.reclaimer_mutex.unlock();
                        self.registry_held = false;
                    }
                    // Handoff must never retain the registry or cache guard.
                    try std.testing.expect(self.cache_mutex.tryLock());
                    self.cache_mutex.unlock(self.io);
                    try std.testing.expect(!self.parked);
                    self.parked = true;
                    self.release.set(self.io);
                }
                fn end(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try std.testing.expect(self.parked);
                    try std.testing.expect(self.cache_mutex.tryLock());
                    self.cache_mutex.unlock(self.io);
                    self.parked = false;
                    self.ends += 1;
                    if (self.mode == .end_failure) return error.Canceled;
                }
            };
            var cache: Cache = .{};
            var f: F = .{ .io = io, .manager = &manager, .mode = mode, .cache_mutex = &cache.mutex };
            const id = try manager.registerReclaimer(.hbc_node_metadata_cache, &f, F.reclaim);
            cache.manager = &manager;
            cache.reclaimer = id;
            defer cache.deinit(io);
            var group: std.Io.Group = .init;
            defer {
                if (f.registry_held) manager.reclaimer_mutex.unlock();
                f.release.set(io);
                group.await(io) catch unreachable;
            }
            try group.concurrent(io, F.run, .{&f});
            try f.reached.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
            if (mode == .registry_contention) {
                try std.testing.expect(manager.reclaimer_mutex.tryLock());
                f.registry_held = true;
            }
            const payload: admission.Payload = .{ .ptr = &f, .vtable = &.{ .checkpoint = F.check, .check_lifetime = F.check, .deadline_ns = F.deadline }, .execution_wait = .{ .ptr = &f, .begin = F.begin, .end = F.end } };
            const control = admission.Port.init(&payload);
            const result = cache.drainCallbacksWithAdmission(io, control);
            switch (mode) {
                .success, .registry_contention => try result,
                else => {
                    try std.testing.expectError(switch (mode) {
                        .begin_failure => error.NotFound,
                        .end_failure => error.Canceled,
                        .expired => error.DeadlineExceeded,
                        else => unreachable,
                    }, result);
                    try std.testing.expectEqual(id, cache.reclaimer);
                    try std.testing.expect(cache.manager == &manager);
                    // New callback acquisition is fenced even if the original
                    // callback has not returned. This call must not block.
                    try std.testing.expectEqual(@as(u64, 0), manager.reclaimForAllocation(.dense_apply_working_set, 1));
                    f.release.set(io);
                    try group.await(io);
                    f.mode = .success;
                    try cache.drainCallbacksWithAdmission(io, control);
                },
            }
            try std.testing.expect(cache.manager == null and cache.reclaimer == 0);
            try std.testing.expect(f.begins != 0 and !f.parked);
            try std.testing.expectEqual(f.begins, f.ends + @as(usize, if (mode == .begin_failure) 1 else 0));
            // Successful preflight and physical deinit are idempotent.
            try cache.drainCallbacksWithAdmission(io, control);
        }
    }
}

test "maintenance runtime cache close retains pinned restore pages until controlled reentry" {
    const Cache = @import("restore_decoder_cache.zig").Cache;
    const Mode = enum { success, begin_failure, end_failure, expired };
    for (std.enums.values(Mode)) |mode| {
        var cache: Cache = .{ .active_pages = 1 };
        defer cache.deinit(std.testing.io);
        const F = struct {
            cache: *Cache,
            mode: Mode,
            begins: usize = 0,
            ends: usize = 0,
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.mode == .expired and self.begins != 0) return error.DeadlineExceeded;
            }
            fn deadline(_: *anyopaque) !?u64 {
                return 1000;
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.begins += 1;
                if (self.mode == .begin_failure) return error.NotFound;
                // The page can return its lease only when cleanup lends CPU;
                // unpin itself must be able to acquire the cache guard.
                self.cache.unpin(std.testing.io);
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.ends += 1;
                try std.testing.expect(self.cache.mutex.tryLock());
                self.cache.mutex.unlock(std.testing.io);
                if (self.mode == .end_failure) return error.Canceled;
            }
        };
        var f: F = .{ .cache = &cache, .mode = mode };
        defer if (cache.active_pages != 0) cache.unpin(std.testing.io);
        const payload: admission.Payload = .{ .ptr = &f, .vtable = &.{ .checkpoint = F.check, .check_lifetime = F.check, .deadline_ns = F.deadline }, .execution_wait = .{ .ptr = &f, .begin = F.begin, .end = F.end } };
        const control = admission.Port.init(&payload);
        const result = cache.drainCallbacksWithAdmission(std.testing.io, control);
        if (mode == .success) try result else {
            try std.testing.expectError(switch (mode) {
                .begin_failure => error.NotFound,
                .end_failure => error.Canceled,
                .expired => error.DeadlineExceeded,
                else => unreachable,
            }, result);
            try std.testing.expectEqual(@as(u32, if (mode == .begin_failure) 1 else 0), cache.active_pages);
            f.mode = .success;
            try cache.drainCallbacksWithAdmission(std.testing.io, control);
        }
        try std.testing.expectEqual(@as(u32, 0), cache.active_pages);
        try std.testing.expectEqual(f.begins, f.ends + @as(usize, if (mode == .begin_failure) 1 else 0));
        try cache.drainCallbacksWithAdmission(std.testing.io, control);
    }
}
