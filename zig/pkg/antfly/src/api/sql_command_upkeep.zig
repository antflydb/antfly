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

//! Independent lease/control upkeep. A fixed I/O lane owns bounded keyset
//! rounds; it neither borrows request memory nor evaluates an abandoned body.
const std = @import("std");
const source = @import("sql_command_producer.zig");
const Server = @import("http_server.zig").ApiHttpServer;
const time = @import("antfly_platform").time;
const A = std.mem.Allocator;
pub const concurrency = 8;
pub const capacity = 128;
pub const Service = struct {
    server: *Server,
    io_impl: std.Io.Threaded,
    stop_event: std.Io.Event = .unset,
    futures: [concurrency]?std.Io.Future(void) = @splat(null),
    mutex: std.Io.Mutex = .init,
    after: ?[32]u8 = null,
    batch: [capacity][32]u8 = undefined,
    batch_len: usize = 0,
    next: usize = 0,
    in_flight: usize = 0,
    due_ns: u64 = 0,
    completed: std.atomic.Value(u64) = .init(0),
    failures: std.atomic.Value(u64) = .init(0),

    /// Stable allocation precedes task creation. A partial start joins every
    /// task before freeing its I/O implementation or server borrow.
    pub fn start(server: *Server) !*Service {
        const self = try server.owner_alloc.create(Service);
        errdefer server.owner_alloc.destroy(self);
        self.* = .{ .server = server, .io_impl = .init(server.owner_alloc, .{ .async_limit = .nothing, .concurrent_limit = .limited(concurrency) }) };
        errdefer self.io_impl.deinit();
        errdefer self.join();
        const io = self.io_impl.io();
        for (&self.futures) |*future| future.* = try io.concurrent(run, .{self});
        return self;
    }
    pub fn stop(self: *Service) void {
        const a = self.server.owner_alloc;
        self.join();
        self.io_impl.deinit();
        a.destroy(self);
    }
    fn join(self: *Service) void {
        const io = self.io_impl.io();
        self.stop_event.set(io);
        for (&self.futures) |*slot| if (slot.*) |*future| {
            @import("protected_future.zig").wait(io, future);
            slot.* = null;
        };
    }
    fn token(self: *const Service) @import("antfly_local_sources").api_operation.CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = struct {
            fn call(raw: *const anyopaque) bool {
                const service: *const Service = @ptrCast(@alignCast(raw));
                return service.stop_event.isSet();
            }
        }.call };
    }
    /// Each round claims a fixed keyset page once. Exhausting workers never
    /// duplicates an in-flight key or creates an unbounded pending job queue.
    fn claim(self: *Service) !?[32]u8 {
        const io = self.io_impl.io();
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        if (self.stop_event.isSet()) return null;
        if (self.next == self.batch_len) {
            if (self.in_flight != 0 or time.monotonicNs() < self.due_ns) return null;
            const store = self.server.txn_sessions.durable orelse return null;
            const page = try source.scanWork(store, self.server.alloc, self.after, capacity);
            defer self.server.alloc.free(page.ids);
            self.after = page.next_after;
            self.batch_len = page.ids.len;
            self.next = 0;
            @memcpy(self.batch[0..page.ids.len], page.ids);
            if (page.ids.len == 0) {
                self.due_ns = time.monotonicNs() +| std.time.ns_per_s;
                return null;
            }
        }
        const id = self.batch[self.next];
        self.next += 1;
        self.in_flight += 1;
        return id;
    }
    fn release(self: *Service) void {
        const io = self.io_impl.io();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        std.debug.assert(self.in_flight != 0);
        self.in_flight -= 1;
        if (self.in_flight == 0 and self.next == self.batch_len and self.after == null)
            self.due_ns = time.monotonicNs() +| std.time.ns_per_s;
    }
    fn run(self: *Service) void {
        const io = self.io_impl.io();
        while (!self.stop_event.isSet()) {
            const item = self.claim() catch blk: {
                _ = self.failures.fetchAdd(1, .monotonic);
                break :blk null;
            };
            if (item) |id| {
                defer self.release();
                var arena = std.heap.ArenaAllocator.init(self.server.alloc);
                defer arena.deinit();
                source.advanceConnection(self.server, arena.allocator(), id, self.token()) catch {
                    _ = self.failures.fetchAdd(1, .monotonic);
                };
                _ = self.completed.fetchAdd(1, .release);
                continue;
            }
            self.stop_event.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(100), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => return,
            };
        }
    }
};
