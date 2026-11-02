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

//! Independent model-loader CPU ownership across the supervised worker pipe.
//! Parent operations never queue while holding the resource ledger mutex.
const std = @import("std");
const rpc = @import("antfly_inference_worker_rpc");
const wire = @import("antfly_inference_worker_wire");
const bridge = @import("antfly_inference_bridge");
const runtime = @import("antfly_runtime_abi");

pub const Parent = struct {
    mutex: std.Io.Mutex = .init,
    handles: std.AutoHashMapUnmanaged(u64, *Entry) = .empty,
    next_key: u64 = 1,
    const Entry = struct {
        refs: std.atomic.Value(usize) = .init(1),
        alloc: std.mem.Allocator,
        owner: runtime.runnable_abi.Owner,
        closed: *const std.atomic.Value(bool),
        cancel_requested: std.atomic.Value(bool) = .init(false),
        handle: ?*anyopaque = null,
        fn canceled(raw: ?*const anyopaque) callconv(.c) u8 {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return @intFromBool(self.closed.load(.acquire) or self.cancel_requested.load(.acquire));
        }
        fn release(self: *@This()) void {
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            if (self.handle) |handle| self.owner.finish(handle);
            self.alloc.destroy(self);
        }
    };

    /// Registry mutation is short. A callback pins its entry and releases the
    /// mutex before scheduler reentry/checkpoint, which may fairly queue.
    pub fn operate(self: *@This(), alloc: std.mem.Allocator, io: std.Io, owner: runtime.runnable_abi.Owner, closed: *const std.atomic.Value(bool), operation: wire.Operation, value: wire.LoadExecution) !u64 {
        self.mutex.lockUncancelable(io);
        var locked = true;
        defer if (locked) self.mutex.unlock(io);
        if (closed.load(.acquire)) return error.ResourceOwnerShuttingDown;
        if (operation == .execution_start) {
            if (value.key != 0 or self.next_key == std.math.maxInt(u64)) return error.InvalidGenerationAdmission;
            self.handles.ensureUnusedCapacity(alloc, 1) catch return error.ResourceTemporarilyUnavailable;
            const entry = alloc.create(Entry) catch return error.ResourceTemporarilyUnavailable;
            entry.* = .{ .alloc = alloc, .owner = owner, .closed = closed };
            errdefer entry.release();
            const borrow = runtime.native_abi.IoBorrow.init(&io);
            const status = owner.start(owner.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.inference), .slots = 1, .allow_wait = 0, .retained_bytes = value.retained_bytes, .cancellation = .{ .context = entry, .is_cancelled = Entry.canceled }, .out_handle = &entry.handle });
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
            if (entry.handle == null) return error.InvalidRunnableTransition;
            const key = self.next_key;
            self.next_key += 1;
            self.handles.putAssumeCapacity(key, entry);
            return key;
        }
        const entry = self.handles.get(value.key) orelse return error.InvalidGenerationAdmission;
        _ = entry.refs.fetchAdd(1, .monotonic);
        if (operation == .execution_finish) _ = self.handles.remove(value.key);
        self.mutex.unlock(io);
        locked = false;
        defer entry.release();
        if (operation == .execution_finish) {
            entry.release(); // Drop registry ownership; the callback pin remains.
            return 0;
        }
        const status = switch (operation) {
            .execution_park => owner.park(entry.handle.?, @backingInt(runtime.runnable_abi.Wait.resource)),
            .execution_enter => owner.enter(entry.handle.?, @backingInt(runtime.runnable_abi.Class.inference)),
            .execution_check => owner.checkpoint(entry.handle.?),
            .execution_cancel => {
                entry.cancel_requested.store(true, .release);
                return 0;
            },
            else => return error.UnsupportedOperation,
        };
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        return 0;
    }

    /// Called only after physical reap. Active callbacks retain their entries
    /// through their own final return; transport close cancels queued reentry.
    pub fn retireAfterJoin(self: *@This(), io: std.Io, alloc: std.mem.Allocator) void {
        self.mutex.lockUncancelable(io);
        var handles = self.handles;
        self.handles = .empty;
        self.mutex.unlock(io);
        var values = handles.valueIterator();
        while (values.next()) |entry| entry.*.release();
        handles.deinit(alloc);
    }
};

pub fn ChildFor(comptime execution: type, comptime broker: type, comptime pool: type) type {
    return struct {
        const Self = @This();
        endpoint: *rpc.Endpoint,
        pub fn admission(self: *@This()) broker.ExecutionAdmission {
            return .{ .context = self, .begin = begin, .finish = finish, .checkpoint = checkpoint };
        }
        const Token = struct {
            provider: *Self,
            key: u64,
            control: broker.ExecutionControl,
            cpu: pool.WorkerScope,
            scope: execution.CheckpointScope = undefined,
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try broker.ExecutionControl.check(&self.control);
                _ = try self.provider.exchange(.execution_check, .{ .key = self.key }, self);
            }
            fn lifetime(raw: ?*anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                try broker.ExecutionControl.check(&self.control);
            }
            fn cancel(raw: ?*anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                _ = try self.provider.call(.execution_cancel, .{ .key = self.key });
            }
            fn park(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try broker.ExecutionControl.check(&self.control);
                _ = try self.provider.call(.execution_park, .{ .key = self.key });
            }
            fn enter(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                while (true) {
                    try broker.ExecutionControl.check(&self.control);
                    _ = self.provider.exchange(.execution_enter, .{ .key = self.key }, self) catch |err| {
                        if (err != error.ResourceTemporarilyUnavailable and err != error.QueryQueueFull and err != error.SchedulerFull) return err;
                        try self.provider.endpoint.io.sleep(.fromMilliseconds(5), .awake);
                        continue;
                    };
                    return;
                }
            }
        };

        fn call(self: *@This(), operation: wire.Operation, value: wire.LoadExecution) !u64 {
            return self.exchange(operation, value, null);
        }
        fn exchange(self: *@This(), operation: wire.Operation, value: wire.LoadExecution, token: ?*Token) !u64 {
            var value_buffer: [128]u8 = undefined;
            var value_writer = std.Io.Writer.fixed(&value_buffer);
            try std.json.Stringify.value(value, .{}, &value_writer);
            var request_buffer: [rpc.max_resource_bytes]u8 = undefined;
            var request = std.Io.Writer.fixed(&request_buffer);
            try std.json.Stringify.value(wire.ResourceRequest{ .operation = operation, .data = value_writer.buffered() }, .{}, &request);
            // A lost reply may have acquired or retired a grant. Exit fences all
            // child computation; the parent keeps ownership until physical reap.
            const result = self.endpoint.callResourceWithControl(request.buffered(), if (token) |active| .{ .ptr = active, .check_fn = Token.lifetime } else .{}, if (token) |active| .{ .ptr = active, .check_fn = Token.cancel } else .{}) catch @import("antfly_platform").process.exitImmediately(1);
            if (result.refusal) |err| return err;
            const reply = result.reply;
            if (reply.code != 0 or reply.detail != 0) return bridge.errorFromStatus(.{ .code = reply.code, .detail = reply.detail });
            return reply.value;
        }
        fn begin(raw: *anyopaque, _: std.Io, _: broker.Key, bytes: usize, control: broker.ExecutionControl) !*anyopaque {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const alloc = self.endpoint.computeAllocator();
            const token = try alloc.create(Token);
            errdefer alloc.destroy(token);
            var original = control;
            const key = while (true) {
                try broker.ExecutionControl.check(&original);
                const key = self.call(.execution_start, .{ .retained_bytes = bytes +| @sizeOf(Token) }) catch |err| {
                    if (err != error.ResourceTemporarilyUnavailable and err != error.QueryQueueFull and err != error.SchedulerFull) return err;
                    try self.endpoint.io.sleep(.fromMilliseconds(5), .awake);
                    continue;
                };
                break key;
            };
            errdefer _ = self.call(.execution_finish, .{ .key = key }) catch @import("antfly_platform").process.exitImmediately(1);
            token.* = .{ .provider = self, .key = key, .control = control, .cpu = try pool.WorkerScope.init(1) };
            token.scope = execution.CheckpointScope.init(.{ .context = token, .check_fn = Token.check, .wait_begin_fn = Token.park, .wait_end_fn = Token.enter });
            return token;
        }
        fn checkpoint(_: *anyopaque, raw: *anyopaque) !void {
            try Token.check(raw);
        }
        fn finish(raw: *anyopaque, handle: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const token: *Token = @ptrCast(@alignCast(handle));
            token.scope.deinit();
            token.cpu.deinit();
            _ = self.call(.execution_finish, .{ .key = token.key }) catch @import("antfly_platform").process.exitImmediately(1);
            self.endpoint.computeAllocator().destroy(token);
        }
    };
}
