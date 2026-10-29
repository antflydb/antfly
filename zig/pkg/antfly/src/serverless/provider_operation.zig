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

//! One borrowed maintenance provider call. Encoding/decoding stay on CPU;
//! providers return only physical readiness loans on the calling task.
const std = @import("std");
const objectstore = @import("objectstore");
const Context = @import("antfly_local_sources").serverless_query_lake_read_context.Context;
const Cancellation = @import("antfly_cancellation").CancellationToken;

pub const Operation = struct {
    context: Context = .{},
    cancellation: Cancellation = .none,

    pub fn check(self: *const Operation) !void {
        try self.cancellation.check();
        try self.context.checkLifetime();
    }
    pub fn ready(self: *const Operation) !void {
        try self.check();
        try self.context.ensureActive();
    }
    fn canceled(raw: *const anyopaque) bool {
        fromConst(raw).check() catch return true;
        return false;
    }
    fn fromConst(raw: *const anyopaque) *const Operation {
        return @ptrCast(@alignCast(raw));
    }
    fn begin(raw: ?*anyopaque) !void {
        const self = fromConst(raw.?);
        try self.check();
        const wait = self.context.wait.?;
        try wait.begin(wait.ptr);
    }
    fn end(raw: ?*anyopaque) !void {
        const self = fromConst(raw.?);
        const wait = self.context.wait.?;
        wait.end(wait.ptr) catch |err| {
            try self.check();
            return err;
        };
        try self.check();
    }
    pub fn token(self: *const Operation) objectstore.CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = canceled, .execution_wait = if (self.context.wait != null) .{
            .ptr = @constCast(self),
            .wait_begin_fn = begin,
            .wait_end_fn = end,
        } else null };
    }
    pub fn get(self: *const Operation, client: *objectstore.Client, bucket: []const u8, key: []const u8, options: objectstore.GetOptions) !objectstore.GetResult {
        try self.ready();
        var opts = options;
        opts.cancellation = self.token();
        var result = client.getObject(bucket, key, opts) catch |err| {
            try self.check();
            return err;
        };
        errdefer result.deinit(client.allocator);
        try self.ready();
        return result;
    }
    pub fn put(self: *const Operation, client: *objectstore.Client, bucket: []const u8, key: []const u8, bytes: []const u8, options: objectstore.PutOptions) !objectstore.PutResult {
        try self.ready();
        var opts = options;
        opts.cancellation = self.token();
        var result = client.putObject(bucket, key, bytes, opts) catch |err| {
            try self.check();
            return err;
        };
        errdefer result.deinit(client.allocator);
        try self.ready();
        return result;
    }
    pub fn stat(self: *const Operation, client: *objectstore.Client, bucket: []const u8, key: []const u8) !objectstore.ObjectMetadata {
        try self.ready();
        var result = client.statObjectWithOptions(bucket, key, .{ .cancellation = self.token() }) catch |err| {
            try self.check();
            return err;
        };
        errdefer result.deinit(client.allocator);
        try self.ready();
        return result;
    }
    pub fn list(self: *const Operation, client: *objectstore.Client, bucket: []const u8, options: objectstore.ListOptions) !objectstore.ListResult {
        try self.ready();
        var opts = options;
        opts.cancellation = self.token();
        var result = client.listObjects(bucket, opts) catch |err| {
            try self.check();
            return err;
        };
        errdefer result.deinit(client.allocator);
        try self.ready();
        return result;
    }
    pub fn sleep(self: *const Operation, milliseconds: u64) !void {
        const io = self.context.io orelse return error.BackendRuntimeIoUnavailable;
        const control: @import("httpx").socket.WriteControl = self.token().execution_wait orelse .{};
        try control.withWait(std.Io.sleep, .{ io, std.Io.Duration.fromMilliseconds(@intCast(milliseconds)), std.Io.Clock.awake });
        try self.ready();
    }
};

/// Preserve an existing store lock across its read sequence. A nonwaiting
/// publication reservation supplies return CPU; disabled/busy reserves keep
/// the original CPU. Never enter an ordinary CPU queue while retaining a lock.
pub const Guard = struct {
    original: Operation,
    mutex: ?*std.atomic.Mutex = null,
    reservation: bool = false,
    parked: bool = false,

    pub fn lock(self: *Guard, mutex: *std.atomic.Mutex) !void {
        try self.original.ready();
        while (!mutex.tryLock()) try self.original.sleep(1);
        self.mutex = mutex;
        errdefer self.deinit();
        try self.original.check();
    }
    pub fn operation(self: *Guard) Operation {
        var context = self.original.context;
        context.checkpoint = null;
        context.additional_checkpoint = null;
        context.wait = .{ .ptr = self, .begin = begin, .end = end };
        return .{ .context = context, .cancellation = self.original.cancellation };
    }
    fn begin(raw: *anyopaque) !void {
        const self: *Guard = @ptrCast(@alignCast(raw));
        try self.original.check();
        if (self.original.context.execution) |execution| {
            self.parked = try execution.parkPublication();
            self.reservation = self.reservation or self.parked;
        }
    }
    fn end(raw: *anyopaque) !void {
        const self: *Guard = @ptrCast(@alignCast(raw));
        if (self.parked) {
            self.parked = false;
            self.original.context.execution.?.resumePublication(.{ .context = self, .check = checkOriginal }) catch |err| {
                try self.original.check();
                return err;
            };
        }
        try self.original.check();
    }
    fn checkOriginal(raw: *anyopaque) !void {
        try @as(*Guard, @ptrCast(@alignCast(raw))).original.check();
    }
    pub fn finish(self: *Guard) !void {
        if (self.mutex) |mutex| mutex.unlock();
        self.mutex = null;
        if (self.reservation) {
            self.reservation = false;
            try self.original.context.execution.?.finishPublication();
        }
        try self.original.ready();
    }
    pub fn deinit(self: *Guard) void {
        if (self.mutex) |mutex| mutex.unlock();
        self.mutex = null;
        if (self.reservation) {
            self.reservation = false;
            self.original.context.execution.?.finishPublication() catch {};
        }
    }
};
