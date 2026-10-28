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

//! Owned post-manifest primary WAL retirement. Backend metadata is never read
//! or mutated by physical callbacks. Publication releases its WAL gate before
//! the caller reenters Backend.mu, allowing a peer waiting under that mutex to
//! finish without a lock cycle. A generation stamp governs receipt adoption.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const io = @import("storage_io.zig");
const wal = @import("wal.zig");
const Backend = @import("../lsm_backend.zig").Backend;
pub var test_hook: ?*const fn (*Owner, admission.Port) anyerror!void = null;
pub var test_prepare_hook: ?*const fn (admission.Port) anyerror!void = null;
pub var test_retirement_hook: ?*const fn (*Owner, admission.Port) anyerror!void = null;
pub var test_abort_hook: ?*const fn (admission.Port) anyerror!void = null;

pub const Owner = struct {
    operation: ?Backend.NormalWalRetirement = null,
    grant: ?io.DurabilityResources = null,
    lock: ?Backend.WalOperationLock = null,
    pending: ?io.ControlFileReadOperation = null,
    path: [std.fs.max_path_bytes]u8 = undefined,
    path_len: usize = 0,
    root: []const u8 = &.{},
    deadline: u64 = 0,
    generation: u64 = 0,
    covered: ?u64 = null,
    stats: wal.RetentionStats = .{},
    completed: bool = false,
    retired: bool = false,

    fn read(self: *Owner, path: []const u8, original: admission.Port) !?u64 {
        const grant = if (self.operation) |*op| &op.resources.? else &self.grant.?;
        self.pending = try grant.beginControlRead(path, 1, original);
        var trace: admission.RejectionTrace = .{};
        const value = self.pending.?.waitWithAdmission(trace.bind(original).?) catch |cause| {
            if (trace.failure) |rejected| return rejected;
            self.pending.?.deinit();
            self.pending = null;
            if (cause == error.FileNotFound) return null;
            return cause;
        };
        const size = value.file_size;
        self.pending.?.deinit();
        self.pending = null;
        return size;
    }
    pub fn drive(self: *Owner, original: admission.Port) !void {
        if (comptime @import("builtin").is_test) if (test_hook) |hook| try hook(self, original);
        self.path_len = (try std.fmt.bufPrint(&self.path, "{s}/wal.log", .{self.root})).len;
        if ((try self.read(self.path[0..self.path_len], original) orelse 0) != 0) return error.ExecutionWaitUnsupported;
        self.operation = try Backend.NormalWalRetirement.prepare(self.root, self.covered);
        // start transfers only after accepting the first read. Keep tentative
        // resources and the gate separately owned through any earlier refusal.
        var op = &self.operation.?;
        op.start(self.grant.?, .{ .lock = self.lock.? }, original) catch |cause| {
            self.operation = null;
            return cause;
        };
        self.grant = null;
        self.lock = null;
        if (comptime @import("builtin").is_test) if (test_hook) |hook| try hook(self, original);
        const result = try op.waitWithAdmission(original);
        self.stats = .{ .oldest_retained_segment = result.oldest_retained_segment, .current_segment = result.current_segment, .checkpoint_covered_through_segment = result.covered_through_segment };
        var segment = @max(@as(u64, 1), result.oldest_retained_segment);
        while (segment <= result.current_segment) : (segment += 1) {
            try original.checkpoint();
            self.path_len = (try std.fmt.bufPrint(&self.path, "{s}/wal/{d:0>20}.log", .{ self.root, segment })).len;
            const size = (try self.read(self.path[0..self.path_len], original)) orelse 0;
            if (size != 0) {
                self.stats.bytes = try std.math.add(u64, self.stats.bytes, size);
                self.stats.segments += 1;
                if (segment == result.current_segment) self.stats.current_segment_bytes = size;
            }
            if (segment == std.math.maxInt(u64)) break;
        }
        if (comptime @import("builtin").is_test) if (test_hook) |hook| try hook(self, original);
        _ = try op.publishWithAdmission(original);
        self.completed = true;
    }
    pub fn abortWithAdmission(self: *Owner, original: admission.Port) !void {
        if (comptime @import("builtin").is_test) if (test_abort_hook) |hook| try hook(original);
        if (self.pending) |*pending| {
            var trace: admission.RejectionTrace = .{};
            _ = pending.waitWithAdmission(trace.bind(original).?) catch blk: {
                if (trace.failure) |rejected| return rejected;
                break :blk io.ControlFileResult{ .file_size = 0, .bytes = &.{} };
            };
            pending.deinit();
            self.pending = null;
        }
        if (self.operation) |*op| try op.abortWithAdmission(original);
        if (self.lock) |*lock| {
            lock.release();
            self.lock = null;
        }
    }
    pub fn joinAndRelease(self: *Owner) void {
        if (self.pending) |*pending| {
            pending.deinit();
            self.pending = null;
        }
        if (self.operation) |*op| op.joinAndRelease();
        if (self.lock) |*lock| {
            lock.release();
            self.lock = null;
        }
    }
    pub fn retireWithAdmission(self: *Owner, original: ?admission.Port) !void {
        if (self.retired) return;
        if (original) |port| {
            if (comptime @import("builtin").is_test) if (test_retirement_hook) |hook| try hook(self, port);
            if (!self.completed) try self.abortWithAdmission(port);
            if (self.operation) |*op| try op.retireWithAdmission(port);
            if (self.grant) |*grant| {
                try grant.retireWithAdmission(port);
                self.grant = null;
            }
        } else self.joinAndRelease();
        if (self.operation) |*op| {
            op.deinit();
            self.operation = null;
        }
        if (self.grant) |*grant| {
            grant.deinit();
            self.grant = null;
        }
        self.retired = true;
    }
};
