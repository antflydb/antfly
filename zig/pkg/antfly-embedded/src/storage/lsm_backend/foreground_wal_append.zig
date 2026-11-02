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

//! A foreground commit borrows only the caller's explicit lendable execution.
//! Backend metadata adoption occurs after the exact accepted IO is joined and
//! its WAL gate released. Publication or rejection transfers staged input to
//! this owner before the producer returns and releases its transaction.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const io = @import("storage_io.zig");
const wal = @import("wal.zig");
const state = @import("state.zig");
const Backend = @import("../lsm_backend.zig").Backend;
pub var test_hook: ?*const fn (*Owner, admission.Port) anyerror!void = null;
pub var test_retire_hook: ?*const fn (*Owner, admission.Port) anyerror!void = null;
pub var test_abort_hook: ?*const fn (admission.Port) anyerror!void = null;
pub const Publication = struct {
    owner: *Owner,
    lock: Backend.WalOperationLock,
    pub fn publish(self: *@This(), result: wal.AppendResult) void {
        self.owner.result = result;
        self.owner.published = true;
    }
    pub fn requireRecovery(self: *@This()) void {
        self.owner.recovery = true;
    }
    pub fn release(self: *@This()) void {
        self.lock.release();
    }
};
pub const Operation = wal.CurrentStateAppend(Publication);
pub const Owner = struct {
    cpu: state.ActiveMemTable.PublicationPreparation = undefined,
    cpu_live: bool = false,
    input_index: ?@FieldType(state.ActiveMemTable, "index").Reclaimer = null,
    input_ordered: ?@FieldType(state.ActiveMemTable, "ordered").Reclaimer = null,
    record: []u8 = &.{},
    record_retired: usize = 0,
    grant: ?io.DurabilityResources = null,
    lock: ?Backend.WalOperationLock = null,
    operation: ?Operation = null,
    result: wal.AppendResult = .{},
    retention: ?wal.AppendRetentionPolicy = null,
    retention_generation: u64 = 0,
    published: bool = false,
    recovery: bool = false,
    retired: bool = false,
    pub fn prepare(self: *@This(), backend: *Backend, incoming: *const state.ActiveMemTable, credit: anytype, original: admission.Port) !void {
        self.cpu = .initBorrowed(&backend.mutable, incoming);
        self.cpu_live = true;
        const record_len = try wal.stateRecordLenWithAdmission(incoming, original);
        if (backend.options.wal_hard_limit_bytes != 0 and record_len > backend.options.wal_hard_limit_bytes) return error.WalRecordTooLarge;
        const bound = try std.math.add(u64, try self.cpu.memoryBoundWithAdmission(original), try std.math.add(u64, record_len, try backend.storage.?.appendResourceBytes(record_len)));
        const boundary = try original.physicalWaitAfterLifetime();
        // Lending callers release CPU. Guarded publication keeps its original
        // CPU and may refuse funding, without waiting for another runnable.
        if (boundary) |lending| try lending.beginWait();
        const funded = credit.growBoundedOversized(bound, 1);
        if (boundary) |lending| try lending.endWait();
        try funded;
        try self.cpu.resumeWithAdmission(backend.allocator, original);
        self.record = try wal.prepareStateRecordWithAdmission(backend.allocator, incoming, original);
        self.grant = (try backend.storage.?.tryPrepareAppendWithAdmission(self.record, original)) orelse return error.ResourceBudgetExceeded;
        if (comptime @import("builtin").is_test) if (test_hook) |hook| try hook(self, original);
    }
    pub fn drive(self: *@This(), backend: *Backend, original: admission.Port) !void {
        self.operation = try Operation.prepare(backend.root_dir.?, self.record.len, .{ .segment_bytes = backend.options.wal_segment_bytes });
        self.operation.?.sync_on_commit = backend.options.wal_sync_on_commit;
        self.operation.?.retention = self.retention;
        self.operation.?.start(self.grant.?, .{ .owner = self, .lock = self.lock.? }, original) catch |cause| {
            self.operation = null;
            return cause;
        };
        self.grant = null;
        self.lock = null;
        if (comptime @import("builtin").is_test) if (test_hook) |hook| try hook(self, original);
        _ = try self.operation.?.publishWithAdmission(original);
    }
    pub fn abortWithAdmission(self: *@This(), original: admission.Port) !void {
        if (comptime @import("builtin").is_test) if (test_abort_hook) |hook| try hook(original);
        if (self.operation) |*operation| try operation.abortWithAdmission(original);
        if (self.lock) |*lock| {
            lock.release();
            self.lock = null;
        }
    }
    pub fn join(self: *@This()) void {
        if (self.operation) |*operation| operation.joinAndRelease();
        if (self.lock) |*lock| {
            lock.release();
            self.lock = null;
        }
    }
    pub fn takeInput(self: *@This(), incoming: *state.ActiveMemTable) void {
        std.debug.assert(self.cpu_live and self.input_index == null and self.input_ordered == null);
        self.cpu.source = .{ .entries = incoming.entries, .arena_owner = incoming.arena_owner };
        self.cpu.source_owned = true;
        self.input_index = .init(incoming.index);
        self.input_ordered = .init(incoming.ordered);
        incoming.* = .{ .ordered_enabled = false };
    }
    pub fn retireWithAdmission(self: *@This(), a: std.mem.Allocator, original: ?admission.Port) !void {
        if (self.retired) return;
        if (self.operation) |*operation| {
            if (original) |port| {
                try operation.abortWithAdmission(port);
                try operation.retireWithAdmission(port);
            }
            operation.deinit();
            self.operation = null;
        }
        if (self.grant) |*grant| {
            if (original) |port| try grant.retireWithAdmission(port) else grant.deinit();
            self.grant = null;
        }
        if (self.input_ordered) |*index| {
            while (true) {
                if (original) |port| {
                    try port.checkpoint();
                    if (comptime @import("builtin").is_test) if (test_retire_hook) |hook| try hook(self, port);
                }
                var credits: usize = 1;
                if (index.step(a, &credits)) break;
            }
            self.input_ordered = null;
        }
        if (self.cpu_live) {
            if (original) |port| try self.cpu.retireWithAdmission(a, port) else self.cpu.deinit(a);
            self.cpu_live = false;
        }
        if (self.input_index) |*index| {
            while (true) {
                if (original) |port| {
                    try port.checkpoint();
                    if (comptime @import("builtin").is_test) if (test_retire_hook) |hook| try hook(self, port);
                }
                var credits: usize = 1;
                if (index.step(a, &credits)) break;
            }
            self.input_index = null;
        }
        while (self.record_retired < self.record.len) {
            if (original) |port| try port.checkpoint();
            const end = @min(self.record.len, self.record_retired + 16 * 1024);
            @memset(self.record[self.record_retired..end], undefined);
            self.record_retired = end;
        }
        if (self.record.len != 0) a.rawFree(self.record, .of(u8), @returnAddress());
        self.record = &.{};
        self.retired = true;
    }
};
