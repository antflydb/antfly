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

//! Retained queue-to-ledger handoff. The coordinator must fence every other
//! backend writer, observer and maintenance operation until retirement, and
//! establish the backend's observed memory baseline before initialization.
//! The caller prepays this fixed owner header and retains its manager/allocator.
//! No Backend.mu is held across admission. This only adopts deletion intent;
//! the normal manifest publication must make that intent durable before delete.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const resources = @import("../resource_manager.zig");
const cleanup = @import("output_cleanup.zig");
const Ledger = @import("obsolete_ledger.zig").Ledger;
const Account = @import("memory_account.zig").Account;

pub fn Consumer(comptime Backend: type) type {
    return struct {
        backend: *Backend,
        ticket: ?*cleanup.Ticket = null,
        candidate: ?Ledger = null,
        path: ?[]u8 = null,
        metadata: ?Backend.MetadataCredit = null,
        credit: ?resources.ConstructionCredit = null,
        reclaimer: ?Ledger.Reclaimer = null,
        meter: ?*Account,
        baseline_bytes: u64,
        baseline_spares: u64,
        baseline_usage: u64,
        bound: ?u64 = null,
        phase: enum { take, prepare, path, copy, edit, adopt, publish, complete } = .take,
        adopted: bool = false,
        published: bool = false,
        retiring: bool = false,
        retired: bool = false,
        retirement_usage: ?u64 = null,

        pub fn init(backend: *Backend) @This() {
            const meter = backend.obsolete_paths.tree.account;
            return .{
                .backend = backend,
                .meter = if (meter) |a| a.retain() else null,
                .baseline_bytes = if (meter) |a| a.bytes.load(.acquire) else 0,
                .baseline_spares = backend.obsolete_paths.spareBytes(),
                .baseline_usage = backend.tracked_in_memory_state_bytes,
            };
        }

        fn pinMeter(self: *@This()) void {
            if (self.meter == null) if (self.candidate) |*candidate| {
                if (candidate.tree.account) |a| self.meter = a.retain();
            };
        }

        pub fn resumeWithAdmission(self: *@This(), original: admission.Port) !void {
            if (self.retiring or self.retired) return error.InvalidArgument;
            const b = self.backend;
            const queue = b.options.unpublished_outputs orelse return error.ExecutionWaitUnsupported;
            const manager = b.options.resource_manager orelse return error.ExecutionWaitUnsupported;
            // Queue and observer must belong to the same accounting domain.
            if (queue.manager != manager) return error.ResourceAccountingMismatch;
            while (self.phase != .complete) {
                try original.checkpoint();
                switch (self.phase) {
                    .take => {
                        self.ticket = try queue.popWithAdmission(original);
                        if (self.ticket == null) {
                            self.phase = .complete;
                            continue;
                        }
                        self.phase = .prepare;
                    },
                    .prepare => {
                        _ = try cleanup.Creation.memoryBound(self.ticket.?.path.len);
                        if (b.obsolete_paths.contains(self.ticket.?.path)) {
                            self.phase = .complete;
                            continue;
                        }
                        if (self.metadata == null) self.metadata = try b.admitCompactionMetadataBytes(32 + self.ticket.?.path.len);
                        if (self.candidate == null) self.candidate = b.obsolete_paths.fork();
                        if (self.bound == null) self.bound = try std.math.add(u64, try self.candidate.?.prepareMemoryBound(1), self.ticket.?.path.len);
                        try manager.prepareUsageWithAdmission(.lsm_in_memory_state, &b.tracked_in_memory_state_bytes, original);
                        if (self.credit == null) self.credit = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, self.bound.?, original);
                        self.candidate.?.prepareWithAdmission(b.allocator, original) catch |cause| {
                            self.pinMeter();
                            return cause;
                        };
                        self.pinMeter();
                        self.phase = .path;
                    },
                    .path => {
                        self.path = try b.allocator.alloc(u8, self.ticket.?.path.len);
                        self.phase = .copy;
                    },
                    .copy => {
                        @memcpy(self.path.?, self.ticket.?.path);
                        self.phase = .edit;
                    },
                    .edit => {
                        self.candidate.?.appendAssumeCapacity(.{ .path = self.path.?, .delete_after_ns = b.nowNs() });
                        self.path = null;
                        self.phase = .adopt;
                    },
                    .adopt => {
                        const added = self.meter.?.bytes.load(.acquire) - self.baseline_bytes + self.candidate.?.spareBytes();
                        std.debug.assert(added <= self.bound.?);
                        try self.credit.?.adoptUsageWithAdmission(.lsm_in_memory_state, &b.tracked_in_memory_state_bytes, added, original);
                        self.adopted = true;
                        self.phase = .publish;
                    },
                    .publish => {
                        // Fixed ownership receipts only; no failure after swap.
                        self.reclaimer = .init(b.obsolete_paths);
                        b.obsolete_paths = self.candidate.?;
                        self.candidate = null;
                        b.obsolete_manifest_dirty = true;
                        b.output_cleanup_retry_at_ns = 0;
                        b.obsolete_reclaim_retry_at_ns = 0;
                        self.metadata.?.commit();
                        self.metadata = null;
                        self.published = true;
                        self.phase = .complete;
                    },
                    .complete => unreachable,
                }
            }
        }

        /// Completed preparation commits a successful/duplicate handoff. Before
        /// completion retirement abandons the private edit and requeues intent.
        /// Every rejected queue/accounting step retains its remaining owner.
        pub fn retireWithAdmission(self: *@This(), original: admission.Port) !void {
            try original.checkpoint();
            if (self.retired) return;
            self.retiring = true;
            const b = self.backend;
            if (self.ticket) |ticket| {
                if (self.phase == .complete) try ticket.retireWithAdmission(original) else try ticket.abandonWithAdmission(original);
                self.ticket = null;
            }
            if (self.metadata) |*metadata| metadata.release();
            self.metadata = null;
            if (self.path) |path| {
                try original.checkpoint();
                b.allocator.free(path);
                self.path = null;
            }
            self.pinMeter();
            if (self.candidate) |candidate| {
                self.reclaimer = .init(candidate);
                self.candidate = null;
            }
            if (self.reclaimer) |*reclaimer| {
                while (true) {
                    try original.checkpoint();
                    var steps: usize = 1;
                    if (reclaimer.step(b.allocator, &steps)) break;
                }
                self.reclaimer = null;
            }
            if (self.retirement_usage == null) {
                const bytes = if (b.obsolete_paths.tree.account) |a| a.bytes.load(.acquire) else 0;
                self.retirement_usage = self.baseline_usage - self.baseline_bytes - self.baseline_spares + bytes + b.obsolete_paths.spareBytes();
                if (self.meter) |a| a.release();
                self.meter = null;
            }
            if (self.adopted) try b.options.resource_manager.?.adjustUsageWithAdmission(.lsm_in_memory_state, &b.tracked_in_memory_state_bytes, self.retirement_usage.?, original);
            if (self.credit) |*credit| credit.retire();
            self.credit = null;
            self.retired = true;
        }

        /// Only after the coordinator has joined all uses and quiesced the
        /// backend. Scoped callers must retain this object and retry instead.
        pub fn deinit(self: *@This()) void {
            if (self.retired) return;
            const b = self.backend;
            if (self.ticket) |ticket| {
                if (self.phase == .complete) ticket.destroy() else ticket.abandon();
                self.ticket = null;
            }
            if (self.metadata) |*metadata| metadata.release();
            self.metadata = null;
            if (self.path) |path| b.allocator.free(path);
            self.path = null;
            self.pinMeter();
            if (self.candidate) |candidate| self.reclaimer = .init(candidate);
            self.candidate = null;
            if (self.reclaimer) |*reclaimer| {
                var steps: usize = std.math.maxInt(usize);
                std.debug.assert(reclaimer.step(b.allocator, &steps));
            }
            self.reclaimer = null;
            if (self.retirement_usage == null) {
                const bytes = if (b.obsolete_paths.tree.account) |a| a.bytes.load(.acquire) else 0;
                self.retirement_usage = self.baseline_usage - self.baseline_bytes - self.baseline_spares + bytes + b.obsolete_paths.spareBytes();
                if (self.meter) |a| a.release();
                self.meter = null;
            }
            if (self.adopted) std.debug.assert(b.options.resource_manager.?.tryObserveUsage(.lsm_in_memory_state, &b.tracked_in_memory_state_bytes, self.retirement_usage.?));
            if (self.credit) |*credit| credit.retire();
            self.credit = null;
            self.retired = true;
        }
    };
}
