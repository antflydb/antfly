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

//! Borrowed replication integration contracts shared by local storage and
//! distributed control. Implementations belong to the HA runtime; this owner
//! must never import a primary, standby, transport, or failover implementation.

const std = @import("std");
const outbox = @import("durable_outbox.zig");
const Namespace = @import("doc_identity_namespace.zig").Namespace;
pub const policy = @import("replication_policy.zig");
const mutation_barrier_mod = @import("../hot_standby/mutation_barrier.zig");

pub const SyncWaitFn = *const fn (
    ctx: *anyopaque,
    publisher_ctx: *anyopaque,
    target_lsn: u64,
    policy: policy.SyncPolicy,
) anyerror!void;

pub const AsyncEffectMirror = struct {
    publisher: Publisher,
    /// Shared across every writer owned by one HA runtime. Mutations hold a
    /// shared lease through WAL publication and the sync durability decision;
    /// seed capture takes the exclusive lease before choosing its checkpoint.
    mutation_barrier: ?*mutation_barrier_mod.MutationBarrier = null,
    /// Serializes the final write-gate check, WAL append, and acknowledgement
    /// with a node-local promotion fence.
    transition_mutex: ?*std.atomic.Mutex = null,
    last_lsn: ?*@import("antfly_platform").atomic.Value(u64) = null,
    failure_count: ?*@import("antfly_platform").atomic.Value(u64) = null,
    sync_policy: policy.SyncPolicy = .{},
    sync_wait_ctx: ?*anyopaque = null,
    sync_wait_fn: ?SyncWaitFn = null,
    last_gate_lsn: ?*@import("antfly_platform").atomic.Value(u64) = null,
    last_gate_action: ?*std.atomic.Value(u8) = null,
    sync_reject_count: ?*@import("antfly_platform").atomic.Value(u64) = null,
    sync_wait_count: ?*@import("antfly_platform").atomic.Value(u64) = null,
    sync_degraded_count: ?*@import("antfly_platform").atomic.Value(u64) = null,
};

pub const AsyncBatchMirror = AsyncEffectMirror;
pub const AsyncMetadataMirror = AsyncEffectMirror;

/// Borrowed publisher operations used by storage. No replication runtime,
/// transport session, slot store, or fencing implementation is exposed here.
/// All referenced runtime state and callback contexts must outlive the DB.
pub const Publisher = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const Identity = struct { table_id: u64, shard_id: u64, timeline_id: u64, epoch: u64 };
    pub const VTable = struct {
        next_lsn: *const fn (*anyopaque) u64,
        identity: *const fn (*anyopaque) Identity,
        publish: *const fn (AsyncEffectMirror, outbox.Kind, []const u8, Namespace) anyerror!u64,
        recover: *const fn (AsyncEffectMirror, outbox.Kind, outbox.DurableHAOutbox, Namespace) anyerror!u64,
        preflight: *const fn (AsyncEffectMirror, bool) anyerror!void,
        complete: *const fn (AsyncEffectMirror, u64) anyerror!void,
    };

    pub fn nextLsn(self: Publisher) u64 {
        return self.vtable.next_lsn(self.ptr);
    }
    pub fn identity(self: Publisher) Identity {
        return self.vtable.identity(self.ptr);
    }
    pub fn publish(self: Publisher, mirror: AsyncEffectMirror, kind: outbox.Kind, payload: []const u8, namespace: Namespace) !u64 {
        std.debug.assert(self.ptr == mirror.publisher.ptr and self.vtable == mirror.publisher.vtable);
        return self.vtable.publish(mirror, kind, payload, namespace);
    }
    pub fn recover(self: Publisher, mirror: AsyncEffectMirror, kind: outbox.Kind, pending: outbox.DurableHAOutbox, namespace: Namespace) !u64 {
        std.debug.assert(self.ptr == mirror.publisher.ptr and self.vtable == mirror.publisher.vtable);
        return self.vtable.recover(mirror, kind, pending, namespace);
    }
    pub fn preflight(self: Publisher, mirror: AsyncEffectMirror) !void {
        std.debug.assert(self.ptr == mirror.publisher.ptr and self.vtable == mirror.publisher.vtable);
        return self.vtable.preflight(mirror, false);
    }
    /// Compiled-owner admission records every preflight decision; physical DB
    /// admission historically records only rejection. Preserve both metrics.
    pub fn preflightRecordingDecision(self: Publisher, mirror: AsyncEffectMirror) !void {
        std.debug.assert(self.ptr == mirror.publisher.ptr and self.vtable == mirror.publisher.vtable);
        return self.vtable.preflight(mirror, true);
    }
    pub fn complete(self: Publisher, mirror: AsyncEffectMirror, lsn: u64) !void {
        std.debug.assert(self.ptr == mirror.publisher.ptr and self.vtable == mirror.publisher.vtable);
        return self.vtable.complete(mirror, lsn);
    }
};

pub const BorrowedWriteGate = struct {
    ptr: *const anyopaque,
    check_fn: *const fn (*const anyopaque) anyerror!void,
    pub fn check(self: BorrowedWriteGate) !void {
        try self.check_fn(self.ptr);
    }
};

pub const FencedWriteGate = struct {
    primary: *const anyopaque,
    fence_store: *const anyopaque,
    node_id: []const u8,
    check_fn: *const fn (FencedWriteGate) anyerror!void,
    pub fn check(self: FencedWriteGate) !void {
        try self.check_fn(self);
    }
};

/// Atomically published write admission, including generation pinning. The
/// engine never dereferences the server's mutable role/progress implementation.
pub const PublishedWriteState = struct {
    ptr: *const anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        check: *const fn (*const anyopaque, ?u64) anyerror!void,
        generation: *const fn (*const anyopaque) u64,
        is_standby: *const fn (*const anyopaque) bool,
    };
    pub fn checkWrite(self: PublishedWriteState, generation: ?u64) !void {
        try self.vtable.check(self.ptr, generation);
    }
    pub fn currentGeneration(self: PublishedWriteState) u64 {
        return self.vtable.generation(self.ptr);
    }
    pub fn isStandbyRole(self: PublishedWriteState) bool {
        return self.vtable.is_standby(self.ptr);
    }
};

pub const SharedWriteGate = struct { state: PublishedWriteState, generation: ?u64 = null };

pub const WriteGate = union(enum) {
    primary: BorrowedWriteGate,
    fenced_primary: FencedWriteGate,
    standby: BorrowedWriteGate,
    shared: SharedWriteGate,

    pub fn check(self: WriteGate) !void {
        switch (self) {
            .primary, .standby => |gate| try gate.check(),
            .fenced_primary => |gate| try gate.check(),
            .shared => |shared| try shared.state.checkWrite(shared.generation),
        }
    }

    pub fn pinned(self: WriteGate) WriteGate {
        return switch (self) {
            .shared => |shared| .{ .shared = .{
                .state = shared.state,
                .generation = shared.generation orelse shared.state.currentGeneration(),
            } },
            else => self,
        };
    }
};

test "storage.hot_standby engine shared admission pins generation across role change" {
    const State = struct {
        generation: u64 = 1,
        standby: bool = false,
        fn cast(ptr: *const anyopaque) *const @This() {
            return @ptrCast(@alignCast(ptr));
        }
        fn check(ptr: *const anyopaque, generation: ?u64) !void {
            const self = cast(ptr);
            if (generation) |expected| if (expected != self.generation) return error.HAFencedPrimary;
            if (self.standby) return error.HAReadOnlyStandby;
        }
        fn current(ptr: *const anyopaque) u64 {
            return cast(ptr).generation;
        }
        fn isStandby(ptr: *const anyopaque) bool {
            return cast(ptr).standby;
        }
        const vtable: PublishedWriteState.VTable = .{ .check = check, .generation = current, .is_standby = isStandby };
    };
    var state: State = .{};
    const gate: WriteGate = .{ .shared = .{ .state = .{ .ptr = &state, .vtable = &State.vtable } } };
    const pinned = gate.pinned();
    try pinned.check();
    state.generation += 1;
    try std.testing.expectError(error.HAFencedPrimary, pinned.check());
    try gate.check();
    state.standby = true;
    try std.testing.expectError(error.HAReadOnlyStandby, gate.check());
    try std.testing.expect(gate.shared.state.isStandbyRole());
}
