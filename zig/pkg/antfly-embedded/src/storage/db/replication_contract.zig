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
//! distributed control. Implementations belong to the hot standby runtime; this owner
//! must never import a primary, standby, transport, or failover implementation.

const std = @import("std");
const outbox = @import("durable_outbox.zig");
const Namespace = @import("doc_identity_namespace.zig").Namespace;
const mutation_barrier_mod = @import("antfly_runtime_abi").mutation_barrier;

/// A borrowed callback capture copied with its binding. Captures may contain
/// pointers and slices, but never owned resources or pointers into themselves.
/// Copying a binding preserves its configuration without allocating or borrowing
/// a temporary adapter. Referenced objects must outlive every binding copy.
pub const BorrowedCapture = struct {
    bytes: [256]u8 align(16) = @splat(0),
    type_id: ?*const anyopaque = null,

    fn typeId(comptime T: type) *const anyopaque {
        const Tag = struct {
            const Value = T;
            var identity: u8 = 0;
        };
        return &Tag.identity;
    }

    pub fn init(comptime T: type, value: T) BorrowedCapture {
        comptime {
            if (@sizeOf(T) > 256 or @alignOf(T) > 16)
                @compileError("borrowed replication capture exceeds capacity or alignment");
        }
        var capture: BorrowedCapture = .{ .type_id = typeId(T) };
        const ptr: *T = @ptrCast(@alignCast(&capture.bytes));
        ptr.* = value;
        return capture;
    }

    pub fn read(self: *const BorrowedCapture, comptime T: type) T {
        comptime {
            if (@sizeOf(T) > 256 or @alignOf(T) > 16)
                @compileError("borrowed replication capture exceeds capacity or alignment");
        }
        std.debug.assert(self.type_id == typeId(T));
        const ptr: *const T = @ptrCast(@alignCast(&self.bytes));
        return ptr.*;
    }
};

pub const CommitRequirements = struct {
    synchronous: bool = false,
    durable_outbox: bool = false,
    preflight: bool = false,
};

pub const AsyncEffectMirror = struct {
    publisher: Publisher,
    /// Shared local mutation lease, including checkpoint capture.
    mutation_barrier: ?*mutation_barrier_mod.MutationBarrier = null,
    /// Serializes write admission, publication, and final acknowledgement.
    transition_mutex: ?*std.atomic.Mutex = null,
    requirements: CommitRequirements = .{},
    capture: BorrowedCapture = .{},

    pub fn eql(self: AsyncEffectMirror, other: AsyncEffectMirror) bool {
        return self.publisher.ptr == other.publisher.ptr and self.publisher.vtable == other.publisher.vtable and
            self.mutation_barrier == other.mutation_barrier and self.transition_mutex == other.transition_mutex and
            std.meta.eql(self.requirements, other.requirements) and self.publisher.vtable.equal_capture(self, other);
    }

    pub fn notePublished(self: AsyncEffectMirror, lsn: u64) void {
        self.publisher.vtable.note_published(self, lsn);
    }
    pub fn noteFailure(self: AsyncEffectMirror) void {
        self.publisher.vtable.note_failure(self);
    }
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
        recover: *const fn (AsyncEffectMirror, outbox.Kind, outbox.DurableReplicationOutbox, Namespace) anyerror!u64,
        preflight: *const fn (AsyncEffectMirror, bool) anyerror!void,
        complete: *const fn (AsyncEffectMirror, u64) anyerror!void,
        note_published: *const fn (AsyncEffectMirror, u64) void = ignorePublished,
        note_failure: *const fn (AsyncEffectMirror) void = ignoreFailure,
        equal_capture: *const fn (AsyncEffectMirror, AsyncEffectMirror) bool = equalEmptyCapture,

        fn equalEmptyCapture(a: AsyncEffectMirror, b: AsyncEffectMirror) bool {
            // Captured publishers must provide semantic equality.
            return a.capture.type_id == null and b.capture.type_id == null;
        }

        fn ignorePublished(_: AsyncEffectMirror, _: u64) void {}
        fn ignoreFailure(_: AsyncEffectMirror) void {}
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
    pub fn recover(self: Publisher, mirror: AsyncEffectMirror, kind: outbox.Kind, pending: outbox.DurableReplicationOutbox, namespace: Namespace) !u64 {
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
    allows_background_work: bool = true,
    pub fn check(self: BorrowedWriteGate) !void {
        try self.check_fn(self.ptr);
    }
};

/// Borrowed admission with a by-value callback capture. Equality belongs to
/// the adapter; comparing opaque bytes would compare undefined padding.
pub const CapturedWriteGate = struct {
    capture: BorrowedCapture,
    check_fn: *const fn (CapturedWriteGate) anyerror!void,
    equal_fn: *const fn (CapturedWriteGate, CapturedWriteGate) bool,
    allows_background_work: bool = true,
    pub fn check(self: CapturedWriteGate) !void {
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
        allows_background_work: *const fn (*const anyopaque) bool,
    };
    pub fn checkWrite(self: PublishedWriteState, generation: ?u64) !void {
        try self.vtable.check(self.ptr, generation);
    }
    pub fn currentGeneration(self: PublishedWriteState) u64 {
        return self.vtable.generation(self.ptr);
    }
    pub fn allowsBackgroundWork(self: PublishedWriteState) bool {
        return self.vtable.allows_background_work(self.ptr);
    }
};

pub const SharedWriteGate = struct { state: PublishedWriteState, generation: ?u64 = null };

pub const WriteGate = union(enum) {
    borrowed: BorrowedWriteGate,
    captured: CapturedWriteGate,
    shared: SharedWriteGate,

    pub fn check(self: WriteGate) !void {
        switch (self) {
            .borrowed => |gate| try gate.check(),
            .captured => |gate| try gate.check(),
            .shared => |shared| try shared.state.checkWrite(shared.generation),
        }
    }

    pub fn allowsBackgroundWork(self: WriteGate) bool {
        return switch (self) {
            .borrowed => |gate| gate.allows_background_work,
            .captured => |gate| gate.allows_background_work,
            .shared => |gate| gate.state.allowsBackgroundWork(),
        };
    }

    /// Maintenance planning needs both scheduling permission and current write
    /// admission, including an owner's pinned generation. A denial leaves work
    /// queued; mutation execution must check again under its commit barriers.
    pub fn allowsBackgroundWrite(self: WriteGate) bool {
        if (!self.allowsBackgroundWork()) return false;
        self.check() catch return false;
        return true;
    }

    pub fn currentGeneration(self: WriteGate) ?u64 {
        return switch (self) {
            .shared => |gate| gate.state.currentGeneration(),
            else => null,
        };
    }

    pub fn eql(self: WriteGate, other: WriteGate) bool {
        return switch (self) {
            .borrowed => |left| switch (other) {
                .borrowed => |right| left.ptr == right.ptr and left.check_fn == right.check_fn and left.allows_background_work == right.allows_background_work,
                else => false,
            },
            .captured => |left| switch (other) {
                .captured => |right| left.check_fn == right.check_fn and left.equal_fn == right.equal_fn and left.allows_background_work == right.allows_background_work and left.equal_fn(left, right),
                else => false,
            },
            .shared => |left| switch (other) {
                .shared => |right| left.state.ptr == right.state.ptr and left.state.vtable == right.state.vtable and left.generation == right.generation,
                else => false,
            },
        };
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
            if (generation) |expected| if (expected != self.generation) return error.StaleWriteAdmission;
            if (self.standby) return error.WriteAdmissionRejected;
        }
        fn current(ptr: *const anyopaque) u64 {
            return cast(ptr).generation;
        }
        fn allowsBackground(ptr: *const anyopaque) bool {
            return !cast(ptr).standby;
        }
        const vtable: PublishedWriteState.VTable = .{ .check = check, .generation = current, .allows_background_work = allowsBackground };
    };
    var state: State = .{};
    const gate: WriteGate = .{ .shared = .{ .state = .{ .ptr = &state, .vtable = &State.vtable } } };
    const pinned = gate.pinned();
    try pinned.check();
    try std.testing.expect(pinned.allowsBackgroundWrite());
    state.generation += 1;
    try std.testing.expectError(error.StaleWriteAdmission, pinned.check());
    try std.testing.expect(!pinned.allowsBackgroundWrite());
    try gate.check();
    try std.testing.expect(gate.allowsBackgroundWrite());
    state.standby = true;
    try std.testing.expectError(error.WriteAdmissionRejected, gate.check());
    try std.testing.expect(!gate.allowsBackgroundWork());
    try std.testing.expect(!gate.allowsBackgroundWrite());
}

pub fn requiresDurableLifecycleReplication(req: @import("types.zig").BatchRequest) bool {
    return req.artifact_catalog != null or req.online_source != null or req.restore_staging != null or req.restore_staging_scope != null or
        req.relational_topology != null or req.relational_generation_gc != null or req.split_transition != null or
        req.split_checkpoint != null or req.split_replication != null or
        req.merge_checkpoint != null or req.merge_replication != null or req.merge_proof_adoption != null;
}

test "storage.hot_standby borrowed admission controls maintenance independently of errors" {
    const Check = struct {
        fn allow(_: *const anyopaque) !void {}
        fn reject(_: *const anyopaque) !void {
            return error.WriteAdmissionRejected;
        }
    };
    var context: u8 = 0;
    const writable: WriteGate = .{ .borrowed = .{ .ptr = &context, .check_fn = Check.allow } };
    const read_only: WriteGate = .{ .borrowed = .{ .ptr = &context, .check_fn = Check.reject, .allows_background_work = false } };
    try writable.check();
    try std.testing.expect(writable.allowsBackgroundWork());
    try std.testing.expect(writable.allowsBackgroundWrite());
    try std.testing.expectError(error.WriteAdmissionRejected, read_only.check());
    try std.testing.expect(!read_only.allowsBackgroundWork());
    try std.testing.expect(!read_only.allowsBackgroundWrite());
    const denied: WriteGate = .{ .borrowed = .{ .ptr = &context, .check_fn = Check.reject } };
    try std.testing.expect(denied.allowsBackgroundWork());
    try std.testing.expect(!denied.allowsBackgroundWrite());
    try std.testing.expect(!writable.eql(read_only));
    try std.testing.expect(writable.eql(writable.pinned()));
}
