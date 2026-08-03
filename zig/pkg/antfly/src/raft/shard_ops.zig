// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const builtin = @import("builtin");
const metadata_actions = @import("../metadata/transition_actions.zig");
const metadata_driver = @import("../metadata/transition_driver.zig");
const metadata_state = @import("../metadata/transition_state.zig");

const PrepareSplitSource = std.meta.fieldInfo(metadata_actions.TransitionAction, .prepare_split_source).type;
const StartSplitSource = std.meta.fieldInfo(metadata_actions.TransitionAction, .start_split_source).type;
const BootstrapSplitDestination = std.meta.fieldInfo(metadata_actions.TransitionAction, .bootstrap_split_destination).type;
const CatchUpSplitDestination = std.meta.fieldInfo(metadata_actions.TransitionAction, .catch_up_split_destination).type;
const FinalizeSplitSource = std.meta.fieldInfo(metadata_actions.TransitionAction, .finalize_split_source).type;
const RollbackSplit = std.meta.fieldInfo(metadata_actions.TransitionAction, .rollback_split).type;
const AcceptMergeReceiver = std.meta.fieldInfo(metadata_actions.TransitionAction, .accept_merge_receiver).type;
const CatchUpMergeReceiver = std.meta.fieldInfo(metadata_actions.TransitionAction, .catch_up_merge_receiver).type;
const FinalizeMerge = std.meta.fieldInfo(metadata_actions.TransitionAction, .finalize_merge).type;
const RollbackMerge = std.meta.fieldInfo(metadata_actions.TransitionAction, .rollback_merge).type;

pub const ShardOperationAdapter = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    context_id: u64 = 0,

    pub const VTable = struct {
        observe_split: *const fn (ptr: *anyopaque, context_id: u64, record: metadata_state.SplitTransitionRecord) anyerror!metadata_state.SplitObservation,
        observe_merge: *const fn (ptr: *anyopaque, context_id: u64, record: metadata_state.MergeTransitionRecord) anyerror!metadata_state.MergeObservation,
        prepare_split_source: *const fn (ptr: *anyopaque, context_id: u64, op: PrepareSplitSource) anyerror!void,
        start_split_source: *const fn (ptr: *anyopaque, context_id: u64, op: StartSplitSource) anyerror!void,
        bootstrap_split_destination: *const fn (ptr: *anyopaque, context_id: u64, op: BootstrapSplitDestination) anyerror!void,
        catch_up_split_destination: *const fn (ptr: *anyopaque, context_id: u64, op: CatchUpSplitDestination) anyerror!void,
        finalize_split_source: *const fn (ptr: *anyopaque, context_id: u64, op: FinalizeSplitSource) anyerror!void,
        rollback_split: *const fn (ptr: *anyopaque, context_id: u64, op: RollbackSplit) anyerror!void,
        accept_merge_receiver: *const fn (ptr: *anyopaque, context_id: u64, op: AcceptMergeReceiver) anyerror!void,
        catch_up_merge_receiver: *const fn (ptr: *anyopaque, context_id: u64, op: CatchUpMergeReceiver) anyerror!void,
        finalize_merge: *const fn (ptr: *anyopaque, context_id: u64, op: FinalizeMerge) anyerror!void,
        rollback_merge: *const fn (ptr: *anyopaque, context_id: u64, op: RollbackMerge) anyerror!void,
    };

    pub fn observeSplit(self: ShardOperationAdapter, record: metadata_state.SplitTransitionRecord) !metadata_state.SplitObservation {
        return try self.vtable.observe_split(self.ptr, self.context_id, record);
    }

    pub fn observeMerge(self: ShardOperationAdapter, record: metadata_state.MergeTransitionRecord) !metadata_state.MergeObservation {
        return try self.vtable.observe_merge(self.ptr, self.context_id, record);
    }

    pub fn execute(self: ShardOperationAdapter, action: metadata_actions.TransitionAction) !void {
        switch (action) {
            .none => {},
            .prepare_split_source => |op| try self.vtable.prepare_split_source(self.ptr, self.context_id, op),
            .start_split_source => |op| try self.vtable.start_split_source(self.ptr, self.context_id, op),
            .bootstrap_split_destination => |op| try self.vtable.bootstrap_split_destination(self.ptr, self.context_id, op),
            .catch_up_split_destination => |op| try self.vtable.catch_up_split_destination(self.ptr, self.context_id, op),
            .finalize_split_source => |op| try self.vtable.finalize_split_source(self.ptr, self.context_id, op),
            .rollback_split => |op| try self.vtable.rollback_split(self.ptr, self.context_id, op),
            .accept_merge_receiver => |op| try self.vtable.accept_merge_receiver(self.ptr, self.context_id, op),
            .catch_up_merge_receiver => |op| try self.vtable.catch_up_merge_receiver(self.ptr, self.context_id, op),
            .finalize_merge => |op| try self.vtable.finalize_merge(self.ptr, self.context_id, op),
            .rollback_merge => |op| try self.vtable.rollback_merge(self.ptr, self.context_id, op),
        }
    }

    pub fn metadataRuntime(self: *const ShardOperationAdapter) metadata_driver.TransitionRuntime {
        return .{
            .ptr = @constCast(self),
            .vtable = &.{
                .observe_split = observeSplitMeta,
                .observe_merge = observeMergeMeta,
                .execute = executeMeta,
            },
        };
    }

    fn observeSplitMeta(ptr: *anyopaque, record: metadata_state.SplitTransitionRecord) !metadata_state.SplitObservation {
        const self: *const ShardOperationAdapter = @ptrCast(@alignCast(ptr));
        return try self.observeSplit(record);
    }

    fn observeMergeMeta(ptr: *anyopaque, record: metadata_state.MergeTransitionRecord) !metadata_state.MergeObservation {
        const self: *const ShardOperationAdapter = @ptrCast(@alignCast(ptr));
        return try self.observeMerge(record);
    }

    fn executeMeta(ptr: *anyopaque, action: metadata_actions.TransitionAction) !void {
        const self: *const ShardOperationAdapter = @ptrCast(@alignCast(ptr));
        try self.execute(action);
    }
};

/// Retains and drains a borrowed shard-operation adapter. The adapter's
/// concrete owner must retain a Registration for exactly as long as its
/// callback state is alive; retiring that registration closes admission and
/// waits for callbacks already in flight.
pub const OwnedShardOperationAdapter = struct {
    const registry_bucket_count = 64;

    const State = struct {
        alloc: std.mem.Allocator,
        downstream: ShardOperationAdapter,
        context_id: u64,
        registry_next: ?*State = null,
        registered: bool = false,
        mutex: std.Io.Mutex = .init,
        drained: std.Io.Condition = .init,
        accepting: bool = true,
        active_calls: usize = 0,
        references: usize = 1,

        fn lock(self: *State) void {
            self.mutex.lockUncancelable(std.Options.debug_io);
        }

        fn unlock(self: *State) void {
            self.mutex.unlock(std.Options.debug_io);
        }

        fn retain(self: *State) void {
            self.lock();
            defer self.unlock();
            self.references = std.math.add(usize, self.references, 1) catch
                @panic("shard operation adapter reference overflow");
        }

        fn release(self: *State) void {
            self.lock();
            std.debug.assert(self.references > 0);
            self.references -= 1;
            const destroy = self.references == 0;
            self.unlock();
            if (destroy) {
                const alloc = self.alloc;
                self.* = undefined;
                alloc.destroy(self);
            }
        }

        fn retireAndDrain(self: *State) void {
            const bucket = registryBucket(self.context_id);
            bucket.lock();
            self.lock();
            if (self.registered) {
                var link = &bucket.head;
                while (link.*) |candidate| {
                    if (candidate == self) {
                        link.* = candidate.registry_next;
                        break;
                    }
                    link = &candidate.registry_next;
                }
                self.registry_next = null;
                self.registered = false;
            }
            // Removing the state from the registry closes admission before the
            // active-call drain. A callback that was dispatched but had not yet
            // acquired its lease now fails through the registry without ever
            // dereferencing this state.
            self.accepting = false;
            bucket.unlock();
            defer self.unlock();
            while (self.active_calls != 0) {
                self.drained.waitUncancelable(std.Options.debug_io, &self.mutex);
            }
        }
    };

    const RegistryBucket = struct {
        mutex: std.Io.Mutex = .init,
        head: ?*State = null,

        fn lock(self: *RegistryBucket) void {
            self.mutex.lockUncancelable(std.Options.debug_io);
        }

        fn unlock(self: *RegistryBucket) void {
            self.mutex.unlock(std.Options.debug_io);
        }
    };

    var registry_buckets: [registry_bucket_count]RegistryBucket =
        [_]RegistryBucket{.{}} ** registry_bucket_count;
    var next_context_id: std.atomic.Value(u64) = .init(1);

    const AdmissionTest = if (builtin.is_test) struct {
        const Gate = struct {
            mutex: std.Io.Mutex = .init,
            changed: std.Io.Condition = .init,
            entered: bool = false,
            released: bool = false,

            fn waitUntilEntered(self: *Gate) void {
                self.mutex.lockUncancelable(std.Options.debug_io);
                defer self.mutex.unlock(std.Options.debug_io);
                while (!self.entered) {
                    self.changed.waitUncancelable(std.Options.debug_io, &self.mutex);
                }
            }

            fn release(self: *Gate) void {
                self.mutex.lockUncancelable(std.Options.debug_io);
                defer self.mutex.unlock(std.Options.debug_io);
                self.released = true;
                self.changed.broadcast(std.Options.debug_io);
            }
        };

        var gate: ?*Gate = null;

        fn pauseBeforeAcquire() void {
            const active = gate orelse return;
            active.mutex.lockUncancelable(std.Options.debug_io);
            defer active.mutex.unlock(std.Options.debug_io);
            active.entered = true;
            active.changed.broadcast(std.Options.debug_io);
            while (!active.released) {
                active.changed.waitUncancelable(std.Options.debug_io, &active.mutex);
            }
        }
    } else struct {
        fn pauseBeforeAcquire() void {}
    };

    const CallLease = struct {
        state: *State,

        fn deinit(self: *CallLease) void {
            const state = self.state;
            state.lock();
            std.debug.assert(state.active_calls > 0);
            state.active_calls -= 1;
            if (state.active_calls == 0) state.drained.broadcast(std.Options.debug_io);
            state.unlock();
            self.* = undefined;
        }
    };

    fn registryBucket(context_id: u64) *RegistryBucket {
        return &registry_buckets[@intCast(context_id % registry_bucket_count)];
    }

    fn allocateContextId() u64 {
        const context_id = next_context_id.fetchAdd(1, .monotonic);
        if (context_id == 0 or context_id == std.math.maxInt(u64)) {
            @panic("shard operation adapter context id exhausted");
        }
        return context_id;
    }

    fn registerState(state: *State) void {
        const bucket = registryBucket(state.context_id);
        bucket.lock();
        defer bucket.unlock();
        state.registry_next = bucket.head;
        state.registered = true;
        bucket.head = state;
    }

    fn acquireRegistered(ptr: *anyopaque, context_id: u64) !CallLease {
        AdmissionTest.pauseBeforeAcquire();
        if (context_id == 0) return error.TransitionOperationsRetired;
        const bucket = registryBucket(context_id);
        bucket.lock();
        defer bucket.unlock();

        const target_address = @intFromPtr(ptr);
        var candidate = bucket.head;
        while (candidate) |state| : (candidate = state.registry_next) {
            if (state.context_id != context_id or @intFromPtr(state) != target_address) continue;
            state.lock();
            defer state.unlock();
            if (!state.accepting) return error.TransitionOperationsRetired;
            state.active_calls = std.math.add(usize, state.active_calls, 1) catch
                return error.TransitionOperationLeaseOverflow;
            return .{ .state = state };
        }
        return error.TransitionOperationsRetired;
    }

    pub const Registration = struct {
        state: ?*State,

        pub fn deinit(self: *Registration) void {
            const state = self.state orelse return;
            self.state = null;
            state.retireAndDrain();
            state.release();
        }
    };

    state: ?*State,

    pub fn init(
        alloc: std.mem.Allocator,
        downstream: ShardOperationAdapter,
    ) !OwnedShardOperationAdapter {
        const state = try alloc.create(State);
        state.* = .{
            .alloc = alloc,
            .downstream = downstream,
            .context_id = allocateContextId(),
        };
        registerState(state);
        return .{ .state = state };
    }

    pub fn deinit(self: *OwnedShardOperationAdapter) void {
        const state = self.state orelse return;
        self.state = null;
        state.retireAndDrain();
        state.release();
    }

    pub fn registration(self: *OwnedShardOperationAdapter) Registration {
        const state = self.state orelse @panic("retired shard operation adapter");
        state.retain();
        return .{ .state = state };
    }

    pub fn adapter(self: *OwnedShardOperationAdapter) ShardOperationAdapter {
        const state = self.state orelse @panic("retired shard operation adapter");
        return .{
            .ptr = state,
            .context_id = state.context_id,
            .vtable = &.{
                .observe_split = observeSplit,
                .observe_merge = observeMerge,
                .prepare_split_source = prepareSplitSource,
                .start_split_source = startSplitSource,
                .bootstrap_split_destination = bootstrapSplitDestination,
                .catch_up_split_destination = catchUpSplitDestination,
                .finalize_split_source = finalizeSplitSource,
                .rollback_split = rollbackSplit,
                .accept_merge_receiver = acceptMergeReceiver,
                .catch_up_merge_receiver = catchUpMergeReceiver,
                .finalize_merge = finalizeMerge,
                .rollback_merge = rollbackMerge,
            },
        };
    }

    fn observeSplit(
        ptr: *anyopaque,
        context_id: u64,
        record: metadata_state.SplitTransitionRecord,
    ) !metadata_state.SplitObservation {
        var lease = try acquireRegistered(ptr, context_id);
        defer lease.deinit();
        return try lease.state.downstream.observeSplit(record);
    }

    fn observeMerge(
        ptr: *anyopaque,
        context_id: u64,
        record: metadata_state.MergeTransitionRecord,
    ) !metadata_state.MergeObservation {
        var lease = try acquireRegistered(ptr, context_id);
        defer lease.deinit();
        return try lease.state.downstream.observeMerge(record);
    }

    fn prepareSplitSource(ptr: *anyopaque, context_id: u64, op: PrepareSplitSource) !void {
        var lease = try acquireRegistered(ptr, context_id);
        defer lease.deinit();
        const downstream = lease.state.downstream;
        try downstream.vtable.prepare_split_source(downstream.ptr, downstream.context_id, op);
    }

    fn startSplitSource(ptr: *anyopaque, context_id: u64, op: StartSplitSource) !void {
        var lease = try acquireRegistered(ptr, context_id);
        defer lease.deinit();
        const downstream = lease.state.downstream;
        try downstream.vtable.start_split_source(downstream.ptr, downstream.context_id, op);
    }

    fn bootstrapSplitDestination(ptr: *anyopaque, context_id: u64, op: BootstrapSplitDestination) !void {
        var lease = try acquireRegistered(ptr, context_id);
        defer lease.deinit();
        const downstream = lease.state.downstream;
        try downstream.vtable.bootstrap_split_destination(downstream.ptr, downstream.context_id, op);
    }

    fn catchUpSplitDestination(ptr: *anyopaque, context_id: u64, op: CatchUpSplitDestination) !void {
        var lease = try acquireRegistered(ptr, context_id);
        defer lease.deinit();
        const downstream = lease.state.downstream;
        try downstream.vtable.catch_up_split_destination(downstream.ptr, downstream.context_id, op);
    }

    fn finalizeSplitSource(ptr: *anyopaque, context_id: u64, op: FinalizeSplitSource) !void {
        var lease = try acquireRegistered(ptr, context_id);
        defer lease.deinit();
        const downstream = lease.state.downstream;
        try downstream.vtable.finalize_split_source(downstream.ptr, downstream.context_id, op);
    }

    fn rollbackSplit(ptr: *anyopaque, context_id: u64, op: RollbackSplit) !void {
        var lease = try acquireRegistered(ptr, context_id);
        defer lease.deinit();
        const downstream = lease.state.downstream;
        try downstream.vtable.rollback_split(downstream.ptr, downstream.context_id, op);
    }

    fn acceptMergeReceiver(ptr: *anyopaque, context_id: u64, op: AcceptMergeReceiver) !void {
        var lease = try acquireRegistered(ptr, context_id);
        defer lease.deinit();
        const downstream = lease.state.downstream;
        try downstream.vtable.accept_merge_receiver(downstream.ptr, downstream.context_id, op);
    }

    fn catchUpMergeReceiver(ptr: *anyopaque, context_id: u64, op: CatchUpMergeReceiver) !void {
        var lease = try acquireRegistered(ptr, context_id);
        defer lease.deinit();
        const downstream = lease.state.downstream;
        try downstream.vtable.catch_up_merge_receiver(downstream.ptr, downstream.context_id, op);
    }

    fn finalizeMerge(ptr: *anyopaque, context_id: u64, op: FinalizeMerge) !void {
        var lease = try acquireRegistered(ptr, context_id);
        defer lease.deinit();
        const downstream = lease.state.downstream;
        try downstream.vtable.finalize_merge(downstream.ptr, downstream.context_id, op);
    }

    fn rollbackMerge(ptr: *anyopaque, context_id: u64, op: RollbackMerge) !void {
        var lease = try acquireRegistered(ptr, context_id);
        defer lease.deinit();
        const downstream = lease.state.downstream;
        try downstream.vtable.rollback_merge(downstream.ptr, downstream.context_id, op);
    }
};

test "shard operation adapter metadata runtime dispatches actions" {
    const Fake = struct {
        split_prepared: bool = false,

        fn adapter(self: *@This()) ShardOperationAdapter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_split = observeSplit,
                    .observe_merge = observeMerge,
                    .prepare_split_source = prepareSplitSource,
                    .start_split_source = noopStartSplitSource,
                    .bootstrap_split_destination = noopBootstrapSplitDestination,
                    .catch_up_split_destination = noopCatchUpSplitDestination,
                    .finalize_split_source = noopFinalizeSplitSource,
                    .rollback_split = noopRollbackSplit,
                    .accept_merge_receiver = noopAcceptMergeReceiver,
                    .catch_up_merge_receiver = noopCatchUpMergeReceiver,
                    .finalize_merge = noopFinalizeMerge,
                    .rollback_merge = noopRollbackMerge,
                },
            };
        }

        fn observeSplit(_: *anyopaque, _: u64, _: metadata_state.SplitTransitionRecord) !metadata_state.SplitObservation {
            return .{
                .status = .{
                    .phase = .prepare,
                    .source_split_phase = .prepare,
                    .bootstrapped = false,
                    .replay_required = false,
                    .replay_caught_up = false,
                    .cutover_ready = false,
                    .destination_ready_for_reads = false,
                    .source_delta_sequence = 0,
                    .dest_delta_sequence = 0,
                },
            };
        }

        fn observeMerge(_: *anyopaque, _: u64, record: metadata_state.MergeTransitionRecord) !metadata_state.MergeObservation {
            const status = @import("../data/mod.zig").MergeTransitionStatus{
                .phase = .prepare,
                .donor_group_id = record.donor_group_id,
                .receiver_group_id = record.receiver_group_id,
                .receiver_accepts_donor_range = false,
                .bootstrapped = false,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = false,
                .receiver_ready_for_reads = false,
                .donor_delta_sequence = 0,
                .receiver_delta_sequence = 0,
            };
            return .{ .donor = status, .receiver = status };
        }

        fn prepareSplitSource(ptr: *anyopaque, _: u64, _: PrepareSplitSource) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.split_prepared = true;
        }

        fn noopStartSplitSource(_: *anyopaque, _: u64, _: StartSplitSource) !void {}
        fn noopBootstrapSplitDestination(_: *anyopaque, _: u64, _: BootstrapSplitDestination) !void {}
        fn noopCatchUpSplitDestination(_: *anyopaque, _: u64, _: CatchUpSplitDestination) !void {}
        fn noopFinalizeSplitSource(_: *anyopaque, _: u64, _: FinalizeSplitSource) !void {}
        fn noopRollbackSplit(_: *anyopaque, _: u64, _: RollbackSplit) !void {}
        fn noopAcceptMergeReceiver(_: *anyopaque, _: u64, _: AcceptMergeReceiver) !void {}
        fn noopCatchUpMergeReceiver(_: *anyopaque, _: u64, _: CatchUpMergeReceiver) !void {}
        fn noopFinalizeMerge(_: *anyopaque, _: u64, _: FinalizeMerge) !void {}
        fn noopRollbackMerge(_: *anyopaque, _: u64, _: RollbackMerge) !void {}
    };

    var fake = Fake{};
    var adapter = fake.adapter();
    const runtime = adapter.metadataRuntime();

    _ = try runtime.observeSplit(.{
        .transition_id = 1,
        .attempt_epoch = 1,
        .source_group_id = 10,
        .destination_group_id = 11,
    });
    try runtime.execute(.{
        .prepare_split_source = .{
            .transition_id = 1,
            .attempt_epoch = 1,
            .source_group_id = 10,
            .destination_group_id = 11,
            .split_key = "doc:m",
        },
    });
    try std.testing.expect(fake.split_prepared);

    var owned = try OwnedShardOperationAdapter.init(std.testing.allocator, fake.adapter());
    defer owned.deinit();
    var registration = owned.registration();
    const guarded = owned.adapter();
    _ = try guarded.observeSplit(.{
        .transition_id = 2,
        .attempt_epoch = 1,
        .source_group_id = 20,
        .destination_group_id = 21,
    });
    registration.deinit();
    try std.testing.expectError(
        error.TransitionOperationsRetired,
        guarded.observeSplit(.{
            .transition_id = 2,
            .attempt_epoch = 1,
            .source_group_id = 20,
            .destination_group_id = 21,
        }),
    );

    // Plain adapter values may outlive their owner. They must fail through the
    // registry without dereferencing state that has already been destroyed.
    owned.deinit();
    var replacement = try OwnedShardOperationAdapter.init(std.testing.allocator, fake.adapter());
    defer replacement.deinit();
    try std.testing.expect(guarded.context_id != replacement.adapter().context_id);
    try std.testing.expectError(
        error.TransitionOperationsRetired,
        guarded.observeSplit(.{
            .transition_id = 2,
            .attempt_epoch = 1,
            .source_group_id = 20,
            .destination_group_id = 21,
        }),
    );

    // Exercise the exact pre-admission window: the callback has been
    // dispatched, but teardown retires and frees its state before it can look
    // up a lease. The stale pointer must only be compared as an address.
    const Worker = struct {
        const Result = enum {
            pending,
            succeeded,
            retired,
            unexpected_error,
        };

        adapter: ShardOperationAdapter,
        result: Result = .pending,

        fn run(self: *@This()) void {
            _ = self.adapter.observeSplit(.{
                .transition_id = 3,
                .attempt_epoch = 1,
                .source_group_id = 30,
                .destination_group_id = 31,
            }) catch |err| {
                self.result = if (err == error.TransitionOperationsRetired)
                    .retired
                else
                    .unexpected_error;
                return;
            };
            self.result = .succeeded;
        }
    };

    var concurrent_owner = try OwnedShardOperationAdapter.init(
        std.testing.allocator,
        fake.adapter(),
    );
    defer concurrent_owner.deinit();
    var concurrent_registration = concurrent_owner.registration();
    defer concurrent_registration.deinit();
    var gate = OwnedShardOperationAdapter.AdmissionTest.Gate{};
    OwnedShardOperationAdapter.AdmissionTest.gate = &gate;
    defer OwnedShardOperationAdapter.AdmissionTest.gate = null;
    var worker = Worker{ .adapter = concurrent_owner.adapter() };
    var thread: ?std.Thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
    defer if (thread) |pending_thread| {
        gate.release();
        pending_thread.join();
    };

    gate.waitUntilEntered();
    concurrent_registration.deinit();
    concurrent_owner.deinit();
    gate.release();
    const joined_thread = thread.?;
    thread = null;
    joined_thread.join();
    try std.testing.expectEqual(Worker.Result.retired, worker.result);
}
