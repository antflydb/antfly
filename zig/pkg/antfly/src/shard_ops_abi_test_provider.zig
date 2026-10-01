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
pub const antfly_sources = @import("source_owner_physical.zig");
const shard = @import("raft/shard_ops.zig");
const actions = @import("metadata/transition_actions.zig");
const state = @import("metadata/transition_state.zig");
const errors = @import("runtime_error_abi.zig");
const Cancel = @import("common/cancellation.zig").CancellationToken;

fn check(ptr: *anyopaque, context: u64) !void {
    if (context != 42) return error.InvalidArgument;
    const failure: *const errors.Status = @ptrCast(@alignCast(ptr));
    if (!failure.isOk()) return errors.errorFromStatus(failure.*);
}
fn topology(ptr: *anyopaque, context: u64, alloc: std.mem.Allocator, group: u64, table: []const u8, request: shard.TopologyReadRequest, cancellation: Cancel) ![]u8 {
    try cancellation.check();
    try check(ptr, context);
    if (group != 17 or !std.mem.eql(u8, table, "rows") or request.transition_id != 9 or request.mode != .merge_copy_receipt) return error.InvalidArgument;
    return alloc.dupe(u8, "{\"receipt\":\"owned payload\",\"index\":9007199254740993}");
}
fn observeSplit(ptr: *anyopaque, context: u64, _: state.SplitTransitionRecord) !state.SplitObservation {
    try check(ptr, context);
    var value = std.mem.zeroes(state.SplitObservation);
    value.source_local_leader = true;
    return value;
}
fn observeMerge(ptr: *anyopaque, context: u64, _: state.MergeTransitionRecord) !state.MergeObservation {
    try check(ptr, context);
    var value = std.mem.zeroes(state.MergeObservation);
    value.donor_local_leader = true;
    return value;
}
fn operation(comptime name: []const u8) @FieldType(shard.ShardOperationAdapter.VTable, name) {
    return struct {
        fn call(ptr: *anyopaque, context: u64, _: @FieldType(actions.TransitionAction, name)) !void {
            try check(ptr, context);
        }
    }.call;
}
const vtable: shard.ShardOperationAdapter.VTable = blk: {
    var value: shard.ShardOperationAdapter.VTable = undefined;
    for (std.meta.fields(shard.ShardOperationAdapter.VTable)) |field| {
        @field(value, field.name) = if (std.mem.eql(u8, field.name, "topology_read")) topology else if (std.mem.eql(u8, field.name, "observe_split")) observeSplit else if (std.mem.eql(u8, field.name, "observe_merge")) observeMerge else operation(field.name);
    }
    break :blk value;
};
export fn shard_adapter_test_provider(out: *shard.ShardOperationAdapter, failure: *errors.Status) callconv(.c) void {
    out.* = .{ .ptr = failure, .context_id = 42, .vtable = &vtable };
}
export fn shard_adapter_test_error_ordinal(failure: errors.Status) callconv(.c) u16 {
    return @intFromError(errors.errorFromStatus(failure));
}

export fn routed_batch_test_provider(out: *@import("api/internal_group_operations.zig").RoutedRaftBatchWriter, failure: *errors.Status) callconv(.c) void {
    const Fixture = struct {
        fn write(ptr: *anyopaque, _: std.mem.Allocator, authority: @import("api/internal_group_operations.zig").RoutedBatchAuthority, group_id: u64, table: []const u8, request: @import("antfly_source_root").antfly_sources.selected_db.types.BatchRequest, forwarding: @import("api/internal_batch_forwarding.zig").Context, context: @import("api/operation.zig").RequestContext) !?void {
            try context.ensureActive();
            try check(ptr, 42);
            if (authority != .transaction or !std.mem.eql(u8, table, "rows") or request.writes.len != 1 or
                !std.mem.eql(u8, request.writes[0].key, "key") or !std.mem.eql(u8, request.writes[0].value, "{\"id\":1}") or forwarding.remaining_ms != 123) return error.InvalidArgument;
            return if (group_id == 17) {} else null;
        }
    };
    out.* = .{ .ptr = failure, .write_fn = Fixture.write };
}

export fn raft_batcher_test_provider(out: *@import("api/table_writes.zig").RaftBatcher, failure: *errors.Status) callconv(.c) void {
    const Batcher = @import("api/table_writes.zig").RaftBatcher;
    const Types = antfly_sources.selected_db.types;
    const Fixture = struct {
        fn batch(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: Types.BatchRequest) !void {
            try check(ptr, 42);
        }
        fn batchCancel(ptr: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, request: Types.BatchRequest, cancellation: Cancel) !void {
            try cancellation.check();
            try batch(ptr, alloc, group, table, request);
        }
        fn batchRouted(ptr: *anyopaque, alloc: std.mem.Allocator, fence: @import("metadata/api.zig").CatalogRouteFence, table: []const u8, request: Types.BatchRequest, cancellation: Cancel) !void {
            try fence.validate();
            try batchCancel(ptr, alloc, fence.route.group_id, table, request, cancellation);
        }
        fn batchContext(ptr: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, request: Types.BatchRequest, context: @import("api/distributed_txn.zig").PreDecisionContext) !void {
            try batchCancel(ptr, alloc, group, table, request, context.cancellation);
        }
        fn status(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: Types.TxnId) !Types.TxnStatus {
            try check(ptr, 42);
            return .committed;
        }
        fn statusUntil(ptr: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, txn: Types.TxnId, _: u64) !Types.TxnStatus {
            return status(ptr, alloc, group, table, txn);
        }
    };
    out.* = .{ .ptr = failure, .vtable = &Batcher.VTable{
        .batch_group = Fixture.batch,
        .batch_group_with_cancellation = Fixture.batchCancel,
        .batch_group_routed_with_cancellation = Fixture.batchRouted,
        .batch_group_local = Fixture.batch,
        .batch_group_local_with_cancellation = Fixture.batchCancel,
        .batch_group_local_with_pre_decision_context = Fixture.batchContext,
        .txn_status_group = Fixture.status,
        .txn_status_group_until = Fixture.statusUntil,
        .txn_status_group_local = Fixture.status,
        .txn_status_group_local_until = Fixture.statusUntil,
    } };
}
export fn shard_adapter_test_call_contract(out: *@import("runtime_native_abi.zig").CallContract) callconv(.c) void {
    const Callback = @FieldType(shard.ShardOperationAdapter.VTable, "catch_up_merge_receiver");
    const Args = std.meta.ArgsTuple(@typeInfo(Callback).pointer.child);
    out.* = @import("runtime_native_abi.zig").CallContract.of("catch_up_merge_receiver", Callback, Args, void);
}
export fn shard_adapter_test_callback_name(size: *usize) callconv(.c) [*]const u8 {
    const name = @typeName(@FieldType(shard.ShardOperationAdapter.VTable, "catch_up_merge_receiver"));
    size.* = name.len;
    return name.ptr;
}

export fn restore_persistence_test_provider(out: *@import("api/restore_jobs.zig").ReplicatedPersistence) callconv(.c) void {
    const Jobs = @import("api/restore_jobs.zig");
    const Fixture = struct {
        var marker: u8 = 0;
        fn load(_: *anyopaque, alloc: std.mem.Allocator) ![]Jobs.ReplicatedPersistence.OwnedRow {
            return alloc.alloc(Jobs.ReplicatedPersistence.OwnedRow, 0);
        }
        fn get(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !?[]u8 {
            return null;
        }
        fn put(_: *anyopaque, _: []const u8, _: []const u8, _: u64) !void {}
        fn delete(_: *anyopaque, _: []const u8, _: u64) !void {}
        fn deleteMany(_: *anyopaque, _: []const []const u8, _: u64) !void {}
        fn create(_: *anyopaque, alloc: std.mem.Allocator, key: []const u8, value: []const u8, plan: []const u8, term: u64) ![]u8 {
            if (!std.mem.eql(u8, key, "job") or !std.mem.eql(u8, plan, "full-plan") or term != 7) return error.InvalidArgument;
            return alloc.dupe(u8, value);
        }
    };
    out.* = Jobs.ReplicatedPersistence.fromLocal(&Fixture.marker, .{ .load = Fixture.load, .get = Fixture.get, .put = Fixture.put, .delete = Fixture.delete, .delete_many = Fixture.deleteMany, .create_with_staging = Fixture.create });
}
