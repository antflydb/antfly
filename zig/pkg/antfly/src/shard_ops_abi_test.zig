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
const errors = @import("runtime_error_abi.zig");
extern fn shard_adapter_test_provider(*shard.ShardOperationAdapter, *errors.Status) callconv(.c) void;
extern fn shard_adapter_test_error_ordinal(errors.Status) callconv(.c) u16;
extern fn shard_adapter_test_call_contract(*@import("runtime_native_abi.zig").CallContract) callconv(.c) void;
extern fn shard_adapter_test_callback_name(*usize) callconv(.c) [*]const u8;
extern fn restore_persistence_test_provider(*@import("api/restore_jobs.zig").ReplicatedPersistence) callconv(.c) void;
extern fn routed_batch_test_provider(*@import("api/internal_group_operations.zig").RoutedRaftBatchWriter, *errors.Status) callconv(.c) void;
extern fn raft_batcher_test_provider(*@import("api/table_writes.zig").RaftBatcher, *errors.Status) callconv(.c) void;

test "shard adapter archive boundary raft batcher translates every mutation and status callback" {
    var failure: errors.Status = .ok;
    var batcher: @import("api/table_writes.zig").RaftBatcher = undefined;
    raft_batcher_test_provider(&batcher, &failure);
    const alloc = std.testing.allocator;
    const fence: @import("metadata/api.zig").CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 17, .range_id = 19, .identity_namespace = .{ .table_id = 7, .shard_id = 17, .range_id = 19 } } };
    const txn: antfly_sources.selected_db.types.TxnId = @splat(3);
    var distinct = false;
    inline for (.{ error.UniqueConstraintViolation, error.ForeignKeyParentMissing, error.ForeignKeyReferenced, error.IntegrityTopologyBusy, error.TopologyChanged, error.RaftBatchWriteOutcomeUnknown, error.RetainedEffectsFull, error.VersionConflict, error.IntentConflict, error.MergePageRequired, error.InvalidResponse, error.InvalidEmbeddingDimensions }) |err| {
        failure = errors.statusFromError(err);
        distinct = distinct or shard_adapter_test_error_ordinal(failure) != @intFromError(err);
        try std.testing.expectError(err, batcher.batchGroup(alloc, 17, "rows", .{}));
        try std.testing.expectError(err, batcher.batchGroupWithCancellation(alloc, 17, "rows", .{}, .none));
        try std.testing.expectError(err, batcher.batchGroupRoutedWithCancellation(alloc, fence, "rows", .{}, .none));
        try std.testing.expectError(err, batcher.batchGroupLocal(alloc, 17, "rows", .{}));
        try std.testing.expectError(err, batcher.batchGroupLocalWithCancellation(alloc, 17, "rows", .{}, .none));
        try std.testing.expectError(err, batcher.batchGroupLocalWithPreDecisionContext(alloc, 17, "rows", .{}, .{}));
        try std.testing.expectError(err, batcher.txnStatusGroup(alloc, 17, "rows", txn));
        try std.testing.expectError(err, batcher.txnStatusGroupUntil(alloc, 17, "rows", txn, 100));
        try std.testing.expectError(err, batcher.txnStatusGroupLocal(alloc, 17, "rows", txn));
        try std.testing.expectError(err, batcher.txnStatusGroupLocalUntil(alloc, 17, "rows", txn, 100));
    }
    try std.testing.expect(distinct);
    failure = .ok;
    try batcher.batchGroup(alloc, 17, "rows", .{});
    try batcher.batchGroupWithCancellation(alloc, 17, "rows", .{}, .none);
    try batcher.batchGroupRoutedWithCancellation(alloc, fence, "rows", .{}, .none);
    try batcher.batchGroupLocal(alloc, 17, "rows", .{});
    try batcher.batchGroupLocalWithCancellation(alloc, 17, "rows", .{}, .none);
    try batcher.batchGroupLocalWithPreDecisionContext(alloc, 17, "rows", .{}, .{});
    try std.testing.expectEqual(antfly_sources.selected_db.types.TxnStatus.committed, try batcher.txnStatusGroup(alloc, 17, "rows", txn));
    try std.testing.expectEqual(antfly_sources.selected_db.types.TxnStatus.committed, try batcher.txnStatusGroupUntil(alloc, 17, "rows", txn, 100));
    try std.testing.expectEqual(antfly_sources.selected_db.types.TxnStatus.committed, try batcher.txnStatusGroupLocal(alloc, 17, "rows", txn));
    try std.testing.expectEqual(antfly_sources.selected_db.types.TxnStatus.committed, try batcher.txnStatusGroupLocalUntil(alloc, 17, "rows", txn, 100));
}

test "shard adapter archive boundary routed writes preserve deterministic errors and optional success" {
    var failure: errors.Status = .ok;
    var writer: @import("api/internal_group_operations.zig").RoutedRaftBatchWriter = undefined;
    routed_batch_test_provider(&writer, &failure);
    const request: @import("antfly_source_root").antfly_sources.selected_db.types.BatchRequest = .{ .writes = &.{.{ .key = "key", .value = "{\"id\":1}" }} };
    const forwarding: @import("api/internal_batch_forwarding.zig").Context = .{ .remaining_ms = 123, .forwards_remaining = 1, .campaign_allowed = false };
    var distinct = false;
    inline for (.{ error.UniqueConstraintViolation, error.ForeignKeyParentMissing, error.ForeignKeyReferenced, error.IntegrityTopologyBusy, error.TopologyChanged, error.RaftBatchWriteOutcomeUnknown, error.RetainedEffectsFull, error.VersionConflict, error.IntentConflict, error.MergePageRequired, error.InvalidResponse, error.InvalidEmbeddingDimensions }) |err| {
        failure = errors.statusFromError(err);
        distinct = distinct or shard_adapter_test_error_ordinal(failure) != @intFromError(err);
        try std.testing.expectError(err, writer.write(std.testing.allocator, .transaction, 17, "rows", request, forwarding, .none));
    }
    try std.testing.expect(distinct);
    failure = .ok;
    try std.testing.expect((try writer.write(std.testing.allocator, .transaction, 17, "rows", request, forwarding, .none)) != null);
    try std.testing.expect((try writer.write(std.testing.allocator, .transaction, 18, "rows", request, forwarding, .none)) == null);
    var canceled: std.atomic.Value(bool) = .init(true);
    try std.testing.expectError(error.Canceled, writer.write(std.testing.allocator, .transaction, 17, "rows", request, forwarding, .fromAtomic(&canceled)));
}

test "shard adapter archive boundary restore persistence attaches ABI5 and transports compound staging" {
    const Jobs = @import("api/restore_jobs.zig");
    var persistence: Jobs.ReplicatedPersistence = undefined;
    restore_persistence_test_provider(&persistence);
    try std.testing.expectEqual(Jobs.ReplicatedPersistence.abi_version, persistence.version);
    var store = Jobs.Store.initWithIo(std.testing.allocator, std.testing.io);
    defer store.deinit();
    try store.attachReplicated(persistence);
    const value = try persistence.createWithStaging(std.testing.allocator, "job", "compact-row", "full-plan", 7);
    defer std.testing.allocator.free(value);
    try std.testing.expectEqualStrings("compact-row", value);
    var old = persistence;
    old.version = 4;
    try std.testing.expectError(error.UnsupportedVersion, old.load(std.testing.allocator));
}

test "shard adapter archive boundary preserves busy retry and topology read errors with independent ordinals" {
    var failure: errors.Status = .ok;
    var adapter: shard.ShardOperationAdapter = undefined;
    shard_adapter_test_provider(&adapter, &failure);
    var owned = try shard.OwnedShardOperationAdapter.init(std.testing.allocator, adapter);
    defer owned.deinit();
    const retained = owned.adapter();
    const Callback = @FieldType(shard.ShardOperationAdapter.VTable, "catch_up_merge_receiver");
    const Args = std.meta.ArgsTuple(@typeInfo(Callback).pointer.child);
    const local_contract = @import("runtime_native_abi.zig").CallContract.of("catch_up_merge_receiver", Callback, Args, void);
    var provider_name_len: usize = 0;
    const provider_name = shard_adapter_test_callback_name(&provider_name_len);
    try std.testing.expectEqualStrings(@typeName(Callback), provider_name[0..provider_name_len]);
    var provider_contract: @import("runtime_native_abi.zig").CallContract = undefined;
    shard_adapter_test_call_contract(&provider_contract);
    try std.testing.expectEqualDeep(local_contract, provider_contract);
    const request: shard.TopologyReadRequest = .{ .transition_id = 9, .attempt_epoch = 3, .mode = .merge_copy_receipt };
    const action: @import("metadata/transition_actions.zig").TransitionAction = .{ .catch_up_merge_receiver = .{ .transition_id = 9, .donor_group_id = 17, .receiver_group_id = 18 } };
    const record: @import("metadata/transition_state.zig").MergeTransitionRecord = .{ .transition_id = 9, .donor_group_id = 17, .receiver_group_id = 18 };
    var different_ordinal = false;
    // A read-only catalog timeout must survive the independent archive's
    // error domain so callers can classify predecision availability safely.
    failure = errors.statusFromError(error.CatalogRoutingSnapshotTimeout);
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, retained.topologyRead(std.testing.allocator, 17, "rows", request, .none));
    for ([_]anyerror{ error.TransitionOperationBusy, error.GroupLeaderUnavailable, error.MergeTransitionNotReady, error.MergeReceiverProjectionNotReady, error.MergeSourceProjectionNotReady, error.MergeSourceProjectionAdvanced, error.StorageBusy, error.OnlineSourcePinMissing, error.Canceled, error.RelationalRewriteTypeChange, error.RelationalRewriteColumnDrop, error.RelationalRewriteRequiresRelational, error.RelationalRewriteBudgetExceeded, error.RelationalExpressionOverflow, error.RelationalExpressionDivisionByZero, error.RelationalExpressionBudgetExceeded, error.InvalidRelationalExpressionInput, error.InvalidRelationalGeneratedValue, error.GeneratedColumnRewriteRequired }) |expected| {
        failure = errors.statusFromError(expected);
        different_ordinal = different_ordinal or shard_adapter_test_error_ordinal(failure) != @intFromError(expected);
        try std.testing.expectError(expected, adapter.execute(action));
        try std.testing.expectError(expected, retained.execute(action));
        try std.testing.expectError(expected, retained.topologyRead(std.testing.allocator, 17, "rows", request, .none));
        inline for (std.meta.fields(@import("metadata/transition_actions.zig").TransitionAction)) |field| {
            if (comptime !std.mem.eql(u8, field.name, "none")) {
                const op = @unionInit(@import("metadata/transition_actions.zig").TransitionAction, field.name, std.mem.zeroes(field.type));
                try std.testing.expectError(expected, adapter.execute(op));
            }
        }
        try std.testing.expectError(expected, adapter.observeMerge(record));
        try std.testing.expectError(expected, adapter.topologyRead(std.testing.allocator, 17, "rows", request, .none));
        const runtime = adapter.metadataRuntime();
        try std.testing.expectError(expected, runtime.execute(action));
        switch (expected) {
            error.RelationalRewriteTypeChange,
            error.RelationalRewriteColumnDrop,
            error.RelationalRewriteRequiresRelational,
            error.RelationalRewriteBudgetExceeded,
            error.RelationalExpressionOverflow,
            error.RelationalExpressionDivisionByZero,
            error.RelationalExpressionBudgetExceeded,
            error.InvalidRelationalExpressionInput,
            error.InvalidRelationalGeneratedValue,
            => try std.testing.expect(@import("api/restore_source_errors.zig").permanent(errors.errorFromStatus(failure))),
            else => {},
        }
    }
    // This assertion proves the test would not pass by accidentally relying
    // on equal compilation-local error integers in the two archives.
    try std.testing.expect(different_ordinal);
    failure = .ok;
    try adapter.execute(action);
    try retained.execute(action);
    inline for (std.meta.fields(@import("metadata/transition_actions.zig").TransitionAction)) |field| {
        if (comptime !std.mem.eql(u8, field.name, "none")) {
            const op = @unionInit(@import("metadata/transition_actions.zig").TransitionAction, field.name, std.mem.zeroes(field.type));
            try adapter.execute(op);
        }
    }
    const observed = try adapter.observeMerge(record);
    try std.testing.expect(observed.donor_local_leader);
    try std.testing.expect(!observed.receiver_local_leader);
    const payload = try adapter.topologyRead(std.testing.allocator, 17, "rows", request, .none);
    defer std.testing.allocator.free(payload);
    try std.testing.expectEqualStrings("{\"receipt\":\"owned payload\",\"index\":9007199254740993}", payload);
    var canceled: std.atomic.Value(bool) = .init(true);
    try std.testing.expectError(error.Canceled, adapter.topologyRead(std.testing.allocator, 17, "rows", request, .fromAtomic(&canceled)));
}
