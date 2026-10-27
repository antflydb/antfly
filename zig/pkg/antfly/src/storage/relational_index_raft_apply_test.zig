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

//! Real two-store apply qualification, not a metadata or consensus certificate.
const std = @import("std");
const local = @import("antfly_local_sources");
const db_mod = local.storage_db_selected_root.db;
const controls = local.storage_db_relational_index_raft_control;
const batch = @import("../data/raft_batch.zig");
const projection = @import("../data/storage/raft_apply_store.zig");
const state = @import("../raft/state_machine/data.zig");
const adapter = @import("server_db_adapter.zig");
const raft_engine = @import("raft_engine");
const a = std.testing.allocator;

pub fn run(command: controls.Command) !void {
    const raw = command.inputs_b64 orelse return error.InvalidPrivateIndexInputs;
    const bytes = try a.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(raw));
    defer a.free(bytes);
    try std.base64.standard.Decoder.decode(bytes, raw);
    const manifest = try local.storage_db_relational_index_inputs.decode(bytes);
    var directory = try local.common_test_directory.TestDirectory.init("raft-index-two-store");
    defer directory.cleanup();
    const native_path = try std.fmt.allocPrint(a, "{s}/native", .{directory.path()});
    defer a.free(native_path);
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .online_source_authority = .raft, .identity_namespace = command.binding.fence.namespace, .primary_backend = .{ .lsm = .{} } };
    var db = try db_mod.DB.open(a, native_path, options);
    var db_live = true;
    defer if (db_live) db.close();
    try db.setSchemaJson(a, manifest.before_json);
    try db.core.store.putBatch(&.{.{ .key = local.storage_db_relational_integrity_catalog.key, .value = manifest.predecessor }}, &.{});
    const Driver = struct {
        database: *db_mod.DB,
        applied: u64 = 0,
        controls: usize = 0,
        before: bool = false,
        after: bool = false,
        fn set(ptr: *anyopaque, _: u64, index: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.applied = index;
        }
        fn apply(ptr: *anyopaque, group: u64, _: ?raft_engine.core.types.Snapshot, entries: []const raft_engine.core.Entry, _: []const raft_engine.core.ReadState) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            for (entries) |entry| {
                var decoded = try batch.decode(a, entry.data);
                defer decoded.deinit(a);
                if (decoded.protocol_barrier_version != null) continue;
                self.controls += 1;
                if (self.before) {
                    self.before = false;
                    return error.RaftApplyWriterUnavailable;
                }
                try adapter.applyOrderedGroup(self.database, group, decoded.batch.req, .{ .term = entry.term, .index = entry.index });
                if (self.after) {
                    self.after = false;
                    return error.RaftApplyWriterUnavailable;
                }
            }
        }
        fn machine(self: *@This(), store: *projection.RaftApplyStore) state.DataStateMachine {
            return .{
                .alloc = a,
                .applied_sink = .{ .ptr = self, .vtable = &.{ .set_applied_index = set } },
                .snapshot_builder = store.snapshotBuilder(),
                .delegate = .{ .ptr = self, .vtable = &.{ .apply_ready = apply } },
            };
        }
    };
    var driver: Driver = .{ .database = &db };
    const group = command.binding.fence.owner_group_id;
    const control_bytes = try batch.encode(a, "docs", .{ .relational_index_owner_control = .{ .control = command } });
    defer a.free(control_bytes);
    {
        const unmounted_path = try std.fmt.allocPrint(a, "{s}/unmounted", .{directory.path()});
        defer a.free(unmounted_path);
        var unmounted = try projection.RaftApplyStore.init(a, .{ .root_dir = unmounted_path, .native_source_delegate = true });
        defer unmounted.deinit();
        var machine = driver.machine(&unmounted);
        machine.delegate = null;
        try std.testing.expectError(error.StorageKernelOwnerUnavailable, machine.stateMachine().applyReady(group, null, &.{.{ .term = 7, .index = 1, .entry_type = .normal, .data = control_bytes }}, &.{}));
        try std.testing.expect(try unmounted.latestBatch(group) == null);
        try std.testing.expectEqual(@as(u64, 0), driver.applied);
        try std.testing.expectEqual(@as(usize, 0), driver.controls);
        try std.testing.expect(try db.orderedApplyReceipt() == null);
    }
    const Cases = struct { version: ?u16, native: bool = true, wrong_group: bool = false, failure: anyerror };
    for ([_]Cases{
        .{ .version = null, .failure = error.UnsupportedBatchProtocolVersion },
        .{ .version = batch.index_owner_control_protocol_version - 1, .failure = error.UnsupportedBatchProtocolVersion },
        .{ .version = batch.index_owner_control_protocol_version, .native = false, .failure = error.StorageKernelOwnerUnavailable },
        .{ .version = batch.index_owner_control_protocol_version, .wrong_group = true, .failure = error.PrivateIndexOwnerJobChanged },
    }, 0..) |case, ordinal| {
        const path = try std.fmt.allocPrint(a, "{s}/negative-{}", .{ directory.path(), ordinal });
        defer a.free(path);
        var store = try projection.RaftApplyStore.init(a, .{ .root_dir = path, .native_source_delegate = case.native });
        defer store.deinit();
        driver.applied = 0;
        driver.controls = 0;
        var machine = driver.machine(&store);
        const actual_group = if (case.wrong_group) group + 1 else group;
        if (case.version) |version| {
            const barrier = try batch.encodeProtocolBarrier(a, "docs", version);
            defer a.free(barrier);
            try machine.stateMachine().applyReady(actual_group, null, &.{.{ .term = 7, .index = 1, .entry_type = .normal, .data = barrier }}, &.{});
        }
        try std.testing.expectError(case.failure, machine.stateMachine().applyReady(actual_group, null, &.{.{ .term = 7, .index = if (case.version != null) 2 else 1, .entry_type = .normal, .data = control_bytes }}, &.{}));
        try std.testing.expectEqual(@as(usize, 0), driver.controls);
        try std.testing.expectEqual(@as(u64, if (case.version != null) 1 else 0), driver.applied);
        try std.testing.expect(try db.orderedApplyReceipt() == null);
    }
    const path = try std.fmt.allocPrint(a, "{s}/positive", .{directory.path()});
    defer a.free(path);
    var store = try projection.RaftApplyStore.init(a, .{ .root_dir = path, .native_source_delegate = true });
    var store_live = true;
    defer if (store_live) store.deinit();
    driver.applied = 0;
    driver.controls = 0;
    var machine = driver.machine(&store);
    const barrier = try batch.encodeProtocolBarrier(a, "docs", batch.index_owner_control_protocol_version);
    defer a.free(barrier);
    try machine.stateMachine().applyReady(group, null, &.{.{ .term = 7, .index = 1, .entry_type = .normal, .data = barrier }}, &.{});
    driver.before = true;
    driver.after = true;
    const entries = [_]raft_engine.core.Entry{.{ .term = 7, .index = 2, .entry_type = .normal, .data = control_bytes }};
    try std.testing.expectError(error.RaftApplyWriterUnavailable, machine.stateMachine().applyReady(group, null, &entries, &.{}));
    try std.testing.expectEqual(@as(u64, 1), driver.applied);
    try std.testing.expect(try db.orderedApplyReceipt() == null);
    try std.testing.expectError(error.RaftApplyWriterUnavailable, machine.stateMachine().applyReady(group, null, &entries, &.{}));
    try std.testing.expectEqual(@as(u64, 1), driver.applied);
    try std.testing.expectEqual(@as(u64, 2), (try db.orderedApplyReceipt()).?.index);
    db.close();
    db_live = false;
    store.deinit();
    store_live = false;
    db = try db_mod.DB.open(a, native_path, options);
    db_live = true;
    store = try projection.RaftApplyStore.init(a, .{ .root_dir = path, .native_source_delegate = true });
    store_live = true;
    machine = driver.machine(&store);
    try machine.stateMachine().applyReady(group, null, &entries, &.{});
    try std.testing.expectEqual(@as(u64, 2), driver.applied);
    for ([_]controls.Effect{ .fenced, .reserved }, 0..) |effect, ordinal| {
        const payload = try batch.encode(a, "docs", .{ .relational_index_owner_control = .{ .control = .{ .effect = effect, .binding = command.binding } } });
        defer a.free(payload);
        try machine.stateMachine().applyReady(group, null, &.{.{ .term = 8, .index = ordinal + 3, .entry_type = .normal, .data = payload }}, &.{});
        const receipt = (try db.raftIndexOwnerReceipt(group, command.binding, effect)).?;
        try std.testing.expect(receipt.position == .raft);
        try std.testing.expectEqual(@as(u64, 8), receipt.position.raft.term);
        try std.testing.expectEqual(@as(u64, ordinal + 3), receipt.position.raft.index);
        try std.testing.expectEqual(@as(u64, ordinal + 3), driver.applied);
    }
    const reserved = (try db.raftIndexOwnerReceipt(group, command.binding, .reserved)).?;
    const retry = try batch.encode(a, "docs", .{ .relational_index_owner_control = .{ .control = .{ .effect = .reserved, .binding = command.binding } } });
    defer a.free(retry);
    try machine.stateMachine().applyReady(group, null, &.{.{ .term = 9, .index = 5, .entry_type = .normal, .data = retry }}, &.{});
    try std.testing.expectEqual(@as(u64, 5), driver.applied);
    try std.testing.expectEqualDeep(reserved, (try db.raftIndexOwnerReceipt(group, command.binding, .reserved)).?);
    const owners = local.storage_db_relational_index_owner_control;
    const jobs = local.storage_db_relational_index_owner_job;
    const build_payload = blk: {
        var reservation = try db.prepareRaftIndexOwnerControl(.{ .effect = .reserved, .binding = command.binding });
        defer reservation.deinit();
        var page = (try owners.PreparedBuild.prepareRaft(a, null, db.core, "docs", &reservation, .{ .records = 1, .time_ns = std.time.ns_per_s }, .{})).?;
        defer page.deinit();
        break :blk try batch.encode(a, "docs", .{ .relational_index_owner_control = .{ .build = page.command } });
    };
    defer a.free(build_payload);
    const build_entry = [_]raft_engine.core.Entry{.{ .term = 9, .index = 6, .entry_type = .normal, .data = build_payload }};
    driver.before = true;
    try std.testing.expectError(error.RaftApplyWriterUnavailable, machine.stateMachine().applyReady(group, null, &build_entry, &.{}));
    try std.testing.expectEqual(@as(u64, 5), (try db.orderedApplyReceipt()).?.index);
    driver.after = true;
    try std.testing.expectError(error.RaftApplyWriterUnavailable, machine.stateMachine().applyReady(group, null, &build_entry, &.{}));
    try std.testing.expectEqual(@as(u64, 6), (try db.orderedApplyReceipt()).?.index);
    try std.testing.expectEqual(@as(u64, 5), driver.applied);
    db.close();
    db_live = false;
    store.deinit();
    store_live = false;
    db = try db_mod.DB.open(a, native_path, options);
    db_live = true;
    store = try projection.RaftApplyStore.init(a, .{ .root_dir = path, .native_source_delegate = true });
    store_live = true;
    machine = driver.machine(&store);
    try machine.stateMachine().applyReady(group, null, &build_entry, &.{});
    try std.testing.expectEqual(@as(u64, 6), driver.applied);
    const binding = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const current = (try jobs.load(&read)).?;
        try owners.PreparedBuild.verifyRecovery(&read, current);
        const head = (try jobs.private_claims.SourceStream.load(&read, current)).?;
        try std.testing.expectEqual(@as(u64, 6), head.last_sequence);
        try std.testing.expectEqual(@as(u64, 1), head.pending());
        break :blk current;
    };
    const cancel_payload = try batch.encode(a, "docs", .{ .relational_index_owner_control = .{ .cancel_intent = controls.Admission.fromBinding(binding) } });
    defer a.free(cancel_payload);
    const cancel_entry = [_]raft_engine.core.Entry{.{ .term = 10, .index = 7, .entry_type = .normal, .data = cancel_payload }};
    driver.after = true;
    try std.testing.expectError(error.RaftApplyWriterUnavailable, machine.stateMachine().applyReady(group, null, &cancel_entry, &.{}));
    try std.testing.expectEqual(@as(u64, 7), (try db.orderedApplyReceipt()).?.index);
    try std.testing.expectEqual(@as(u64, 6), driver.applied);
    try machine.stateMachine().applyReady(group, null, &cancel_entry, &.{});
    const cleanup_payload = try batch.encode(a, "docs", .{ .relational_index_owner_control = .{ .cancel_cleanup = binding } });
    defer a.free(cleanup_payload);
    try machine.stateMachine().applyReady(group, null, &.{.{ .term = 10, .index = 8, .entry_type = .normal, .data = cleanup_payload }}, &.{});
    try std.testing.expectEqual(@as(u64, 8), driver.applied);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 8), (try jobs.requireCancellationCleanup(&read, binding)).raft.index);
        try jobs.private_claims.SourceStream.requireCanceledRetired(&read, binding);
    }
    var gc_index: u64 = 8;
    while (true) {
        const payload = blk: {
            var page = (try local.storage_db_relational_index_gc.Page.prepareGeneration(a, null, db.core, binding.index)) orelse break;
            defer page.deinit();
            break :blk try batch.encode(a, "docs", .{ .relational_index_owner_control = .{ .cancel_artifacts = try page.command(binding) } });
        };
        defer a.free(payload);
        gc_index += 1;
        try std.testing.expect(gc_index <= 10);
        const entry = [_]raft_engine.core.Entry{.{ .term = 10, .index = gc_index, .entry_type = .normal, .data = payload }};
        if (gc_index == 9) {
            driver.after = true;
            try std.testing.expectError(error.RaftApplyWriterUnavailable, machine.stateMachine().applyReady(group, null, &entry, &.{}));
            try std.testing.expectEqual(@as(u64, 9), (try db.orderedApplyReceipt()).?.index);
            try std.testing.expectEqual(@as(u64, 8), driver.applied);
            db.close();
            db_live = false;
            store.deinit();
            store_live = false;
            db = try db_mod.DB.open(a, native_path, options);
            db_live = true;
            store = try projection.RaftApplyStore.init(a, .{ .root_dir = path, .native_source_delegate = true });
            store_live = true;
            machine = driver.machine(&store);
        }
        try machine.stateMachine().applyReady(group, null, &entry, &.{});
        try std.testing.expectEqual(gc_index, driver.applied);
    }
    try std.testing.expectEqual(@as(u64, 10), gc_index);
    const complete_payload = try batch.encode(a, "docs", .{ .relational_index_owner_control = .{ .cancel_complete = binding } });
    defer a.free(complete_payload);
    try machine.stateMachine().applyReady(group, null, &.{.{ .term = 11, .index = 11, .entry_type = .normal, .data = complete_payload }}, &.{});
    const canceled = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        break :blk (try jobs.Canceled.load(&read)).?;
    };
    const intent: @FieldType(owners.CancellationReleaseOutcome, "intent") = .{ .canceled = canceled, .metadata_decision = @splat(12) };
    const release_payload = try batch.encode(a, "docs", .{ .relational_index_owner_control = .{ .cancel_release = intent } });
    defer a.free(release_payload);
    const release_entry = [_]raft_engine.core.Entry{.{ .term = 12, .index = 12, .entry_type = .normal, .data = release_payload }};
    driver.after = true;
    try std.testing.expectError(error.RaftApplyWriterUnavailable, machine.stateMachine().applyReady(group, null, &release_entry, &.{}));
    try std.testing.expectEqual(@as(u64, 11), driver.applied);
    db.close();
    db_live = false;
    store.deinit();
    store_live = false;
    db = try db_mod.DB.open(a, native_path, options);
    db_live = true;
    store = try projection.RaftApplyStore.init(a, .{ .root_dir = path, .native_source_delegate = true });
    store_live = true;
    machine = driver.machine(&store);
    try machine.stateMachine().applyReady(group, null, &release_entry, &.{});
    try std.testing.expectEqual(@as(u64, 12), driver.applied);
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expect(try jobs.load(&read) == null);
    try local.storage_db_relational_integrity_topology.requireUnfenced(&read);
}
