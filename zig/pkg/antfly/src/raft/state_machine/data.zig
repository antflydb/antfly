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
const raft_engine = @import("raft_engine");
const applied_sink_mod = @import("applied_sink.zig");
const mod = @import("mod.zig");
const completion_protocol = @import("../../common/completion_entry_protocol.zig");

pub const DataStateMachine = struct {
    /// Installed by the managed runtime before replica restoration. These
    /// callbacks borrow the retained group guard, never a catalog lookup.
    pub const CompletionSource = struct {
        ptr: *anyopaque,
        owns: *const fn (*anyopaque, u64, u64, u64, []const u8) anyerror!bool,
        progress: *const fn (*anyopaque, u64) anyerror!?raft_engine.runtime.completion_admission_iface.Progress,
        snapshot_allowed: *const fn (*anyopaque, u64) anyerror!void,
        has_backing: *const fn (*anyopaque, u64) bool,
    };
    completion_source: ?CompletionSource = null,
    alloc: std.mem.Allocator,
    applied_sink: applied_sink_mod.AppliedIndexSink,
    snapshot_builder: ?mod.SnapshotBuilder = null,
    delegate: ?raft_engine.runtime.storage_iface.StateMachine = null,

    pub fn stateMachine(self: *DataStateMachine) raft_engine.runtime.storage_iface.StateMachine {
        return .{
            .ptr = self,
            .vtable = &.{
                .prepare_snapshot = prepareSnapshot,
                .build_snapshot = buildSnapshot,
                .apply_ready = applyReady,
                .is_apply_retryable = isApplyRetryable,
                .retire_group = retireGroup,
            },
        };
    }

    fn prepareSnapshot(
        ptr: *anyopaque,
        group_id: raft_engine.core.types.GroupId,
        applied_index: raft_engine.core.types.Index,
    ) !?raft_engine.runtime.storage_iface.SnapshotSource {
        const self: *DataStateMachine = @ptrCast(@alignCast(ptr));
        try self.requireSnapshotCompatible(group_id);
        const builder = self.snapshot_builder orelse return null;
        return try builder.prepareSnapshot(group_id, applied_index);
    }

    fn buildSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: raft_engine.core.types.GroupId) !?[]u8 {
        const self: *DataStateMachine = @ptrCast(@alignCast(ptr));
        try self.requireSnapshotCompatible(group_id);
        const builder = self.snapshot_builder orelse return null;
        return try builder.buildSnapshot(alloc, group_id);
    }

    fn applyReady(
        ptr: *anyopaque,
        group_id: raft_engine.core.types.GroupId,
        snapshot: ?raft_engine.core.types.Snapshot,
        committed_entries: []const raft_engine.core.Entry,
        read_states: []const raft_engine.core.ReadState,
    ) !void {
        const self: *DataStateMachine = @ptrCast(@alignCast(ptr));
        if (snapshot != null) try self.requireSnapshotCompatible(group_id);
        var ordinary_start: usize = 0;
        var pending_snapshot = snapshot;
        for (committed_entries, 0..) |entry, position| {
            const existing_progress = if (self.completion_source) |source|
                try source.progress(source.ptr, group_id)
            else
                null;
            const covered = if (existing_progress) |progress| entry.index <= progress.index else false;
            const has_backing = if (self.completion_source) |source| source.has_backing(source.ptr, group_id) else false;
            const durable_noop = has_backing and entry.entry_type == .normal and entry.data.len == 0 and self.applied_sink.supportsDurableNoop();
            if (!covered and !durable_noop and !try self.isProtected(group_id, entry)) continue;
            // Split at every protected entry. Neither encoding nor shadow
            // projection may touch its bytes after acceptance.
            if (position > ordinary_start or pending_snapshot != null) {
                try self.applyOrdinary(group_id, pending_snapshot, committed_entries[ordinary_start..position], &.{});
                pending_snapshot = null;
            }
            if (durable_noop and !covered) {
                // Full-sync WAL authenticates this exact empty committed entry
                // and predecessor. It has no DB effect or shadow projection.
                try self.applied_sink.setDurableNoop(group_id, entry);
                if (self.delegate) |delegate|
                    try delegate.applyReady(group_id, null, committed_entries[position .. position + 1], &.{});
                ordinary_start = position + 1;
                continue;
            }
            const delegate = self.delegate orelse return error.CompletionAdmissionUnavailable;
            if (covered) {
                // Authenticate covered replay before invoking the delegate.
                // Its DATA implementation only republishes process-local read
                // progress; it must never re-plan an older JSON resolution.
                try self.applied_sink.setNativeApplied(group_id, existing_progress.?, entry);
                try delegate.applyReady(group_id, null, committed_entries[position .. position + 1], &.{});
                ordinary_start = position + 1;
                continue;
            }
            try delegate.applyReady(group_id, null, committed_entries[position .. position + 1], &.{});
            const source = self.completion_source orelse return error.CompletionAdmissionUnavailable;
            const progress = try source.progress(source.ptr, group_id) orelse return error.CompletionAdmissionUnavailable;
            // Concrete sink authenticates the permanent receipt and this exact
            // (possibly older covered) entry against the durable Raft log.
            try self.applied_sink.setNativeApplied(group_id, progress, entry);
            ordinary_start = position + 1;
        }
        if (ordinary_start < committed_entries.len or pending_snapshot != null or committed_entries.len == 0) {
            try self.applyOrdinary(group_id, pending_snapshot, committed_entries[ordinary_start..], read_states);
        } else if (read_states.len != 0) {
            if (self.delegate) |delegate| try delegate.applyReady(group_id, null, &.{}, read_states);
        }
    }

    fn isProtected(self: *DataStateMachine, group_id: u64, entry: raft_engine.core.Entry) !bool {
        if (entry.entry_type != .normal) return false;
        const owned = if (self.completion_source) |source|
            try source.owns(source.ptr, group_id, entry.term, entry.index, entry.data)
        else
            false;
        if (!owned and completion_protocol.looksLike(entry.data)) return error.CompletionAdmissionUnavailable;
        return owned;
    }

    fn requireSnapshotCompatible(self: *DataStateMachine, group_id: u64) !void {
        if (self.completion_source) |source| {
            try source.snapshot_allowed(source.ptr, group_id);
            if (try source.progress(source.ptr, group_id) != null) return error.SnapshotInstallUnsupported;
        }
    }

    fn applyOrdinary(
        self: *DataStateMachine,
        group_id: u64,
        snapshot: ?raft_engine.core.types.Snapshot,
        committed_entries: []const raft_engine.core.Entry,
        read_states: []const raft_engine.core.ReadState,
    ) !void {
        if (snapshot) |value| {
            if (self.snapshot_builder) |snapshot_builder| {
                const installed = snapshot_builder.installSnapshot(self.alloc, group_id, value.metadata.index, value.data) catch |err|
                    return normalizeDurableProjectionApplyError(err);
                if (!installed) return error.SnapshotInstallUnsupported;
            }
        }
        if (committed_entries.len > 0) {
            if (self.snapshot_builder) |snapshot_builder| {
                const payload = try mod.encodeCommittedEntries(self.alloc, committed_entries);
                defer self.alloc.free(payload);
                snapshot_builder.applyBatch(.{
                    .group_id = group_id,
                    .commit_index = committed_entries[committed_entries.len - 1].index,
                    .entries_bytes = payload,
                }) catch |err| return normalizeDurableProjectionApplyError(err);
            }
        }
        if (self.delegate) |delegate| try delegate.applyReady(group_id, snapshot, committed_entries, read_states);
        const applied_index = if (committed_entries.len > 0)
            committed_entries[committed_entries.len - 1].index
        else if (snapshot) |value|
            value.metadata.index
        else
            0;
        if (applied_index > 0) try self.applied_sink.setAppliedIndex(group_id, applied_index);
    }

    fn isApplyRetryable(_: *anyopaque, _: u64, err: anyerror) bool {
        // Both durable projection admission and the document delegate normalize
        // replay-safe capacity/owner contention to this boundary.
        return err == error.RaftApplyWriterUnavailable;
    }

    fn retireGroup(ptr: *anyopaque, group_id: raft_engine.core.types.GroupId) void {
        const self: *DataStateMachine = @ptrCast(@alignCast(ptr));
        if (self.delegate) |delegate| delegate.retireGroup(group_id);
    }
};

/// The durable data projection can reject owner creation or an atomic batch
/// while the process memory envelope is saturated. The Raft entry is already
/// committed, and both snapshot installation and batch publication are
/// retry-safe, so keep the Ready pending and let the production progress loop
/// retry after capacity returns. Other storage errors remain fatal with their
/// original identity.
fn normalizeDurableProjectionApplyError(err: anyerror) anyerror {
    return if (err == error.ResourceBudgetExceeded)
        error.RaftApplyWriterUnavailable
    else
        err;
}

test "data state machine defers durable projection resource exhaustion" {
    const FaultingBuilder = struct {
        apply_calls: usize = 0,

        fn builder(self: *@This()) mod.SnapshotBuilder {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_snapshot = buildSnapshot,
                    .apply_batch = applyBatch,
                },
            };
        }

        fn buildSnapshot(_: *anyopaque, alloc: std.mem.Allocator, _: u64) ![]u8 {
            return try alloc.dupe(u8, &.{});
        }

        fn applyBatch(ptr: *anyopaque, _: mod.ApplyBatch) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.apply_calls += 1;
            return error.ResourceBudgetExceeded;
        }
    };

    var builder = FaultingBuilder{};
    var state_machine = DataStateMachine{
        .alloc = std.testing.allocator,
        .applied_sink = applied_sink_mod.noopAppliedIndexSink(),
        .snapshot_builder = builder.builder(),
    };
    try std.testing.expectError(
        error.RaftApplyWriterUnavailable,
        state_machine.stateMachine().applyReady(17, null, &.{.{
            .term = 3,
            .index = 9,
            .data = @constCast("entry"),
        }}, &.{}),
    );
    try std.testing.expectEqual(@as(usize, 1), builder.apply_calls);
    try std.testing.expect(
        normalizeDurableProjectionApplyError(error.OutOfMemory) == error.OutOfMemory,
    );
}

test "workload admission protected DATA apply skips projection and allocation with exact receipt" {
    const Fixture = struct {
        calls: [12]u64 = @splat(0),
        count: usize = 0,
        native: ?applied_sink_mod.NativeProgress = null,
        mismatch: bool = false,
        snapshot_blocked: bool = false,
        reject_noop: bool = false,
        backing: bool = true,
        projected_index: u64 = 0,
        fn hasBacking(raw: *anyopaque, _: u64) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.backing;
        }
        fn snapshotAllowed(raw: *anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.snapshot_blocked) return error.SnapshotInstallUnsupported;
        }
        fn record(self: *@This(), value: u64) void {
            self.calls[self.count] = value;
            self.count += 1;
        }
        fn owns(_: *anyopaque, _: u64, _: u64, _: u64, bytes: []const u8) !bool {
            return std.mem.eql(u8, bytes, "AFCENTRY-owned") or std.mem.eql(u8, bytes, "resolve-owned");
        }
        fn progress(raw: *anyopaque, _: u64) !?applied_sink_mod.NativeProgress {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.native;
        }
        fn apply(raw: *anyopaque, _: u64, _: ?raft_engine.core.types.Snapshot, entries: []const raft_engine.core.Entry, _: []const raft_engine.core.ReadState) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (entries) |entry| {
                self.record(entry.index);
                if (try owns(raw, 0, entry.term, entry.index, entry.data)) {
                    self.native = .{ .term = entry.term, .index = entry.index, .payload_digest = completion_protocol.payloadDigest(entry.data) };
                    if (self.mismatch) self.native.?.payload_digest[0] ^= 1;
                }
            }
        }
        fn project(raw: *anyopaque, batch: mod.ApplyBatch) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const entries = try mod.decodeCommittedEntries(std.testing.allocator, batch.entries_bytes);
            defer std.testing.allocator.free(entries);
            for (entries) |entry| {
                try std.testing.expect(!try owns(raw, 0, entry.term, entry.index, entry.data));
                self.record(100 + entry.index);
                self.projected_index = entry.index;
            }
        }
        fn build(raw: *anyopaque, alloc: std.mem.Allocator, _: u64) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return std.fmt.allocPrint(alloc, "{d}", .{self.projected_index});
        }
        fn ordinary(raw: *anyopaque, _: u64, index: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.record(200 + index);
        }
        fn noop(raw: *anyopaque, _: u64, entry: raft_engine.core.Entry) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.reject_noop) return error.CompletionAdmissionUnavailable;
            self.record(400 + entry.index);
        }
        fn protected(raw: *anyopaque, _: u64, _: applied_sink_mod.NativeProgress, entry: raft_engine.core.Entry) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.record(300 + entry.index);
        }
    };
    var fixture = Fixture{};
    var rejecting = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var machine = DataStateMachine{
        .alloc = rejecting.allocator(),
        .completion_source = .{ .ptr = &fixture, .owns = Fixture.owns, .progress = Fixture.progress, .snapshot_allowed = Fixture.snapshotAllowed, .has_backing = Fixture.hasBacking },
        .applied_sink = .{ .ptr = &fixture, .vtable = &.{ .set_applied_index = Fixture.ordinary, .set_native_applied = Fixture.protected, .set_durable_noop = Fixture.noop } },
        .delegate = .{ .ptr = &fixture, .vtable = &.{ .apply_ready = Fixture.apply } },
        .snapshot_builder = .{ .ptr = &fixture, .vtable = &.{ .build_snapshot = Fixture.build, .apply_batch = Fixture.project } },
    };
    const noop_entry: raft_engine.core.Entry = .{ .term = 2, .index = 1, .data = @constCast("") };
    try machine.stateMachine().applyReady(1, null, &.{noop_entry}, &.{});
    try std.testing.expectEqualSlices(u64, &.{ 401, 1 }, fixture.calls[0..fixture.count]);
    try std.testing.expectEqual(@as(usize, 0), rejecting.alloc_index);
    fixture = .{ .reject_noop = true };
    try std.testing.expectError(error.CompletionAdmissionUnavailable, machine.stateMachine().applyReady(1, null, &.{noop_entry}, &.{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.count);
    fixture = .{ .backing = false };
    machine.alloc = std.testing.allocator;
    try machine.stateMachine().applyReady(1, null, &.{noop_entry}, &.{});
    try std.testing.expectEqualSlices(u64, &.{ 101, 1, 201 }, fixture.calls[0..fixture.count]);
    const legacy_snapshot = (try machine.stateMachine().buildSnapshot(std.testing.allocator, 1)).?;
    defer std.testing.allocator.free(legacy_snapshot);
    try std.testing.expectEqualStrings("1", legacy_snapshot);
    fixture = .{};
    machine.alloc = rejecting.allocator();
    fixture.snapshot_blocked = true;
    try std.testing.expectError(error.SnapshotInstallUnsupported, machine.stateMachine().buildSnapshot(std.testing.allocator, 1));
    fixture.snapshot_blocked = false;
    const protected_entry: raft_engine.core.Entry = .{ .term = 2, .index = 2, .data = @constCast("AFCENTRY-owned") };
    try machine.stateMachine().applyReady(1, null, &.{protected_entry}, &.{});
    try std.testing.expectEqualSlices(u64, &.{ 2, 302 }, fixture.calls[0..fixture.count]);
    try std.testing.expectEqual(@as(usize, 0), rejecting.alloc_index);
    try std.testing.expectError(error.SnapshotInstallUnsupported, machine.stateMachine().buildSnapshot(std.testing.allocator, 1));
    fixture = .{};
    machine.alloc = std.testing.allocator;
    try machine.stateMachine().applyReady(1, null, &.{
        .{ .term = 2, .index = 1, .data = @constCast("ordinary") },
        protected_entry,
        .{ .term = 2, .index = 3, .data = @constCast("resolve-owned") },
        .{ .term = 2, .index = 4, .data = @constCast("ordinary-after") },
    }, &.{});
    try std.testing.expectEqualSlices(u64, &.{ 101, 1, 201, 2, 302, 3, 303, 104, 4, 204 }, fixture.calls[0..fixture.count]);
    // A permanent newer receipt covers an old JSON resolution even when the
    // retained-cell classifier no longer recognizes it. Verify the durable
    // prefix first and never feed it through the ordinary shadow projection.
    fixture.count = 0;
    try machine.stateMachine().applyReady(1, null, &.{.{ .term = 2, .index = 1, .data = @constCast("old-json-resolution") }}, &.{});
    try std.testing.expectEqualSlices(u64, &.{ 301, 1 }, fixture.calls[0..fixture.count]);
    fixture = .{ .mismatch = true };
    try std.testing.expectError(error.CompletionAdmissionUnavailable, machine.stateMachine().applyReady(1, null, &.{protected_entry}, &.{}));
    try std.testing.expectEqualSlices(u64, &.{2}, fixture.calls[0..fixture.count]);
    fixture = .{};
    machine.completion_source = null;
    try std.testing.expectError(error.CompletionAdmissionUnavailable, machine.stateMachine().applyReady(1, null, &.{protected_entry}, &.{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.count);
}
