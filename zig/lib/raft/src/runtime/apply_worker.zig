// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const core = @import("../core/mod.zig");
const storage_iface = @import("storage_iface.zig");

const ApplyTask = struct {
    group_id: core.types.GroupId,
    snapshot: ?core.types.Snapshot,
    entries: []core.Entry,
    read_states: []core.ReadState,

    fn deinit(self: *ApplyTask, alloc: std.mem.Allocator) void {
        if (self.snapshot) |*snapshot| snapshot.deinit(alloc);
        core.types.freeEntries(alloc, self.entries);
        for (self.read_states) |*read_state| read_state.deinit(alloc);
        if (self.read_states.len > 0) alloc.free(self.read_states);
        self.* = undefined;
    }
};

pub const QueuedApplyWorker = struct {
    alloc: std.mem.Allocator,
    state_machine: storage_iface.StateMachine,
    tasks: std.ArrayListUnmanaged(ApplyTask) = .empty,

    pub fn init(alloc: std.mem.Allocator, state_machine: storage_iface.StateMachine) QueuedApplyWorker {
        return .{
            .alloc = alloc,
            .state_machine = state_machine,
        };
    }

    pub fn deinit(self: *QueuedApplyWorker) void {
        for (self.tasks.items) |*task| task.deinit(self.alloc);
        self.tasks.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn queue(self: *QueuedApplyWorker) storage_iface.ApplyQueue {
        return .{
            .ptr = self,
            .vtable = &.{
                .enqueue_apply = enqueueApply,
                .drain = drain,
                .abort = abort,
            },
        };
    }

    fn enqueueApply(
        ptr: *anyopaque,
        group_id: core.types.GroupId,
        snapshot: ?core.types.Snapshot,
        committed_entries: []const core.Entry,
        read_states: []const core.ReadState,
    ) !void {
        const self: *QueuedApplyWorker = @ptrCast(@alignCast(ptr));
        var cloned_read_states = try self.alloc.alloc(core.ReadState, read_states.len);
        var cloned_read_state_count: usize = 0;
        var read_states_owned = true;
        errdefer if (read_states_owned) {
            for (cloned_read_states[0..cloned_read_state_count]) |*read_state| read_state.deinit(self.alloc);
            if (cloned_read_states.len > 0) self.alloc.free(cloned_read_states);
        };
        for (read_states, 0..) |read_state, i| {
            cloned_read_states[i] = try read_state.clone(self.alloc);
            cloned_read_state_count += 1;
        }

        var cloned_snapshot = if (snapshot) |value| try value.clone(self.alloc) else null;
        var snapshot_owned = true;
        errdefer if (snapshot_owned) if (cloned_snapshot) |*value| value.deinit(self.alloc);
        const cloned_entries = try core.types.cloneEntries(self.alloc, committed_entries);
        var entries_owned = true;
        errdefer if (entries_owned) core.types.freeEntries(self.alloc, cloned_entries);
        var task: ApplyTask = .{
            .group_id = group_id,
            .snapshot = cloned_snapshot,
            .entries = cloned_entries,
            .read_states = cloned_read_states,
        };
        read_states_owned = false;
        snapshot_owned = false;
        entries_owned = false;
        self.tasks.append(self.alloc, task) catch |err| {
            task.deinit(self.alloc);
            return err;
        };
    }

    fn drain(ptr: *anyopaque) !void {
        const self: *QueuedApplyWorker = @ptrCast(@alignCast(ptr));
        var completed: usize = 0;
        defer {
            if (completed > 0) {
                const remaining = self.tasks.items.len - completed;
                std.mem.copyForwards(ApplyTask, self.tasks.items[0..remaining], self.tasks.items[completed..]);
                self.tasks.items.len = remaining;
            }
        }
        while (completed < self.tasks.items.len) : (completed += 1) {
            const task = &self.tasks.items[completed];
            try self.state_machine.applyReady(task.group_id, task.snapshot, task.entries, task.read_states);
            task.deinit(self.alloc);
        }
    }

    fn abort(ptr: *anyopaque) void {
        const self: *QueuedApplyWorker = @ptrCast(@alignCast(ptr));
        for (self.tasks.items) |*task| task.deinit(self.alloc);
        self.tasks.clearRetainingCapacity();
    }
};

test "queued apply worker drains queued tasks into state machine" {
    const Recorder = struct {
        entries: usize = 0,

        fn iface(self: *@This()) storage_iface.StateMachine {
            return .{
                .ptr = self,
                .vtable = &.{
                    .apply_ready = applyReady,
                },
            };
        }

        fn applyReady(
            ptr: *anyopaque,
            group_id: core.types.GroupId,
            _: ?core.types.Snapshot,
            committed_entries: []const core.Entry,
            read_states: []const core.ReadState,
        ) !void {
            _ = group_id;
            _ = read_states;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entries += committed_entries.len;
        }
    };

    var recorder = Recorder{};
    var worker = QueuedApplyWorker.init(std.testing.allocator, recorder.iface());
    defer worker.deinit();

    var entry = core.Entry{
        .term = 1,
        .index = 1,
        .entry_type = .normal,
        .data = try std.testing.allocator.dupe(u8, "x"),
    };
    defer entry.deinit(std.testing.allocator);

    try worker.queue().enqueueApply(1, null, &.{entry}, &.{});
    try worker.queue().drain();
    try std.testing.expectEqual(@as(usize, 1), recorder.entries);
}

test "queued apply worker consumes successful prefix when a later task fails" {
    const Recorder = struct {
        first_applies: usize = 0,
        second_applies: usize = 0,
        fail_second: bool = true,

        fn stateMachine(self: *@This()) storage_iface.StateMachine {
            return .{
                .ptr = self,
                .vtable = &.{ .apply_ready = applyReady },
            };
        }

        fn applyReady(
            ptr: *anyopaque,
            group_id: core.types.GroupId,
            _: ?core.types.Snapshot,
            _: []const core.Entry,
            _: []const core.ReadState,
        ) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 1) {
                self.first_applies += 1;
                return;
            }
            if (self.fail_second) return error.InjectedApplyFailure;
            self.second_applies += 1;
        }
    };

    var recorder = Recorder{};
    var worker = QueuedApplyWorker.init(std.testing.allocator, recorder.stateMachine());
    defer worker.deinit();
    try worker.queue().enqueueApply(1, null, &.{.{ .term = 1, .index = 1 }}, &.{});
    try worker.queue().enqueueApply(2, null, &.{.{ .term = 1, .index = 1 }}, &.{});

    try std.testing.expectError(error.InjectedApplyFailure, worker.queue().drain());
    try std.testing.expectEqual(@as(usize, 1), recorder.first_applies);
    try std.testing.expectEqual(@as(usize, 1), worker.tasks.items.len);

    recorder.fail_second = false;
    try worker.queue().drain();
    try std.testing.expectEqual(@as(usize, 1), recorder.first_applies);
    try std.testing.expectEqual(@as(usize, 1), recorder.second_applies);
    try std.testing.expectEqual(@as(usize, 0), worker.tasks.items.len);
}
