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
    entries: []core.Entry,
    read_states: []core.ReadState,

    fn deinit(self: *ApplyTask, alloc: std.mem.Allocator) void {
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
            },
        };
    }

    fn enqueueApply(
        ptr: *anyopaque,
        group_id: core.types.GroupId,
        committed_entries: []const core.Entry,
        read_states: []const core.ReadState,
    ) !void {
        const self: *QueuedApplyWorker = @ptrCast(@alignCast(ptr));
        var cloned_read_states = try self.alloc.alloc(core.ReadState, read_states.len);
        errdefer self.alloc.free(cloned_read_states);
        for (read_states, 0..) |read_state, i| cloned_read_states[i] = try read_state.clone(self.alloc);

        try self.tasks.append(self.alloc, .{
            .group_id = group_id,
            .entries = try core.types.cloneEntries(self.alloc, committed_entries),
            .read_states = cloned_read_states,
        });
    }

    fn drain(ptr: *anyopaque) !void {
        const self: *QueuedApplyWorker = @ptrCast(@alignCast(ptr));
        for (self.tasks.items) |*task| {
            try self.state_machine.applyReady(task.group_id, task.entries, task.read_states);
            task.deinit(self.alloc);
        }
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

    try worker.queue().enqueueApply(1, &.{entry}, &.{});
    try worker.queue().drain();
    try std.testing.expectEqual(@as(usize, 1), recorder.entries);
}
