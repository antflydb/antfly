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

pub const InMemoryDiskBatcher = struct {
    alloc: std.mem.Allocator,
    stores: std.AutoHashMapUnmanaged(core.types.GroupId, *core.MemoryStorage) = .empty,

    pub fn init(alloc: std.mem.Allocator) InMemoryDiskBatcher {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *InMemoryDiskBatcher) void {
        self.stores.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn registerStore(self: *InMemoryDiskBatcher, group_id: core.types.GroupId, store: *core.MemoryStorage) !void {
        try self.stores.put(self.alloc, group_id, store);
    }

    pub fn unregisterStore(self: *InMemoryDiskBatcher, group_id: core.types.GroupId) bool {
        return self.stores.remove(group_id);
    }

    pub fn batcher(self: *InMemoryDiskBatcher) storage_iface.DiskBatcher {
        return .{
            .ptr = self,
            .vtable = &.{
                .begin_batch = beginBatch,
            },
        };
    }

    fn beginBatch(ptr: *anyopaque) !storage_iface.PersistBatch {
        const self: *InMemoryDiskBatcher = @ptrCast(@alignCast(ptr));
        _ = self;
        return .{
            .ptr = ptr,
            .vtable = &.{
                .persist_ready = persistReady,
                .finish = finish,
            },
        };
    }

    fn persistReady(ptr: *anyopaque, group_id: core.types.GroupId, ready: core.Ready) !void {
        const self: *InMemoryDiskBatcher = @ptrCast(@alignCast(ptr));
        const store = self.stores.get(group_id) orelse return error.UnknownGroup;

        if (ready.snapshot) |snapshot| try store.applySnapshot(snapshot);
        if (ready.hard_state) |hard_state| store.setHardState(hard_state);
        if (ready.entries.len > 0) try store.append(ready.entries);
    }

    fn finish(ptr: *anyopaque) !void {
        _ = ptr;
    }
};

test "in-memory disk batcher persists to registered stores" {
    var batcher = InMemoryDiskBatcher.init(std.testing.allocator);
    defer batcher.deinit();

    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    try batcher.registerStore(1, &store);

    const batch = try batcher.batcher().beginBatch();
    defer batch.finish() catch unreachable;

    var entry = core.Entry{
        .term = 1,
        .index = 1,
        .entry_type = .normal,
        .data = try std.testing.allocator.dupe(u8, "x"),
    };
    defer entry.deinit(std.testing.allocator);

    try batch.persistReady(1, .{
        .hard_state = .{ .current_term = 1, .commit_index = 1 },
        .entries = &.{entry},
    });

    try std.testing.expectEqual(@as(core.types.Index, 1), store.hard_state.commit_index);
    try std.testing.expectEqual(@as(usize, 1), store.entries_state.items.len);
}
