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
const enrichment_runtime = @import("enrichment_runtime.zig");

pub const GroupRuntimeHandle = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        stop: *const fn (ptr: *anyopaque) anyerror!void,
        deinit: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator) void,
    };

    pub fn stop(self: GroupRuntimeHandle) !void {
        try self.vtable.stop(self.ptr);
    }

    pub fn deinit(self: GroupRuntimeHandle, alloc: std.mem.Allocator) void {
        self.vtable.deinit(self.ptr, alloc);
    }
};

pub const GroupRuntimeFactory = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        start_runtime: *const fn (ptr: *anyopaque, group_id: u64) anyerror!GroupRuntimeHandle,
    };

    pub fn startRuntime(self: GroupRuntimeFactory, group_id: u64) !GroupRuntimeHandle {
        return try self.vtable.start_runtime(self.ptr, group_id);
    }
};

pub const DbEnrichmentExecutorConfig = struct {
    backend: enrichment_runtime.ExecutorBackend = .threaded,
};

pub const DbEnrichmentExecutor = struct {
    alloc: std.mem.Allocator,
    cfg: DbEnrichmentExecutorConfig,
    factory: GroupRuntimeFactory,
    handles: std.AutoHashMapUnmanaged(u64, GroupRuntimeHandle) = .empty,

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: DbEnrichmentExecutorConfig,
        factory: GroupRuntimeFactory,
    ) DbEnrichmentExecutor {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .factory = factory,
        };
    }

    pub fn deinit(self: *DbEnrichmentExecutor) void {
        var it = self.handles.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.stop() catch {};
            entry.value_ptr.deinit(self.alloc);
        }
        self.handles.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn executor(self: *DbEnrichmentExecutor) enrichment_runtime.EnrichmentExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .start_group = startGroup,
                .stop_group = stopGroup,
                .is_active = isActive,
                .backend = backend,
            },
        };
    }

    fn startGroup(ptr: *anyopaque, group_id: u64) !void {
        const self: *DbEnrichmentExecutor = @ptrCast(@alignCast(ptr));
        const entry = try self.handles.getOrPut(self.alloc, group_id);
        if (entry.found_existing) return;
        errdefer _ = self.handles.remove(group_id);
        entry.value_ptr.* = try self.factory.startRuntime(group_id);
    }

    fn stopGroup(ptr: *anyopaque, group_id: u64) !void {
        const self: *DbEnrichmentExecutor = @ptrCast(@alignCast(ptr));
        const handle = self.handles.get(group_id) orelse return;
        try handle.stop();
        handle.deinit(self.alloc);
        _ = self.handles.remove(group_id);
    }

    fn isActive(ptr: *anyopaque, group_id: u64) bool {
        const self: *DbEnrichmentExecutor = @ptrCast(@alignCast(ptr));
        return self.handles.contains(group_id);
    }

    fn backend(ptr: *anyopaque) enrichment_runtime.ExecutorBackend {
        const self: *DbEnrichmentExecutor = @ptrCast(@alignCast(ptr));
        return self.cfg.backend;
    }
};

test "db enrichment executor starts and stops per-group runtimes" {
    const FakeRuntime = struct {
        stopped: bool = false,

        fn stop(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.stopped = true;
        }

        fn deinit(ptr: *anyopaque, alloc: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            alloc.destroy(self);
        }

        fn handle(self: *@This()) GroupRuntimeHandle {
            return .{
                .ptr = self,
                .vtable = &.{
                    .stop = stop,
                    .deinit = deinit,
                },
            };
        }
    };

    const Factory = struct {
        alloc: std.mem.Allocator,
        started: std.ArrayListUnmanaged(u64) = .empty,

        fn deinit(self: *@This()) void {
            self.started.deinit(self.alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) GroupRuntimeFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .start_runtime = startRuntime,
                },
            };
        }

        fn startRuntime(ptr: *anyopaque, group_id: u64) !GroupRuntimeHandle {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.started.append(self.alloc, group_id);
            const runtime = try self.alloc.create(FakeRuntime);
            runtime.* = .{};
            return runtime.handle();
        }
    };

    var factory = Factory{ .alloc = std.testing.allocator };
    defer factory.deinit();

    var executor = DbEnrichmentExecutor.init(
        std.testing.allocator,
        .{ .backend = .threaded },
        factory.iface(),
    );
    defer executor.deinit();

    const iface = executor.executor();
    try iface.startGroup(5001);
    try std.testing.expect(iface.isActive(5001));
    try std.testing.expectEqual(@as(enrichment_runtime.ExecutorBackend, .threaded), iface.backend());

    try iface.stopGroup(5001);
    try std.testing.expect(!iface.isActive(5001));
    try std.testing.expectEqualSlices(u64, &.{5001}, factory.started.items);
}
