// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Stable optional safepoints and search feedback for SimIo.

const std = @import("std");
const ids = @import("id.zig");
const task = @import("sim_io_task.zig");

pub const Config = struct {
    enabled: bool = false,
    map_digest: u64 = 0,
};

pub const Hit = struct {
    id: ids.StableId,
    count: u64,
};

pub const Instrumentation = struct {
    allocator: std.mem.Allocator,
    config: Config,
    hits: std.AutoHashMapUnmanaged(ids.StableId, u64) = .empty,

    pub fn init(allocator: std.mem.Allocator, config: Config) Instrumentation {
        return .{ .allocator = allocator, .config = config };
    }

    pub fn deinit(self: *Instrumentation) void {
        self.hits.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn safepoint(self: *Instrumentation, kernel: *task.Kernel, stable_id: ids.StableId) std.Io.Cancelable!void {
        if (stable_id == 0) return error.Canceled;
        const entry = self.hits.getOrPut(self.allocator, stable_id) catch return error.Canceled;
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* +|= 1;
        if (!self.config.enabled) return;
        kernel.yieldCurrentTask() catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            else => return error.Canceled,
        };
    }

    pub fn count(self: *const Instrumentation, stable_id: ids.StableId) u64 {
        return self.hits.get(stable_id) orelse 0;
    }

    pub fn snapshot(self: *const Instrumentation, allocator: std.mem.Allocator) ![]Hit {
        const result = try allocator.alloc(Hit, self.hits.count());
        var iterator = self.hits.iterator();
        var index: usize = 0;
        while (iterator.next()) |entry| : (index += 1) result[index] = .{
            .id = entry.key_ptr.*,
            .count = entry.value_ptr.*,
        };
        std.mem.sort(Hit, result, {}, struct {
            fn lessThan(_: void, left: Hit, right: Hit) bool {
                return left.id < right.id;
            }
        }.lessThan);
        return result;
    }
};
