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

//! Exercise the complete boundary with independently reopened physical resources.
const std = @import("std");
const memory = @import("lake_execution_memory.zig");
const limits = @import("../serverless/build/lake_build_limits.zig");
const A = std.mem.Allocator;
const State = struct {
    allocator: A,
    calls: usize = 0,
    forced: ??usize = null,
    fn query(raw: *const anyopaque) callconv(.c) memory.Value {
        const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
        self.calls += 1;
        return .fromOptional(if (self.forced) |value| value else limits.WorkingSetAllocator.headroom(self.allocator));
    }
    fn capability(self: *@This()) memory.Capability {
        return .{ .context = self, .query = query };
    }
};

test "external lake execution headroom validates version and layouts before callback" {
    var state: State = .{ .allocator = std.testing.allocator };
    var cap = state.capability();
    cap.version += 1;
    cap.context = @ptrFromInt(1);
    cap.query = @as(@typeInfo(@FieldType(memory.Capability, "query")).optional.child, @ptrFromInt(4096));
    try std.testing.expectError(error.InvalidArgument, memory.BorrowedAllocator.init(std.testing.allocator, cap));
    cap = state.capability();
    cap.value_layout.size += 1;
    try std.testing.expectError(error.InvalidArgument, memory.BorrowedAllocator.init(std.testing.allocator, cap));
    cap = state.capability();
    cap.contract.method_id +%= 1;
    try std.testing.expectError(error.InvalidArgument, memory.BorrowedAllocator.init(std.testing.allocator, cap));
    cap = state.capability();
    cap.context = null;
    try std.testing.expectError(error.InvalidArgument, memory.BorrowedAllocator.init(std.testing.allocator, cap));
    cap = state.capability();
    cap.query = null;
    try std.testing.expectError(error.InvalidArgument, memory.BorrowedAllocator.init(std.testing.allocator, cap));
    try std.testing.expectEqual(@as(usize, 0), state.calls);
}

test "external lake execution headroom is dynamic and preserves nested hard limits" {
    var parent = try limits.WorkingSetAllocator.init(std.testing.allocator, .{ .max_working_set_bytes = 128 });
    defer parent.deinit();
    var state: State = .{ .allocator = parent.allocator() };
    var bridge = try memory.BorrowedAllocator.init(parent.allocator(), state.capability());
    var child = try limits.WorkingSetAllocator.init(bridge.allocator(), .{ .max_working_set_bytes = 256 });
    defer child.deinit();
    const a = child.allocator();
    try std.testing.expectEqual(@as(?usize, 128), limits.WorkingSetAllocator.headroom(a));
    const bytes = try a.alloc(u8, 80);
    try std.testing.expectEqual(@as(?usize, 48), limits.WorkingSetAllocator.headroom(a));
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 49));
    try std.testing.expect(parent.limitExceeded());
    a.free(bytes);
    try std.testing.expectEqual(@as(?usize, 128), limits.WorkingSetAllocator.headroom(a));
    try std.testing.expect(state.calls >= 3);
}

test "external lake execution unknown and low creator headroom remain conservative" {
    var unknown = try memory.BorrowedAllocator.init(std.testing.allocator, null);
    try std.testing.expect(limits.WorkingSetAllocator.headroom(unknown.allocator()) == null);
    var state: State = .{ .allocator = std.testing.allocator, .forced = @as(?usize, 0) };
    var limited = try memory.BorrowedAllocator.init(std.testing.allocator, state.capability());
    try std.testing.expectEqual(@as(?usize, 0), limits.WorkingSetAllocator.headroom(limited.allocator()));
    state.forced = @as(?usize, 4096);
    try std.testing.expectEqual(@as(?usize, 4096), limits.WorkingSetAllocator.headroom(limited.allocator()));
    try std.testing.expect((memory.Value{ .known = 2 }).optional() == null);
}
