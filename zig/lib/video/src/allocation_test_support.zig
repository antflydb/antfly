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

//! Checked allocation-failure campaigns with optional successful-allocation traces.
const std = @import("std");

pub const CheckedAllocator = struct {
    safe: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{ .stack_trace_frames = 0, .check_write_after_free = true }),
    traces: bool,

    pub fn init() !CheckedAllocator {
        return .{ .traces = try allocationTracesEnabled() };
    }

    pub fn allocator(self: *@This()) std.mem.Allocator {
        return if (self.traces) std.testing.allocator else self.safe.allocator();
    }

    pub fn deinit(self: *@This()) void {
        if (self.safe.deinit() != 0) @panic("video test allocator leaked");
    }
};

// The failure schedule counts allocations. An opportunistic in-place resize
// can change that count as the backing heap shifts between replays (for
// example, when Linux can extend a mapping). Drive the explicit allocation
// fallback consistently while retaining the backing allocator's safety checks.
const StableBacking = struct {
    backing: std.mem.Allocator,

    fn allocator(self: *StableBacking) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = std.mem.Allocator.noResize,
            .remap = std.mem.Allocator.noRemap,
            .free = free,
        } };
    }

    fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, return_address: usize) ?[*]u8 {
        const self: *StableBacking = @ptrCast(@alignCast(ptr));
        return self.backing.rawAlloc(len, alignment, return_address);
    }

    fn free(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, return_address: usize) void {
        const self: *StableBacking = @ptrCast(@alignCast(ptr));
        self.backing.rawFree(bytes, alignment, return_address);
    }
};

pub fn checkAllAllocationFailures(comptime run: anytype, args: anytype) !void {
    // Keep exhaustive failure injection and SafeAllocator's leak, mismatch,
    // double-free and write-after-free checks. Capturing successful allocation
    // stacks in every replay dominates these repeated campaigns on Darwin.
    // FailingAllocator still retains the stack of the injected failure.
    var checked = try CheckedAllocator.init();
    defer checked.deinit();
    var stable = StableBacking{ .backing = checked.allocator() };
    std.testing.checkAllAllocationFailures(stable.allocator(), run, args) catch |err| {
        std.debug.print("allocation-failure campaign returned {s}\n", .{@errorName(err)});
        return err;
    };
}

fn allocationTracesEnabled() !bool {
    const value = std.testing.environ.getAlloc(std.testing.allocator, "ANTFLY_TEST_ALLOCATOR_TRACES") catch |err| switch (err) {
        error.EnvironmentVariableMissing => return false,
        else => return err,
    };
    defer std.testing.allocator.free(value);
    return std.mem.eql(u8, value, "1") or std.ascii.eqlIgnoreCase(value, "true") or
        std.ascii.eqlIgnoreCase(value, "yes") or std.ascii.eqlIgnoreCase(value, "on");
}
