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

const std = @import("std");
const builtin = @import("builtin");

pub fn cOrFallback(fallback: std.mem.Allocator) std.mem.Allocator {
    if (comptime builtin.link_libc) return std.heap.c_allocator;
    return fallback;
}

pub fn processAllocator(fallback: std.mem.Allocator) std.mem.Allocator {
    return cOrFallback(fallback);
}

/// Return unused libc heap pages to the operating system after a coarse
/// process-lifetime teardown boundary. glibc normally retains freed arenas for
/// reuse, which is efficient for steady allocation shapes but can preserve the
/// peak RSS of a server that rotates between differently shaped multi-GiB
/// models. `malloc_trim` examines every arena on supported glibc versions and
/// is therefore intentionally reserved for cold paths such as model eviction,
/// not individual frees or request completion.
///
/// The operation is advisory: `false` means either that this target has no
/// supported process allocator purge or that glibc did not release pages.
pub fn reclaimUnusedProcessMemory() bool {
    if (comptime builtin.os.tag == .linux and builtin.link_libc and builtin.abi.isGnu()) {
        return glibc.malloc_trim(0) != 0;
    }
    return false;
}

const glibc = if (builtin.os.tag == .linux and builtin.link_libc and builtin.abi.isGnu()) struct {
    extern "c" fn malloc_trim(pad: usize) c_int;
} else struct {};

test "process allocator reclamation is a portable best-effort operation" {
    _ = reclaimUnusedProcessMemory();
}

/// Process-lifetime fallback for shared caches and independently owned work.
/// Browser instances have one execution thread and use the WASM heap; native
/// threaded owners retain the scalable allocator and its cross-thread frees.
pub fn concurrentFallback() std.mem.Allocator {
    if (comptime builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64) return std.heap.wasm_allocator;
    if (comptime builtin.single_threaded) return std.heap.page_allocator;
    return std.heap.smp_allocator;
}

/// Keep allocation-failure enumeration independent of the backing allocator's
/// ability to resize or relocate a block on a particular iteration. SafeAllocator
/// can remap blocks differently as its backing memory layout changes. Refusing
/// remaps exercises the allocate/copy/free fallback and retains leak detection.
pub fn checkAllAllocationFailures(backing: std.mem.Allocator, comptime test_fn: anytype, extra_args: anytype) !void {
    var stable: Stable = .{ .backing = backing };
    try std.testing.checkAllAllocationFailures(stable.allocator(), test_fn, extra_args);
}

const Stable = struct {
    backing: std.mem.Allocator,
    fn allocator(self: *Stable) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn alloc(context: *anyopaque, size: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Stable = @ptrCast(@alignCast(context));
        return self.backing.rawAlloc(size, alignment, ra);
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(context: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Stable = @ptrCast(@alignCast(context));
        self.backing.rawFree(bytes, alignment, ra);
    }
};
