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
