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
    if (comptime builtin.link_libc) {
        if (heapAccountingEnabled()) return heap_accounting_allocator;
        return std.heap.c_allocator;
    }
    return fallback;
}

/// Opt-in diagnostic: `ANTFLY_HEAP_ACCOUNTING=1` routes the libc process
/// allocator through a live-byte counter so resident memory can be split into
/// live heap and allocator-retained pages. The decision is made once, before
/// the first allocation, so the default path stays the bare libc allocator.
pub const HeapAccountingStats = struct {
    enabled: bool = false,
    /// Signed: memory handed across the storage-kernel boundary is allocated
    /// on one side and freed on the other, so only the sum over both ledgers
    /// is meaningful.
    live_bytes: i64 = 0,
    peak_live_bytes: u64 = 0,
    allocated_bytes_total: u64 = 0,
    allocations_total: u64 = 0,
};

pub fn heapAccountingStats() HeapAccountingStats {
    if (comptime !builtin.link_libc) return .{};
    if (!heapAccountingEnabled()) return .{};
    return .{
        .enabled = true,
        .live_bytes = heap_live_bytes.load(.monotonic),
        .peak_live_bytes = heap_peak_live_bytes.load(.monotonic),
        .allocated_bytes_total = heap_allocated_bytes_total.load(.monotonic),
        .allocations_total = heap_allocations_total.load(.monotonic),
    };
}

const HeapAccountingState = enum(u8) { undecided, disabled, enabled };

var heap_accounting_state: std.atomic.Value(HeapAccountingState) = .init(.undecided);
// Signed: a block can be freed through a different handle than the one that
// allocated it, and a diagnostic must not wrap when that happens.
var heap_live_bytes: std.atomic.Value(i64) = .init(0);
var heap_peak_live_bytes: std.atomic.Value(u64) = .init(0);
var heap_allocated_bytes_total: std.atomic.Value(u64) = .init(0);
var heap_allocations_total: std.atomic.Value(u64) = .init(0);

fn heapAccountingEnabled() bool {
    if (comptime !builtin.link_libc) return false;
    const state = heap_accounting_state.load(.acquire);
    if (state != .undecided) return state == .enabled;
    const raw = std.c.getenv("ANTFLY_HEAP_ACCOUNTING");
    const enabled = if (raw) |value| value[0] == '1' else false;
    heap_accounting_state.store(if (enabled) .enabled else .disabled, .release);
    return enabled;
}

const heap_accounting_allocator: std.mem.Allocator = .{
    .ptr = undefined,
    .vtable = &.{
        .alloc = heapAccountingAlloc,
        .resize = heapAccountingResize,
        .remap = heapAccountingRemap,
        .free = heapAccountingFree,
    },
};

fn heapAccountingGrow(len: usize) void {
    const delta: i64 = @intCast(len);
    const live = heap_live_bytes.fetchAdd(delta, .monotonic) + delta;
    _ = heap_allocated_bytes_total.fetchAdd(len, .monotonic);
    _ = heap_peak_live_bytes.fetchMax(@intCast(@max(live, 0)), .monotonic);
}

fn heapAccountingShrink(len: usize) void {
    _ = heap_live_bytes.fetchSub(@intCast(len), .monotonic);
}

fn heapAccountingResized(old_len: usize, new_len: usize) void {
    if (new_len >= old_len) {
        heapAccountingGrow(new_len - old_len);
    } else {
        heapAccountingShrink(old_len - new_len);
    }
}

fn heapAccountingAlloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
    const child = std.heap.c_allocator;
    const ptr = child.vtable.alloc(child.ptr, len, alignment, ret_addr) orelse return null;
    _ = heap_allocations_total.fetchAdd(1, .monotonic);
    heapAccountingGrow(len);
    return ptr;
}

fn heapAccountingResize(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
    const child = std.heap.c_allocator;
    if (!child.vtable.resize(child.ptr, memory, alignment, new_len, ret_addr)) return false;
    heapAccountingResized(memory.len, new_len);
    return true;
}

fn heapAccountingRemap(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
    const child = std.heap.c_allocator;
    const ptr = child.vtable.remap(child.ptr, memory, alignment, new_len, ret_addr) orelse return null;
    heapAccountingResized(memory.len, new_len);
    return ptr;
}

fn heapAccountingFree(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
    const child = std.heap.c_allocator;
    child.vtable.free(child.ptr, memory, alignment, ret_addr);
    heapAccountingShrink(memory.len);
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
    if (comptime builtin.os.tag == .macos and builtin.link_libc) {
        // Every zone, no byte goal: release whatever freed regions the default
        // and nano zones are still holding dirty.
        return darwin.malloc_zone_pressure_relief(null, 0) != 0;
    }
    return false;
}

/// Whether `reclaimUnusedProcessMemory` can do anything on this target. musl's
/// allocator exposes no purge entry point.
pub fn processMemoryReclaimSupported() bool {
    return comptime builtin.link_libc and
        ((builtin.os.tag == .linux and builtin.abi.isGnu()) or builtin.os.tag == .macos);
}

const darwin = if (builtin.os.tag == .macos and builtin.link_libc) struct {
    extern "c" fn malloc_zone_pressure_relief(zone: ?*anyopaque, goal: usize) usize;
} else struct {};

const glibc = if (builtin.os.tag == .linux and builtin.link_libc and builtin.abi.isGnu()) struct {
    extern "c" fn malloc_trim(pad: usize) c_int;
    extern "c" fn mallopt(param: c_int, value: c_int) c_int;
    const M_TRIM_THRESHOLD: c_int = -1;
    const M_MMAP_THRESHOLD: c_int = -3;
    const M_ARENA_MAX: c_int = -8;
} else struct {};

/// Envelopes at or below this size tune the allocator for footprint. Larger
/// processes keep libc defaults, so their throughput is untouched.
pub const small_process_envelope_bytes: u64 = 4 * 1024 * 1024 * 1024;

/// Call once at startup, before worker threads exist. glibc gives every
/// thread its own arena (up to eight per core) and grows its mmap threshold
/// as large blocks are freed, so a busy node retains several times its live
/// heap. Inside a small envelope, cap the arena count near one per GiB and
/// pin the thresholds so freed large blocks go straight back to the kernel.
pub fn configureForProcessEnvelope(limit_bytes: u64) void {
    if (comptime !(builtin.os.tag == .linux and builtin.link_libc and builtin.abi.isGnu())) return;
    if (limit_bytes == 0 or limit_bytes > small_process_envelope_bytes) return;
    const arenas: c_int = @intCast(@max(@as(u64, 2), limit_bytes / (1024 * 1024 * 1024)));
    _ = glibc.mallopt(glibc.M_ARENA_MAX, arenas);
    _ = glibc.mallopt(glibc.M_MMAP_THRESHOLD, 128 * 1024);
    _ = glibc.mallopt(glibc.M_TRIM_THRESHOLD, 128 * 1024);
}

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
