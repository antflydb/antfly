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

// MetalTensor — JIT-free tensor wrapper for the Metal runtime's public surface.
//
// Carries host-backed storage (`data`/`len` pointing into c_allocator memory,
// the original representation) and — optionally — an attached device-backed
// storage (an opaque MTLBuffer handle plus byte range within it). Today every
// tensor has valid host fields; device storage is an additive capability used
// by the new `*_device` C ABI. Callers can promote a host tensor to device
// via `ensureDevice` (upload) or pull a device tensor back to host via
// `toHostSlice` (lazy blit with a cached mirror).
//
// Keeping the host fields at the top level means existing callers that pass
// `tensor.data` / `tensor.len` / `tensor.owned_by_c_allocator` keep working
// verbatim. Phase 3 migrates hot-path sites to device-resident lifetimes.

const std = @import("std");

pub const max_dims: usize = 8;

pub const StorageMode = enum(c_int) {
    shared = 0,
    private = 1,
};

pub const DType = enum(u8) {
    f32 = 0,
};

const DeviceBufferRef = struct {
    handle: *anyopaque,
    runtime: *anyopaque,
    byte_len: usize,
    ref_count: usize,
    released: bool,
    release_on_drop: bool,
};

pub const MemoryStats = struct {
    device_owned_buffers_created: u64 = 0,
    device_owned_buffers_released: u64 = 0,
    device_owned_bytes_created: u64 = 0,
    device_owned_bytes_released: u64 = 0,
    device_owned_live_bytes: u64 = 0,
    device_owned_peak_live_bytes: u64 = 0,
    device_owned_live_lt_256kb_bytes: u64 = 0,
    device_owned_peak_lt_256kb_bytes: u64 = 0,
    device_owned_live_256kb_1mb_bytes: u64 = 0,
    device_owned_peak_256kb_1mb_bytes: u64 = 0,
    device_owned_live_1mb_4mb_bytes: u64 = 0,
    device_owned_peak_1mb_4mb_bytes: u64 = 0,
    device_owned_live_4mb_16mb_bytes: u64 = 0,
    device_owned_peak_4mb_16mb_bytes: u64 = 0,
    device_owned_live_ge_16mb_bytes: u64 = 0,
    device_owned_peak_ge_16mb_bytes: u64 = 0,
    device_borrowed_tensors_created: u64 = 0,
    retained_device_views_created: u64 = 0,
    host_mirror_allocations: u64 = 0,
    host_mirror_frees: u64 = 0,
    host_mirror_live_bytes: u64 = 0,
    host_mirror_peak_live_bytes: u64 = 0,
    host_mirror_download_bytes: u64 = 0,
    shared_host_aliases: u64 = 0,
    to_host_calls: u64 = 0,
    to_host_device_calls: u64 = 0,
};

var memory_stats = MemoryStats{};
var to_host_trace_count: usize = 0;
var owned_alloc_trace_count: usize = 0;
const owned_peak_snapshot_capacity: usize = 16384;
const owned_peak_snapshot_label_len: usize = 96;

const OwnedAllocationRecord = struct {
    handle: ?*anyopaque = null,
    bytes: usize = 0,
    dims: [max_dims]i32 = [_]i32{0} ** max_dims,
    rank: u8 = 0,
    seq: u64 = 0,
    label: [owned_peak_snapshot_label_len]u8 = [_]u8{0} ** owned_peak_snapshot_label_len,
    label_len: u8 = 0,
    active: bool = false,

    fn labelSlice(self: *const OwnedAllocationRecord) []const u8 {
        return self.label[0..self.label_len];
    }
};

var owned_allocation_records = [_]OwnedAllocationRecord{.{}} ** owned_peak_snapshot_capacity;
var owned_allocation_next_slot: usize = 0;
var owned_allocation_sequence: u64 = 0;
var owned_allocation_live_record_count: usize = 0;
var owned_allocation_lost_record_count: u64 = 0;
var owned_peak_snapshot_print_count: usize = 0;
var owned_alloc_context: []const u8 = "";

fn getenvUsize(comptime name: [*:0]const u8) ?usize {
    if (comptime @import("builtin").os.tag == .freestanding) return null;
    const c = @cImport(@cInclude("stdlib.h"));
    const value = c.getenv(name) orelse return null;
    const slice = std.mem.span(value);
    if (slice.len == 0) return null;
    return std.fmt.parseUnsigned(usize, slice, 10) catch null;
}

fn traceToHostLimit() usize {
    return getenvUsize("TERMITE_METAL_TRACE_TO_HOST_LIMIT") orelse 0;
}

fn traceToHostStackLimit() usize {
    return getenvUsize("TERMITE_METAL_TRACE_TO_HOST_STACK_LIMIT") orelse 0;
}

fn traceOwnedAllocLimit() usize {
    return getenvUsize("TERMITE_METAL_TRACE_OWNED_ALLOC_LIMIT") orelse 0;
}

fn traceOwnedAllocMinBytes() usize {
    return getenvUsize("TERMITE_METAL_TRACE_OWNED_ALLOC_MIN_BYTES") orelse 0;
}

fn ownedPeakSnapshotTop() usize {
    const requested = getenvUsize("TERMITE_METAL_OWNED_PEAK_SNAPSHOT_TOP") orelse 0;
    return @min(requested, 32);
}

fn ownedPeakSnapshotLimit() usize {
    return getenvUsize("TERMITE_METAL_OWNED_PEAK_SNAPSHOT_LIMIT") orelse 8;
}

fn ownedPeakSnapshotMinBytes() u64 {
    return getenvUsize("TERMITE_METAL_OWNED_PEAK_SNAPSHOT_MIN_BYTES") orelse 0;
}

// The owned-allocation record table is consumed only by the (env-gated) peak
// snapshot / alloc trace. Maintaining it costs an O(capacity) slot scan per
// device buffer alloc/free, so skip it entirely unless a diagnostic is enabled.
// Cached because the environment is constant for the process lifetime; the cache
// is a file-scope var so resetMemoryStats can clear it, keeping the recording
// gate consistent with the (live-evaluated) print gate across test resets.
var owned_allocation_recording_enabled_cache: ?bool = null;
fn ownedAllocationRecordingEnabled() bool {
    if (owned_allocation_recording_enabled_cache) |c| return c;
    const enabled = ownedPeakSnapshotTop() > 0 or traceOwnedAllocLimit() > 0;
    owned_allocation_recording_enabled_cache = enabled;
    return enabled;
}

pub fn setOwnedAllocationContext(label: []const u8) void {
    owned_alloc_context = label;
}

pub fn clearOwnedAllocationContext() void {
    owned_alloc_context = "";
}

fn copyLabel(dst: *[owned_peak_snapshot_label_len]u8, src: []const u8) u8 {
    const n = @min(src.len, dst.len);
    if (n > 0) @memcpy(dst[0..n], src[0..n]);
    if (n < dst.len) @memset(dst[n..], 0);
    return @intCast(n);
}

fn noteOwnedAllocationRecord(handle: *anyopaque, byte_len: usize, dims: []const i32) void {
    if (!ownedAllocationRecordingEnabled()) return;
    var slot_index: ?usize = null;
    for (&owned_allocation_records, 0..) |*record, idx| {
        if (!record.active) {
            slot_index = idx;
            break;
        }
    }
    if (slot_index == null and owned_allocation_records.len > 0) {
        for (0..owned_allocation_records.len) |_| {
            const idx = owned_allocation_next_slot;
            owned_allocation_next_slot = (owned_allocation_next_slot + 1) % owned_allocation_records.len;
            if (!owned_allocation_records[idx].active) {
                slot_index = idx;
                break;
            }
        }
    }
    const idx = slot_index orelse {
        owned_allocation_lost_record_count += 1;
        return;
    };
    var dims_buf = [_]i32{0} ** max_dims;
    const rank = @min(dims.len, max_dims);
    for (dims[0..rank], 0..) |dim, axis| dims_buf[axis] = dim;
    owned_allocation_sequence += 1;
    var label_buf = [_]u8{0} ** owned_peak_snapshot_label_len;
    const label_len = copyLabel(&label_buf, owned_alloc_context);
    owned_allocation_records[idx] = .{
        .handle = handle,
        .bytes = byte_len,
        .dims = dims_buf,
        .rank = @intCast(rank),
        .seq = owned_allocation_sequence,
        .label = label_buf,
        .label_len = label_len,
        .active = true,
    };
    owned_allocation_live_record_count += 1;
}

fn forgetOwnedAllocationRecord(handle: *anyopaque) void {
    if (!ownedAllocationRecordingEnabled()) return;
    for (&owned_allocation_records) |*record| {
        if (!record.active or record.handle != handle) continue;
        record.active = false;
        record.handle = null;
        owned_allocation_live_record_count -|= 1;
        return;
    }
}

fn maybeRecordTop(top: []OwnedAllocationRecord, used: *usize, record: OwnedAllocationRecord) void {
    const cap = top.len;
    if (cap == 0) return;
    var pos: usize = 0;
    while (pos < used.* and top[pos].bytes >= record.bytes) : (pos += 1) {}
    if (pos >= cap) return;
    if (used.* < cap) used.* += 1;
    var idx = used.* - 1;
    while (idx > pos) : (idx -= 1) top[idx] = top[idx - 1];
    top[pos] = record;
}

fn printOwnedPeakSnapshot() void {
    const top_n = ownedPeakSnapshotTop();
    if (top_n == 0) return;
    if (memory_stats.device_owned_peak_live_bytes < ownedPeakSnapshotMinBytes()) return;
    if (owned_peak_snapshot_print_count >= ownedPeakSnapshotLimit()) return;
    owned_peak_snapshot_print_count += 1;

    var top = [_]OwnedAllocationRecord{.{}} ** 32;
    var used: usize = 0;
    var live_bytes: u64 = 0;
    for (owned_allocation_records) |record| {
        if (!record.active) continue;
        live_bytes += @intCast(record.bytes);
        maybeRecordTop(top[0..top_n], &used, record);
    }

    std.debug.print(
        "metal_owned_peak_snapshot: peak={d} live={d} live_records={d} tracked_live_records={d} lost_records={d} top={d}\n",
        .{
            memory_stats.device_owned_peak_live_bytes,
            live_bytes,
            owned_allocation_live_record_count,
            used,
            owned_allocation_lost_record_count,
            top_n,
        },
    );
    for (top[0..used], 0..) |record, i| {
        std.debug.print(
            "metal_owned_peak_snapshot_entry: index={d} bytes={d} seq={d} label=\"{s}\" dims=[",
            .{ i + 1, record.bytes, record.seq, record.labelSlice() },
        );
        for (record.dims[0..record.rank], 0..) |dim, axis| {
            if (axis != 0) std.debug.print(",", .{});
            std.debug.print("{d}", .{dim});
        }
        std.debug.print("]\n", .{});
    }
}

fn updateOwnedCreateBucket(byte_len: usize) void {
    const bytes: u64 = @intCast(byte_len);
    if (byte_len < 256 * 1024) {
        memory_stats.device_owned_live_lt_256kb_bytes += bytes;
        memory_stats.device_owned_peak_lt_256kb_bytes = @max(memory_stats.device_owned_peak_lt_256kb_bytes, memory_stats.device_owned_live_lt_256kb_bytes);
    } else if (byte_len < 1024 * 1024) {
        memory_stats.device_owned_live_256kb_1mb_bytes += bytes;
        memory_stats.device_owned_peak_256kb_1mb_bytes = @max(memory_stats.device_owned_peak_256kb_1mb_bytes, memory_stats.device_owned_live_256kb_1mb_bytes);
    } else if (byte_len < 4 * 1024 * 1024) {
        memory_stats.device_owned_live_1mb_4mb_bytes += bytes;
        memory_stats.device_owned_peak_1mb_4mb_bytes = @max(memory_stats.device_owned_peak_1mb_4mb_bytes, memory_stats.device_owned_live_1mb_4mb_bytes);
    } else if (byte_len < 16 * 1024 * 1024) {
        memory_stats.device_owned_live_4mb_16mb_bytes += bytes;
        memory_stats.device_owned_peak_4mb_16mb_bytes = @max(memory_stats.device_owned_peak_4mb_16mb_bytes, memory_stats.device_owned_live_4mb_16mb_bytes);
    } else {
        memory_stats.device_owned_live_ge_16mb_bytes += bytes;
        memory_stats.device_owned_peak_ge_16mb_bytes = @max(memory_stats.device_owned_peak_ge_16mb_bytes, memory_stats.device_owned_live_ge_16mb_bytes);
    }
}

fn updateOwnedReleaseBucket(byte_len: usize) void {
    const bytes: u64 = @intCast(byte_len);
    if (byte_len < 256 * 1024) {
        memory_stats.device_owned_live_lt_256kb_bytes -|= bytes;
    } else if (byte_len < 1024 * 1024) {
        memory_stats.device_owned_live_256kb_1mb_bytes -|= bytes;
    } else if (byte_len < 4 * 1024 * 1024) {
        memory_stats.device_owned_live_1mb_4mb_bytes -|= bytes;
    } else if (byte_len < 16 * 1024 * 1024) {
        memory_stats.device_owned_live_4mb_16mb_bytes -|= bytes;
    } else {
        memory_stats.device_owned_live_ge_16mb_bytes -|= bytes;
    }
}

fn noteDeviceOwnedCreate(handle: *anyopaque, byte_len: usize, dims: []const i32) void {
    memory_stats.device_owned_buffers_created += 1;
    memory_stats.device_owned_bytes_created += @intCast(byte_len);
    memory_stats.device_owned_live_bytes += @intCast(byte_len);
    updateOwnedCreateBucket(byte_len);
    noteOwnedAllocationRecord(handle, byte_len, dims);
    const previous_peak = memory_stats.device_owned_peak_live_bytes;
    memory_stats.device_owned_peak_live_bytes = @max(memory_stats.device_owned_peak_live_bytes, memory_stats.device_owned_live_bytes);
    const limit = traceOwnedAllocLimit();
    if (limit > 0 and owned_alloc_trace_count < limit and byte_len >= traceOwnedAllocMinBytes()) {
        owned_alloc_trace_count += 1;
        std.debug.print(
            "metal_owned_alloc_trace: bytes={d} live={d} peak={d} new_peak={} dims=[",
            .{ byte_len, memory_stats.device_owned_live_bytes, memory_stats.device_owned_peak_live_bytes, memory_stats.device_owned_peak_live_bytes > previous_peak },
        );
        for (dims, 0..) |dim, i| {
            if (i != 0) std.debug.print(",", .{});
            std.debug.print("{d}", .{dim});
        }
        std.debug.print("]\n", .{});
    }
    if (memory_stats.device_owned_peak_live_bytes > previous_peak) printOwnedPeakSnapshot();
}

fn noteDeviceOwnedRelease(handle: *anyopaque, byte_len: usize) void {
    memory_stats.device_owned_buffers_released += 1;
    memory_stats.device_owned_bytes_released += @intCast(byte_len);
    memory_stats.device_owned_live_bytes -|= @intCast(byte_len);
    updateOwnedReleaseBucket(byte_len);
    forgetOwnedAllocationRecord(handle);
}

fn noteHostMirrorAlloc(byte_len: usize) void {
    memory_stats.host_mirror_allocations += 1;
    memory_stats.host_mirror_download_bytes += @intCast(byte_len);
    memory_stats.host_mirror_live_bytes += @intCast(byte_len);
    memory_stats.host_mirror_peak_live_bytes = @max(memory_stats.host_mirror_peak_live_bytes, memory_stats.host_mirror_live_bytes);
}

fn noteHostMirrorFree(byte_len: usize) void {
    memory_stats.host_mirror_frees += 1;
    memory_stats.host_mirror_live_bytes -|= @intCast(byte_len);
}

pub fn memoryStatsSnapshot() MemoryStats {
    return memory_stats;
}

pub fn resetMemoryStats() void {
    memory_stats = .{};
    owned_alloc_trace_count = 0;
    owned_peak_snapshot_print_count = 0;
    owned_allocation_next_slot = 0;
    owned_allocation_sequence = 0;
    owned_allocation_live_record_count = 0;
    owned_allocation_lost_record_count = 0;
    owned_alloc_context = "";
    owned_allocation_recording_enabled_cache = null;
    owned_allocation_records = [_]OwnedAllocationRecord{.{}} ** owned_peak_snapshot_capacity;
}

pub const DeviceStorage = struct {
    /// Shared owned buffer record. Released when the last MetalTensor view
    /// drops it.
    ref: *DeviceBufferRef,
    byte_offset: usize,
    byte_len: usize,
    /// When present, the host fields (`data`/`len`) point to this cached
    /// mirror. `mirror_owned` tracks whether deinit must free it.
    mirror_owned: bool = false,
};

extern fn termite_metal_buffer_alloc(
    runtime: *anyopaque,
    length: usize,
    storage_mode: c_int,
) ?*anyopaque;
extern fn termite_metal_buffer_release(handle: *anyopaque) void;
extern fn termite_metal_decode_runtime_release_buffer(runtime: *anyopaque, handle: *anyopaque) void;
extern fn termite_metal_buffer_contents(handle: *anyopaque) ?*anyopaque;
extern fn termite_metal_buffer_download(
    runtime: *anyopaque,
    handle: *anyopaque,
    offset: usize,
    dst: *anyopaque,
    length: usize,
) c_int;
extern fn termite_metal_decode_runtime_flush_active_frame(runtime: *anyopaque) c_int;
extern fn termite_metal_decode_runtime_has_active_frame(runtime: *anyopaque) c_int;
extern fn termite_metal_decode_runtime_retain_active_frame_buffer(runtime: *anyopaque, handle: *anyopaque) c_int;
extern fn termite_metal_buffer_upload(
    runtime: *anyopaque,
    handle: *anyopaque,
    offset: usize,
    src: *const anyopaque,
    length: usize,
) c_int;
extern fn termite_metal_buffer_copy(
    runtime: *anyopaque,
    src_handle: *anyopaque,
    src_offset: usize,
    dst_handle: *anyopaque,
    dst_offset: usize,
    length: usize,
) c_int;

pub const MetalTensor = struct {
    shape_buf: [max_dims]i32 = [_]i32{0} ** max_dims,
    shape_len: u8 = 0,
    dtype: DType = .f32,

    /// Host-backed pointer. For device-only tensors this is populated lazily
    /// by `toHostSlice` (or immediately when the device buffer is Shared).
    data: [*]f32,
    /// Element count (f32 slots) of the host view.
    len: usize,
    /// Frees `data[0..len]` via `std.heap.c_allocator.free` in `deinit`.
    owned_by_c_allocator: bool,

    /// Optional device-backed storage. When present, `data`/`len` may either
    /// alias the Shared-storage contents pointer (zero-copy) or be a cached
    /// host mirror (populated by `toHostSlice`).
    device: ?DeviceStorage = null,

    pub fn borrowed(data: [*]f32, len: usize, dims: []const i32) MetalTensor {
        std.debug.assert(dims.len <= max_dims);
        var t = MetalTensor{
            .shape_len = @intCast(dims.len),
            .data = data,
            .len = len,
            .owned_by_c_allocator = false,
        };
        for (dims, 0..) |axis_dim, i| t.shape_buf[i] = axis_dim;
        return t;
    }

    pub fn owned(data: []f32, dims: []const i32) MetalTensor {
        std.debug.assert(dims.len <= max_dims);
        var t = MetalTensor{
            .shape_len = @intCast(dims.len),
            .data = data.ptr,
            .len = data.len,
            .owned_by_c_allocator = true,
        };
        for (dims, 0..) |axis_dim, i| t.shape_buf[i] = axis_dim;
        return t;
    }

    /// Copy `source` into a freshly-allocated c_allocator buffer and return a
    /// MetalTensor that owns it. Convenient for slot storage where the caller
    /// only has a borrowed view of weights and wants a persistent copy.
    pub fn ownedCloneFrom(source: []const f32, dims: []const i32) !MetalTensor {
        const buf = try std.heap.c_allocator.alloc(f32, source.len);
        @memcpy(buf, source);
        return MetalTensor.owned(buf, dims);
    }

    /// Wrap an existing MTLBuffer handle as a device-resident tensor. Takes
    /// ownership: `deinit` will release the handle via
    /// `termite_metal_buffer_release`. Host `data`/`len` are left as a
    /// placeholder with len=0 — callers must go through `toHostSlice` to
    /// materialize host storage.
    pub fn deviceOwned(
        runtime: *anyopaque,
        handle: *anyopaque,
        byte_offset: usize,
        byte_len: usize,
        dims: []const i32,
    ) MetalTensor {
        std.debug.assert(dims.len <= max_dims);
        var t = MetalTensor{
            .shape_len = @intCast(dims.len),
            .data = @ptrFromInt(@alignOf(f32)),
            .len = 0,
            .owned_by_c_allocator = false,
            .device = .{
                .ref = createDeviceBufferRef(runtime, handle, true, byte_len),
                .byte_offset = byte_offset,
                .byte_len = byte_len,
            },
        };
        noteDeviceOwnedCreate(handle, byte_len, dims);
        for (dims, 0..) |axis_dim, i| t.shape_buf[i] = axis_dim;
        return t;
    }

    /// Wrap a runtime-owned device buffer without taking ownership of the
    /// underlying MTLBuffer handle. Views retain this wrapper only; dropping
    /// the last tensor does not release the Metal buffer.
    pub fn deviceBorrowed(
        runtime: *anyopaque,
        handle: *anyopaque,
        byte_offset: usize,
        byte_len: usize,
        dims: []const i32,
    ) MetalTensor {
        std.debug.assert(dims.len <= max_dims);
        var t = MetalTensor{
            .shape_len = @intCast(dims.len),
            .data = @ptrFromInt(@alignOf(f32)),
            .len = 0,
            .owned_by_c_allocator = false,
            .device = .{
                .ref = createDeviceBufferRef(runtime, handle, false, byte_len),
                .byte_offset = byte_offset,
                .byte_len = byte_len,
            },
        };
        memory_stats.device_borrowed_tensors_created += 1;
        for (dims, 0..) |axis_dim, i| t.shape_buf[i] = axis_dim;
        return t;
    }

    fn createDeviceBufferRef(runtime: *anyopaque, handle: *anyopaque, release_on_drop: bool, byte_len: usize) *DeviceBufferRef {
        const ref = std.heap.c_allocator.create(DeviceBufferRef) catch @panic("Metal buffer ref alloc failed");
        ref.* = .{
            .handle = handle,
            .runtime = runtime,
            .byte_len = byte_len,
            .ref_count = 1,
            .released = false,
            .release_on_drop = release_on_drop,
        };
        return ref;
    }

    fn retainDeviceBuffer(ref: *DeviceBufferRef) !*DeviceBufferRef {
        if (ref.released or ref.ref_count == 0) return error.ReleasedDeviceBuffer;
        ref.ref_count += 1;
        memory_stats.retained_device_views_created += 1;
        return ref;
    }

    fn releaseDeviceBuffer(ref: *DeviceBufferRef) void {
        if (ref.ref_count == 0) return;
        ref.ref_count -= 1;
        if (ref.ref_count == 0 and ref.release_on_drop and !ref.released) {
            termite_metal_decode_runtime_release_buffer(ref.runtime, ref.handle);
            ref.released = true;
            noteDeviceOwnedRelease(ref.handle, ref.byte_len);
        }
        if (ref.ref_count == 0) {
            std.heap.c_allocator.destroy(ref);
        }
    }

    /// Allocate a fresh device buffer of `byte_len` bytes on the runtime's
    /// Metal device and wrap it as an owned device tensor.
    pub fn deviceAllocate(
        runtime: *anyopaque,
        byte_len: usize,
        mode: StorageMode,
        dims: []const i32,
    ) !MetalTensor {
        const handle = termite_metal_buffer_alloc(runtime, byte_len, @intFromEnum(mode)) orelse
            return error.MetalBufferAllocFailed;
        var tensor = deviceOwned(runtime, handle, 0, byte_len, dims);
        errdefer tensor.deinit();
        try tensor.retainForActiveFrame();
        return tensor;
    }

    pub fn retainedCopy(self: *const MetalTensor) !MetalTensor {
        if (self.device) |d| {
            return deviceView(try retainDeviceBuffer(d.ref), d.byte_offset, d.byte_len, self.shape());
        }
        return ownedCloneFrom(self.data[0..self.len], self.shape());
    }

    fn deviceView(
        ref: *DeviceBufferRef,
        byte_offset: usize,
        byte_len: usize,
        dims: []const i32,
    ) MetalTensor {
        std.debug.assert(dims.len <= max_dims);
        var t = MetalTensor{
            .shape_len = @intCast(dims.len),
            .data = @ptrFromInt(@alignOf(f32)),
            .len = 0,
            .owned_by_c_allocator = false,
            .device = .{
                .ref = ref,
                .byte_offset = byte_offset,
                .byte_len = byte_len,
            },
        };
        for (dims, 0..) |axis_dim, i| t.shape_buf[i] = axis_dim;
        return t;
    }

    pub fn retainedView(
        self: *const MetalTensor,
        byte_offset_delta: usize,
        byte_len: usize,
        dims: []const i32,
    ) !MetalTensor {
        if (self.device) |d| {
            if (byte_offset_delta + byte_len > d.byte_len) return error.InvalidTensorShape;
            return deviceView(try retainDeviceBuffer(d.ref), d.byte_offset + byte_offset_delta, byte_len, dims);
        }
        const start = byte_offset_delta / @sizeOf(f32);
        const count = byte_len / @sizeOf(f32);
        if (start + count > self.len) return error.InvalidTensorShape;
        return ownedCloneFrom(self.data[start .. start + count], dims);
    }

    pub fn retainedStorageView(
        self: *const MetalTensor,
        byte_offset_delta: usize,
        byte_len: usize,
        dims: []const i32,
    ) !MetalTensor {
        if (self.device) |d| {
            if (byte_offset_delta + byte_len > d.ref.byte_len - d.byte_offset) return error.InvalidTensorShape;
            return deviceView(try retainDeviceBuffer(d.ref), d.byte_offset + byte_offset_delta, byte_len, dims);
        }
        return self.retainedView(byte_offset_delta, byte_len, dims);
    }

    pub fn copiedView(
        self: *const MetalTensor,
        byte_offset_delta: usize,
        byte_len: usize,
        dims: []const i32,
    ) !MetalTensor {
        if (self.device) |d| {
            if (byte_offset_delta + byte_len > d.byte_len) return error.InvalidTensorShape;
            const copied = termite_metal_buffer_alloc(d.ref.runtime, byte_len, @intFromEnum(StorageMode.private)) orelse
                return error.MetalBufferAllocFailed;
            errdefer termite_metal_buffer_release(copied);
            if (termite_metal_buffer_copy(d.ref.runtime, d.ref.handle, d.byte_offset + byte_offset_delta, copied, 0, byte_len) != 0) {
                return error.MetalBufferCopyFailed;
            }
            return deviceOwned(d.ref.runtime, copied, 0, byte_len, dims);
        }
        const start = byte_offset_delta / @sizeOf(f32);
        const count = byte_len / @sizeOf(f32);
        if (start + count > self.len) return error.InvalidTensorShape;
        return ownedCloneFrom(self.data[start .. start + count], dims);
    }

    pub fn deinit(self: *MetalTensor) void {
        if (self.device) |*d| {
            // If the host view was a c_allocator-owned mirror, free it.
            if (d.mirror_owned and self.owned_by_c_allocator) {
                noteHostMirrorFree(self.len * @sizeOf(f32));
                std.heap.c_allocator.free(self.data[0..self.len]);
            }
            releaseDeviceBuffer(d.ref);
        } else if (self.owned_by_c_allocator) {
            std.heap.c_allocator.free(self.data[0..self.len]);
        }
        self.* = undefined;
    }

    pub fn shape(self: *const MetalTensor) []const i32 {
        return self.shape_buf[0..self.shape_len];
    }

    pub fn dim(self: MetalTensor, axis: usize) i32 {
        return self.shape_buf[axis];
    }

    pub fn ndim(self: MetalTensor) usize {
        return self.shape_len;
    }

    /// Element count (f32 slots). For device-only tensors this reports the
    /// device byte length in f32 units even before a host mirror exists.
    pub fn elemCount(self: *const MetalTensor) usize {
        if (self.device) |d| return d.byte_len / @sizeOf(f32);
        return self.len;
    }

    /// Host-backed view. Assumes the tensor has valid host storage — device
    /// tensors without a materialized host mirror will return the placeholder
    /// slice (len=0). Callers that might see device-only tensors should use
    /// `toHostSlice` instead.
    pub fn slice(self: MetalTensor) []f32 {
        return self.data[0..self.len];
    }

    pub fn isDevice(self: *const MetalTensor) bool {
        return self.device != null;
    }

    pub fn isOwnedDeviceBuffer(self: *const MetalTensor) bool {
        const d = self.device orelse return false;
        if (d.ref.released or d.ref.ref_count == 0) return false;
        return d.ref.release_on_drop;
    }

    pub fn deviceHandle(self: *const MetalTensor) ?*anyopaque {
        if (self.device) |d| {
            if (d.ref.released or d.ref.ref_count == 0) return null;
            return d.ref.handle;
        }
        return null;
    }

    pub fn deviceByteOffset(self: *const MetalTensor) usize {
        if (self.device) |d| return d.byte_offset;
        return 0;
    }

    pub fn deviceByteLen(self: *const MetalTensor) usize {
        if (self.device) |d| return d.byte_len;
        return self.len * @sizeOf(f32);
    }

    pub fn retainForActiveFrame(self: *const MetalTensor) !void {
        const d = self.device orelse return;
        const handle = self.deviceHandle() orelse return error.ReleasedDeviceBuffer;
        const rc = termite_metal_decode_runtime_retain_active_frame_buffer(d.ref.runtime, handle);
        if (rc != 0) return error.MetalFrameRetainFailed;
    }

    pub fn invalidateHostMirror(self: *MetalTensor) void {
        if (self.device) |*d| {
            if (d.mirror_owned and self.owned_by_c_allocator) {
                noteHostMirrorFree(self.len * @sizeOf(f32));
                std.heap.c_allocator.free(self.data[0..self.len]);
            }
            self.data = @ptrFromInt(@alignOf(f32));
            self.len = 0;
            self.owned_by_c_allocator = false;
            d.mirror_owned = false;
        }
    }

    pub fn copyInto(self: *const MetalTensor, dst: *MetalTensor) !void {
        const dst_dev = dst.device orelse return error.UnsupportedTensorType;
        if (dst_dev.ref.released or dst_dev.ref.ref_count == 0) return error.ReleasedDeviceBuffer;
        const byte_len = self.deviceByteLen();
        if (byte_len != dst.deviceByteLen()) return error.InvalidTensorShape;

        dst.invalidateHostMirror();

        if (self.device) |src_dev| {
            if (src_dev.ref.released or src_dev.ref.ref_count == 0) return error.ReleasedDeviceBuffer;
            const rc = termite_metal_buffer_copy(
                dst_dev.ref.runtime,
                src_dev.ref.handle,
                src_dev.byte_offset,
                dst_dev.ref.handle,
                dst_dev.byte_offset,
                byte_len,
            );
            if (rc != 0) return error.MetalBufferCopyFailed;
            return;
        }

        if (self.len * @sizeOf(f32) != byte_len) return error.InvalidTensorShape;
        const rc = termite_metal_buffer_upload(
            dst_dev.ref.runtime,
            dst_dev.ref.handle,
            dst_dev.byte_offset,
            @ptrCast(self.data),
            byte_len,
        );
        if (rc != 0) return error.MetalBufferUploadFailed;
    }

    /// Return a host-backed `[]f32` view. For pure host tensors this is
    /// zero-cost. For device-backed tensors this reuses the cached mirror
    /// if present, otherwise aliases the Shared-storage contents pointer
    /// when available, or allocates + downloads from Private storage.
    pub fn toHostSlice(self: *MetalTensor) ![]f32 {
        memory_stats.to_host_calls += 1;
        if (self.device == null) return self.data[0..self.len];
        memory_stats.to_host_device_calls += 1;
        const dev = &self.device.?;
        if (dev.ref.released or dev.ref.ref_count == 0) return error.ReleasedDeviceBuffer;
        const count = dev.byte_len / @sizeOf(f32);
        const trace_limit = traceToHostLimit();
        if (to_host_trace_count < trace_limit) {
            to_host_trace_count += 1;
            std.debug.print(
                "metal_tensor_to_host: caller=0x{x} bytes={d} shape={any} cached_len={d}\n",
                .{ @returnAddress(), dev.byte_len, self.shape(), self.len },
            );
            if (to_host_trace_count <= traceToHostStackLimit()) {
                std.debug.dumpCurrentStackTrace(.{
                    .first_address = @returnAddress(),
                    .allow_unsafe_unwind = false,
                });
            }
        }

        // Mirror already populated by a prior call — host fields hold count
        // elements, either aliasing Shared contents or a downloaded Private
        // mirror. Reuse it, but only after the same active-frame flush the
        // first-read path performs below: a runtime frame may still be
        // queuing GPU writes to this buffer (in-place updates, planned-frame
        // commands), so returning the cached mirror without draining the
        // frame is a stale-readback corruption class. The flush is a cheap
        // no-op when no frame is active.
        if (self.len == count) {
            const frame_was_active = termite_metal_decode_runtime_has_active_frame(dev.ref.runtime) != 0;
            if (termite_metal_decode_runtime_flush_active_frame(dev.ref.runtime) != 0) {
                return error.MetalFrameSyncFailed;
            }
            if (frame_was_active and dev.mirror_owned) {
                // Owned mirrors are Private-storage snapshots (Shared
                // mirrors alias device memory and are coherent after the
                // flush). Refresh the snapshot so writes queued since it was
                // taken become visible.
                const rc = termite_metal_buffer_download(
                    dev.ref.runtime,
                    dev.ref.handle,
                    dev.byte_offset,
                    @ptrCast(self.data),
                    dev.byte_len,
                );
                if (rc != 0) return error.MetalBufferDownloadFailed;
            }
            return self.data[0..self.len];
        }

        // If this tensor was produced inside an active runtime frame, host
        // materialization is an explicit synchronization boundary. Drain the
        // current frame and reopen it so higher-level frame ownership can
        // continue after the host read.
        if (termite_metal_decode_runtime_flush_active_frame(dev.ref.runtime) != 0) {
            return error.MetalFrameSyncFailed;
        }

        // Fast path: Shared storage exposes its contents pointer directly.
        if (termite_metal_buffer_contents(dev.ref.handle)) |raw| {
            const base: [*]f32 = @ptrCast(@alignCast(raw));
            const start = dev.byte_offset / @sizeOf(f32);
            self.data = base + start;
            self.len = count;
            self.owned_by_c_allocator = false;
            dev.mirror_owned = false;
            memory_stats.shared_host_aliases += 1;
            return self.data[0..self.len];
        }

        // Fallback: allocate a c_allocator mirror and download Private bytes.
        const buf = try std.heap.c_allocator.alloc(f32, count);
        errdefer std.heap.c_allocator.free(buf);
        const rc = termite_metal_buffer_download(
            dev.ref.runtime,
            dev.ref.handle,
            dev.byte_offset,
            @ptrCast(buf.ptr),
            dev.byte_len,
        );
        if (rc != 0) return error.MetalBufferDownloadFailed;
        self.data = buf.ptr;
        self.len = buf.len;
        self.owned_by_c_allocator = true;
        dev.mirror_owned = true;
        noteHostMirrorAlloc(dev.byte_len);
        return buf;
    }

    /// Synchronize host-visible state with the final device contents.
    /// Drains any active runtime frame so queued GPU writes land, then
    /// refreshes an owned (Private-storage) host mirror that may have
    /// been snapshotted before later device writes. Shared-storage
    /// mirrors alias device memory and are coherent once the frame is
    /// drained; device tensors without a cached mirror need no refresh
    /// (their first `toHostSlice` downloads fresh bytes). Host-only
    /// tensors are a no-op. Used for values that escape an execution
    /// scope (graph outputs read after the owning frame completed).
    pub fn syncHostMirror(self: *MetalTensor) !void {
        if (self.device) |*dev| {
            if (dev.ref.released or dev.ref.ref_count == 0) return error.ReleasedDeviceBuffer;
            if (termite_metal_decode_runtime_flush_active_frame(dev.ref.runtime) != 0) {
                return error.MetalFrameSyncFailed;
            }
            const count = dev.byte_len / @sizeOf(f32);
            if (self.len == count and dev.mirror_owned) {
                const rc = termite_metal_buffer_download(
                    dev.ref.runtime,
                    dev.ref.handle,
                    dev.byte_offset,
                    @ptrCast(self.data),
                    dev.byte_len,
                );
                if (rc != 0) return error.MetalBufferDownloadFailed;
            }
        }
    }

    /// Diagnostic only: abs-sum of the RAW device bytes via a fresh
    /// download (frame-flushed), bypassing any cached host mirror and
    /// mutating no tensor state. Host-only tensors sum their host data.
    pub fn debugRawDeviceAbsSum(self: *MetalTensor) !f64 {
        const dev = if (self.device) |*d| d else {
            var acc: f64 = 0;
            for (self.data[0..self.len]) |v| acc += @abs(v);
            return acc;
        };
        if (dev.ref.released or dev.ref.ref_count == 0) return error.ReleasedDeviceBuffer;
        if (termite_metal_decode_runtime_flush_active_frame(dev.ref.runtime) != 0) {
            return error.MetalFrameSyncFailed;
        }
        const count = dev.byte_len / @sizeOf(f32);
        const buf = try std.heap.c_allocator.alloc(f32, count);
        defer std.heap.c_allocator.free(buf);
        const rc = termite_metal_buffer_download(
            dev.ref.runtime,
            dev.ref.handle,
            dev.byte_offset,
            @ptrCast(buf.ptr),
            dev.byte_len,
        );
        if (rc != 0) return error.MetalBufferDownloadFailed;
        var acc: f64 = 0;
        for (buf) |v| acc += @abs(v);
        return acc;
    }

    /// Diagnostic only: CPU reference for a last-dim row-sum reduce over
    /// the RAW device bytes (fresh frame-flushed download). Returns the
    /// sum of |row sums| so it is directly comparable with an abs-sum of
    /// the reduce kernel's output.
    pub fn debugRawDeviceRowSumAbs(self: *MetalTensor, row_count: usize, row_width: usize) !f64 {
        const dev = if (self.device) |*d| d else return error.UnsupportedTensorType;
        if (dev.ref.released or dev.ref.ref_count == 0) return error.ReleasedDeviceBuffer;
        if (termite_metal_decode_runtime_flush_active_frame(dev.ref.runtime) != 0) {
            return error.MetalFrameSyncFailed;
        }
        const count = dev.byte_len / @sizeOf(f32);
        if (row_count * row_width > count) return error.InvalidTensorShape;
        const buf = try std.heap.c_allocator.alloc(f32, count);
        defer std.heap.c_allocator.free(buf);
        const rc = termite_metal_buffer_download(
            dev.ref.runtime,
            dev.ref.handle,
            dev.byte_offset,
            @ptrCast(buf.ptr),
            dev.byte_len,
        );
        if (rc != 0) return error.MetalBufferDownloadFailed;
        var acc: f64 = 0;
        for (0..row_count) |r| {
            var row_sum: f64 = 0;
            for (0..row_width) |c| row_sum += buf[r * row_width + c];
            acc += @abs(row_sum);
        }
        return acc;
    }
};

test "MetalTensor borrowed does not free" {
    const allocator = std.testing.allocator;
    const buf = try allocator.alloc(f32, 4);
    defer allocator.free(buf);
    @memset(buf, 1.5);
    const shape_arr = [_]i32{ 2, 2 };
    var t = MetalTensor.borrowed(buf.ptr, buf.len, &shape_arr);
    try std.testing.expectEqual(@as(usize, 2), t.ndim());
    try std.testing.expectEqual(@as(i32, 2), t.dim(0));
    t.deinit();
    try std.testing.expectEqual(@as(f32, 1.5), buf[0]);
}

test "MetalTensor owned frees its buffer" {
    const shape_arr = [_]i32{4};
    var t = try MetalTensor.ownedCloneFrom(&[_]f32{ 1, 2, 3, 4 }, &shape_arr);
    try std.testing.expect(t.owned_by_c_allocator);
    try std.testing.expectEqual(@as(usize, 4), t.elemCount());
    t.deinit();
}

test "MetalTensor retainedCopy reports stale device refs without aborting" {
    const ref = try std.heap.c_allocator.create(DeviceBufferRef);
    defer std.heap.c_allocator.destroy(ref);
    ref.* = .{
        .handle = @ptrFromInt(@alignOf(f32)),
        .runtime = @ptrFromInt(@alignOf(f32)),
        .byte_len = 4 * @sizeOf(f32),
        .ref_count = 0,
        .released = true,
        .release_on_drop = true,
    };
    const shape_arr = [_]i32{4};
    var stale = MetalTensor.deviceView(ref, 0, 4 * @sizeOf(f32), &shape_arr);
    defer stale.deinit();

    try std.testing.expectError(error.ReleasedDeviceBuffer, stale.retainedCopy());
}
