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

// MetalTensor — MLX-free tensor wrapper for the Metal runtime's public surface.
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

fn noteDeviceOwnedCreate(byte_len: usize) void {
    memory_stats.device_owned_buffers_created += 1;
    memory_stats.device_owned_bytes_created += @intCast(byte_len);
    memory_stats.device_owned_live_bytes += @intCast(byte_len);
    memory_stats.device_owned_peak_live_bytes = @max(memory_stats.device_owned_peak_live_bytes, memory_stats.device_owned_live_bytes);
}

fn noteDeviceOwnedRelease(byte_len: usize) void {
    memory_stats.device_owned_buffers_released += 1;
    memory_stats.device_owned_bytes_released += @intCast(byte_len);
    memory_stats.device_owned_live_bytes -|= @intCast(byte_len);
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
        noteDeviceOwnedCreate(byte_len);
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
            noteDeviceOwnedRelease(ref.byte_len);
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
        // mirror. Reuse it.
        if (self.len == count) return self.data[0..self.len];

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
