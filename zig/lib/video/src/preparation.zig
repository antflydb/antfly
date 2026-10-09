// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const media = @import("antfly_media");
const image = @import("antfly_image").processing;
const apple = @import("backends/apple.zig");
const supported = @import("builtin").os.tag == .macos;
extern fn av_preparer_create(*anyopaque, *?*anyopaque) i32;
extern fn av_preparer_destroy(*anyopaque) void;
extern fn av_coefficients_create(*anyopaque, [*]const u32, usize, [*]const i32, usize, [*]const u32, usize, [*]const i32, usize, *?*anyopaque) i32;
extern fn av_coefficients_destroy(*anyopaque) void;
extern fn av_prepare_submit(*anyopaque, *anyopaque, [*]const u32, *anyopaque, *?*anyopaque) i32;
extern fn av_prepare_native_submit(*anyopaque, *anyopaque, *anyopaque, [*]const u32, *anyopaque, *?*anyopaque) i32;
extern fn av_prepare_host_submit(*anyopaque, [*]const u8, usize, [*]const u8, usize, [*]const u32, *anyopaque, *?*anyopaque) i32;
extern fn av_prepare_rgba_submit(*anyopaque, [*]const u8, usize, [*]const u32, *anyopaque, *?*anyopaque) i32;
extern fn av_prepared_gpu_seconds(*anyopaque) f64;
extern fn av_preparer_device_bytes(*anyopaque) u64;
extern fn av_prepared_poll(*anyopaque) c_int;
extern fn av_prepared_buffer(*anyopaque) *anyopaque;
extern fn av_prepared_copy(*anyopaque, [*]f32, usize) i32;
extern fn av_prepared_release_source(*anyopaque) c_int;
extern fn av_prepared_destroy(*anyopaque) void;

pub const Rotation = enum(u32) { none, clockwise90, half_turn, clockwise270 };
pub const Matrix = enum(u32) { bt601, bt709 };
pub const Options = struct {
    /// Geometry comes from the pinned processor adapter, never inferred from FPS.
    width: u32,
    height: u32,
    /// Required explicit SDR color matrix; range comes from the NV12 surface.
    matrix: Matrix,
    rotation: Rotation = .none,
    /// Rescale to [0,1], then center for HF vision patch linear inputs if requested.
    centered: bool = true,
    /// Torchvision rounds intermediate RGB8 stages; Pillow truncates them.
    torchvision: bool = false,
    max_soft_tokens: usize = 140,
    max_source_pixels: usize = 16 * 1024 * 1024,
    max_scratch_bytes: usize = 128 * 1024 * 1024,
    /// Copied RGBA input only; independent of resize scratch/output admission.
    max_host_staging_bytes: usize = 128 * 1024 * 1024,
};
pub const Geometry = struct {
    width: u32,
    height: u32,
    patches: usize,
    soft_tokens: usize,
    pub fn values(self: Geometry) usize {
        return self.patches * 768;
    }
};
pub fn geometry(source_width: u32, source_height_u32: u32, options: Options) !Geometry {
    if (source_width == 0 or source_height_u32 == 0 or options.width == 0 or options.height == 0) return error.InvalidVideoGeometry;
    // Match the qualified texture/display bounds even when caller budgets grow.
    if (source_width > 16_384 or source_height_u32 > 16_384 or options.width > 16_384 or options.height > 16_384) return error.ResourceLimitExceeded;
    if (options.width % 48 != 0 or options.height % 48 != 0) return error.InvalidVideoGeometry;
    const pixels = try std.math.mul(usize, options.width, options.height);
    const source_pixels = try std.math.mul(usize, source_width, source_height_u32);
    const patches = pixels / 256;
    if (patches / 9 > options.max_soft_tokens or source_pixels > options.max_source_pixels) return error.ResourceLimitExceeded;
    const source_height = if (options.rotation == .clockwise90 or options.rotation == .clockwise270) source_width else source_height_u32;
    const horizontal_bytes = try std.math.mul(usize, try std.math.mul(usize, options.width, source_height), 3);
    const output_bytes = try std.math.mul(usize, pixels, 3 * @sizeOf(f32));
    const rgb_bytes = try std.math.mul(usize, source_pixels, 3);
    // One conservative bound covers both reference and device paths, including
    // CPU result/patch repacking. Coefficient storage is checked separately.
    const scratch = try std.math.add(usize, try std.math.add(usize, horizontal_bytes, rgb_bytes), try std.math.mul(usize, output_bytes, 2));
    if (scratch > options.max_scratch_bytes / 2) return error.ResourceLimitExceeded;
    return .{ .width = options.width, .height = options.height, .patches = patches, .soft_tokens = patches / 9 };
}
fn sourcePoint(x: usize, y: usize, width: usize, height: usize, rotation: Rotation) [2]usize {
    return switch (rotation) {
        .none => .{ x, y },
        .clockwise90 => .{ y, height - 1 - x },
        .half_turn => .{ width - 1 - x, height - 1 - y },
        .clockwise270 => .{ width - 1 - y, x },
    };
}
fn hostSample(bytes: []const u8, offset: usize, bit_depth: u8) u16 {
    return if (bit_depth == 8) bytes[offset] else std.mem.readInt(u16, bytes[offset..][0..2], .little);
}
fn rgb(planes: [2]apple.Plane, x: usize, y: usize, format: apple.Format, matrix: Matrix, bit_depth: u8, chroma_format: u8) [3]u8 {
    const bytes: usize = if (bit_depth == 8) 1 else 2;
    const scale: f32 = @floatFromInt(@as(u32, 1) << @as(u5, @intCast(bit_depth - 8)));
    var luma: f32 = @as(f32, @floatFromInt(hostSample(planes[0].bytes, y * planes[0].stride + x * bytes, bit_depth))) / scale;
    const sub_x: usize = if (chroma_format == 3) 1 else 2;
    const uv = (y / (if (chroma_format == 1) @as(usize, 2) else 1)) * planes[1].stride + (x / sub_x) * 2 * bytes;
    var u: f32 = if (chroma_format == 0) 0 else @as(f32, @floatFromInt(hostSample(planes[1].bytes, uv, bit_depth))) / scale - 128;
    var v: f32 = if (chroma_format == 0) 0 else @as(f32, @floatFromInt(hostSample(planes[1].bytes, uv + bytes, bit_depth))) / scale - 128;
    if (format == .nv12_video) {
        luma = (luma - 16) * (255.0 / 219.0);
        u *= 255.0 / 224.0;
        v *= 255.0 / 224.0;
    } else if (bit_depth != 8) {
        const maximum: f32 = @floatFromInt((@as(u32, 1) << @as(u5, @intCast(bit_depth))) - 1);
        const full_scale = 255 * scale / maximum;
        luma *= full_scale;
        u *= full_scale;
        v *= full_scale;
    }
    const values: [3]f32 = if (matrix == .bt709)
        .{ luma + 1.5748 * v, luma - 0.187324 * u - 0.468124 * v, luma + 1.8556 * u }
    else
        .{ luma + 1.402 * v, luma - 0.344136 * u - 0.714136 * v, luma + 1.772 * u };
    var output: [3]u8 = undefined;
    for (&output, values) |*out, value| out.* = @intFromFloat(std.math.clamp(@floor(value + 0.5), 0, 255));
    return output;
}
/// Reference path deliberately materializes host RGB. Production Metal path
/// imports NV12 and keeps both resize passes and patch packing on device.
pub const HostSurface = struct {
    /// Right-aligned little-endian u16 samples when depth exceeds 8.
    bit_depth: u8 = 8,
    chroma_format: u8 = 1,
    width: u32,
    height: u32,
    format: apple.Format,
    planes: [2]apple.Plane,
    pub fn validate(self: HostSurface) !void {
        if (self.bit_depth < 8 or self.bit_depth > 14 or self.chroma_format > 3) return error.UnsupportedSurfaceFormat;
        if (self.width == 0 or self.height == 0) return error.InvalidVideoGeometry;
        if (self.width > 16_384 or self.height > 16_384) return error.ResourceLimitExceeded;
        for (self.planes, 0..) |plane, i| {
            if (i == 1 and self.chroma_format == 0) {
                if (plane.width != 0 or plane.height != 0 or plane.stride != 0 or plane.bytes.len != 0) return error.UnsupportedSurfaceFormat;
                continue;
            }
            const width = if (i == 0 or self.chroma_format == 3) self.width else (self.width + 1) / 2;
            const height = if (i == 0 or self.chroma_format >= 2) self.height else (self.height + 1) / 2;
            const row = try std.math.mul(usize, width, (if (i == 0) @as(usize, 1) else 2) * (if (self.bit_depth == 8) @as(usize, 1) else 2));
            const size = try std.math.mul(usize, plane.stride, height);
            if (plane.width != width or plane.height != height or plane.stride < row or plane.bytes.len < size) return error.UnsupportedSurfaceFormat;
        }
    }
};
pub fn reference(allocator: std.mem.Allocator, surface: *const apple.Surface, options: Options, control: media.source.Control) ![]f32 {
    try control.check();
    _ = try geometry(surface.width, surface.height, options);
    var mapping = try surface.map();
    defer mapping.deinit();
    return referenceHost(allocator, .{ .width = surface.width, .height = surface.height, .format = surface.format, .planes = mapping.planes }, options, control);
}
/// Portable borrowed NV12 boundary. Plane storage must survive the synchronous
/// call. No Apple framework or native device is needed for the reference path.
pub fn referenceHost(allocator: std.mem.Allocator, host: HostSurface, options: Options, control: media.source.Control) ![]f32 {
    try control.check();
    const g = try geometry(host.width, host.height, options);
    const rotated = options.rotation == .clockwise90 or options.rotation == .clockwise270;
    const width = if (rotated) host.height else host.width;
    const height = if (rotated) host.width else host.height;
    try host.validate();
    const bytes = try allocator.alloc(u8, @as(usize, width) * height * 3);
    defer allocator.free(bytes);
    for (0..height) |y| {
        try control.check();
        for (0..width) |x| {
            const point = sourcePoint(x, y, host.width, host.height, options.rotation);
            @memcpy(bytes[(y * width + x) * 3 ..][0..3], &rgb(host.planes, point[0], point[1], host.format, options.matrix, host.bit_depth, host.chroma_format));
        }
    }
    return prepareRgb(allocator, bytes, width, height, g, options, control);
}
/// Portable RGBA input, including pure Zig MJPEG output. Uses the same resize
/// coefficients/patch packing as the NV12 reference after color conversion.
pub fn referenceRgba(allocator: std.mem.Allocator, bytes: []const u8, source_width: u32, source_height: u32, options: Options, control: media.source.Control) ![]f32 {
    try control.check();
    const g = try geometry(source_width, source_height, options);
    if (bytes.len != @as(usize, source_width) * source_height * 4) return error.InvalidVideoGeometry;
    const rotated = options.rotation == .clockwise90 or options.rotation == .clockwise270;
    const width = if (rotated) source_height else source_width;
    const height = if (rotated) source_width else source_height;
    const rgb_bytes = try allocator.alloc(u8, @as(usize, width) * height * 3);
    defer allocator.free(rgb_bytes);
    for (0..height) |y| {
        try control.check();
        for (0..width) |x| {
            const point = sourcePoint(x, y, source_width, source_height, options.rotation);
            const offset = (point[1] * source_width + point[0]) * 4;
            @memcpy(rgb_bytes[(y * width + x) * 3 ..][0..3], bytes[offset..][0..3]);
        }
    }
    return prepareRgb(allocator, rgb_bytes, width, height, g, options, control);
}
fn prepareRgb(allocator: std.mem.Allocator, bytes: []const u8, width: usize, height: usize, g: Geometry, options: Options, control: media.source.Control) ![]f32 {
    // Shared image control follows the same deadline through both resize passes.
    var scope = image.work_control.Scope.enter(.{ .context = control.context, .check_fn = control.check_fn });
    defer scope.deinit();
    const chw = try image.preprocessDecodedRectScaledWithResample(allocator, .{ .data = bytes, .width = @intCast(width), .height = @intCast(height), .format = .rgb8 }, g.width, g.height, .{ 0, 0, 0 }, .{ 1, 1, 1 }, 1.0 / 255.0, if (options.torchvision) .torchvision_bicubic else .pillow_bicubic);
    defer allocator.free(chw);
    const result = try allocator.alloc(f32, g.values());
    errdefer allocator.free(result);
    for (0..g.height) |y| {
        try control.check();
        for (0..g.width) |x| {
            const patch = (y / 16) * (g.width / 16) + x / 16;
            for (0..3) |c| {
                const value = chw[c * @as(usize, g.width) * g.height + y * g.width + x];
                result[patch * 768 + ((y % 16) * 16 + x % 16) * 3 + c] = if (options.centered) 2 * (value - 0.5) else value;
            }
        }
    }
    return result;
}
const Axis = struct {
    allocator: std.mem.Allocator,
    table: []u32,
    weights: []i32,
    fn bound(source: usize, target: usize) !usize {
        // Bound the coefficient builder before allocation. Each destination has
        // <= 4*ceil(scale)+2 taps, plus starts/offsets/float staging and packed table.
        const taps = try std.math.add(usize, try std.math.mul(usize, 4, std.math.divCeil(usize, source, target) catch return error.ResourceLimitExceeded), 2);
        return std.math.mul(usize, target, try std.math.add(usize, try std.math.mul(usize, taps, 32), 64));
    }
    fn init(allocator: std.mem.Allocator, source: usize, target: usize, max_bytes: usize, torchvision: bool) !Axis {
        if (try bound(source, target) > max_bytes) return error.ResourceLimitExceeded;
        var axis = try image.buildPillowBicubicAxis(allocator, source, target, torchvision);
        defer axis.deinit();
        // Lift Torchvision int16 coefficients into the shader's fixed 22-bit
        // scale without changing the two-pass integer rounding.
        for (axis.weights) |*weight| weight.* *= @as(i32, 1) << @intCast(22 - axis.precision_bits);
        const table = try allocator.alloc(u32, target * 3);
        errdefer allocator.free(table);
        for (0..target) |i| {
            table[i * 3] = std.math.cast(u32, axis.starts[i]) orelse return error.ResourceLimitExceeded;
            table[i * 3 + 1] = std.math.cast(u32, axis.offsets[i]) orelse return error.ResourceLimitExceeded;
            table[i * 3 + 2] = std.math.cast(u32, axis.offsets[i + 1] - axis.offsets[i]) orelse return error.ResourceLimitExceeded;
            var absolute: u64 = 0;
            for (axis.weights[axis.offsets[i]..axis.offsets[i + 1]]) |weight| absolute += @abs(@as(i64, weight));
            if (absolute * 255 + (1 << 21) > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
        }
        return .{ .allocator = allocator, .table = table, .weights = try allocator.dupe(i32, axis.weights) };
    }
    fn deinit(self: *Axis) void {
        self.allocator.free(self.table);
        self.allocator.free(self.weights);
    }
};
/// Owns a queue/pipelines on the caller's existing Metal device. Single-consumer.
pub const Metal = struct {
    handle: *anyopaque,
    /// One bounded resident geometry. Commands retain replaced buffers through
    /// completion; this owner is single-consumer even across concurrent jobs.
    coefficients: ?*anyopaque = null,
    coefficient_key: [5]u32 = .{ 0, 0, 0, 0, 0 },
    coefficient_bytes: usize = 0,
    coefficient_build_bound: usize = 0,
    coefficient_limit: usize = 16 * 1024 * 1024,
    cache_reservation: media.admission.Token = .{},
    pub fn init(device: *anyopaque) !Metal {
        if (!supported) return error.UnsupportedVideoBackend;
        var out: ?*anyopaque = null;
        if (av_preparer_create(device, &out) != 0) return error.MetalPreparationUnavailable;
        return .{ .handle = out.? };
    }
    pub fn deinit(self: *Metal) void {
        if (supported) {
            if (self.coefficients) |resident| av_coefficients_destroy(resident);
            av_preparer_destroy(self.handle);
            self.cache_reservation.deinit();
        }
        self.* = undefined;
    }
    /// Reserve a persistent coefficient budget on a shared pool. Call before
    /// submitting work. Destruction fences the queue before returning capacity.
    pub fn admit(self: *Metal, pool: *media.admission.Pool, max_coefficient_bytes: usize) !void {
        if (!supported) return error.UnsupportedVideoBackend;
        if (self.coefficients != null or self.cache_reservation.pool != null) return error.MetalPreparerAlreadyUsed;
        self.cache_reservation = try pool.acquire(.{ .device_bytes = max_coefficient_bytes });
        self.coefficient_limit = max_coefficient_bytes;
    }
    /// Device-wide allocated size, including other work using this device.
    pub fn deviceAllocatedBytes(self: *const Metal) u64 {
        if (!supported) return 0;
        return av_preparer_device_bytes(self.handle);
    }
    /// Evict the resident geometry without invalidating submitted commands.
    pub fn clearCoefficients(self: *Metal) void {
        if (supported) if (self.coefficients) |resident| av_coefficients_destroy(resident);
        self.coefficients = null;
        self.coefficient_bytes = 0;
        self.coefficient_build_bound = 0;
    }
    fn ensureCoefficients(self: *Metal, allocator: std.mem.Allocator, width: u32, height: u32, g: Geometry, options: Options, control: media.source.Control) !usize {
        const rotated = options.rotation == .clockwise90 or options.rotation == .clockwise270;
        const key: [5]u32 = .{ if (rotated) height else width, if (rotated) width else height, g.width, g.height, @intFromBool(options.torchvision) };
        if (self.coefficients != null and std.mem.eql(u32, &key, &self.coefficient_key)) {
            if (self.coefficient_build_bound > options.max_scratch_bytes / 4) return error.ResourceLimitExceeded;
            return 0;
        }
        var xaxis = try Axis.init(allocator, key[0], key[2], options.max_scratch_bytes / 4, options.torchvision);
        defer xaxis.deinit();
        var yaxis = try Axis.init(allocator, key[1], key[3], options.max_scratch_bytes / 4, options.torchvision);
        defer yaxis.deinit();
        try control.check();
        const byte_count = (xaxis.table.len + xaxis.weights.len + yaxis.table.len + yaxis.weights.len) * 4;
        if (byte_count > self.coefficient_limit) return error.ResourceLimitExceeded;
        var resident: ?*anyopaque = null;
        if (av_coefficients_create(self.handle, xaxis.table.ptr, xaxis.table.len, xaxis.weights.ptr, xaxis.weights.len, yaxis.table.ptr, yaxis.table.len, yaxis.weights.ptr, yaxis.weights.len, &resident) != 0) return error.MetalPreparationFailed;
        if (self.coefficients) |old| av_coefficients_destroy(old);
        self.coefficients = resident.?;
        self.coefficient_key = key;
        self.coefficient_bytes = byte_count;
        self.coefficient_build_bound = @max(try Axis.bound(key[0], key[2]), try Axis.bound(key[1], key[3]));
        return self.coefficient_bytes;
    }
    pub fn submit(self: *Metal, allocator: std.mem.Allocator, surface: *const apple.Surface, options: Options, control: media.source.Control) !Prepared {
        if (!supported) return error.UnsupportedVideoBackend;
        try control.check();
        var scope = image.work_control.Scope.enter(.{ .context = control.context, .check_fn = control.check_fn });
        defer scope.deinit();
        const g = try geometry(surface.width, surface.height, options);
        const uploaded = try self.ensureCoefficients(allocator, surface.width, surface.height, g, options, control);
        const params = [_]u32{ surface.width, surface.height, g.width, g.height, @backingInt(options.rotation), @backingInt(options.matrix), @intFromBool(surface.format == .nv12_full), @intFromBool(options.centered), 8, 1 };
        var out: ?*anyopaque = null;
        if (av_prepare_submit(self.handle, surface.handle, &params, self.coefficients.?, &out) != 0) return error.MetalPreparationFailed;
        return .{ .handle = out.?, .geometry = g, .coefficient_staging_bytes = uploaded };
    }
    /// Caller-owned integer Metal plane textures on this preparer's device.
    /// R8/RG8Uint or R16/RG16Uint contain right-aligned native samples. The
    /// command retains textures until completion, without pixel readback/copy.
    pub const NativeSurface = struct {
        y: *anyopaque,
        uv: *anyopaque,
        width: u32,
        height: u32,
        bit_depth: u8,
        chroma_format: u8,
        full_range: bool = false,
    };
    pub fn submitNative(self: *Metal, allocator: std.mem.Allocator, surface: NativeSurface, options: Options, control: media.source.Control) !Prepared {
        if (!supported) return error.UnsupportedVideoBackend;
        try control.check();
        if (surface.bit_depth < 8 or surface.bit_depth > 14 or surface.chroma_format < 1 or surface.chroma_format > 3) return error.UnsupportedSurfaceFormat;
        const g = try geometry(surface.width, surface.height, options);
        const uploaded = try self.ensureCoefficients(allocator, surface.width, surface.height, g, options, control);
        const params = [_]u32{ surface.width, surface.height, g.width, g.height, @backingInt(options.rotation), @backingInt(options.matrix), @intFromBool(surface.full_range), @intFromBool(options.centered), surface.bit_depth, surface.chroma_format };
        var out: ?*anyopaque = null;
        if (av_prepare_native_submit(self.handle, surface.y, surface.uv, &params, self.coefficients.?, &out) != 0) return error.MetalPreparationFailed;
        return .{ .handle = out.?, .geometry = g, .coefficient_staging_bytes = uploaded };
    }
    /// Upload native planes directly, without host RGB conversion. Metal owns
    /// its copies before return; padded byte strides remain explicit.
    pub fn submitHost(self: *Metal, allocator: std.mem.Allocator, surface: HostSurface, options: Options, control: media.source.Control) !Prepared {
        if (!supported) return error.UnsupportedVideoBackend;
        try control.check();
        try surface.validate();
        if (surface.chroma_format == 0) return error.UnsupportedSurfaceFormat;
        const g = try geometry(surface.width, surface.height, options);
        const size = try std.math.add(usize, surface.planes[0].bytes.len, surface.planes[1].bytes.len);
        if (size > options.max_host_staging_bytes) return error.ResourceLimitExceeded;
        const uploaded = try self.ensureCoefficients(allocator, surface.width, surface.height, g, options, control);
        const params = [_]u32{ surface.width, surface.height, g.width, g.height, @backingInt(options.rotation), @backingInt(options.matrix), @intFromBool(surface.format == .nv12_full), @intFromBool(options.centered), surface.bit_depth, surface.chroma_format };
        var out: ?*anyopaque = null;
        if (av_prepare_host_submit(self.handle, surface.planes[0].bytes.ptr, surface.planes[0].stride, surface.planes[1].bytes.ptr, surface.planes[1].stride, &params, self.coefficients.?, &out) != 0) return error.MetalPreparationFailed;
        return .{ .handle = out.?, .geometry = g, .native_staging_bytes = size, .coefficient_staging_bytes = uploaded };
    }
    /// Copies tightly packed RGBA into owned Metal storage before returning.
    /// The producer may free its input immediately. Alpha/matrix are ignored
    /// as in referenceRgba; explicit rotation and centering still apply.
    pub fn submitRgba(self: *Metal, allocator: std.mem.Allocator, bytes: []const u8, width: u32, height: u32, options: Options, control: media.source.Control) !Prepared {
        if (!supported) return error.UnsupportedVideoBackend;
        try control.check();
        var scope = image.work_control.Scope.enter(.{ .context = control.context, .check_fn = control.check_fn });
        defer scope.deinit();
        const g = try geometry(width, height, options);
        const size = std.math.mul(usize, try std.math.mul(usize, width, height), 4) catch return error.ResourceLimitExceeded;
        if (bytes.len != size) return error.InvalidVideoGeometry;
        if (size > options.max_host_staging_bytes) return error.ResourceLimitExceeded;
        const uploaded = try self.ensureCoefficients(allocator, width, height, g, options, control);
        const params = [_]u32{ width, height, g.width, g.height, @backingInt(options.rotation), 0, 0, @intFromBool(options.centered), 8, 1 };
        var out: ?*anyopaque = null;
        if (av_prepare_rgba_submit(self.handle, bytes.ptr, bytes.len, &params, self.coefficients.?, &out) != 0) return error.MetalPreparationFailed;
        return .{ .handle = out.?, .geometry = g, .rgba_staging_bytes = bytes.len, .coefficient_staging_bytes = uploaded };
    }
};
/// Owns input import/staging, GPU command, and patch buffer. Destroy fences in-flight
/// work, including after deadline/cancellation. No pixel readback on submit.
pub const Prepared = struct {
    handle: *anyopaque,
    geometry: Geometry,
    /// Logical bytes copied by newBufferWithBytes, not physical PCIe/DMA bytes.
    rgba_staging_bytes: usize = 0,
    native_staging_bytes: usize = 0,
    coefficient_staging_bytes: usize = 0,
    pub fn wait(self: *const Prepared, io: std.Io, control: media.source.Control) !void {
        if (!supported) return error.UnsupportedVideoBackend;
        while (true) {
            try control.check();
            switch (av_prepared_poll(self.handle)) {
                1 => return,
                -1 => return error.MetalPreparationFailed,
                else => try std.Io.sleep(io, .fromMilliseconds(1), .awake),
            }
        }
    }
    /// After completion, discard input import/staging/command/intermediates while
    /// retaining the output buffer. Idempotent; wait before calling.
    pub fn releaseSource(self: *Prepared) !void {
        if (!supported) return error.UnsupportedVideoBackend;
        if (av_prepared_release_source(self.handle) != 0) return error.MetalPreparationNotReady;
    }
    /// Borrowed id<MTLBuffer>; wait before consuming on another queue. For a
    /// resident inference consumer this is the handoff, not a host tensor copy.
    pub fn buffer(self: *const Prepared) !*anyopaque {
        if (!supported) return error.UnsupportedVideoBackend;
        if (av_prepared_poll(self.handle) != 1) return error.MetalPreparationNotReady;
        return av_prepared_buffer(self.handle);
    }
    /// Completed command execution duration, excluding queue wait and CPU setup.
    /// Preserved after releaseSource. Some drivers report zero if unavailable.
    pub fn gpuSeconds(self: *const Prepared) !f64 {
        if (!supported) return error.UnsupportedVideoBackend;
        const seconds = av_prepared_gpu_seconds(self.handle);
        if (seconds < 0) return error.MetalPreparationNotReady;
        return seconds;
    }
    /// Explicit oracle/debug readback. Model integration should use buffer().
    pub fn readback(self: *const Prepared, allocator: std.mem.Allocator) ![]f32 {
        if (!supported) return error.UnsupportedVideoBackend;
        const values = try allocator.alloc(f32, self.geometry.values());
        errdefer allocator.free(values);
        if (av_prepared_copy(self.handle, values.ptr, values.len) != 0) return error.MetalPreparationNotReady;
        return values;
    }
    pub fn deinit(self: *Prepared) void {
        if (supported) av_prepared_destroy(self.handle);
        self.* = undefined;
    }
};
