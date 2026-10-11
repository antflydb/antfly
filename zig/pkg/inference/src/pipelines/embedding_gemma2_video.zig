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

//! Ordered, bounded video frames using the pinned EmbeddingGemma 2 processor.
const std = @import("std");
const media = @import("antfly_media");
const video = @import("antfly_video");
const projector = @import("../architectures/gemma4_projector.zig");
const CT = @import("../ops/ops.zig").CT;
const ComputeBackend = @import("../ops/ops.zig").ComputeBackend;
const build_options = @import("build_options");
const MetalCompute = @import("../ops/metal_compute.zig").MetalCompute;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
pub const Encoded = struct {
    embeddings: []f32,
    tensor: ?CT = null,
    backend: ?*const ComputeBackend = null,
    frame_tokens: []usize,
    pub fn deinit(self: Encoded, a: std.mem.Allocator) void {
        a.free(self.embeddings);
        if (self.tensor) |tensor| self.backend.?.free(tensor);
        a.free(self.frame_tokens);
    }
};

/// Sample logical presentation frames, then map them to decode packet indexes.
/// The FPS policy matches HF; timestamp selection remains a separate video API.
pub fn select(a: std.mem.Allocator, reader: *const media.mp4.Reader) ![]usize {
    if (reader.packets.len == 0 or reader.track.timescale == 0) return error.EmptyVideo;
    const order = try a.alloc(usize, reader.packets.len);
    defer a.free(order);
    for (order, 0..) |*index, i| index.* = i;
    const Less = struct {
        fn less(packets: []const media.mp4.Packet, l: usize, r: usize) bool {
            return packets[l].pts < packets[r].pts or (packets[l].pts == packets[r].pts and l < r);
        }
    };
    std.mem.sort(usize, order, @as([]const media.mp4.Packet, reader.packets), Less.less);
    const first = reader.packets[order[0]].pts;
    const last = reader.packets[order[order.len - 1]];
    const ticks = @as(i128, last.pts) + last.duration - first;
    if (ticks <= 0) return error.InvalidSamplingMetadata;
    const seconds = @as(f64, @floatFromInt(ticks)) / @as(f64, @floatFromInt(reader.track.timescale));
    const sampled = try video.sampling.embeddingGemma2(a, .{ .total_frames = order.len, .fps = @as(f64, @floatFromInt(order.len)) / seconds, .duration_seconds = seconds }, .{});
    for (sampled) |*index| index.* = order[index.*];
    return sampled;
}

fn policy(track: media.mp4.Track, width: u32, height: u32) !video.preparation.Options {
    return policySignal(track, width, height, .{});
}
fn policySignal(track: media.mp4.Track, width: u32, height: u32, signal: video.h264.Color) !video.preparation.Options {
    const display = try video.display.layout(track, width, height, .presentation);
    var transfer: u16 = signal.transfer orelse 2;
    var declared_matrix: u16 = signal.matrix orelse 6;
    var primaries: u16 = signal.primaries orelse 2;
    if (track.color_info.len != 0) {
        const c = track.color_info;
        if (c.len < 10 or (!std.mem.eql(u8, c[0..4], "nclx") and !std.mem.eql(u8, c[0..4], "nclc"))) return error.UnsupportedVideoColor;
        if (std.mem.eql(u8, c[0..4], "nclx") and c.len < 11) return error.UnsupportedVideoColor;
        const container_primaries = std.mem.readInt(u16, c[4..6], .big);
        const container_transfer = std.mem.readInt(u16, c[6..8], .big);
        const container_matrix = std.mem.readInt(u16, c[8..10], .big);
        if (signal.primaries) |value| if (container_primaries != 2 and value != container_primaries) return error.UnsupportedVideoColor;
        if (signal.transfer) |value| if (container_transfer != 2 and value != container_transfer) return error.UnsupportedVideoColor;
        if (signal.matrix) |value| if (container_matrix != 2 and value != container_matrix) return error.UnsupportedVideoColor;
        if (container_primaries != 2) primaries = container_primaries;
        if (container_transfer != 2) transfer = container_transfer;
        if (container_matrix != 2) declared_matrix = container_matrix;
    }
    const matrix: video.preparation.Matrix = switch (declared_matrix) {
        1 => .bt709,
        5, 6 => .bt601,
        9 => .bt2020_ncl,
        else => return error.UnsupportedVideoColor,
    };
    const tone: video.preparation.ToneMapping = switch (transfer) {
        1, 2, 6, 13 => .none,
        16 => .pq_reinhard_100,
        18 => .hlg_reinhard_100,
        else => return error.UnsupportedVideoColor,
    };
    if (tone != .none) {
        // Require explicit consistent primaries; never guess an HDR gamut.
        if (track.codec != .avc or !((matrix == .bt2020_ncl and primaries == 9) or (matrix == .bt709 and primaries == 1))) return error.UnsupportedVideoColor;
    } else if (matrix == .bt2020_ncl or (primaries != 2 and primaries != 1 and primaries != 5 and primaries != 6)) return error.UnsupportedVideoColor;
    const g = projector.embeddingGemma2VideoGeometry(display.width, display.height);
    // Apply SAR through target aspect; a single separable resample avoids an
    // intermediate stretched raster and its additional RGB8 rounding.
    return .{ .width = g.width, .height = g.height, .matrix = matrix, .rotation = display.rotation, .tone_mapping = tone, .torchvision = true, .rgb_rounding = .q13_components };
}

// Model sampling assumes one progressive access-unit picture per MP4 sample.
// Field/partition transport packets need a logical-picture index before FPS parity.
fn validateFrameConfig(cfg: video.h264.Config) !void {
    if (!cfg.frame_only or cfg.profile == 88) return error.UnsupportedVideoProfile;
}
fn parameterSession(a: std.mem.Allocator, reader: *media.mp4.Reader) !?*video.h264_dynamic.Session {
    if (reader.track.codec != .avc) return null;
    if (reader.track.inband_parameter_sets) {
        const session = try video.h264_dynamic.Session.init(a, reader, .{});
        errdefer session.deinit();
        for (session.entries.items) |entry| {
            try validateFrameConfig(entry.config);
            _ = try policySignal(reader.track, @intCast(entry.config.width), @intCast(entry.config.height), entry.config.color);
        }
        return session;
    }
    const cfg = try video.h264.configParse(a, reader.track.avcc);
    defer cfg.groups.deinit(a);
    try validateFrameConfig(cfg);
    _ = try policySignal(reader.track, @intCast(cfg.width), @intCast(cfg.height), cfg.color);
    return null;
}

/// Conservative decoded-pixel charge used before admitted HTTP model execution.
pub fn inspectPixels(bytes: []const u8, control: Control) !u64 {
    const adapter = control.imageWorkControl();
    var source = media.source.Source{ .allocator = std.heap.page_allocator, .storage = .{ .borrowed = bytes }, .identity = "video-preflight", .control = .{ .context = adapter.context, .check_fn = adapter.check_fn }, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
    var reader = try media.mp4.Reader.init(std.heap.page_allocator, &source, .{});
    defer reader.deinit();
    // AVC combines VUI and container declarations in parameterSession below.
    if (reader.track.codec != .avc) _ = try policy(reader.track, reader.track.width, reader.track.height);
    const selected = try select(std.heap.page_allocator, &reader);
    defer std.heap.page_allocator.free(selected);
    const configurations = try parameterSession(std.heap.page_allocator, &reader);
    defer if (configurations) |session| session.deinit();
    const static_config = if (reader.track.codec == .avc and configurations == null) try video.h264.configParse(std.heap.page_allocator, reader.track.avcc) else null;
    defer if (static_config) |cfg| cfg.groups.deinit(std.heap.page_allocator);
    var pixels: u64 = 0;
    for (selected) |index| {
        try control.check();
        const cfg = if (configurations) |session| try session.configAt(index) else static_config;
        const width = if (cfg) |value| value.width else reader.track.width;
        const height = if (cfg) |value| value.height else reader.track.height;
        pixels = try std.math.add(u64, pixels, try std.math.mul(u64, width, height));
    }
    return pixels;
}

pub fn encode(a: std.mem.Allocator, cb: *const ComputeBackend, bytes: []const u8, control: Control) !Encoded {
    if (bytes.len == 0 or bytes.len > 64 * 1024 * 1024) return error.ResourceLimitExceeded;
    const adapter = control.imageWorkControl();
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "embedding-video", .control = .{ .context = adapter.context, .check_fn = adapter.check_fn }, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 } };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    const selected = try select(a, &reader);
    defer a.free(selected);
    const configurations = try parameterSession(a, &reader);
    defer if (configurations) |session| session.deinit();
    const frame_tokens = try a.alloc(usize, selected.len);
    errdefer a.free(frame_tokens);
    @memset(frame_tokens, 0);
    const output = try a.alloc(f32, if (cb.kind() == .metal) 0 else selected.len * 140 * 512);
    defer a.free(output);
    var signal = video.h264.Color{};
    if (reader.track.codec == .avc and !reader.track.inband_parameter_sets) {
        const cfg = try video.h264.configParse(a, reader.track.avcc);
        defer cfg.groups.deinit(a);
        signal = cfg.color;
    }
    var preparer: ?video.preparation.Metal = null;
    if (comptime build_options.enable_metal) {
        if (cb.kind() == .metal) preparer = try video.preparation.Metal.init(try MetalCompute.videoDevice(cb));
    }
    defer if (preparer) |*value| value.deinit();
    const resident = if (cb.kind() == .metal) try a.alloc(?CT, selected.len) else null;
    defer if (resident) |tensors| {
        for (tensors) |tensor| if (tensor) |value| cb.free(value);
        a.free(tensors);
    };
    if (resident) |tensors| @memset(tensors, null);
    var capture = Capture{ .native_batching = cb.kind() == .native, .resident = resident, .signal = signal, .selected = selected, .io = control.io orelse cb.getIo(), .metal = if (preparer) |*value| value else null, .a = a, .cb = cb, .track = reader.track, .control = source.control, .output = output, .frame_tokens = frame_tokens };
    defer capture.discardNativePending();
    var jobs: [2]ProjectionJob = undefined;
    var scheduler = ProjectionScheduler{ .queue = .init(&jobs), .capture = &capture };
    var projection_thread: ?std.Thread = null;
    if (resident != null) {
        if (capture.io == null) return error.MissingVideoIo;
        capture.scheduler = &scheduler;
        projection_thread = try std.Thread.spawn(.{}, ProjectionScheduler.run, .{&scheduler});
    }
    defer if (projection_thread) |thread| {
        scheduler.aborted.store(true, .release);
        scheduler.queue.close(capture.io.?);
        thread.join();
    };
    const decode_error: ?anyerror = if (decodeSelection(a, &reader, selected, configurations, &capture)) |_| null else |err| err;
    if (decode_error != null) scheduler.aborted.store(true, .release);
    if (projection_thread) |thread| {
        scheduler.queue.close(capture.io.?);
        thread.join();
        projection_thread = null;
        if (scheduler.failure) |err| return err;
    }
    if (decode_error) |err| return err;
    try capture.flushNativePending();
    if (resident) |tensors| {
        // Each unique packet owns exactly one projection. Presentation duplicates
        // refer to that projection and are appended in sampling order.
        const first = std.mem.indexOfScalar(usize, selected, selected[0]).?;
        var combined = try cb.ensureDeviceResidentOwned(try cb.primReshape(tensors[first] orelse return error.InvalidTensorShape, &.{ @intCast(frame_tokens[0]), 512 }));
        errdefer cb.free(combined);
        var rows = frame_tokens[0];
        for (selected[1..], 1..) |index, slot| {
            const owner = std.mem.indexOfScalar(usize, selected, index).?;
            if (frame_tokens[slot] == 0) return error.InvalidTensorShape;
            const next = try cb.primConcatPrim(combined, tensors[owner] orelse return error.InvalidTensorShape, 0, &.{ @intCast(rows), 512 }, &.{ @intCast(frame_tokens[slot]), 512 });
            cb.free(combined);
            combined = next;
            rows += frame_tokens[slot];
        }
        return .{ .embeddings = try a.alloc(f32, 0), .tensor = combined, .backend = cb, .frame_tokens = frame_tokens };
    }
    // Callbacks arrive in decode order; compact in presentation slot order.
    var count: usize = 0;
    for (frame_tokens, 0..) |tokens, slot| {
        if (tokens == 0 or tokens > 140) return error.InvalidTensorShape;
        const n = tokens * 512;
        std.mem.copyForwards(f32, output[count..][0..n], output[slot * 140 * 512 ..][0..n]);
        count += n;
    }
    return .{ .embeddings = try a.dupe(f32, output[0..count]), .frame_tokens = frame_tokens };
}
fn decodeSelection(a: std.mem.Allocator, reader: *media.mp4.Reader, selected: []const usize, configurations: ?*video.h264_dynamic.Session, capture: *Capture) !void {
    const control = reader.input.control;
    if (reader.track.codec == .mjpeg) {
        for (selected, 0..) |index, slot| {
            try control.check();
            if (capture.copyDuplicate(slot)) continue;
            var frame = try video.mjpeg.decodeFrame(a, reader, index, .{});
            defer frame.deinit();
            const options = try policy(reader.track, frame.width, frame.height);
            if (capture.metal) |metal| {
                const prepared = try metal.submitRgba(a, frame.rgba, frame.width, frame.height, options, control);
                try capture.projectGpu(slot, prepared, options);
                continue;
            }
            const patches = try video.preparation.referenceRgba(a, frame.rgba, frame.width, frame.height, options, control);
            defer a.free(patches);
            try capture.project(slot, patches, options);
        }
    } else if (capture.metal != null and !reader.track.inband_parameter_sets and compatibleApple(reader.track.avcc)) {
        var batch = try video.apple.decodeTo(a, reader, selected, .{ .require_hardware = false, .seek_mode = .verified_idr }, .{ .context = capture, .accept = Capture.publishApple });
        defer batch.deinit();
    } else {
        // Decode references once for the whole selection, never once per frame.
        if (configurations) |session| _ = try session.decodeSelected(selected, capture, Capture.publish) else _ = try video.h264_sparse.decodeSelected(a, reader, selected, .{ .max_total_packets = 8192 }, capture, Capture.publish);
    }
}

fn compatibleApple(avcc: []const u8) bool {
    video.avc.validateConfig(avcc) catch return false;
    return true;
}
const ProjectionJob = struct { slot: usize, prepared: video.preparation.Prepared, options: video.preparation.Options };

/// Two queued preparations plus one executing projection. The producer owns
/// decoding/preparation; this worker exclusively owns model backend operations.
/// Closing on failure wakes blocked producers; every queued GPU lease is drained.
const ProjectionScheduler = struct {
    queue: std.Io.Queue(ProjectionJob),
    capture: *Capture,
    aborted: std.atomic.Value(bool) = .init(false),
    failure: ?anyerror = null,
    fn run(self: *ProjectionScheduler) void {
        const capture = self.capture;
        const io = capture.io.?;
        var jobs: [2]ProjectionJob = undefined;
        while (true) {
            // A closed queue returns a final partial batch. Decode can continue
            // filling the bounded queue while this worker runs the previous pair.
            const count = self.queue.getUncancelable(io, &jobs, 2) catch break;
            if (self.aborted.load(.acquire) or self.failure != null) {
                for (jobs[0..count]) |*job| job.prepared.deinit();
                continue;
            }
            capture.projectGpuBatchNow(jobs[0..count]) catch |err| {
                self.failure = err;
                self.queue.close(io);
                continue;
            };
            for (jobs[0..count]) |job| {
                for (capture.selected, 0..) |index, slot| if (index == capture.selected[job.slot]) {
                    capture.frame_tokens[slot] = capture.frame_tokens[job.slot];
                };
            }
        }
    }
};

const NativeProjectionJob = struct { slot: usize, patches: []f32, options: video.preparation.Options };

const Capture = struct {
    a: std.mem.Allocator,
    cb: *const ComputeBackend,
    track: media.mp4.Track,
    control: media.source.Control,
    output: []f32,
    resident: ?[]?CT = null,
    frame_tokens: []usize,
    selected: []const usize,
    signal: video.h264.Color = .{},
    metal: ?*video.preparation.Metal = null,
    io: ?std.Io = null,
    scheduler: ?*ProjectionScheduler = null,
    native_batching: bool = false,
    native_pending: ?NativeProjectionJob = null,
    claimed: [32]bool = @splat(false),
    fn copyDuplicate(self: *Capture, slot: usize) bool {
        const first = std.mem.indexOfScalar(usize, self.selected, self.selected[slot]).?;
        if (self.scheduler != null or self.native_batching) return self.claimed[first];
        if (self.frame_tokens[first] == 0) return false;
        const n = self.frame_tokens[first] * 512;
        if (self.resident == null and first != slot) @memcpy(self.output[slot * 140 * 512 ..][0..n], self.output[first * 140 * 512 ..][0..n]);
        self.frame_tokens[slot] = self.frame_tokens[first];
        return true;
    }
    fn discardNativePending(self: *Capture) void {
        if (self.native_pending) |pending| self.a.free(pending.patches);
        self.native_pending = null;
    }
    fn commitNative(self: *Capture, slot: usize, embeddings: []const f32, tokens: usize) !void {
        try self.save(slot, .{ .embeddings = embeddings, .tokens = tokens });
        for (self.selected, 0..) |index, duplicate| if (index == self.selected[slot] and duplicate != slot) {
            @memcpy(self.output[duplicate * 140 * 512 ..][0..embeddings.len], embeddings);
            self.frame_tokens[duplicate] = tokens;
        };
    }
    fn flushNativePending(self: *Capture) !void {
        const pending = self.native_pending orelse return;
        self.native_pending = null;
        defer self.a.free(pending.patches);
        try self.control.check();
        const encoded = try projector.encodeEmbeddingGemma2VideoPatches(self.cb, self.a, pending.patches, pending.options.width, pending.options.height);
        defer self.a.free(encoded.embeddings);
        try self.commitNative(pending.slot, encoded.embeddings, encoded.tokens);
    }
    fn projectNativeBatch(self: *Capture, slot: usize, patches: []const f32, options: video.preparation.Options) !void {
        if (self.native_pending) |pending| {
            if (pending.options.width != options.width or pending.options.height != options.height) {
                try self.flushNativePending();
            } else {
                self.native_pending = null;
                defer self.a.free(pending.patches);
                if (pending.patches.len != patches.len) return error.InvalidTensorShape;
                const combined = try self.a.alloc(f32, patches.len * 2);
                defer self.a.free(combined);
                @memcpy(combined[0..patches.len], pending.patches);
                @memcpy(combined[patches.len..], patches);
                const input = try self.cb.fromFloat32Shape(combined, &.{ @intCast(combined.len / 768), 768 });
                defer self.cb.free(input);
                const encoded = try projector.encodeEmbeddingGemma2VideoBatchTensorResident(self.cb, self.a, input, options.width, options.height, 2);
                defer self.cb.free(encoded.tensor);
                const embeddings = try self.cb.toFloat32(encoded.tensor, self.a);
                defer self.a.free(embeddings);
                if (encoded.tokens % 2 != 0 or embeddings.len != encoded.tokens * 512) return error.InvalidTensorShape;
                const per_frame = encoded.tokens / 2;
                try self.commitNative(pending.slot, embeddings[0 .. per_frame * 512], per_frame);
                try self.commitNative(slot, embeddings[per_frame * 512 ..], per_frame);
                self.claimed[slot] = true;
                return;
            }
        }
        self.native_pending = .{ .slot = slot, .patches = try self.a.dupe(f32, patches), .options = options };
        self.claimed[slot] = true;
    }
    fn project(self: *Capture, slot: usize, patches: []const f32, options: video.preparation.Options) !void {
        try self.control.check();
        if (self.native_batching) return self.projectNativeBatch(slot, patches, options);
        if (self.resident) |tensors| {
            const input = try self.cb.fromFloat32Shape(patches, &.{ @intCast(patches.len / 768), 768 });
            defer self.cb.free(input);
            const encoded = try projector.encodeEmbeddingGemma2VideoTensorResident(self.cb, self.a, input, options.width, options.height);
            tensors[slot] = encoded.tensor;
            self.frame_tokens[slot] = encoded.tokens;
            return;
        }
        const encoded = try projector.encodeEmbeddingGemma2VideoPatches(self.cb, self.a, patches, options.width, options.height);
        defer self.a.free(encoded.embeddings);
        try self.save(slot, encoded);
    }
    fn save(self: *Capture, slot: usize, encoded: anytype) !void {
        if (encoded.tokens == 0 or encoded.tokens > 140 or encoded.embeddings.len != encoded.tokens * 512) return error.InvalidTensorShape;
        @memcpy(self.output[slot * 140 * 512 ..][0..encoded.embeddings.len], encoded.embeddings);
        self.frame_tokens[slot] = encoded.tokens;
    }
    fn projectGpu(self: *Capture, slot: usize, value: video.preparation.Prepared, options: video.preparation.Options) !void {
        if (self.scheduler) |scheduler| {
            const owner = std.mem.indexOfScalar(usize, self.selected, self.selected[slot]).?;
            var prepared = value;
            errdefer prepared.deinit();
            try self.control.check();
            try scheduler.queue.putOneUncancelable(self.io.?, .{ .slot = owner, .prepared = prepared, .options = options });
            self.claimed[owner] = true;
            return;
        }
        return self.projectGpuNow(slot, value, options);
    }
    /// Consumes every preparation even when imports or projection fail.
    fn projectGpuBatchNow(self: *Capture, jobs: []ProjectionJob) !void {
        if (jobs.len == 1) return self.projectGpuNow(jobs[0].slot, jobs[0].prepared, jobs[0].options);
        if (jobs.len != 2) return error.InvalidTensorShape;
        if (jobs[0].options.width != jobs[1].options.width or jobs[0].options.height != jobs[1].options.height) {
            self.projectGpuNow(jobs[0].slot, jobs[0].prepared, jobs[0].options) catch |err| {
                jobs[1].prepared.deinit();
                return err;
            };
            return self.projectGpuNow(jobs[1].slot, jobs[1].prepared, jobs[1].options);
        }
        var live = [_]bool{ true, true };
        defer for (jobs, 0..) |*job, i| {
            if (live[i]) job.prepared.deinit();
        };
        if (comptime build_options.enable_metal) {
            const io = self.io orelse return error.MissingVideoIo;
            var inputs: [2]CT = undefined;
            var imported: usize = 0;
            defer for (inputs[0..imported]) |input| self.cb.free(input);
            for (jobs, 0..) |*job, i| {
                try self.control.check();
                try job.prepared.wait(io, self.control);
                inputs[i] = try MetalCompute.importVideoBuffer(self.cb, try job.prepared.buffer(), job.prepared.geometry.values());
                imported += 1;
                job.prepared.deinit();
                live[i] = false;
            }
            const options = jobs[0].options;
            const rows = @as(usize, options.width) * options.height / 256;
            const input_shape = [_]i64{ @intCast(rows), 768 };
            const input = try self.cb.primConcatPrim(inputs[0], inputs[1], 0, &input_shape, &input_shape);
            defer self.cb.free(input);
            const encoded = try projector.encodeEmbeddingGemma2VideoBatchTensorResident(self.cb, self.a, input, options.width, options.height, 2);
            defer self.cb.free(encoded.tensor);
            if (encoded.tokens % 2 != 0) return error.InvalidTensorShape;
            const per_frame = encoded.tokens / 2;
            const shape = [_]i64{ @intCast(encoded.tokens), 512 };
            // Slice both before publishing either: an allocation failure cannot
            // leave one slot with a partially committed batched result.
            var outputs: [2]CT = undefined;
            var sliced: usize = 0;
            errdefer for (outputs[0..sliced]) |output| self.cb.free(output);
            for (0..2) |i| {
                outputs[i] = try self.cb.primSlice(encoded.tensor, &.{ @intCast(i * per_frame), 0 }, &.{ @intCast((i + 1) * per_frame), 512 }, &.{ 1, 1 }, &shape);
                sliced += 1;
            }
            const tensors = self.resident orelse return error.UnsupportedVideoBackend;
            for (jobs, 0..) |job, i| {
                tensors[job.slot] = outputs[i];
                self.frame_tokens[job.slot] = per_frame;
            }
        } else return error.UnsupportedVideoBackend;
    }
    fn projectGpuNow(self: *Capture, slot: usize, value: video.preparation.Prepared, options: video.preparation.Options) !void {
        var prepared = value;
        var live = true;
        defer if (live) prepared.deinit();
        if (comptime build_options.enable_metal) {
            const io = self.io orelse return error.MissingVideoIo;
            try prepared.wait(io, self.control);
            const input = try MetalCompute.importVideoBuffer(self.cb, try prepared.buffer(), prepared.geometry.values());
            defer self.cb.free(input);
            // Give sole ownership to inference before runtime reuse can occur.
            prepared.deinit();
            live = false;
            const encoded = try projector.encodeEmbeddingGemma2VideoTensorResident(self.cb, self.a, input, options.width, options.height);
            if (self.resident) |tensors| {
                tensors[slot] = encoded.tensor;
                self.frame_tokens[slot] = encoded.tokens;
            } else {
                defer self.cb.free(encoded.tensor);
                const embeddings = try self.cb.toFloat32(encoded.tensor, self.a);
                defer self.a.free(embeddings);
                try self.save(slot, .{ .embeddings = embeddings, .tokens = encoded.tokens });
            }
        } else return error.UnsupportedVideoBackend;
    }
    fn validateRange(self: *const Capture, full: bool) !void {
        const c = self.track.color_info;
        if (c.len >= 11 and std.mem.eql(u8, c[0..4], "nclx") and (c[10] & 128 != 0) != full) return error.UnsupportedVideoColor;
    }
    fn publishApple(raw: *anyopaque, frame: *const video.apple.Frame) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        try self.validateRange(frame.surface.format.fullRange());
        const options = try policySignal(self.track, frame.surface.width, frame.surface.height, self.signal);
        // Duplicate logical samples must receive the same projection.
        const first = std.mem.indexOfScalar(usize, self.selected, frame.decode_index) orelse return error.InvalidPacketIndex;
        if (self.copyDuplicate(first)) return;
        const prepared = try self.metal.?.submit(self.a, &frame.surface, options, self.control);
        try self.projectGpu(first, prepared, options);
        if (self.scheduler != null) return;
        const n = self.frame_tokens[first] * 512;
        for (self.selected, 0..) |index, slot| if (index == frame.decode_index and slot != first) {
            if (self.resident == null) @memcpy(self.output[slot * 140 * 512 ..][0..n], self.output[first * 140 * 512 ..][0..n]);
            self.frame_tokens[slot] = self.frame_tokens[first];
        };
    }
    fn publish(raw: *anyopaque, slot: usize, frame: *const video.h264.Frame) !void {
        const self: *Capture = @ptrCast(@alignCast(raw));
        if (self.copyDuplicate(slot)) return;
        try self.validateRange(frame.full_range);
        const options = try policySignal(self.track, frame.width, frame.height, frame.color);
        if (self.metal) |metal| {
            const prepared = try metal.submitHost(self.a, frame.host(), options, self.control);
            return self.projectGpu(slot, prepared, options);
        }
        const patches = try video.preparation.referenceHost(self.a, frame.host(), options, self.control);
        defer self.a.free(patches);
        try self.project(slot, patches, options);
    }
};

test "embeddinggemma2 video sampling maps presentation order and uniform long clips" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg.mov") }, .identity = "original-mjpeg-fixture" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var packets: [100]media.mp4.Packet = undefined;
    for (&packets, 0..) |*packet, i| packet.* = .{ .offset = 0, .size = 1, .pts = @intCast(99 - i), .dts = @intCast(i), .media_pts = @intCast(99 - i), .duration = 1, .sync = true };
    var synthetic = reader;
    synthetic.packets = &packets;
    synthetic.track.timescale = 1;
    const indexes = try select(a, &synthetic);
    defer a.free(indexes);
    try std.testing.expectEqual(@as(usize, 32), indexes.len);
    try std.testing.expectEqual(@as(usize, 99), indexes[0]);
    try std.testing.expectEqual(@as(usize, 0), indexes[31]);
    for (indexes[1..], indexes[0 .. indexes.len - 1]) |next, previous| try std.testing.expect(next < previous);
    const g = projector.embeddingGemma2VideoGeometry(1920, 1080);
    try std.testing.expect(g.tokens <= 140 and g.width % 48 == 0 and g.height % 48 == 0);
    _ = try policy(reader.track, 1920, 1080);
    synthetic.track.pixel_aspect.horizontal = 2;
    _ = try policy(synthetic.track, 1920, 1080);
    try std.testing.expectError(error.Timeout, encode(a, undefined, "x", .{ .deadline_ns = 0 }));
}

test "embeddinggemma2 video repeated samples reuse projection and validate color declarations" {
    const a = std.testing.allocator;
    const output = try a.alloc(f32, 3 * 140 * 512);
    defer a.free(output);
    @memset(output, 0);
    @memset(output[0..512], 0.25);
    @memset(output[140 * 512 ..][0..512], 0.75);
    var counts = [_]usize{ 1, 1, 0 };
    var capture = Capture{ .a = a, .cb = undefined, .track = undefined, .control = .{}, .output = output, .frame_tokens = &counts, .selected = &.{ 2, 0, 2 } };
    try std.testing.expect(capture.copyDuplicate(2));
    try std.testing.expectEqualSlices(f32, output[0..512], output[2 * 140 * 512 ..][0..512]);
    try std.testing.expectEqual(@as(usize, 1), counts[2]);
    counts[0] = 0;
    try std.testing.expect(!capture.copyDuplicate(2));

    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg.mov") }, .identity = "color-policy-fixture" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    try std.testing.expectEqual(video.preparation.Matrix.bt709, (try policySignal(reader.track, 640, 480, .{ .matrix = 1, .transfer = 1 })).matrix);
    try std.testing.expectError(error.UnsupportedVideoColor, policySignal(reader.track, 640, 480, .{ .transfer = 16 }));
    var track = reader.track;
    track.color_info = "nclx\x00\x01\x00\x01\x00\x06\x80";
    try std.testing.expectError(error.UnsupportedVideoColor, policySignal(track, 640, 480, .{ .matrix = 1 }));
    _ = try policy(track, 640, 480);
    capture.track = track;
    try capture.validateRange(true);
    try std.testing.expectError(error.UnsupportedVideoColor, capture.validateRange(false));
    track.color_info = track.color_info[0..10];
    try std.testing.expectError(error.UnsupportedVideoColor, policy(track, 640, 480));
}

test "embeddinggemma2 model VFR qualification preserves pinned average FPS indices and compressed pixels" {
    const a = std.testing.allocator;
    var original_source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg.mov") }, .identity = "cfr" };
    var original = try media.mp4.Reader.init(a, &original_source, .{});
    defer original.deinit();
    var vfr_source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg-vfr.mov") }, .identity = "vfr" };
    var vfr = try media.mp4.Reader.init(a, &vfr_source, .{});
    defer vfr.deinit();
    const indexes = try select(a, &vfr);
    defer a.free(indexes);
    try std.testing.expectEqualSlices(usize, &.{ 0, 4 }, indexes);
    try std.testing.expectEqual(@as(i64, 4096), vfr.packets[4].pts);
    try std.testing.expectEqual(@as(u32, 16384), vfr.track.timescale);
    try std.testing.expect(vfr.packets[0].duration != vfr.packets[4].duration);
    for (original.packets, 0..) |_, index| {
        var cfr_packet = try original.readPacket(index);
        defer cfr_packet.deinit();
        var vfr_packet = try vfr.readPacket(index);
        defer vfr_packet.deinit();
        try std.testing.expectEqualSlices(u8, cfr_packet.bytes, vfr_packet.bytes);
    }
    // Orthogonal display metadata now drives rotation and model target aspect.
    var track = vfr.track;
    track.display_matrix = .{ 0, 65536, 0, -65536, 0, 0, 0, 0, 1073741824 };
    try std.testing.expectEqual(video.preparation.Rotation.clockwise90, (try policy(track, 640, 480)).rotation);
    track = vfr.track;
    track.pixel_aspect = .{ .horizontal = 2, .vertical = 1 };
    const aspect = try policy(track, 640, 480);
    try std.testing.expect(aspect.width > aspect.height * 2);
    track = vfr.track;
    track.color_info = "nclx\x00\x01\x00\x10\x00\x06\x80";
    try std.testing.expectError(error.UnsupportedVideoColor, policy(track, 640, 480));
}

test "embeddinggemma2 video HDR rendering requires consistent gamut and transfer" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg.mov") }, .identity = "HDR policy" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var track = reader.track;
    track.codec = .avc;
    track.color_info = "nclx\x00\x09\x00\x10\x00\x09\x00";
    const pq = try policySignal(track, 640, 480, .{ .matrix = 9, .transfer = 16, .primaries = 9 });
    try std.testing.expectEqual(video.preparation.Matrix.bt2020_ncl, pq.matrix);
    try std.testing.expectEqual(video.preparation.ToneMapping.pq_reinhard_100, pq.tone_mapping);
    try std.testing.expectError(error.UnsupportedVideoColor, policySignal(track, 640, 480, .{ .transfer = 18 }));
    try std.testing.expectError(error.UnsupportedVideoColor, policySignal(track, 640, 480, .{ .primaries = 1 }));
    track.color_info = "nclx\x00\x09\x00\x12\x00\x09\x00";
    try std.testing.expectEqual(video.preparation.ToneMapping.hlg_reinhard_100, (try policy(track, 640, 480)).tone_mapping);
    track.color_info = "nclx\x00\x02\x00\x10\x00\x09\x00";
    try std.testing.expectError(error.UnsupportedVideoColor, policy(track, 640, 480));
}
