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

//! Source-owned model-aware chunk execution. Sink borrows results until return.
const std = @import("std");
const chunker = @import("inference_chunker");
const planning = chunker.planning;
const output_types = chunker.outputs;
pub const OutputCoverage = struct { name: []const u8, required: bool, complete: usize = 0, failed: usize = 0, skipped: usize = 0 };
pub const OutputExecutor = struct {
    context: *anyopaque,
    execute: *const fn (*anyopaque, std.mem.Allocator, Payload, planning.Descriptor, ?EmbeddingContext, ?Output, ?anyerror, bool) anyerror![]const output_types.Result,
};
const image = @import("antfly_image");
const pdf = @import("antfly_pdf");
const media = @import("antfly_media");
const video = @import("antfly_video");
const audio = @import("audio.zig");
const control_mod = @import("../execution_control.zig");
pub const Control = control_mod.InferenceExecutionControl;
pub const PreparedVideoOutputs = struct {
    context: *anyopaque,
    deinit: *const fn (*anyopaque) void,
};
pub const Payload = union(enum) {
    text: []const u8,
    raster: image.BorrowedRasterAttachment,
    pcm: audio.PcmAudio,
    video: struct { preparation_join: ?struct { context: *anyopaque, join: *const fn (*anyopaque) void } = null, prepared_outputs: ?PreparedVideoOutputs = null, audio: ?audio.PcmAudio = null, audio_start_ns: ?i64 = null, audio_failure: ?anyerror = null, webm: ?@import("embedding_gemma2_video.zig").WebmInput = null, bytes: []const u8, reader: ?*const media.mp4.Reader = null, policy: video.sampling.TimestampPolicy, source_timestamps: bool = false, selection: ?*const @import("embedding_gemma2_video.zig").ReaderSelection = null, packet_base: usize = 0, plan: ?*const @import("embedding_gemma2_video.zig").Plan = null, ahead: ?*@import("embedding_gemma2.zig").VideoAhead = null, prepared: ?@import("embedding_gemma2.zig").VideoPreparedHook = null },
};
pub const Preflight = struct { video_plan: ?@import("embedding_gemma2_video.zig").Plan = null, input_tokens: usize, picture_count: ?u32 = null, selected_pictures: ?[]const u64 = null, picture_basis: ?planning.PictureBasis = null };
pub const Output = struct { vector: []f32, input_tokens: usize };
pub const EmbeddingContext = struct { model: []const u8, model_identity: [64]u8, model_revision: [64]u8, task_type: []const u8 };
pub const Executor = struct {
    context: *anyopaque,
    model: planning.Model,
    describe: ?*const fn (*anyopaque) EmbeddingContext = null,
    count_text: *const fn (*anyopaque, []const u8) anyerror!usize,
    preflight: ?*const fn (*anyopaque, Payload) anyerror!Preflight = null,
    count_overlap: ?*const fn (*anyopaque, []const u8) anyerror!usize = null,
    audio_tokens: ?*const fn (*anyopaque, usize, u32) anyerror!usize = null,
    embed: *const fn (*anyopaque, std.mem.Allocator, Payload) anyerror!Output,
    embed_video_batch: ?*const fn (*anyopaque, std.mem.Allocator, []const Payload) anyerror![]Output = null,
    prepare_video_outputs: ?*const fn (*anyopaque, std.mem.Allocator, Payload, Preflight) anyerror!PreparedVideoOutputs = null,
    start_video: ?*const fn (*anyopaque, Payload, *media.admission.Pool) anyerror!?*@import("embedding_gemma2.zig").VideoAhead = null,
};
pub const Limits = @import("media_chunk_limits.zig").Limits;
pub const Request = struct {
    index_cache: ?*@import("media_index_cache.zig").Cache = null,
    index_cache_admission: ?@import("media_index_cache.zig").Cache.Admission = null,
    outputs: ?output_types.Config = null,
    output_executor: ?OutputExecutor = null,
    transcribe_boundary: bool = false,
    audio_boundary: bool = false,
    needs_audio: bool = false,
    execute_outputs: bool = true,
    limits: Limits = .{},
    input: chunker.Input,
    /// Borrow immutable provider context through completed execution/cancellation.
    native_source: ?*const media.source.Source = null,
    native_transport_bytes: usize = 0,
    policy: planning.Policy = .{},
    source_id: ?[]const u8 = null,
    source_version: []const u8 = "",
    continuation: ?[]const u8 = null,
    execute: bool = true,
    include_content: bool = false,
    /// Native artifact consumers may retain text without encoding media copies.
    native_content: bool = false,
    allow_partial: bool = false,
    max_chunks: usize = 0,
    max_working_bytes: usize = 512 * 1024 * 1024,
};
pub const Item = struct {
    identity: ?planning.Identity = null,
    context: ?EmbeddingContext = null,
    descriptor: planning.Descriptor,
    id: []const u8,
    mime_type: []const u8,
    outputs: []const output_types.Result = &.{},
    embedding: ?[]const f32 = null,
    failure: ?anyerror = null,
    text: ?[]const u8 = null,
    data: ?[]const u8 = null,
};
pub const Sink = struct {
    context: *anyopaque,
    accept: *const fn (*anyopaque, Item) anyerror!void,
};
pub const Summary = struct {
    identity: planning.Identity,
    start_ordinal: u64,
    next_ordinal: u64,
    emitted: usize = 0,
    failed: usize = 0,
    retry_ordinal: ?u64 = null,
    complete: bool = false,
    /// Enumeration completion is independent of output failures.
    source_complete: bool = false,
    degraded: bool = false,
    output_counts: [16]OutputCoverage = @splat(.{ .name = "", .required = true }),
    output_count: usize = 0,
};
pub const Runner = struct {
    allocator: std.mem.Allocator,
    executor: Executor,
    request: Request,
    control: Control,
    summary: Summary,
    sink: Sink,
    pool: media.admission.Pool,
    ordinal: u64 = 0,
    stopped: bool = false,
    page_box: ?[4]f64 = null,
    page_rotation: ?i32 = null,
    audio_provider: ?@import("media_chunking_audio.zig").Provider = null,
    audio_failure: ?anyerror = null,
    pub fn init(a: std.mem.Allocator, request: Request, executor: Executor, control: Control, sink: Sink) !Runner {
        try control.check();
        if (request.outputs) |config| {
            try config.validate();
            if (request.output_executor == null) return error.InvalidChunkOutputs;
        }
        try request.policy.validate();
        try request.limits.validate();
        if (request.max_chunks > request.limits.max_chunks or request.max_working_bytes == 0) return error.InvalidChunkPolicy;
        const bytes = switch (request.input) {
            .text => |text| text,
            .binary => |b| b.data,
            .source => return error.UnresolvedChunkSource,
        };
        const mime = switch (request.input) {
            .text => "text/plain",
            .binary => |b| b.mime_type,
            .source => return error.UnresolvedChunkSource,
        };
        const identity = if (request.native_source) |source| try planning.Identity.initPinned(source.identity, mime, request.source_version, executor.model, request.policy) else try planning.Identity.init(bytes, mime, request.source_version, executor.model, request.policy);
        const start = if (request.continuation) |cursor| try identity.parseContinuation(cursor) else 0;
        var self = Runner{ .allocator = a, .executor = executor, .request = request, .control = control, .sink = sink, .summary = .{ .identity = identity, .start_ordinal = start, .next_ordinal = start }, .pool = .{ .limits = .{ .host_bytes = request.max_working_bytes } } };
        if (request.outputs) |config| {
            self.summary.output_count = config.targets.len;
            for (config.targets, 0..) |target, i| self.summary.output_counts[i] = .{ .name = target.name, .required = target.required };
        }
        return self;
    }
    fn check(raw: ?*const anyopaque) !void {
        const self: *const Runner = @ptrCast(@alignCast(raw.?));
        try self.control.check();
    }
    fn interested(self: *Runner) !bool {
        try self.control.check();
        if (self.ordinal < self.summary.start_ordinal) return false;
        if (self.request.max_chunks != 0 and self.summary.emitted >= self.request.max_chunks) {
            self.stopped = true;
            return false;
        }
        return true;
    }
    /// Returns false after an explicit output cap. Every accepted item advances
    /// the continuation only after its consumer acknowledges successful delivery.
    fn emit(self: *Runner, shape: planning.Descriptor, payload: Payload, mime: []const u8) !bool {
        return self.emitReady(shape, payload, mime, null, null);
    }
    /// Consumes ready even when the sink stops or rejects publication.
    fn emitReady(self: *Runner, shape: planning.Descriptor, payload: Payload, mime: []const u8, ready: ?Output, measured_ready: ?Preflight) !bool {
        var selected_pictures: ?[]const u64 = if (measured_ready) |measured| measured.selected_pictures else null;
        defer if (selected_pictures) |pictures| self.allocator.free(pictures);
        var result: ?Output = ready;
        defer if (result) |value| self.allocator.free(value.vector);
        const ordinal = self.ordinal;
        self.ordinal = try std.math.add(u64, ordinal, 1);
        if (ordinal < self.summary.start_ordinal) return true;
        if (self.request.max_chunks != 0 and self.summary.emitted >= self.request.max_chunks) {
            self.stopped = true;
            return false;
        }
        try self.control.check();
        var descriptor = shape;
        descriptor.ordinal = ordinal;
        if (payload == .raster and descriptor.coordinates.raster_width == null) {
            descriptor.coordinates.raster_width = payload.raster.width;
            descriptor.coordinates.raster_height = payload.raster.height;
        }
        if (descriptor.coordinates.kind == .pdf) {
            descriptor.coordinates.page_box = self.page_box;
            descriptor.coordinates.page_rotation = self.page_rotation;
        }
        if (self.executor.preflight) |preflight| {
            const measured = measured_ready orelse try preflight(self.executor.context, payload);
            selected_pictures = measured.selected_pictures;
            descriptor.coordinates.selected_pictures = measured.selected_pictures;
            descriptor.coordinates.picture_basis = measured.picture_basis;
            descriptor.input_tokens = measured.input_tokens;
            if (measured.picture_count) |n| descriptor.coordinates.picture_count = n;
            if (measured.input_tokens > try self.executor.model.capacity()) return error.ChunkUnitTooLarge;
        }
        try descriptor.coordinates.validate();
        var id: [85]u8 = undefined;
        var item = Item{ .identity = self.summary.identity, .context = if (self.executor.describe) |describe| describe(self.executor.context) else null, .descriptor = descriptor, .id = self.summary.identity.cursor(ordinal, &id), .mime_type = mime };
        if (self.request.execute) {
            if (ready == null) result = self.executor.embed(self.executor.context, self.allocator, payload) catch |err| blk: {
                // A busy provider is admission pressure, including after an
                // acknowledged prefix. Never publish it as a failed chunk or
                // lose the cursor needed to resume that exact ordinal.
                const failure = if (err == error.QueueFull) error.ResourceTemporarilyUnavailable else err;
                if (!self.request.allow_partial or abort(failure)) return failure;
                item.failure = failure;
                break :blk null;
            };
            if (result) |value| {
                if (value.input_tokens > self.executor.model.context_tokens) return error.ChunkUnitTooLarge;
                for (value.vector) |v| if (!std.math.isFinite(v)) return error.InvalidEmbeddingOutput;
                item.embedding = value.vector;
                item.descriptor.input_tokens = value.input_tokens;
            }
        }
        // Speculative preparation may borrow the same immutable range cache or
        // decoder as secondary consumers. Join after primary model work, before
        // audio reads and output execution, while retaining backbone overlap.
        if (payload == .video) if (payload.video.preparation_join) |join| join.join(join.context);
        var output_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer output_arena.deinit();
        var output_payload = payload;
        var borrowed_audio: ?@import("media_chunking_audio.zig").Lease = null;
        defer if (borrowed_audio) |*lease| lease.deinit(self.allocator);
        if (payload == .video and self.request.needs_audio) {
            output_payload.video.audio_failure = self.audio_failure;
            if (self.audio_provider) |provider| {
                const start: u128 = @intCast(descriptor.coordinates.start_ns orelse return error.InvalidChunkCoordinates);
                const end: u128 = @intCast(descriptor.coordinates.end_ns orelse return error.InvalidChunkCoordinates);
                const first = std.math.cast(usize, @divFloor(start * provider.sample_rate + std.time.ns_per_s - 1, std.time.ns_per_s)) orelse return error.InvalidChunkCoordinates;
                const last = std.math.cast(usize, @divFloor(end * provider.sample_rate + std.time.ns_per_s - 1, std.time.ns_per_s)) orelse return error.InvalidChunkCoordinates;
                borrowed_audio = if (first >= provider.samples) .{ .pcm = .{ .samples = &.{}, .sample_rate = provider.sample_rate } } else provider.read(provider.context, self.allocator, first, @min(last, provider.samples)) catch |err| blk: {
                    if (abort(err)) return err;
                    output_payload.video.audio_failure = err;
                    break :blk null;
                };
                if (borrowed_audio) |lease| {
                    output_payload.video.audio = lease.pcm;
                    output_payload.video.audio_start_ns = std.math.cast(i64, @divFloor(@as(u128, first) * std.time.ns_per_s, provider.sample_rate)) orelse return error.InvalidChunkCoordinates;
                }
            }
        }
        if (self.request.execute_outputs) {
            if (self.request.output_executor) |consumer| item.outputs = try consumer.execute(consumer.context, output_arena.allocator(), output_payload, descriptor, item.context, result, item.failure, self.request.allow_partial);
        }
        var required_failure = item.failure != null;
        var optional_failure = false;
        var coverage = self.summary.output_counts;
        if (if (self.request.execute_outputs) self.request.outputs else null) |config| {
            for (config.targets, 0..) |target, index| {
                var seen: usize = 0;
                for (item.outputs) |value| {
                    if (!std.mem.eql(u8, value.name, target.name)) continue;
                    try value.validate(target);
                    if (value.kind != target.kind or value.required != target.required or !std.mem.eql(u8, value.model, target.model) or value.sub_index != seen) return error.InvalidChunkOutput;
                    seen += 1;
                    if (value.status == .failed) {
                        coverage[index].failed += 1;
                        if (target.required) required_failure = true else optional_failure = true;
                    } else if (value.status == .skipped) coverage[index].skipped += 1 else coverage[index].complete += 1;
                }
                if (seen == 0) return error.InvalidChunkOutput;
            }
            for (item.outputs) |value| {
                var known = false;
                for (config.targets) |target| if (std.mem.eql(u8, value.name, target.name)) {
                    known = true;
                    break;
                };
                if (!known) return error.InvalidChunkOutput;
            }
        }
        if (self.request.native_content and payload == .text) item.text = payload.text;
        var content: ?[]u8 = null;
        defer if (content) |bytes| self.allocator.free(bytes);
        if (self.request.include_content) switch (payload) {
            .text => |text| item.text = text,
            .pcm => |pcm| {
                content = try audio.wav.encodeInterleaved(self.allocator, pcm.samples, 1, .{ .audio_format = 3, .sample_rate = pcm.sample_rate, .bits_per_sample = 32 });
                item.data = content;
            },
            .raster => |raster| {
                const pixels = try self.allocator.alloc(u8, @as(usize, raster.width) * raster.height * 4);
                defer self.allocator.free(pixels);
                for (0..raster.height) |row| @memcpy(pixels[row * raster.width * 4 ..][0 .. raster.width * 4], raster.bytes[row * raster.stride_bytes ..][0 .. raster.width * 4]);
                content = try chunker.png.encodeRgba(self.allocator, raster.width, raster.height, pixels);
                item.data = content;
            },
            .video => {}, // Original source + interval is the replay recipe.
        };
        try self.sink.accept(self.sink.context, item);
        self.summary.emitted += 1;
        self.summary.output_counts = coverage;
        self.summary.degraded = self.summary.degraded or optional_failure;
        self.summary.failed += @intFromBool(required_failure);
        if (required_failure and self.summary.retry_ordinal == null) self.summary.retry_ordinal = ordinal;
        self.summary.next_ordinal = self.ordinal;
        return true;
    }
    pub fn run(self: *Runner) !Summary {
        const scope = image.work_control.Scope.enter(self.control.imageWorkControl());
        defer scope.deinit();
        try self.control.check();
        if (self.request.native_source) |source| {
            try self.readSource(source);
        } else switch (self.request.input) {
            .source => return error.UnresolvedChunkSource,
            .text => |text| try self.textChunks(text, null),
            .binary => |binary| {
                const essence = binary.mime_type[0 .. std.mem.indexOfScalar(u8, binary.mime_type, ';') orelse binary.mime_type.len];
                if (std.ascii.startsWithIgnoreCase(essence, "text/") or std.ascii.eqlIgnoreCase(essence, "application/json")) {
                    try self.textChunks(binary.data, null);
                } else if (std.ascii.eqlIgnoreCase(essence, "application/pdf")) {
                    try self.document(binary.data);
                } else if (std.ascii.eqlIgnoreCase(essence, "image/gif")) {
                    try self.animation(binary.data);
                } else if (std.ascii.startsWithIgnoreCase(essence, "image/")) {
                    try self.still(binary.data);
                } else if (std.ascii.startsWithIgnoreCase(essence, "audio/")) {
                    try self.sound(binary.data);
                } else if (std.ascii.startsWithIgnoreCase(essence, "video/")) {
                    try self.movie(binary.data);
                } else return error.UnsupportedChunkMediaType;
            },
        }
        if (!self.stopped and self.ordinal < self.summary.start_ordinal) return error.InvalidChunkContinuation;
        self.summary.source_complete = !self.stopped;
        self.summary.complete = self.summary.source_complete and self.summary.failed == 0;
        return self.summary;
    }
    fn textChunks(self: *Runner, bytes: []const u8, page: ?u32) !void {
        var cursor = planning.Text{ .text = bytes, .page = page, .policy = self.request.policy, .model = self.executor.model, .counter = .{ .context = self.executor.context, .count = self.executor.count_text, .count_overlap = self.executor.count_overlap }, .control = .{ .context = self, .check_fn = check } };
        while (try cursor.next()) |descriptor| {
            if (!try self.emit(descriptor, .{ .text = bytes[descriptor.coordinates.start_byte.?..descriptor.coordinates.end_byte.?] }, "text/plain")) break;
        }
    }
    fn rasterChunks(self: *Runner, input: image.BorrowedRasterAttachment, page: ?u32) !void {
        var cursor = planning.Image{ .width = input.width, .height = input.height, .policy = self.request.policy };
        while (try cursor.next()) |shape| {
            var descriptor = shape;
            descriptor.coordinates.raster_width = input.width;
            descriptor.coordinates.raster_height = input.height;
            if (page) |number| {
                descriptor.coordinates.kind = .pdf;
                descriptor.coordinates.page = number;
            }
            const box = descriptor.coordinates.region.?;
            const width = box[2] - box[0];
            const height = box[3] - box[1];
            const start = @as(usize, box[1]) * input.stride_bytes + box[0] * 4;
            const end = start + @as(usize, height - 1) * input.stride_bytes + width * 4;
            var cropped = input;
            cropped.bytes = input.bytes[start..end];
            cropped.width = width;
            cropped.height = height;
            cropped.page_number = page;
            try cropped.validate();
            if (!try self.emit(descriptor, .{ .raster = cropped }, "image/png")) break;
        }
    }
    fn still(self: *Runner, bytes: []const u8) !void {
        const info = try image.inspectEncoded(bytes);
        try image.DecodeLimits.inference_default.validate(info.width, info.height);
        var permit = try self.pool.acquire(.{ .host_bytes = @as(u64, info.width) * info.height * 8 });
        defer permit.deinit();
        const decoded = try @import("image.zig").decode(self.allocator, bytes);
        defer decoded.deinit(self.allocator);
        const rgba = try self.allocator.alloc(u8, @as(usize, decoded.width) * decoded.height * 4);
        defer self.allocator.free(rgba);
        for (0..@as(usize, decoded.width) * decoded.height) |i| {
            @memcpy(rgba[i * 4 ..][0..3], decoded.data[i * 3 ..][0..3]);
            rgba[i * 4 + 3] = 255;
        }
        try self.rasterChunks(.{ .bytes = rgba, .width = decoded.width, .height = decoded.height, .stride_bytes = @as(usize, decoded.width) * 4 }, null);
    }
    fn animation(self: *Runner, bytes: []const u8) !void {
        const info = try image.inspectEncoded(bytes);
        var permit = try self.pool.acquire(.{ .host_bytes = @as(u64, info.width) * info.height * 12 });
        defer permit.deinit();
        const Capture = struct {
            runner: *Runner,
            time_ns: i64 = 0,
            fn accept(raw: *anyopaque, index: usize, frame: image.gif.BorrowedFrame) !bool {
                const self_: *@This() = @ptrCast(@alignCast(raw));
                try self_.runner.control.check();
                const end = try std.math.add(i64, self_.time_ns, @as(i64, frame.delay_ms) * std.time.ns_per_ms);
                const shape = planning.Descriptor{ .ordinal = 0, .coordinates = .{ .kind = .gif, .start_ns = self_.time_ns, .end_ns = end, .first_picture = index, .picture_count = 1 }, .input_tokens = 0 };
                self_.time_ns = end;
                return self_.runner.emit(shape, .{ .raster = .{ .bytes = frame.rgba, .width = frame.width, .height = frame.height, .stride_bytes = @as(usize, frame.width) * 4 } }, "image/png");
            }
        };
        var capture = Capture{ .runner = self };
        var visited = image.gif.VisitResult{};
        try image.gif.visitFramesAllocBounded(self.allocator, bytes, self.request.max_working_bytes, .{ .context = &capture, .accept = Capture.accept, .result = &visited });
        if (!visited.complete) self.stopped = true;
    }
    fn sound(self: *Runner, bytes: []const u8) !void {
        const providers = @import("media_chunking_audio.zig");
        if (audio.detectFormat(bytes)) |format| if (format == .wav) {
            var wav = providers.Wav{ .parsed = try audio.wav.parseData(bytes) };
            try self.soundWindows(wav.provider());
            return;
        };
        if (try providers.Stream.start(self.allocator, bytes, self.control)) |stream| {
            defer stream.deinit();
            try self.soundWindows(stream.provider());
            if (!self.stopped) try stream.finish();
            return;
        }
        var permit = try self.pool.acquire(.{ .host_bytes = self.request.max_working_bytes });
        defer permit.deinit();
        const adapter = self.control.imageWorkControl();
        var decoded = try audio.decodeBounded(self.allocator, bytes, .{ .control = .{ .context = adapter.context, .check_fn = adapter.check_fn } }, self.request.max_working_bytes);
        defer decoded.deinit();
        var pcm = providers.Pcm{ .pcm = .{ .samples = decoded.samples, .sample_rate = decoded.sample_rate } };
        try self.soundWindows(pcm.provider());
    }
    fn soundWindows(self: *Runner, provider: @import("media_chunking_audio.zig").Provider) !void {
        if (provider.sample_rate == 0) return error.UnsupportedAudioFormat;
        const end: i64 = std.math.cast(i64, @divFloor(@as(u128, provider.samples) * std.time.ns_per_s + provider.sample_rate - 1, provider.sample_rate)) orelse return error.TimestampOverflow;
        var cursor = planning.Temporal{ .kind = .audio, .end_ns = end, .policy = self.request.policy, .model = self.executor.model, .picture_tokens = 0 };
        while (true) {
            if (cursor.offset_ns >= cursor.end_ns) break;
            if (self.request.max_chunks != 0 and self.summary.emitted >= self.request.max_chunks) {
                self.stopped = true;
                break;
            }
            if (provider.probe) |probe| {
                // A bounded tail lookahead preserves the same minimum-duration
                // final-window policy as a provider with a known total length.
                const first: usize = @intCast(@divFloor(@as(u128, @intCast(cursor.offset_ns)) * provider.sample_rate + std.time.ns_per_s - 1, std.time.ns_per_s));
                const lookahead_ns = @as(u128, @intCast(cursor.offset_ns)) + @as(u128, @intCast(@min(self.request.policy.window_ns, self.executor.model.max_audio_ns))) + @as(u128, @intCast(self.executor.model.min_audio_ns));
                const stop = std.math.cast(usize, @divFloor(lookahead_ns * provider.sample_rate, std.time.ns_per_s) + 1) orelse return error.TimestampOverflow;
                if (try probe(provider.context, first, stop)) |samples| cursor.end_ns = @intCast(@divFloor(@as(u128, samples) * std.time.ns_per_s + provider.sample_rate - 1, provider.sample_rate));
            }
            const planned = (try cursor.next()) orelse break;
            var descriptor = planned;
            const first: usize = @intCast(@divFloor(@as(u128, @intCast(descriptor.coordinates.start_ns.?)) * provider.sample_rate + std.time.ns_per_s - 1, std.time.ns_per_s));
            var last: usize = @intCast(@min(provider.samples, @divFloor(@as(u128, @intCast(descriptor.coordinates.end_ns.?)) * provider.sample_rate, std.time.ns_per_s)));
            if (last <= first) continue;
            if (self.request.max_chunks != 0 and self.summary.emitted >= self.request.max_chunks) {
                self.stopped = true;
                break;
            }
            if (self.executor.audio_tokens) |count| {
                var lo = first;
                var hi = last;
                while (lo < hi) {
                    const mid = lo + (hi - lo + 1) / 2;
                    const cost = count(self.executor.context, mid - first, provider.sample_rate) catch |err| {
                        if (err == error.EmptyEmbeddingInput) {
                            lo = mid;
                            continue;
                        }
                        return err;
                    };
                    if (cost <= try self.executor.model.capacity()) lo = mid else hi = mid - 1;
                }
                if (lo < last) {
                    last = lo;
                    const stop: i64 = @intCast(@divFloor(@as(u128, last) * std.time.ns_per_s, provider.sample_rate));
                    if (stop <= descriptor.coordinates.start_ns.? + self.request.policy.overlap_ns) return error.ChunkUnitTooLarge;
                    cursor.offset_ns = stop - self.request.policy.overlap_ns;
                }
            }
            if (self.ordinal < self.summary.start_ordinal and self.request.policy.audio_pause_ns == 0 and (self.executor.audio_tokens != null or self.executor.preflight == null)) {
                self.ordinal = try std.math.add(u64, self.ordinal, 1);
                continue;
            }
            var window = try provider.read(provider.context, self.allocator, first, last);
            defer window.deinit(self.allocator);
            if (provider.final_samples) |length| if (length(provider.context)) |samples| {
                cursor.end_ns = @intCast(@divFloor(@as(u128, samples) * std.time.ns_per_s + provider.sample_rate - 1, provider.sample_rate));
            };
            last = first + window.pcm.samples.len;
            if (last == first) break;
            if (if (self.executor.audio_tokens == null) self.executor.preflight else null) |preflight| {
                var lo = first;
                var hi = last;
                while (lo < hi) {
                    const mid = lo + (hi - lo + 1) / 2;
                    const measured = preflight(self.executor.context, .{ .pcm = .{ .samples = window.pcm.samples[0 .. mid - first], .sample_rate = provider.sample_rate } }) catch |err| {
                        if (err == error.EmptyEmbeddingInput) {
                            lo = mid;
                            continue;
                        }
                        return err;
                    };
                    if (measured.input_tokens <= try self.executor.model.capacity()) lo = mid else hi = mid - 1;
                }
                if (lo < last) {
                    last = lo;
                    const stop = std.math.cast(i64, @divFloor(@as(u128, last) * std.time.ns_per_s, provider.sample_rate)) orelse return error.TimestampOverflow;
                    if (stop <= descriptor.coordinates.start_ns.? + self.request.policy.overlap_ns) return error.ChunkUnitTooLarge;
                    descriptor.coordinates.end_ns = stop;
                    cursor.offset_ns = stop - self.request.policy.overlap_ns;
                }
            }
            if (self.request.policy.audio_pause_ns > 0 and last < provider.samples) {
                const rate = provider.sample_rate;
                const silence: usize = @intCast(@min(last - first, @divFloor(@as(u128, @intCast(self.request.policy.audio_pause_ns)) * rate + std.time.ns_per_s - 1, std.time.ns_per_s)));
                const search: usize = @intCast(@min(last - first, @divFloor(@as(u128, @intCast(self.request.policy.audio_pause_search_ns)) * rate, std.time.ns_per_s)));
                const floor = @max(first + 1, last - search);
                const threshold = @as(f32, @floatFromInt(self.request.policy.audio_silence_milli)) / 1000;
                var silent_samples: usize = 0;
                var position = last;
                while (position > floor) {
                    position -= 1;
                    if (@abs(window.pcm.samples[position - first]) <= threshold) silent_samples += 1 else silent_samples = 0;
                    if (silence != 0 and silent_samples >= silence) {
                        last = position + @max(@as(usize, 1), silence / 2);
                        break;
                    }
                }
            }
            descriptor.coordinates.start_sample = first;
            descriptor.coordinates.end_sample = last;
            descriptor.coordinates.sample_rate = provider.sample_rate;
            descriptor.coordinates.start_ns = @intCast(@divFloor(@as(u128, first) * std.time.ns_per_s, provider.sample_rate));
            descriptor.coordinates.end_ns = @intCast(@divFloor(@as(u128, last) * std.time.ns_per_s + provider.sample_rate - 1, provider.sample_rate));
            const actual_samples = if (provider.final_samples) |length| length(provider.context) orelse provider.samples else provider.samples;
            if (last < actual_samples) {
                const stop: i64 = @intCast(@divFloor(@as(u128, last) * std.time.ns_per_s, provider.sample_rate));
                if (stop <= descriptor.coordinates.start_ns.? + self.request.policy.overlap_ns) return error.ChunkUnitTooLarge;
                cursor.offset_ns = stop - self.request.policy.overlap_ns;
            }
            if (last <= first) continue;
            if (!try self.emit(descriptor, .{ .pcm = .{ .samples = window.pcm.samples[0 .. last - first], .sample_rate = provider.sample_rate } }, "audio/wav")) break;
        }
    }
    fn pdfCancelled(raw: ?*const anyopaque) bool {
        const self: *const Runner = @ptrCast(@alignCast(raw.?));
        self.control.check() catch return true;
        return false;
    }
    fn document(self: *Runner, bytes: []const u8) !void {
        var parsed = try pdf.reader.Reader.initWithDecodeLimitsAndCancellation(self.allocator, bytes, .{}, .{ .context = self, .is_cancelled_fn = pdfCancelled });
        defer parsed.deinit();
        const count = try parsed.pageCount();
        for (1..count + 1) |page| {
            try self.control.check();
            const box = try parsed.extractPageBox(page);
            self.page_box = .{ box.min_x, box.min_y, box.max_x, box.max_y };
            self.page_rotation = try parsed.extractPageRotation(page);
            const text_bytes = try parsed.extractPageTextAlloc(page);
            defer self.allocator.free(text_bytes);
            if (std.mem.trim(u8, text_bytes, &std.ascii.whitespace).len != 0) {
                try self.textChunks(text_bytes, @intCast(page));
            } else {
                var rendered = try pdf.renderParsedPageRasterAdaptiveAlloc(self.allocator, &parsed, page, 144, 16 * 1024 * 1024, 4096);
                defer rendered.deinit(self.allocator);
                try self.rasterChunks(.{ .bytes = rendered.bytes, .width = rendered.width, .height = rendered.height, .stride_bytes = rendered.stride }, @intCast(page));
            }
            if (self.stopped) break;
        }
    }
    fn readSource(self: *Runner, supplied: *const media.source.Source) !void {
        if (supplied.length() == 0) return error.EmptySource;
        if (supplied.limits.max_read_bytes == 0) return error.ResourceLimitExceeded;
        if (self.request.input != .binary) return error.InvalidChunkSource;
        const adapter = self.control.imageWorkControl();
        var source = media.source.Source{ .allocator = self.allocator, .storage = supplied.storage, .identity = supplied.identity, .limits = supplied.limits, .admission_pool = &self.pool, .control = .{ .context = adapter.context, .check_fn = adapter.check_fn } };
        const mime = self.request.input.binary.mime_type;
        if (std.ascii.startsWithIgnoreCase(mime, "video/") and !self.request.audio_boundary) {
            var prefix = try source.read(0, @intCast(@min(4, source.length())));
            const webm = prefix.bytes.len == 4 and std.mem.eql(u8, prefix.bytes, "\x1a\x45\xdf\xa3");
            prefix.deinit();
            if (webm) {
                var audio_track: ?@import("media_chunking_audio_source.zig").Track = if (self.request.needs_audio) @import("media_chunking_audio_source.zig").Track.init(self.allocator, &source, self.control, self.request.max_working_bytes, self.request.limits.max_index_packets) catch |err| blk: {
                    if (abort(err) and err != error.AudioTooLarge) return err;
                    self.audio_failure = err;
                    break :blk null;
                } else null;
                defer if (audio_track) |*track_owner| track_owner.deinit();
                if (audio_track) |*track_owner| self.audio_provider = track_owner.provider();
                defer self.audio_provider = null;
                const identity = @import("media_index_cache.zig").Cache.key(&source, true, self.request.limits.max_index_packets, self.request.max_working_bytes);
                const cached = if (identity) |key| if (self.request.index_cache) |cache| cache.get(key) else null else null;
                defer if (cached) |entry| self.request.index_cache.?.release(entry);
                if (cached) |entry| {
                    var reader = entry.data.webm.reader(self.allocator, &source);
                    try self.webmWindows(&reader);
                    return;
                }
                var reader = try media.webm.Reader.init(self.allocator, &source, .{ .max_packets = self.request.limits.max_index_packets });
                defer reader.deinit();
                if (identity) |key| if (self.request.index_cache) |cache| if (self.request.index_cache_admission) |admission| cache.remember(std.heap.smp_allocator, key, .{ .webm = reader }, admission);
                try self.webmWindows(&reader);
                return;
            } else {
                var audio_track: ?@import("media_chunking_audio_source.zig").Track = if (self.request.needs_audio) @import("media_chunking_audio_source.zig").Track.init(self.allocator, &source, self.control, self.request.max_working_bytes, self.request.limits.max_index_packets) catch |err| blk: {
                    if (abort(err) and err != error.AudioTooLarge) return err;
                    self.audio_failure = err;
                    break :blk null;
                } else null;
                defer if (audio_track) |*track_owner| track_owner.deinit();
                if (audio_track) |*track_owner| self.audio_provider = track_owner.provider();
                defer self.audio_provider = null;
                const identity = @import("media_index_cache.zig").Cache.key(&source, false, self.request.limits.max_index_packets, self.request.max_working_bytes);
                const cached = if (identity) |key| if (self.request.index_cache) |cache| cache.get(key) else null else null;
                defer if (cached) |entry| self.request.index_cache.?.release(entry);
                if (cached) |entry| {
                    var reader = entry.data.mp4.reader(self.allocator, &source);
                    var owner = try @import("media_chunking_video.zig").Owner.init(self.allocator, &reader, &self.pool, self.request.limits);
                    defer owner.deinit();
                    try self.videoWindows(&.{}, &reader, try owner.durationNs(), owner.origin, reader.track.timescale, &owner, null);
                    return;
                }
                var reader = try media.mp4.Reader.init(self.allocator, &source, .{ .max_samples = self.request.limits.max_index_packets, .max_index_bytes = @min(self.request.max_working_bytes, try std.math.mul(usize, self.request.limits.max_index_packets, @sizeOf(media.mp4.Packet))) });
                defer reader.deinit();
                if (identity) |key| if (self.request.index_cache) |cache| if (self.request.index_cache_admission) |admission| cache.remember(std.heap.smp_allocator, key, .{ .mp4 = reader }, admission);
                var owner = try @import("media_chunking_video.zig").Owner.init(self.allocator, &reader, &self.pool, self.request.limits);
                defer owner.deinit();
                try self.videoWindows(&.{}, &reader, try owner.durationNs(), owner.origin, reader.track.timescale, &owner, null);
                return;
            }
        }
        if ((self.request.audio_boundary and std.ascii.startsWithIgnoreCase(mime, "video/")) or (std.ascii.startsWithIgnoreCase(mime, "audio/") and audio.detectFormatFromMime(mime) == .mp4)) {
            var audio_track = try @import("media_chunking_audio_source.zig").Track.init(self.allocator, &source, self.control, self.request.max_working_bytes, self.request.limits.max_index_packets);
            defer audio_track.deinit();
            try self.soundWindows(audio_track.provider());
            return;
        }
        if (audio.detectFormatFromMime(mime)) |format| if (format == .wav) {
            var wav = @import("media_chunking_audio.zig").SourceWav{ .layout = try audio.wav.readSourceLayout(&source), .source = &source };
            try self.soundWindows(try wav.provider());
            return;
        };
        // Still/document decoders own a bounded contiguous input. Keep this
        // explicit spool separate from the source-backed coded-video path.
        const length = std.math.cast(usize, source.length()) orelse return error.ResourceLimitExceeded;
        if (length > self.request.max_working_bytes) return error.ResourceLimitExceeded;
        const bytes = try self.allocator.alloc(u8, length);
        defer self.allocator.free(bytes);
        var offset: usize = 0;
        while (offset < length) {
            var lease = try source.read(offset, @min(length - offset, source.limits.max_read_bytes));
            defer lease.deinit();
            @memcpy(bytes[offset..][0..lease.bytes.len], lease.bytes);
            offset += lease.bytes.len;
        }
        const essence = mime[0 .. std.mem.indexOfScalar(u8, mime, ';') orelse mime.len];
        if (std.ascii.startsWithIgnoreCase(essence, "text/") or std.ascii.eqlIgnoreCase(essence, "application/json")) try self.textChunks(bytes, null) else if (std.ascii.eqlIgnoreCase(essence, "application/pdf")) try self.document(bytes) else if (std.ascii.eqlIgnoreCase(essence, "image/gif")) try self.animation(bytes) else if (std.ascii.startsWithIgnoreCase(essence, "image/")) try self.still(bytes) else if (std.ascii.startsWithIgnoreCase(essence, "audio/")) try self.sound(bytes) else if (std.ascii.startsWithIgnoreCase(essence, "video/")) try self.movie(bytes) else return error.UnsupportedChunkMediaType;
    }
    fn movie(self: *Runner, bytes: []const u8) !void {
        if (self.request.audio_boundary) return self.sound(bytes);
        const adapter = self.control.imageWorkControl();
        const content_identity = std.fmt.bytesToHex(self.summary.identity.source, .lower);
        var source = media.source.Source{ .allocator = self.allocator, .storage = .{ .borrowed = bytes }, .identity = &content_identity, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 }, .admission_pool = &self.pool, .control = .{ .context = adapter.context, .check_fn = adapter.check_fn } };
        if (bytes.len >= 4 and std.mem.eql(u8, bytes[0..4], "\x1a\x45\xdf\xa3")) {
            var audio_track: ?@import("media_chunking_audio_source.zig").Track = if (self.request.needs_audio) @import("media_chunking_audio_source.zig").Track.init(self.allocator, &source, self.control, self.request.max_working_bytes, self.request.limits.max_index_packets) catch |err| blk: {
                if (abort(err) and err != error.AudioTooLarge) return err;
                self.audio_failure = err;
                break :blk null;
            } else null;
            defer if (audio_track) |*track_owner| track_owner.deinit();
            if (audio_track) |*track_owner| self.audio_provider = track_owner.provider();
            defer self.audio_provider = null;
            var reader = try media.webm.Reader.init(self.allocator, &source, .{ .max_packets = self.request.limits.max_index_packets });
            defer reader.deinit();
            try self.webmWindows(&reader);
        } else {
            var audio_track: ?@import("media_chunking_audio_source.zig").Track = if (self.request.needs_audio) @import("media_chunking_audio_source.zig").Track.init(self.allocator, &source, self.control, self.request.max_working_bytes, self.request.limits.max_index_packets) catch |err| blk: {
                if (abort(err) and err != error.AudioTooLarge) return err;
                self.audio_failure = err;
                break :blk null;
            } else null;
            defer if (audio_track) |*track_owner| track_owner.deinit();
            if (audio_track) |*track_owner| self.audio_provider = track_owner.provider();
            defer self.audio_provider = null;
            var input = try @import("embedding_gemma2_video.zig").PacketInput.init(self.allocator, &source, bytes);
            defer input.deinit();
            var owner = try @import("media_chunking_video.zig").Owner.init(self.allocator, &input.reader, &self.pool, self.request.limits);
            defer owner.deinit();
            try self.videoWindows(bytes, &input.reader, try owner.durationNs(), owner.origin, input.reader.track.timescale, &owner, null);
        }
    }
    fn webmWindows(self: *Runner, reader: *media.webm.Reader) !void {
        var end: i128 = std.math.minInt(i128);
        var origin: i64 = std.math.maxInt(i64);
        var indexed = try video.webm_pictures.index(self.allocator, reader, .{ .max_packets = self.request.limits.max_index_packets, .max_pictures = self.request.limits.max_index_packets, .decoder = .{ .av1_all_layers = false, .control = reader.input.control } });
        defer indexed.deinit();
        var last_pts: i64 = std.math.minInt(i64);
        var last_duration: ?u64 = null;
        for (indexed.pictures) |picture| {
            if (picture.pts >= last_pts) {
                last_pts = picture.pts;
                last_duration = picture.duration;
            }
            origin = @min(origin, picture.pts);
            end = @max(end, @as(i128, picture.pts) + (picture.duration orelse 0));
        }
        if (origin == std.math.maxInt(i64)) return error.EmptyVideo;
        if (reader.duration_seconds) |duration| {
            end = @max(end, @as(i128, origin) + @as(i128, @intFromFloat(duration * std.time.ns_per_s)));
        } else if (last_duration == null or last_duration.? <= 0) return error.UnknownVideoDuration;
        if (end <= origin) return error.UnknownVideoDuration;
        const timeline_bytes = try std.math.add(usize, try video.sampling.Timeline.storageBytes(indexed.pictures.len), try std.math.mul(usize, indexed.pictures.len, @sizeOf(video.sampling.TimedPicture)));
        var timeline_reservation = try self.pool.acquire(.{ .host_bytes = timeline_bytes });
        defer timeline_reservation.deinit();
        const timed = try self.allocator.alloc(video.sampling.TimedPicture, indexed.pictures.len);
        defer self.allocator.free(timed);
        for (timed, indexed.pictures) |*dest, picture| dest.* = .{ .pts = picture.pts, .duration = picture.duration };
        var timeline = try video.sampling.Timeline.init(self.allocator, timed, reader.input.control);
        defer timeline.deinit();
        var continuation = video.webm_pictures.Continuation{ .allocator = self.allocator, .reader = reader };
        defer continuation.deinit();
        var apple_continuation = @import("embedding_gemma2_video.zig").WebmDecoderContinuation{ .allocator = self.allocator, .reader = reader };
        defer apple_continuation.deinit();
        try self.videoWindows(&.{}, null, end - origin, origin, 1_000_000_000, null, .{ .reader = reader, .index = &indexed, .timeline = &timeline, .continuation = &continuation, .apple_continuation = &apple_continuation });
    }
    const VideoSource = struct { bytes: []const u8, reader: ?*const media.mp4.Reader, origin: i64, timescale: u32, owner: ?*@import("media_chunking_video.zig").Owner, webm: ?@import("embedding_gemma2_video.zig").WebmInput };
    const VideoSlot = struct {
        shape: planning.Descriptor = undefined,
        payload: Payload = undefined,
        window: ?@import("media_chunking_video.zig").Window = null,
        ahead: ?*@import("embedding_gemma2.zig").VideoAhead = null,
        initialized: bool = false,
        failure: ?anyerror = null,
        measured: ?Preflight = null,
        measured_allocator: ?std.mem.Allocator = null,
        fn deinit(self: *@This()) void {
            if (self.ahead) |ahead| ahead.deinit();
            if (self.initialized) if (self.payload.video.prepared_outputs) |prepared| prepared.deinit(prepared.context);
            if (self.measured) |measured| if (measured.selected_pictures) |pictures| self.measured_allocator.?.free(pictures);
            if (self.window) |*window| window.deinit();
            self.* = .{};
        }
    };
    fn prepareVideoSlot(self: *Runner, source: VideoSource, slot: *VideoSlot) !void {
        var shape = &slot.shape;
        shape.coordinates.start_tick = std.math.cast(i64, @as(i128, source.origin) + @divFloor(@as(i128, shape.coordinates.start_ns.?) * source.timescale, std.time.ns_per_s)) orelse return error.TimestampOverflow;
        shape.coordinates.end_tick = std.math.cast(i64, @as(i128, source.origin) + @divFloor(@as(i128, shape.coordinates.end_ns.?) * source.timescale + std.time.ns_per_s - 1, std.time.ns_per_s)) orelse return error.TimestampOverflow;
        shape.coordinates.timescale = source.timescale;
        const policy = video.sampling.TimestampPolicy{ .start_ns = shape.coordinates.start_ns.?, .end_ns = shape.coordinates.end_ns, .step_ns = @intCast(self.request.policy.step_ns), .max_frames = @min(self.request.policy.max_frames, self.executor.model.max_frames), .overflow = .truncate, .hold = self.request.policy.video_hold != 0 };
        if (source.owner) |owner| {
            slot.window = try owner.prepare(policy);
            errdefer {
                slot.window.?.deinit();
                slot.window = null;
            }
            const window = &slot.window.?;
            try window.bind(policy, &self.pool);
            if (window.selection) |*selection| {
                selection.continuation = &owner.continuation;
                selection.packet_base = window.packet_base;
            }
            slot.payload = .{ .video = .{ .bytes = &.{}, .reader = &window.coded.reader, .policy = policy, .source_timestamps = true, .selection = if (window.selection) |*selection| selection else null, .packet_base = window.packet_base } };
        } else slot.payload = .{ .video = .{ .bytes = source.bytes, .reader = source.reader, .webm = source.webm, .policy = policy } };
        slot.initialized = true;
    }
    fn cacheVideoPreflight(self: *Runner, slot: *VideoSlot) !void {
        if (slot.measured == null) if (self.executor.preflight) |preflight| {
            const measured = try preflight(self.executor.context, slot.payload);
            errdefer if (measured.selected_pictures) |pictures| self.allocator.free(pictures);
            if (measured.input_tokens > try self.executor.model.capacity()) return error.ChunkUnitTooLarge;
            slot.measured = measured;
            slot.measured_allocator = self.allocator;
        };
        if (slot.payload.video.prepared_outputs == null) if (self.executor.prepare_video_outputs) |prepare| {
            slot.payload.video.prepared_outputs = try prepare(self.executor.context, self.allocator, slot.payload, slot.measured orelse return error.InvalidInputShape);
        };
    }
    const VideoSchedule = struct {
        runner: *Runner,
        source: VideoSource,
        next: ?*VideoSlot,
        second: ?*VideoSlot = null,
        cache_preflight: bool = false,
        work: ?*VideoSlot = null,
        thread: ?std.Thread = null,
        fn join(self: *@This()) void {
            if (self.thread) |thread| thread.join();
            self.thread = null;
        }
        fn joinOpaque(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.join();
        }
        fn ready(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const next = self.next orelse return;
            self.next = null; // At most one wave, even for a multi-part executor.
            self.work = next;
            if (comptime @import("builtin").single_threaded or @import("builtin").cpu.arch.isWasm() or @import("builtin").os.tag == .freestanding) return self.perform();
            self.thread = std.Thread.spawn(.{}, perform, .{self}) catch {
                // A missing speculative worker must not deny serial progress.
                self.work = null;
                return;
            };
        }
        fn perform(self: *@This()) void {
            const next = self.work orelse return;
            if (!next.initialized) self.runner.prepareVideoSlot(self.source, next) catch |err| {
                if (@import("embedding_gemma2.zig").videoSpeculationPressure(err)) return;
                next.failure = err;
                return;
            };
            if (self.cache_preflight) {
                self.runner.cacheVideoPreflight(next) catch |err| {
                    if (!@import("embedding_gemma2.zig").videoSpeculationPressure(err)) next.failure = err;
                    return;
                };
                if (self.second) |second| {
                    if (!second.initialized) self.runner.prepareVideoSlot(self.source, second) catch |err| {
                        if (!@import("embedding_gemma2.zig").videoSpeculationPressure(err)) second.failure = err;
                        return;
                    };
                    self.runner.cacheVideoPreflight(second) catch |err| {
                        if (!@import("embedding_gemma2.zig").videoSpeculationPressure(err)) second.failure = err;
                        return;
                    };
                }
            }
            const start = self.runner.executor.start_video orelse return;
            next.ahead = start(self.runner.executor.context, next.payload, &self.runner.pool) catch |err| {
                // Speculation must never deny a request which fits serially.
                // Malformed input is reported when this window is reached.
                if (!@import("embedding_gemma2.zig").videoSpeculationPressure(err)) next.failure = err;
                return;
            };
        }
    };
    fn videoBatches(self: *Runner, source: VideoSource, cursor: *planning.Temporal) !void {
        // Two current windows plus two metadata slots and one pixel wave ahead. Slots
        // never move: reader/selection pointers and producer leases stay valid.
        var slots = [_]VideoSlot{ .{}, .{}, .{}, .{} };
        defer for (&slots) |*slot| slot.deinit();
        var known: [4]bool = @splat(false);
        var active: usize = 0;
        while (true) {
            try self.control.check();
            if (self.request.max_chunks != 0 and self.summary.emitted >= self.request.max_chunks) {
                self.stopped = cursor.offset_ns < cursor.end_ns or known[active];
                return;
            }
            var count: usize = 0;
            while (count < 2 and (self.request.max_chunks == 0 or self.summary.emitted + count < self.request.max_chunks)) {
                const index = (active + count) % slots.len;
                const slot = &slots[index];
                if (!known[index]) {
                    slot.shape = (try cursor.next()) orelse break;
                    known[index] = true;
                }
                if (self.ordinal < self.summary.start_ordinal) {
                    self.ordinal = try std.math.add(u64, self.ordinal, 1);
                    known[index] = false;
                    continue;
                }
                if (slot.failure) |err| return err;
                if (!slot.initialized) self.prepareVideoSlot(source, slot) catch |err| {
                    if (count == 0 or !@import("embedding_gemma2.zig").videoSpeculationPressure(err)) return err;
                    break;
                };
                slot.payload.video.ahead = slot.ahead;
                self.cacheVideoPreflight(slot) catch |err| {
                    if (count == 0 or !@import("embedding_gemma2.zig").videoSpeculationPressure(err)) return err;
                    break;
                };
                if (slot.measured) |*measured| if (measured.video_plan) |*plan| {
                    slot.payload.video.plan = plan;
                };
                count += 1;
            }
            if (count == 0) return;
            const following = (active + count) % slots.len;
            if (!known[following] and (self.request.max_chunks == 0 or self.summary.emitted + count < self.request.max_chunks)) {
                if (try cursor.next()) |shape| {
                    slots[following].shape = shape;
                    known[following] = true;
                }
            }
            const second = (following + 1) % slots.len;
            if (!known[second] and (self.request.max_chunks == 0 or self.summary.emitted + count + 1 < self.request.max_chunks)) {
                if (try cursor.next()) |shape| {
                    slots[second].shape = shape;
                    known[second] = true;
                }
            }
            var schedule = VideoSchedule{ .runner = self, .source = source, .next = if (known[following]) &slots[following] else null, .second = if (known[second]) &slots[second] else null, .cache_preflight = true };
            defer schedule.join();
            if (self.executor.start_video != null or self.executor.prepare_video_outputs != null) {
                slots[(active + count - 1) % slots.len].payload.video.prepared = .{ .context = &schedule, .ready = VideoSchedule.ready };
                // Every emitted row waits before any secondary source access.
                for (0..count) |row| slots[(active + row) % slots.len].payload.video.preparation_join = .{ .context = &schedule, .join = VideoSchedule.joinOpaque };
            }
            var payloads: [2]Payload = undefined;
            for (payloads[0..count], 0..) |*payload, i| payload.* = slots[(active + i) % slots.len].payload;
            const outputs: ?[]Output = if (count > 1) self.executor.embed_video_batch.?(self.executor.context, self.allocator, payloads[0..count]) catch |err| blk: {
                if (!@import("embedding_gemma2.zig").videoSpeculationPressure(err) and err != error.BatchIncompatible and (!self.request.allow_partial or abort(err))) return err;
                // Compatibility rejection precedes projection and leaves
                // the current producer usable by independent execution.
                if (err == error.BatchIncompatible) break :blk null;
                schedule.join();
                // A following producer cannot remain active while an earlier
                // dependency span is replayed against the shared source/codec.
                if (slots[following].ahead) |ahead| {
                    ahead.deinit();
                    slots[following].ahead = null;
                    slots[following].payload.video.ahead = null;
                }
                // A failed batch may have consumed preparation. Join/drain each
                // old producer before replaying individual windows.
                for (0..count) |i| {
                    const slot = &slots[(active + i) % slots.len];
                    if (slot.ahead) |ahead| ahead.deinit();
                    slot.ahead = null;
                    slot.payload.video.ahead = null;
                }
                break :blk null;
            } else null;
            var between = VideoSchedule{ .runner = self, .source = source, .next = if (outputs == null and count > 1) &slots[(active + 1) % slots.len] else null, .cache_preflight = true };
            defer between.join();
            if (outputs == null and count > 1 and (self.executor.start_video != null or self.executor.prepare_video_outputs != null)) {
                slots[active].payload.video.prepared = .{ .context = &between, .ready = VideoSchedule.ready };
                slots[active].payload.video.preparation_join = .{ .context = &between, .join = VideoSchedule.joinOpaque };
            }
            var consumed: usize = 0;
            defer if (outputs) |values| {
                for (values[consumed..]) |value| self.allocator.free(value.vector);
                self.allocator.free(values);
            };
            if (outputs) |values| if (values.len != count) return error.InvalidEmbeddingOutput;
            for (0..count) |i| {
                if (i != 0) between.join();
                const slot = &slots[(active + i) % slots.len];
                if (slot.failure) |err| return err;
                slot.payload.video.ahead = slot.ahead;
                const ready = if (outputs) |values| values[i] else null;
                if (outputs != null) consumed += 1;
                const measured = slot.measured;
                slot.measured = null;
                slot.payload.video.plan = null;
                if (!try self.emitReady(slot.shape, slot.payload, self.request.input.binary.mime_type, ready, measured)) return;
            }
            schedule.join();
            for (0..count) |i| {
                const index = (active + i) % slots.len;
                slots[index].deinit();
                known[index] = false;
            }
            active = following;
        }
    }
    fn videoWindows(self: *Runner, bytes: []const u8, reader: ?*const media.mp4.Reader, duration: i128, origin: i64, timescale: u32, owner: ?*@import("media_chunking_video.zig").Owner, webm: ?@import("embedding_gemma2_video.zig").WebmInput) !void {
        const source = VideoSource{ .bytes = bytes, .reader = reader, .origin = origin, .timescale = timescale, .owner = owner, .webm = webm };
        const end = std.math.cast(i64, duration) orelse return error.TimestampOverflow;
        var cursor = planning.Temporal{ .kind = .video, .end_ns = end, .policy = self.request.policy, .model = self.executor.model, .picture_tokens = 142 };
        if (self.request.execute and self.executor.embed_video_batch != null) return self.videoBatches(source, &cursor);
        var slots = [_]VideoSlot{ .{}, .{} };
        defer for (&slots) |*slot| slot.deinit();
        var active: usize = 0;
        slots[0].shape = (try cursor.next()) orelse return;
        while (true) {
            if (self.ordinal < self.summary.start_ordinal) {
                self.ordinal = try std.math.add(u64, self.ordinal, 1);
                slots[active].shape = (try cursor.next()) orelse break;
                continue;
            }
            if (self.request.max_chunks != 0 and self.summary.emitted >= self.request.max_chunks) {
                self.stopped = true;
                break;
            }
            const current = &slots[active];
            if (current.failure) |err| return err;
            if (!current.initialized) try self.prepareVideoSlot(source, current);
            current.payload.video.ahead = current.ahead;
            if (self.executor.prepare_video_outputs != null) try self.cacheVideoPreflight(current);
            const next_shape = if (self.request.max_chunks != 0 and self.summary.emitted + 1 >= self.request.max_chunks) null else try cursor.next();
            const next = &slots[active ^ 1];
            if (next_shape) |shape| next.shape = shape;
            var schedule = VideoSchedule{ .runner = self, .source = source, .next = if (next_shape != null) next else null, .cache_preflight = self.executor.prepare_video_outputs != null };
            defer schedule.join(); // Includes failure, cancellation and early sink termination.
            if (self.request.execute and (self.executor.start_video != null or self.executor.prepare_video_outputs != null)) {
                current.payload.video.prepared = .{ .context = &schedule, .ready = VideoSchedule.ready };
                current.payload.video.preparation_join = .{ .context = &schedule, .join = VideoSchedule.joinOpaque };
            }
            const measured = current.measured;
            current.measured = null;
            if (!try self.emitReady(current.shape, current.payload, self.request.input.binary.mime_type, null, measured)) break;
            current.deinit();
            if (next_shape == null) {
                if (self.request.max_chunks != 0 and self.summary.emitted >= self.request.max_chunks and cursor.offset_ns < end) self.stopped = true;
                break;
            }
            active ^= 1;
        }
    }
};
pub fn abort(err: anyerror) bool {
    return switch (err) {
        error.Cancelled, error.Canceled, error.Timeout, error.DeadlineExceeded, error.OutOfMemory, error.MemoryBudgetExceeded, error.SharedAdmissionExceeded, error.ResourceTemporarilyUnavailable, error.ConcurrencyUnavailable, error.InferenceAdmissionQueueFull, error.QueueFull, error.ResourceLimitExceeded, error.AudioTooLarge, error.StreamTooLong, error.RemoteReadBudgetExceeded, error.RemoteRequestBudgetExceeded, error.ChunkWorkingBudgetExceeded, error.ChunkDependencyBudgetExceeded, error.ChunkCodedWindowBudgetExceeded, error.ChunkEventBudgetExceeded => true,
        else => false,
    };
}

const Qualification = struct {
    seen: usize = 0,
    kinds: [6]usize = @splat(0),
    ordinals: [128]u64 = @splat(0),
    last_end: i64 = 0,
    frames: usize = 0,
    fn count(_: *anyopaque, text_bytes: []const u8) !usize {
        return std.unicode.utf8CountCodepoints(text_bytes);
    }
    fn embed(_: *anyopaque, a: std.mem.Allocator, payload: Payload) !Output {
        const tokens: usize = switch (payload) {
            .text => |text_bytes| try std.unicode.utf8CountCodepoints(text_bytes),
            .raster => |r| blk: {
                try r.validate();
                break :blk 1;
            },
            .pcm => |pcm| blk: {
                if (pcm.samples.len == 0 or pcm.sample_rate == 0) return error.InvalidInputShape;
                break :blk 1;
            },
            .video => |v| blk: {
                try v.policy.validate();
                break :blk 1;
            },
        };
        return .{ .vector = try a.dupe(f32, &.{ 0.6, 0.8 }), .input_tokens = tokens };
    }
    fn accept(raw: *anyopaque, item: Item) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.seen >= self.ordinals.len) return error.ResourceLimitExceeded;
        self.ordinals[self.seen] = item.descriptor.ordinal;
        self.seen += 1;
        self.kinds[@backingInt(item.descriptor.coordinates.kind)] += 1;
        if (item.descriptor.coordinates.end_ns) |end| self.last_end = end;
        try item.descriptor.coordinates.validate();
        if (item.descriptor.coordinates.region != null) try std.testing.expect(item.descriptor.coordinates.raster_width != null);
        try std.testing.expect(item.id.len >= 66);
        try std.testing.expect(item.embedding != null and item.failure == null);
        if (item.descriptor.coordinates.kind == .gif) self.frames += 1;
    }
    fn executor(self: *@This()) Executor {
        return .{ .context = self, .count_text = count, .embed = embed, .model = .{ .planning_identity = "independent-unit-executor", .context_tokens = 8192 } };
    }
};
test "model aware media streams text coverage and source-bound continuations without duplicate delivery" {
    const a = std.testing.allocator;
    var result = Qualification{};
    var runner = try Runner.init(a, .{ .input = .{ .text = "hello 世界 across chunks" }, .policy = .{ .target_tokens = 5, .overlap_tokens = 1 }, .max_chunks = 1 }, result.executor(), .{}, .{ .context = &result, .accept = Qualification.accept });
    const first = try runner.run();
    try std.testing.expect(!first.complete);
    try std.testing.expectEqual(@as(usize, 1), first.emitted);
    var bytes: [85]u8 = undefined;
    const continuation = first.identity.cursor(first.next_ordinal, &bytes);
    var resumed = try Runner.init(a, .{ .input = .{ .text = "hello 世界 across chunks" }, .policy = .{ .target_tokens = 5, .overlap_tokens = 1 }, .continuation = continuation }, result.executor(), .{}, .{ .context = &result, .accept = Qualification.accept });
    try std.testing.expect((try resumed.run()).complete);
    for (result.ordinals[0..result.seen], 0..) |ordinal, expected| try std.testing.expectEqual(expected, ordinal);
    try std.testing.expectError(error.StaleChunkContinuation, Runner.init(a, .{ .input = .{ .text = "changed source" }, .policy = .{ .target_tokens = 5, .overlap_tokens = 1 }, .continuation = continuation }, result.executor(), .{}, .{ .context = &result, .accept = Qualification.accept }));
}
test "model aware media image regions borrow padded crops and animation preserves all composited frames" {
    const a = std.testing.allocator;
    const encoded = try chunker.png.encodeRgba(a, 2, 2, &.{ 1, 2, 3, 255, 4, 5, 6, 255, 7, 8, 9, 255, 10, 11, 12, 255 });
    defer a.free(encoded);
    var result = Qualification{};
    var runner = try Runner.init(a, .{ .input = .{ .binary = .{ .mime_type = "image/png", .data = encoded } }, .policy = .{ .tile_width = 1, .tile_height = 1 } }, result.executor(), .{}, .{ .context = &result, .accept = Qualification.accept });
    try std.testing.expect((try runner.run()).complete);
    try std.testing.expectEqual(@as(usize, 4), result.kinds[@backingInt(planning.Kind.image)]);
    const hex = "4749463839610100010000000021ff0b4e45545343415045322e30030100000021f90401050000002c000000000100010081000000ff000000ff0000000002024c010021f90401070000002c000000000100010081000000ff000000ff0000000002025401003b";
    var gif: [hex.len / 2]u8 = undefined;
    _ = try std.fmt.hexToBytes(&gif, hex);
    runner = try Runner.init(a, .{ .input = .{ .binary = .{ .mime_type = "image/gif", .data = &gif } } }, result.executor(), .{}, .{ .context = &result, .accept = Qualification.accept });
    try std.testing.expect((try runner.run()).complete);
    try std.testing.expectEqual(@as(usize, 2), result.frames);
    try std.testing.expectEqual(@as(i64, 120 * std.time.ns_per_ms), result.last_end);
}
test "model aware media audio retains the complete source tail and PDF preserves text and image page identities" {
    const a = std.testing.allocator;
    const encoded = try audio.wav.encodeInterleaved(a, &.{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7 }, 1, .{ .audio_format = 3, .sample_rate = 1000, .bits_per_sample = 32 });
    defer a.free(encoded);
    var result = Qualification{};
    var runner = try Runner.init(a, .{ .input = .{ .binary = .{ .mime_type = "audio/wav", .data = encoded } }, .policy = .{ .window_ns = 3 * std.time.ns_per_ms } }, result.executor(), .{}, .{ .context = &result, .accept = Qualification.accept });
    try std.testing.expect((try runner.run()).complete);
    try std.testing.expectEqual(@as(usize, 3), result.kinds[@backingInt(planning.Kind.audio)]);
    try std.testing.expectEqual(@as(i64, 7 * std.time.ns_per_ms), result.last_end);
    runner = try Runner.init(a, .{ .input = .{ .binary = .{ .mime_type = "application/pdf", .data = @embedFile("../testdata/chunk-pages.pdf") } } }, result.executor(), .{}, .{ .context = &result, .accept = Qualification.accept });
    try std.testing.expect((try runner.run()).complete);
    try std.testing.expectEqual(@as(usize, 2), result.kinds[@backingInt(planning.Kind.pdf)]);
}
test "model aware media video windows use original MP4 and WebM timing and stop explicitly at caps" {
    const a = std.testing.allocator;
    var result = Qualification{};
    var runner = try Runner.init(a, .{ .input = .{ .binary = .{ .mime_type = "video/quicktime", .data = @embedFile("../testdata/video/mjpeg-vfr.mov") } }, .policy = .{ .window_ns = 500 * std.time.ns_per_ms, .step_ns = 250 * std.time.ns_per_ms }, .max_chunks = 1 }, result.executor(), .{}, .{ .context = &result, .accept = Qualification.accept });
    const first = try runner.run();
    try std.testing.expectEqual(@as(usize, 1), first.emitted);
    runner = try Runner.init(a, .{ .input = .{ .binary = .{ .mime_type = "video/webm", .data = @embedFile("../testdata/video/vp9.webm") } } }, result.executor(), .{}, .{ .context = &result, .accept = Qualification.accept });
    try std.testing.expect((try runner.run()).complete);
    try std.testing.expect(result.kinds[@backingInt(planning.Kind.video)] >= 2);
}
test "model aware media consumer failure and cancellation do not acknowledge output" {
    const Fail = struct {
        fn accept(_: *anyopaque, _: Item) !void {
            return error.ConsumerFailure;
        }
        fn cancel(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    var result = Qualification{};
    var runner = try Runner.init(std.testing.allocator, .{ .input = .{ .text = "abc" } }, result.executor(), .{}, .{ .context = &result, .accept = Fail.accept });
    try std.testing.expectError(error.ConsumerFailure, runner.run());
    try std.testing.expectEqual(@as(u64, 0), runner.summary.next_ordinal);
    runner.control = .{ .check_fn = Fail.cancel };
    try std.testing.expectError(error.Cancelled, runner.run());
    try std.testing.expectEqual(@as(u64, 0), runner.summary.next_ordinal);
}

test "model aware media GIF streams beyond the legacy fifty-frame cap" {
    const a = std.testing.allocator;
    const hex = "4749463839610100010000000021ff0b4e45545343415045322e30030100000021f90401050000002c000000000100010081000000ff000000ff0000000002024c010021f90401070000002c000000000100010081000000ff000000ff0000000002025401003b";
    var source: [hex.len / 2]u8 = undefined;
    _ = try std.fmt.hexToBytes(&source, hex);
    const first = std.mem.indexOf(u8, &source, &.{ 0x21, 0xf9 }).?;
    const second = first + 2 + std.mem.indexOf(u8, source[first + 2 ..], &.{ 0x21, 0xf9 }).?;
    var bytes = std.ArrayList(u8).empty;
    defer bytes.deinit(a);
    try bytes.appendSlice(a, source[0..first]);
    for (0..60) |_| try bytes.appendSlice(a, source[first..second]);
    try bytes.append(a, 0x3b);
    var result = Qualification{};
    var runner = try Runner.init(a, .{ .input = .{ .binary = .{ .mime_type = "image/gif", .data = bytes.items } }, .max_working_bytes = 64 * 1024 }, result.executor(), .{}, .{ .context = &result, .accept = Qualification.accept });
    const summary = try runner.run();
    try std.testing.expect(summary.complete);
    try std.testing.expectEqual(@as(usize, 60), result.frames);
    try std.testing.expectEqual(@as(i64, 3 * std.time.ns_per_s), result.last_end);
}

test "model aware media audio sample clocks and pause boundaries cover the original samples" {
    const Capture = struct {
        covered: [1000]bool = @splat(false),
        fn accept(raw: *anyopaque, item: Item) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const coordinates = item.descriptor.coordinates;
            const first = coordinates.start_sample.?;
            const last = coordinates.end_sample.?;
            try std.testing.expect(last > first and last <= self.covered.len);
            for (self.covered[@intCast(first)..@intCast(last)]) |*value| value.* = true;
        }
    };
    const a = std.testing.allocator;
    for ([_]u32{ 1000, 44100 }) |rate| {
        var samples: [1000]f32 = @splat(0.2);
        @memset(samples[300..500], 0);
        const encoded = try audio.wav.encodeInterleaved(a, &samples, 1, .{ .audio_format = 3, .sample_rate = rate, .bits_per_sample = 32 });
        defer a.free(encoded);
        var output = Capture{};
        var executor = Qualification{};
        var runner = try Runner.init(a, .{ .input = .{ .binary = .{ .mime_type = "audio/wav", .data = encoded } }, .policy = .{ .window_ns = @intCast(@as(u64, 500) * std.time.ns_per_s / rate), .audio_pause_ns = @intCast(@as(u64, 50) * std.time.ns_per_s / rate) } }, executor.executor(), .{}, .{ .context = &output, .accept = Capture.accept });
        try std.testing.expect((try runner.run()).complete);
        for (output.covered) |covered| try std.testing.expect(covered);
    }
}

test "model aware media pinned MP4 ranges exceed the model whole-file ceiling without full reads" {
    const Provider = struct {
        const fixture = @embedFile("../testdata/video/mjpeg-vfr.mov");
        reads: usize = 0,
        bytes: usize = 0,
        fn read(raw: *anyopaque, offset: u64, out: []u8, control: media.source.Control) !usize {
            try control.check();
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            self.bytes += out.len;
            var free: [8]u8 = undefined;
            std.mem.writeInt(u32, free[0..4], 80 * 1024 * 1024, .big);
            @memcpy(free[4..8], "free");
            for (out, 0..) |*byte, index| {
                const position = offset + index;
                byte.* = if (position < fixture.len) fixture[@intCast(position)] else if (position - fixture.len < free.len) free[@intCast(position - fixture.len)] else 0;
            }
            return out.len;
        }
    };
    var provider = Provider{};
    var source = media.source.Source{ .allocator = std.testing.allocator, .identity = "immutable-large-mjpeg-v1", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.read, .length = Provider.fixture.len + 80 * 1024 * 1024 } } };
    var result = Qualification{};
    var runner = try Runner.init(std.testing.allocator, .{ .input = .{ .binary = .{ .mime_type = "video/quicktime", .data = &.{} } }, .native_source = &source, .source_version = "v1", .max_working_bytes = 1024 * 1024 }, result.executor(), .{}, .{ .context = &result, .accept = Qualification.accept });
    try std.testing.expect((try runner.run()).complete);
    try std.testing.expectEqual(@as(usize, 1), result.seen);
    try std.testing.expect(provider.reads > 0 and provider.bytes < 128 * 1024);
}

test "model aware media multi-hour WAV ranges retain one window and resume without reading the prefix" {
    const Provider = struct {
        header: [44]u8,
        bytes: usize = 0,
        fn read(raw: *anyopaque, offset: u64, out: []u8, control: media.source.Control) !usize {
            try control.check();
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.bytes += out.len;
            @memset(out, 0);
            if (offset < self.header.len) {
                const first: usize = @intCast(offset);
                const count = @min(out.len, self.header.len - first);
                @memcpy(out[0..count], self.header[first..][0..count]);
            }
            return out.len;
        }
    };
    const a = std.testing.allocator;
    const tiny = try audio.wav.encodeInterleaved(a, &.{0}, 1, .{ .audio_format = 3, .sample_rate = 48000, .bits_per_sample = 32 });
    defer a.free(tiny);
    var provider = Provider{ .header = tiny[0..44].* };
    const data_bytes = 2 * 60 * 60 * 48000 * 4;
    std.mem.writeInt(u32, provider.header[4..8], data_bytes + 36, .little);
    std.mem.writeInt(u32, provider.header[40..44], data_bytes, .little);
    var source = media.source.Source{ .allocator = a, .identity = "immutable-two-hour-wav-v1", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.read, .length = data_bytes + 44 } }, .limits = .{ .max_read_bytes = 64 * 1024, .max_retained_bytes = 128 * 1024, .max_total_bytes = 8 * 1024 * 1024 } };
    var result = Qualification{};
    var runner = try Runner.init(a, .{ .input = .{ .binary = .{ .mime_type = "audio/wav", .data = &.{} } }, .native_source = &source, .source_version = "v1", .max_working_bytes = 8 * 1024 * 1024, .max_chunks = 1 }, result.executor(), .{}, .{ .context = &result, .accept = Qualification.accept });
    const first = try runner.run();
    try std.testing.expect(!first.complete);
    try std.testing.expect(provider.bytes < 6 * 1024 * 1024);
    var cursor: [85]u8 = undefined;
    // Resume a distant acknowledged prefix without downloading its PCM again.
    const continuation = first.identity.cursor(100, &cursor);
    provider.bytes = 0;
    runner = try Runner.init(a, .{ .input = .{ .binary = .{ .mime_type = "audio/wav", .data = &.{} } }, .native_source = &source, .source_version = "v1", .max_working_bytes = 8 * 1024 * 1024, .max_chunks = 1, .continuation = continuation }, result.executor(), .{}, .{ .context = &result, .accept = Qualification.accept });
    try std.testing.expect(!(try runner.run()).complete);
    try std.testing.expectEqual(@as(u64, 100), result.ordinals[1]);
    try std.testing.expect(provider.bytes < 6 * 1024 * 1024);
}

test "model aware media WebM retains one verified index across range-backed model windows" {
    const Provider = struct {
        const fixture = @embedFile("../testdata/video/vp9.webm");
        const padding = 80 * 1024 * 1024;
        header: [48]u8,
        tail: [9]u8,
        bytes: usize = 0,
        fn read(raw: *anyopaque, offset: u64, out: []u8, control: media.source.Control) !usize {
            try control.check();
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.bytes += out.len;
            @memset(out, 0);
            for (out, 0..) |*byte, i| {
                const at = offset + i;
                if (at < self.header.len) byte.* = self.header[@intCast(at)] else if (at < fixture.len) byte.* = fixture[@intCast(at)] else if (at - fixture.len < self.tail.len) byte.* = self.tail[@intCast(at - fixture.len)];
            }
            return out.len;
        }
    };
    const Inspect = struct {
        fn preflight(_: *anyopaque, payload: Payload) !Preflight {
            const movie = payload.video;
            try std.testing.expectEqual(@as(usize, 0), movie.bytes.len);
            try std.testing.expect(movie.webm != null);
            const model = @import("embedding_gemma2.zig");
            const group = model.Group{ .content = &.{.{ .sampled_video = .{ .webm = movie.webm, .sampling = .{ .timestamps = movie.policy } } }} };
            try model.validateGroup(group, .{});
            const plan = try @import("embedding_gemma2_video.zig").inspectConfiguredPlan(std.testing.allocator, &.{}, null, .{}, .{ .webm = movie.webm, .sampling = .{ .timestamps = movie.policy } }, false);
            try std.testing.expect(plan.pictures > 0);
            return .{ .input_tokens = plan.pictures * 142 };
        }
    };
    var provider = Provider{ .header = Provider.fixture[0..48].*, .tail = @splat(0) };
    std.mem.writeInt(u64, provider.header[40..48], (@as(u64, 1) << 56) | (Provider.fixture.len - 48 + Provider.padding), .big);
    provider.tail[0] = 0xec;
    std.mem.writeInt(u64, provider.tail[1..9], (@as(u64, 1) << 56) | (Provider.padding - 9), .big);
    var source = media.source.Source{ .allocator = std.testing.allocator, .identity = "immutable-large-webm-v1", .storage = .{ .range = .{ .context = &provider, .read_at = Provider.read, .length = Provider.fixture.len + Provider.padding } } };
    var result = Qualification{};
    var executor = result.executor();
    executor.preflight = Inspect.preflight;
    var runner = try Runner.init(std.testing.allocator, .{ .input = .{ .binary = .{ .mime_type = "video/webm", .data = &.{} } }, .native_source = &source, .source_version = "v1", .max_working_bytes = 512 * 1024 * 1024 }, executor, .{}, .{ .context = &result, .accept = Qualification.accept });
    try std.testing.expect((try runner.run()).complete);
    try std.testing.expect(result.seen > 0);
    try std.testing.expect(provider.bytes < 256 * 1024);
}

test "model aware media streaming audio retains only forward windows and overlap" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "tone.mp3", "codec-corpus/owned-he-aac-v2-48000-mixed.m4a", "codec-corpus/owned-he-aac-48000-mono.m4a" }) |name| {
        const root_path = try std.fmt.allocPrint(a, "zig/lib/audio/testdata/{s}", .{name});
        defer a.free(root_path);
        const package_path = try std.fmt.allocPrint(a, "../../lib/audio/testdata/{s}", .{name});
        defer a.free(package_path);
        const bytes = std.Io.Dir.cwd().readFileAlloc(std.testing.io, root_path, a, .limited(1024 * 1024)) catch try std.Io.Dir.cwd().readFileAlloc(std.testing.io, package_path, a, .limited(1024 * 1024));
        defer a.free(bytes);
        var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = a, .limit = 8 * 1024 * 1024 };
        const stream = (try @import("media_chunking_audio.zig").Stream.start(bounded.allocator(), bytes, .{ .io = std.testing.io })) orelse return error.TestUnexpectedResult;
        var live = true;
        defer if (live) stream.deinit();
        var decoded = try audio.decode(a, bytes, .{});
        defer decoded.deinit();
        const provider = stream.provider();
        var first: usize = 0;
        while (first < provider.samples) {
            const last = @min(provider.samples, first + 4000);
            const window = try provider.read(provider.context, a, first, last);
            try std.testing.expectEqualSlices(f32, decoded.samples[first .. first + window.pcm.samples.len], window.pcm.samples);
            if (provider.final_samples.?(provider.context)) |total| {
                try std.testing.expectEqual(decoded.samples.len, total);
                break;
            }
            first = last - 400;
        }
        try stream.finish();
        stream.deinit();
        live = false;
        try std.testing.expectEqual(@as(usize, 0), bounded.live);
    }
}

test "model aware media immutable MP4 windows bind metadata without reading or copying coded packets" {
    const a = std.testing.allocator;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 128 * 1024 * 1024 } };
    defer std.debug.assert(pool.snapshot().host_bytes == 0);
    var source = media.source.Source{ .allocator = a, .identity = "immutable-view", .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg-vfr.mov") }, .admission_pool = &pool };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var owner = try @import("media_chunking_video.zig").Owner.init(a, &reader, &pool, .{});
    defer owner.deinit();
    const policy = video.sampling.TimestampPolicy{ .end_ns = 500 * std.time.ns_per_ms, .step_ns = 250 * std.time.ns_per_ms };
    var first = try owner.prepare(policy);
    defer first.deinit();
    try first.bind(policy, &pool);
    try std.testing.expect(first.coded.snapshot == null);
    try std.testing.expect(first.coded.reader.input == reader.input);
    const source_reads = source.reads;
    const selected = try first.selection.?.select(a, &first.coded.reader, .{ .source_timestamps = policy }, .{});
    defer a.free(selected);
    try std.testing.expectEqual(source_reads, source.reads);
    var second = try owner.prepare(policy);
    defer second.deinit();
    try second.bind(policy, &pool);
    try std.testing.expectEqual(source_reads, source.reads);
    for (first.selection.?.projection_keys, second.selection.?.projection_keys) |left, right| try std.testing.expect(std.meta.eql(left, right));
    const shifted_policy = video.sampling.TimestampPolicy{ .start_ns = 250 * std.time.ns_per_ms, .end_ns = 750 * std.time.ns_per_ms, .step_ns = 250 * std.time.ns_per_ms };
    var shifted = try owner.prepare(shifted_policy);
    defer shifted.deinit();
    try shifted.bind(shifted_policy, &pool);
    var overlaps: usize = 0;
    for (first.selection.?.projection_keys) |left| for (shifted.selection.?.projection_keys) |right| {
        if (left.picture == right.picture) {
            try std.testing.expect(std.meta.eql(left, right));
            overlaps += 1;
        }
    };
    try std.testing.expect(overlaps > 0);
    try std.testing.expectEqual(source_reads, source.reads);
}

test "model aware media video yields speculative pressure and schedules only one next window" {
    const a = std.testing.allocator;
    const Scheduled = struct {
        output: Qualification = .{},
        starts: usize = 0,
        backbone_started: std.atomic.Value(bool) = .init(false),
        fn start(raw: *anyopaque, payload: Payload, _: *media.admission.Pool) !?*@import("embedding_gemma2.zig").VideoAhead {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(payload == .video);
            const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
            while (!self.backbone_started.load(.acquire)) {
                if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
                try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
            }
            self.starts += 1;
            return error.QueueFull;
        }
        fn embed(raw: *anyopaque, allocator: std.mem.Allocator, payload: Payload) !Output {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.backbone_started.store(false, .release);
            if (payload.video.prepared) |hook| try hook.ready(hook.context);
            // A synchronous planning/start callback would time out above.
            self.backbone_started.store(true, .release);
            return Qualification.embed(raw, allocator, payload);
        }
    };
    var state = Scheduled{};
    var executor = state.output.executor();
    executor.context = &state;
    executor.embed = Scheduled.embed;
    executor.start_video = Scheduled.start;
    var runner = try Runner.init(a, .{ .input = .{ .binary = .{ .mime_type = "video/quicktime", .data = @embedFile("../testdata/video/mjpeg-vfr.mov") } }, .policy = .{ .window_ns = 500 * std.time.ns_per_ms, .step_ns = 250 * std.time.ns_per_ms } }, executor, .{}, .{ .context = &state.output, .accept = Qualification.accept });
    const summary = try runner.run();
    try std.testing.expect(summary.complete);
    try std.testing.expect(summary.emitted > 1);
    try std.testing.expectEqual(summary.emitted - 1, state.starts);
}

fn videoOwnerAllocationCheck(a: std.mem.Allocator) !void {
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = 128 * 1024 * 1024 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media.admission.Resources{}));
    var source = media.source.Source{ .allocator = a, .identity = "immutable-index-allocation", .storage = .{ .borrowed = @embedFile("../testdata/video/mjpeg-vfr.mov") }, .admission_pool = &pool };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var owner = try @import("media_chunking_video.zig").Owner.init(a, &reader, &pool, .{});
    defer owner.deinit();
    var window = try owner.prepare(.{ .end_ns = 500 * std.time.ns_per_ms, .step_ns = 250 * std.time.ns_per_ms });
    defer window.deinit();
}
test "model aware media presentation index releases every allocation and admission on failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, videoOwnerAllocationCheck, .{});
}

test "model aware media busy provider preserves acknowledged prefix even with partial results" {
    const Busy = struct {
        calls: usize = 0,
        fn embed(raw: *anyopaque, a: std.mem.Allocator, payload: Payload) !Output {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == 2) return error.QueueFull;
            return Qualification.embed(raw, a, payload);
        }
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |allow_partial| {
        var result = Qualification{};
        var busy = Busy{};
        var executor = result.executor();
        executor.context = &busy;
        executor.embed = Busy.embed;
        var request = Request{ .input = .{ .text = "hello world across chunks" }, .allow_partial = allow_partial, .policy = .{ .target_tokens = 5, .overlap_tokens = 1 } };
        var runner = try Runner.init(a, request, executor, .{}, .{ .context = &result, .accept = Qualification.accept });
        try std.testing.expectError(error.ResourceTemporarilyUnavailable, runner.run());
        try std.testing.expectEqual(@as(usize, 1), result.seen);
        try std.testing.expectEqual(@as(u64, 1), runner.summary.next_ordinal);
        try std.testing.expectEqual(@as(usize, 0), runner.summary.failed);
        var bytes: [85]u8 = undefined;
        request.continuation = runner.summary.identity.cursor(runner.summary.next_ordinal, &bytes);
        var resumed = try Runner.init(a, request, result.executor(), .{}, .{ .context = &result, .accept = Qualification.accept });
        try std.testing.expect((try resumed.run()).complete);
        for (result.ordinals[0..result.seen], 0..) |ordinal, expected| try std.testing.expectEqual(expected, ordinal);
        try std.testing.expectEqual(media.admission.Resources{}, runner.pool.snapshot());
    }
    try std.testing.expect(abort(error.QueueFull));
}

test "model aware media video backbone cohorts preserve acknowledgements and pressure fallback" {
    const Fake = struct {
        prepared: std.atomic.Value(usize) = .init(0),
        released: std.atomic.Value(usize) = .init(0),
        batched: usize = 0,
        serial: usize = 0,
        preflights: usize = 0,
        seen: [3]bool = @splat(false),
        accepted: usize = 0,
        deny: bool = false,
        fail_sink: bool = false,
        fn prepare(raw: *anyopaque, a: std.mem.Allocator, _: Payload, measured: Preflight) !PreparedVideoOutputs {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 144), measured.input_tokens);
            const Lease = struct {
                a: std.mem.Allocator,
                released: *std.atomic.Value(usize),
                fn deinit(context: *anyopaque) void {
                    const lease: *@This() = @ptrCast(@alignCast(context));
                    _ = lease.released.fetchAdd(1, .monotonic);
                    lease.a.destroy(lease);
                }
            };
            const lease = try a.create(Lease);
            lease.* = .{ .a = a, .released = &self.released };
            _ = self.prepared.fetchAdd(1, .monotonic);
            return .{ .context = lease, .deinit = Lease.deinit };
        }
        fn count(_: *anyopaque, _: []const u8) !usize {
            return 1;
        }
        fn preflight(raw: *anyopaque, payload: Payload) !Preflight {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const index: usize = @intCast(@divTrunc(payload.video.policy.start_ns, std.time.ns_per_s));
            try std.testing.expect(!self.seen[index]);
            self.seen[index] = true;
            self.preflights += 1;
            return .{ .input_tokens = 144 };
        }
        fn start(_: *anyopaque, _: Payload, _: *media.admission.Pool) !?*@import("embedding_gemma2.zig").VideoAhead {
            return null;
        }
        fn output(a: std.mem.Allocator, payload: Payload) !Output {
            const vector = try a.alloc(f32, 1);
            vector[0] = @floatFromInt(payload.video.policy.start_ns);
            return .{ .vector = vector, .input_tokens = 144 };
        }
        fn embed(raw: *anyopaque, a: std.mem.Allocator, payload: Payload) !Output {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.serial += 1;
            try std.testing.expect(payload.video.prepared_outputs != null);
            if (payload.video.prepared) |hook| try hook.ready(hook.context);
            return output(a, payload);
        }
        fn batch(raw: *anyopaque, a: std.mem.Allocator, payloads: []const Payload) ![]Output {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.batched += 1;
            if (self.deny) return error.ResourceTemporarilyUnavailable;
            if (payloads[payloads.len - 1].video.prepared) |hook| try hook.ready(hook.context);
            const values = try a.alloc(Output, payloads.len);
            errdefer a.free(values);
            var initialized: usize = 0;
            errdefer for (values[0..initialized]) |value| a.free(value.vector);
            for (values, payloads) |*value, payload| {
                try std.testing.expect(payload.video.prepared_outputs != null);
                value.* = try output(a, payload);
                initialized += 1;
            }
            return values;
        }
        fn accept(raw: *anyopaque, item: Item) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail_sink and self.accepted == 1) return error.ConsumerFailure;
            try std.testing.expectEqual(@as(u64, @intCast(self.accepted)), item.descriptor.ordinal);
            try std.testing.expectEqual(@as(f32, @floatFromInt(self.accepted * std.time.ns_per_s)), item.embedding.?[0]);
            self.accepted += 1;
        }
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |deny| {
        var fake = Fake{ .deny = deny };
        const executor = Executor{ .context = &fake, .model = .{ .planning_identity = "test-video-batch", .context_tokens = 8192 }, .count_text = Fake.count, .preflight = Fake.preflight, .prepare_video_outputs = Fake.prepare, .embed = Fake.embed, .embed_video_batch = Fake.batch };
        var runner = try Runner.init(a, .{ .input = .{ .binary = .{ .mime_type = "video/mp4", .data = &.{} } }, .policy = .{ .window_ns = std.time.ns_per_s, .step_ns = std.time.ns_per_s } }, executor, .{}, .{ .context = &fake, .accept = Fake.accept });
        try runner.videoWindows(&.{}, null, 3 * std.time.ns_per_s, 0, 1, null, null);
        try std.testing.expectEqual(@as(usize, 3), runner.summary.emitted);
        try std.testing.expectEqual(@as(usize, 3), fake.preflights);
        try std.testing.expectEqual(@as(usize, 1), fake.batched);
        try std.testing.expectEqual(@as(usize, if (deny) 3 else 1), fake.serial);
        try std.testing.expectEqual(media.admission.Resources{}, runner.pool.snapshot());
        try std.testing.expectEqual(fake.prepared.load(.monotonic), fake.released.load(.monotonic));
    }
    var fake = Fake{ .fail_sink = true };
    const executor = Executor{ .context = &fake, .model = .{ .planning_identity = "test-video-batch", .context_tokens = 8192 }, .count_text = Fake.count, .preflight = Fake.preflight, .prepare_video_outputs = Fake.prepare, .embed = Fake.embed, .embed_video_batch = Fake.batch };
    var runner = try Runner.init(a, .{ .input = .{ .binary = .{ .mime_type = "video/mp4", .data = &.{} } }, .policy = .{ .window_ns = std.time.ns_per_s, .step_ns = std.time.ns_per_s } }, executor, .{}, .{ .context = &fake, .accept = Fake.accept });
    try std.testing.expectError(error.ConsumerFailure, runner.videoWindows(&.{}, null, 3 * std.time.ns_per_s, 0, 1, null, null));
    try std.testing.expectEqual(@as(u64, 1), runner.summary.next_ordinal);
    try std.testing.expectEqual(fake.prepared.load(.monotonic), fake.released.load(.monotonic));
    var cursor: [85]u8 = undefined;
    var request = runner.request;
    request.continuation = runner.summary.identity.cursor(1, &cursor);
    fake.fail_sink = false;
    fake.seen = @splat(false);
    runner = try Runner.init(a, request, executor, .{}, .{ .context = &fake, .accept = Fake.accept });
    try runner.videoWindows(&.{}, null, 3 * std.time.ns_per_s, 0, 1, null, null);
    try std.testing.expectEqual(@as(usize, 3), fake.accepted);
    try std.testing.expectEqual(@as(u64, 3), runner.summary.next_ordinal);
    try std.testing.expectEqual(media.admission.Resources{}, runner.pool.snapshot());
    try std.testing.expectEqual(fake.prepared.load(.monotonic), fake.released.load(.monotonic));
}

test "model aware media outputs preserve boundary plans and distinguish required coverage from optional degradation" {
    const Fixture = struct {
        required: bool = false,
        reject_delivery: bool = false,
        fn execute(raw: *anyopaque, a: std.mem.Allocator, _: Payload, _: planning.Descriptor, _: ?EmbeddingContext, primary: ?Output, _: ?anyerror, _: bool) anyerror![]const output_types.Result {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const values = try a.alloc(output_types.Result, 2);
            values[0] = .{ .name = "clip", .kind = .embedding, .required = true, .model = "clip", .model_identity = "1111111111111111111111111111111111111111111111111111111111111111", .model_revision = "2222222222222222222222222222222222222222222222222222222222222222", .recipe_id = "3333333333333333333333333333333333333333333333333333333333333333", .status = .complete, .embedding = primary.?.vector };
            values[1] = .{ .name = "events", .kind = .classify, .required = self.required, .model = "events", .model_identity = "1111111111111111111111111111111111111111111111111111111111111111", .model_revision = "2222222222222222222222222222222222222222222222222222222222222222", .recipe_id = "3333333333333333333333333333333333333333333333333333333333333333", .status = .failed, .code = "InvalidClassifierOutput" };
            return values;
        }
        fn accept(raw: *anyopaque, item: Item) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.reject_delivery) return error.BrokenPipe;
            try std.testing.expectEqual(@as(usize, 2), item.outputs.len);
            try std.testing.expect(item.outputs[0].embedding != null);
            try std.testing.expectEqualStrings("InvalidClassifierOutput", item.outputs[1].code.?);
        }
    };
    const a = std.testing.allocator;
    var fixture = Fixture{};
    var target = [_]output_types.Target{ .{ .name = "clip", .kind = .embedding, .model = "clip" }, .{ .name = "events", .kind = .classify, .model = "events", .required = false } };
    var executor = Qualification{};
    const request = Request{ .input = .{ .text = "hello" }, .outputs = .{ .boundary_model = "clip", .targets = &target }, .output_executor = .{ .context = &fixture, .execute = Fixture.execute }, .allow_partial = true };
    var runner = try Runner.init(a, request, executor.executor(), .{}, .{ .context = &fixture, .accept = Fixture.accept });
    const optional = try runner.run();
    try std.testing.expect(optional.complete and optional.source_complete and optional.degraded);
    try std.testing.expectEqual(@as(usize, 1), optional.output_counts[0].complete);
    try std.testing.expectEqual(@as(usize, 1), optional.output_counts[1].failed);
    fixture.required = true;
    target[1].required = true;
    runner = try Runner.init(a, request, executor.executor(), .{}, .{ .context = &fixture, .accept = Fixture.accept });
    const required = try runner.run();
    try std.testing.expect(!required.complete and required.source_complete and !required.degraded);
    try std.testing.expectEqualSlices(u8, &optional.identity.plan, &required.identity.plan);
    try std.testing.expectEqual(@as(usize, 1), required.failed);
    fixture.reject_delivery = true;
    runner = try Runner.init(a, request, executor.executor(), .{}, .{ .context = &fixture, .accept = Fixture.accept });
    try std.testing.expectError(error.BrokenPipe, runner.run());
    try std.testing.expectEqual(@as(u64, 0), runner.summary.next_ordinal);
    try std.testing.expectEqual(@as(usize, 0), runner.summary.output_counts[1].failed);
}
