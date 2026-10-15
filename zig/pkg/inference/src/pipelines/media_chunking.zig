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
const image = @import("antfly_image");
const pdf = @import("antfly_pdf");
const media = @import("antfly_media");
const video = @import("antfly_video");
const audio = @import("audio.zig");
const control_mod = @import("../execution_control.zig");
pub const Control = control_mod.InferenceExecutionControl;
pub const Payload = union(enum) {
    text: []const u8,
    raster: image.BorrowedRasterAttachment,
    pcm: audio.PcmAudio,
    video: struct { webm: ?@import("embedding_gemma2_video.zig").WebmInput = null, bytes: []const u8, reader: ?*const media.mp4.Reader = null, policy: video.sampling.TimestampPolicy, source_timestamps: bool = false, selection: ?*const @import("embedding_gemma2_video.zig").ReaderSelection = null, packet_base: usize = 0 },
};
pub const Preflight = struct { input_tokens: usize, picture_count: ?u32 = null, selected_pictures: ?[]const u64 = null, picture_basis: ?planning.PictureBasis = null };
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
};
pub const Limits = @import("media_chunk_limits.zig").Limits;
pub const Request = struct {
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
    pub fn init(a: std.mem.Allocator, request: Request, executor: Executor, control: Control, sink: Sink) !Runner {
        try control.check();
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
        return .{ .allocator = a, .executor = executor, .request = request, .control = control, .sink = sink, .summary = .{ .identity = identity, .start_ordinal = start, .next_ordinal = start }, .pool = .{ .limits = .{ .host_bytes = request.max_working_bytes } } };
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
        var selected_pictures: ?[]const u64 = null;
        defer if (selected_pictures) |pictures| self.allocator.free(pictures);
        if (self.executor.preflight) |preflight| {
            const measured = try preflight(self.executor.context, payload);
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
        var result: ?Output = null;
        defer if (result) |value| self.allocator.free(value.vector);
        if (self.request.execute) {
            result = self.executor.embed(self.executor.context, self.allocator, payload) catch |err| blk: {
                if (!self.request.allow_partial or abort(err)) return err;
                item.failure = err;
                break :blk null;
            };
            if (result) |value| {
                if (value.input_tokens > self.executor.model.context_tokens) return error.ChunkUnitTooLarge;
                for (value.vector) |v| if (!std.math.isFinite(v)) return error.InvalidEmbeddingOutput;
                item.embedding = value.vector;
                item.descriptor.input_tokens = value.input_tokens;
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
        self.summary.failed += @intFromBool(item.failure != null);
        if (item.failure != null and self.summary.retry_ordinal == null) self.summary.retry_ordinal = ordinal;
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
        self.summary.complete = !self.stopped and self.summary.failed == 0;
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
        while (try cursor.next()) |planned| {
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
            if (last < provider.samples) {
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
        if (std.ascii.startsWithIgnoreCase(mime, "video/")) {
            var prefix = try source.read(0, @intCast(@min(4, source.length())));
            const webm = prefix.bytes.len == 4 and std.mem.eql(u8, prefix.bytes, "\x1a\x45\xdf\xa3");
            prefix.deinit();
            if (webm) {
                var reader = try media.webm.Reader.init(self.allocator, &source, .{ .max_packets = self.request.limits.max_index_packets });
                defer reader.deinit();
                try self.webmWindows(&reader);
                return;
            } else {
                var reader = try media.mp4.Reader.init(self.allocator, &source, .{ .max_samples = self.request.limits.max_index_packets, .max_index_bytes = @min(self.request.max_working_bytes, try std.math.mul(usize, self.request.limits.max_index_packets, @sizeOf(media.mp4.Packet))) });
                defer reader.deinit();
                var owner = try @import("media_chunking_video.zig").Owner.init(self.allocator, &reader, &self.pool, self.request.limits);
                defer owner.deinit();
                try self.videoWindows(&.{}, &reader, try owner.durationNs(), owner.origin, reader.track.timescale, &owner, null);
                return;
            }
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
        const adapter = self.control.imageWorkControl();
        const content_identity = std.fmt.bytesToHex(self.summary.identity.source, .lower);
        var source = media.source.Source{ .allocator = self.allocator, .storage = .{ .borrowed = bytes }, .identity = &content_identity, .limits = .{ .max_total_bytes = 512 * 1024 * 1024 }, .admission_pool = &self.pool, .control = .{ .context = adapter.context, .check_fn = adapter.check_fn } };
        if (bytes.len >= 4 and std.mem.eql(u8, bytes[0..4], "\x1a\x45\xdf\xa3")) {
            var reader = try media.webm.Reader.init(self.allocator, &source, .{ .max_packets = self.request.limits.max_index_packets });
            defer reader.deinit();
            try self.webmWindows(&reader);
        } else {
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
        try self.videoWindows(&.{}, null, end - origin, origin, 1_000_000_000, null, .{ .reader = reader, .index = &indexed });
    }
    fn videoWindows(self: *Runner, bytes: []const u8, reader: ?*const media.mp4.Reader, duration: i128, origin: i64, timescale: u32, owner: ?*@import("media_chunking_video.zig").Owner, webm: ?@import("embedding_gemma2_video.zig").WebmInput) !void {
        const end = std.math.cast(i64, duration) orelse return error.TimestampOverflow;
        var cursor = planning.Temporal{ .kind = .video, .end_ns = end, .policy = self.request.policy, .model = self.executor.model, .picture_tokens = 142 };
        while (try cursor.next()) |planned| {
            var shape = planned;
            shape.coordinates.start_tick = std.math.cast(i64, @as(i128, origin) + @divFloor(@as(i128, shape.coordinates.start_ns.?) * timescale, std.time.ns_per_s)) orelse return error.TimestampOverflow;
            shape.coordinates.end_tick = std.math.cast(i64, @as(i128, origin) + @divFloor(@as(i128, shape.coordinates.end_ns.?) * timescale + std.time.ns_per_s - 1, std.time.ns_per_s)) orelse return error.TimestampOverflow;
            shape.coordinates.timescale = timescale;
            const policy = video.sampling.TimestampPolicy{ .start_ns = shape.coordinates.start_ns.?, .end_ns = shape.coordinates.end_ns, .step_ns = @intCast(self.request.policy.step_ns), .max_frames = @min(self.request.policy.max_frames, self.executor.model.max_frames), .overflow = .truncate, .hold = self.request.policy.video_hold != 0 };
            if (self.ordinal < self.summary.start_ordinal) {
                self.ordinal = try std.math.add(u64, self.ordinal, 1);
                continue;
            }
            if (self.request.max_chunks != 0 and self.summary.emitted >= self.request.max_chunks) {
                self.stopped = true;
                break;
            }
            if (owner) |source_owner| {
                var prepared = try source_owner.prepare(policy);
                defer prepared.deinit();
                try prepared.bind(policy, &self.pool);
                if (prepared.selection) |*selection| {
                    selection.continuation = &source_owner.continuation;
                    selection.packet_base = prepared.packet_base;
                }
                if (!try self.emit(shape, .{ .video = .{ .bytes = &.{}, .reader = &prepared.coded.reader, .policy = policy, .source_timestamps = true, .selection = if (prepared.selection) |*selection| selection else null, .packet_base = prepared.packet_base } }, self.request.input.binary.mime_type)) break;
            } else if (!try self.emit(shape, .{ .video = .{ .bytes = bytes, .reader = reader, .webm = webm, .policy = policy } }, self.request.input.binary.mime_type)) break;
        }
    }
};
pub fn abort(err: anyerror) bool {
    return switch (err) {
        error.Cancelled, error.Canceled, error.Timeout, error.DeadlineExceeded, error.OutOfMemory, error.MemoryBudgetExceeded, error.SharedAdmissionExceeded, error.ResourceTemporarilyUnavailable, error.ConcurrencyUnavailable, error.InferenceAdmissionQueueFull, error.ResourceLimitExceeded, error.AudioTooLarge, error.StreamTooLong, error.RemoteReadBudgetExceeded, error.RemoteRequestBudgetExceeded, error.ChunkWorkingBudgetExceeded, error.ChunkDependencyBudgetExceeded, error.ChunkCodedWindowBudgetExceeded, error.ChunkEventBudgetExceeded => true,
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
    const bytes = std.Io.Dir.cwd().readFileAlloc(std.testing.io, "zig/lib/audio/testdata/tone.mp3", a, .limited(1024 * 1024)) catch try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "../../lib/audio/testdata/tone.mp3", a, .limited(1024 * 1024));
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
        try std.testing.expectEqualSlices(f32, decoded.samples[first..last], window.pcm.samples);
        if (last == provider.samples) break;
        first = last - 400;
    }
    try stream.finish();
    stream.deinit();
    live = false;
    try std.testing.expectEqual(@as(usize, 0), bounded.live);
}
