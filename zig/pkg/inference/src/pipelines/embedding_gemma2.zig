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

//! Ordered text/image/audio/video groups, one embedding per group.
const std = @import("std");
const factory = @import("../architectures/session_factory.zig");
const encoder = @import("../architectures/embedding_gemma2.zig");
const projector = @import("../architectures/gemma4_projector.zig");
const Session = @import("../backends/session.zig").Session;
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const media_admission = @import("antfly_media").admission;
const scoring = @import("antfly_decisions").scoring;

fn visionCacheCanRelieve(pressure: @import("../runtime/tier/memory.zig").AdmissionPressure) bool {
    return switch (pressure) {
        .shared_unified, .external_budget, .live_host => true,
        .domain_backend, .domain_combined => |backend| backend == .gpu,
        else => false,
    };
}

fn admitWithVisionCacheReclaim(session: Session, request: @import("../backends/session.zig").RunRequest, control: Control) !@import("../backends/session.zig").RunPermit {
    return admitReclaimingVisionCache(session, request, control, session, factory.reclaimEmbeddingGemma2VisionWeights, factory.reclaimEmbeddingGemma2LanguageWeights);
}

/// Optional caches cannot starve an unchanged request reservation. Prefer
/// vision reclamation; yield language constants only if pressure persists.
/// Each class is attempted once, with no waiting or native locks across admit.
fn admitReclaimingVisionCache(session: Session, request: @import("../backends/session.zig").RunRequest, control: Control, context: anytype, comptime reclaim_vision: anytype, comptime reclaim_language: anytype) !@import("../backends/session.zig").RunPermit {
    try control.check();
    var pressure: ?@import("../runtime/tier/memory.zig").AdmissionPressure = null;
    return session.admitWithPressure(request, &pressure) catch |err| blk: {
        var denied: anyerror = err;
        inline for (.{ reclaim_vision, reclaim_language }) |reclaim| {
            const reason = pressure orelse return denied;
            if (!visionCacheCanRelieve(reason)) return denied;
            try control.check();
            const bytes = try reclaim(context, control);
            try control.check();
            if (bytes != 0) {
                pressure = null;
                if (session.admitWithPressure(request, &pressure)) |permit| {
                    break :blk permit;
                } else |failure| denied = failure;
            }
        }
        return denied;
    };
}
pub const SampledVideo = struct { webm: ?@import("embedding_gemma2_video.zig").WebmInput = null, bytes: []const u8 = &.{}, reader: ?*const @import("antfly_media").mp4.Reader = null, selection: ?*const @import("embedding_gemma2_video.zig").ReaderSelection = null, sampling: @import("embedding_gemma2_video.zig").Sampling };
pub const Part = union(enum) { text: []const u8, image: []const u8, audio: []const u8, pcm: @import("audio.zig").PcmAudio, video: []const u8, sampled_video: SampledVideo, raster: @import("antfly_image").BorrowedRasterAttachment };

/// Final response materialization; normalization already completed on device.
fn readNormalizedVector(cb: *const @import("../ops/ops.zig").ComputeBackend, a: std.mem.Allocator, pooled: @import("../ops/ops.zig").CT, dimensions: usize) ![]f32 {
    const vector = try cb.toFloat32(pooled, a);
    errdefer a.free(vector);
    if (vector.len != dimensions) return error.InvalidEmbeddingDimensions;
    var nonzero = false;
    for (vector) |value| {
        if (!std.math.isFinite(value)) return error.InvalidEmbeddingDecisionOutput;
        nonzero = nonzero or value != 0;
    }
    if (!nonzero) return error.InvalidEmbeddingDecisionOutput;
    return vector;
}

fn partBytes(part: Part) []const u8 {
    return switch (part) {
        .raster => |raster| raster.bytes,
        .pcm => |pcm| std.mem.sliceAsBytes(pcm.samples),
        .sampled_video => |value| if (value.reader != null) &.{} else value.bytes,
        inline else => |bytes| bytes,
    };
}
pub const Group = struct { title: ?[]const u8 = null, content: []const Part };
pub const MediaTimings = struct {
    preprocess_ns: u64 = 0,
    gpu_frame_ns: u128 = 0,
    frame_submits: u64 = 0,
    frame_wait_ns: u128 = 0,
    vision_ns: u64 = 0,
    audio_ns: u64 = 0,
    pack_ns: u64 = 0,
    encoder_ns: u64 = 0,
    pool_readback_ns: u64 = 0,
    resident_cohorts: usize = 0,
    host_cohorts: usize = 0,
    preparation_jobs: usize = 0,
    preparation_backpressure_retries: usize = 0,
    max_prepared_images: usize = 0,
    max_prepared_audio: usize = 0,
    download_bytes: ?u64 = null,
    readbacks: ?u64 = null,
    device_owned_peak_bytes: ?u64 = null,
};
pub const Options = struct {
    video_decoder: @import("antfly_video").decoding.Options = .{},
    video_ahead: ?*VideoAhead = null,
    video_prepared: ?VideoPreparedHook = null,
    task_type: []const u8 = "RETRIEVAL_DOCUMENT",
    dimensions: usize = 768,
    resident_images: bool = true,
    resident_audio: bool = true,
    /// At most one admitted CPU preparation wave ahead of accelerator work.
    prepare_ahead: bool = true,
    /// Optional caller-owned observation for synchronous offline profiling.
    /// GPU stages enqueue asynchronously; only final readback is synchronized.
    timings: ?*MediaTimings = null,
};
fn mediaNow() u64 {
    return @import("antfly_platform").time.monotonicNs();
}
fn residentImagesEnabled(cb: *const ComputeBackend, options: Options) bool {
    return options.resident_images and (cb.kind() == .metal or cb.kind() == .cuda) and cb.supportsCopyRows2D() and cb.vtable.concatRows2D != null and cb.vtable.sliceRows2D != null;
}
pub const ReaderSelection = @import("embedding_gemma2_video.zig").ReaderSelection;
pub const LiveStream = @import("embedding_gemma2_live_stream.zig").Stream;
pub const LiveStreamDelivery = @import("embedding_gemma2_live_stream.zig").Delivery;
pub const FrameCache = @import("embedding_gemma2_video.zig").FrameCache;
pub const VideoPreparedHook = struct {
    context: *anyopaque,
    ready: *const fn (*anyopaque) anyerror!void,
};
/// One request-owned preparation wave, independent of a model call's workspace.
/// The source/window and request pool outlive this owner; closing always joins
/// the producer before returning permits or allowing another decoder consumer.
pub const VideoAhead = struct {
    allocator: std.mem.Allocator,
    plan: @import("embedding_gemma2_video.zig").Plan,
    movie: SampledVideo,
    cache: ?*FrameCache,
    decoder: @import("antfly_video").decoding.Options,
    bounded: @import("../runtime/bounded_allocator.zig").BoundedAllocator,
    pool: media_admission.Pool,
    request_reservation: media_admission.Token,
    permit: @import("../backends/session.zig").RunPermit,
    guard: @import("../execution_control.zig").UninterruptibleGuard,
    producer: ?*@import("embedding_gemma2_video.zig").Prefetch = null,
    released: bool = false,
    pub fn start(session: Session, movie: SampledVideo, control: Control, cache: ?*FrameCache, request_pool: *media_admission.Pool, decoder: @import("antfly_video").decoding.Options) !?*VideoAhead {
        try control.check();
        if (comptime @import("builtin").single_threaded or @import("builtin").cpu.arch.isWasm() or @import("builtin").os.tag == .freestanding) return null;
        const context = try factory.videoPreparationContext(session);
        const io = control.io orelse context.io orelse return null;
        const a = workspaceBackingAllocator(session.backend());
        const available = request_pool.limits.host_bytes -| request_pool.snapshot().host_bytes;
        const host: usize = @intCast(@min(256 * 1024 * 1024, available));
        if (host < 32 * 1024 * 1024) return null;
        var request_reservation = try request_pool.acquire(.{ .host_bytes = host });
        errdefer request_reservation.deinit();
        const plan = blk: {
            var preflight = try session.admitHostPreprocess(host);
            defer preflight.deinit();
            var inspection = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = a, .limit = host };
            defer std.debug.assert(inspection.live == 0);
            break :blk try @import("embedding_gemma2_video.zig").inspectConfiguredPlan(inspection.allocator(), movie.bytes, movie.reader, control, .{ .sampling = movie.sampling, .selection = movie.selection, .webm = movie.webm, .decoder = decoder }, session.backend() == .metal);
        };
        const device = if (session.backend() == .metal) try plan.deviceBytes() else 0;
        var permit = try session.admitRetainedMedia(host, device);
        errdefer permit.deinit();
        var guard = try control.enterUninterruptible(session.interruption());
        errdefer guard.deinit();
        const self = try a.create(VideoAhead);
        errdefer a.destroy(self);
        self.* = .{ .allocator = a, .plan = plan, .movie = movie, .cache = cache, .decoder = decoder, .bounded = .{ .backing = a, .limit = host }, .pool = .{ .limits = .{ .host_bytes = host, .device_bytes = device, .commands = plan.commands } }, .request_reservation = request_reservation, .permit = permit, .guard = guard };
        self.producer = try @import("embedding_gemma2_video.zig").Prefetch.startConfigured(self.bounded.allocator(), io, context.device, movie.bytes, movie.reader, control, .{ .sampling = movie.sampling, .selection = movie.selection, .webm = movie.webm, .cache = cache, .pool = &self.pool, .decoder = decoder });
        return self;
    }
    pub fn matches(self: *const VideoAhead, movie: SampledVideo, decoder: @import("antfly_video").decoding.Options, session: Session, cache: ?*FrameCache) bool {
        return self.permit.session.ptr == session.ptr and self.permit.session.vtable == session.vtable and self.cache == cache and self.decoder.policy == decoder.policy and self.decoder.telemetry == decoder.telemetry and self.movie.reader == movie.reader and self.movie.selection == movie.selection and self.movie.bytes.len == movie.bytes.len and (movie.bytes.len == 0 or self.movie.bytes.ptr == movie.bytes.ptr) and std.meta.eql(self.movie.sampling, movie.sampling) and std.meta.eql(self.movie.webm, movie.webm);
    }
    pub fn release(self: *VideoAhead) void {
        if (self.released) return;
        if (self.producer) |producer| producer.deinit();
        self.producer = null;
        std.debug.assert(self.bounded.live == 0);
        std.debug.assert(std.meta.eql(self.pool.snapshot(), media_admission.Resources{}));
        self.guard.deinit();
        self.request_reservation.deinit();
        self.permit.deinit();
        self.released = true;
    }
    pub fn abandon(self: *VideoAhead) void {
        self.release();
    }
    pub fn deinit(self: *VideoAhead) void {
        self.abandon();
        self.allocator.destroy(self);
    }
};
pub const Result = struct { vector: []f32, input_tokens: usize };

const CT = @import("../ops/ops.zig").CT;
const SoftTokens = struct { id: i64, embeddings: []f32, tokens: usize, tensor: ?CT = null };

/// Keep live workspace accounting independent of the platform allocator. On
/// macOS libc reuses large CPU buffers instead of remapping them per operation.
/// Other backends and platforms retain their existing allocation policy.
pub fn workspaceBackingAllocator(backend: @import("../backends/backends.zig").BackendType) std.mem.Allocator {
    if (comptime @import("builtin").os.tag == .macos and @import("builtin").link_libc) {
        if (backend == .native) return std.heap.c_allocator;
    }
    return std.heap.smp_allocator;
}

/// Embed a finite, presentation-complete HLS/DASH packet window. The caller
/// provides preroll/reorder context and ends windows at configuration epochs.
/// Model lock, admission, cancellation and CPU/Metal scheduling match embed().
pub fn embedLiveWindow(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, segments: []const @import("antfly_media").live_mp4.Segment, sampling: @import("antfly_video").sampling.TimestampPolicy, caption: ?[]const u8, options: Options, lock: ?*std.atomic.Mutex, control: ?Control, pool: ?*@import("antfly_media").admission.Pool) !Result {
    try sampling.validate();
    const effective = control orelse Control{};
    const adapter = effective.imageWorkControl();
    var window_permit = try session.admitHostPreprocess(192 * 1024 * 1024);
    defer window_permit.deinit();
    var window = try @import("antfly_video").live_windows.join(out, "live-model-window", segments, .{ .stream = .{ .admission_pool = pool, .control = .{ .context = adapter.context, .check_fn = adapter.check_fn } } });
    defer window.deinit();
    const parts = [_]Part{ .{ .sampled_video = .{ .reader = &window.reader, .sampling = .{ .timestamps = sampling } } }, .{ .text = caption orelse "" } };
    return embed(out, session, tokenizer, .{ .content = parts[0..if (caption != null) 2 else 1] }, options, lock, control);
}

pub const LiveResult = struct {
    result: ?Result,
    source_start_ns: i64,
    source_end_ns: i64,
    policy: @import("antfly_video").sampling.TimestampPolicy,
    ordinal: usize,
    epoch: usize,
    dash_timing: ?@import("antfly_media").hls.DashTiming,
    hls_timing: ?@import("antfly_media").live_mp4.HlsTiming,
};
/// Pump currently published units and embed one transactional live window.
/// Null means explicit reload is required unless the transport has ended.
/// Failed inference/commit preserves the coordinator's retry cursor.
pub fn embedNextLive(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, coordinator: *@import("antfly_video").live_coordinator.Coordinator, reader: anytype, caption: ?[]const u8, options: Options, lock: ?*std.atomic.Mutex, control: ?Control) !?LiveResult {
    return embedNextLiveCached(out, session, tokenizer, coordinator, reader, caption, options, lock, control, null);
}
/// Cache belongs to this immutable model session and outlives each joined producer.
pub fn embedNextLiveCached(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, coordinator: *@import("antfly_video").live_coordinator.Coordinator, reader: anytype, caption: ?[]const u8, options: Options, lock: ?*std.atomic.Mutex, control: ?Control, cache: ?*FrameCache) !?LiveResult {
    const effective = control orelse Control{};
    try effective.check();
    if (cache) |owner| if (owner.session.ptr != session.ptr or owner.session.vtable != session.vtable) return error.InvalidVideoCacheSession;
    if (coordinator.options.window.stream.max_bytes > 64 * 1024 * 1024 or coordinator.options.window.max_packets > 8192) return error.ResourceLimitExceeded;
    var window_permit = try session.admitHostPreprocess(512 * 1024 * 1024);
    defer window_permit.deinit();
    const adapter = effective.imageWorkControl();
    const old_control = coordinator.options.window.stream.control;
    coordinator.options.window.stream.control = .{ .context = adapter.context, .check_fn = adapter.check_fn };
    defer coordinator.options.window.stream.control = old_control;
    var prepared = (try coordinator.pull(reader)) orelse return null;
    defer prepared.deinit();
    const result = try embedPreparedLiveCached(out, session, tokenizer, coordinator, &prepared, caption, options, lock, control, cache);
    errdefer if (result.result) |value| out.free(value.vector);
    try coordinator.commit(&prepared);
    return result;
}
/// Compute an independently owned vector without advancing the live window.
/// Streaming delivery retains it until output acknowledgement and commit succeed.
pub fn embedPreparedLiveCached(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, coordinator: *@import("antfly_video").live_coordinator.Coordinator, prepared: *@import("antfly_video").live_coordinator.Prepared, caption: ?[]const u8, options: Options, lock: ?*std.atomic.Mutex, control: ?Control, cache: ?*FrameCache) !LiveResult {
    const effective = control orelse Control{};
    try effective.check();
    const sampling = @import("embedding_gemma2_video.zig").Sampling{ .source_timestamps = prepared.policy };
    var selection: ?@import("embedding_gemma2_video.zig").ReaderSelection = if (prepared.has_pictures and prepared.logical != null) try @import("embedding_gemma2_video.zig").ReaderSelection.init(out, &prepared.window.reader, sampling, &prepared.logical.?, coordinator.options.window.stream.admission_pool, coordinator.options.window.stream.control) else null;
    defer if (selection) |*owner| owner.deinit();
    const parts = [_]Part{ .{ .sampled_video = .{ .reader = &prepared.window.reader, .sampling = sampling, .selection = if (selection) |*owner| owner else null } }, .{ .text = caption orelse "" } };
    const result: ?Result = if (prepared.has_pictures) try embedCached(out, session, tokenizer, .{ .content = parts[0..if (caption != null) 2 else 1] }, options, lock, control, cache) else null;
    errdefer if (result) |value| out.free(value.vector);
    return .{ .result = result, .source_start_ns = prepared.source_start_ns, .source_end_ns = prepared.source_end_ns, .policy = prepared.policy, .ordinal = prepared.ordinal, .epoch = prepared.window.epoch, .dash_timing = prepared.dash_timing, .hls_timing = prepared.hls_timing };
}

pub fn embed(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, group: Group, options: Options, lock: ?*std.atomic.Mutex, control: ?Control) !Result {
    return embedCached(out, session, tokenizer, group, options, lock, control, null);
}
pub fn videoSpeculationPressure(err: anyerror) bool {
    return err == error.ResourceTemporarilyUnavailable or err == error.OutOfMemory or err == error.SharedAdmissionExceeded or err == error.MemoryBudgetExceeded or err == error.InferenceAdmissionQueueFull or err == error.ConcurrencyUnavailable;
}
pub fn embedCached(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, group: Group, options: Options, lock: ?*std.atomic.Mutex, control: ?Control, cache: ?*FrameCache) !Result {
    if (cache) |owner| {
        if (owner.session.ptr != session.ptr or owner.session.vtable != session.vtable) return error.InvalidVideoCacheSession;
        if (options.video_ahead == null) owner.control = control orelse .{};
    }
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = workspaceBackingAllocator(session.backend()), .limit = 512 * 1024 * 1024 };
    defer std.debug.assert(bounded.live == 0);
    const result = embedScoped(bounded.allocator(), session, tokenizer, group, options, lock, control, cache) catch |err| blk: {
        if (err == error.SpeculativeVideoAdmissionDenied) {
            options.video_ahead.?.abandon();
            var serial = options;
            serial.video_ahead = null;
            break :blk try embedScoped(bounded.allocator(), session, tokenizer, group, serial, lock, control, cache);
        }
        return if ((err == error.OutOfMemory and bounded.denied) or err == error.SharedAdmissionExceeded) error.MemoryBudgetExceeded else err;
    };
    defer bounded.allocator().free(result.vector);
    return .{ .vector = try out.dupe(f32, result.vector), .input_tokens = result.input_tokens };
}

fn embedScoped(a: std.mem.Allocator, session: Session, tokenizer: Tokenizer, group: Group, options: Options, lock: ?*std.atomic.Mutex, control: ?Control, cache: ?*FrameCache) !Result {
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.UnsupportedEmbeddingGemma2Backend;
    try validateGroup(group, options);
    if (options.video_ahead) |ahead| {
        if (group.content.len != 1 or group.content[0] != .sampled_video or !ahead.matches(group.content[0].sampled_video, options.video_decoder, session, cache) or ahead.released) return error.InvalidVideoReaderSelection;
    }
    var input_bytes: usize = 0;
    var has_video = false;
    for (group.content) |part| {
        input_bytes = std.math.add(usize, input_bytes, partBytes(part).len) catch return error.ResourceLimitExceeded;
        has_video = has_video or part == .video or part == .sampled_video;
        switch (part) {
            .text => {},
            .image, .raster, .video, .sampled_video => if (!cfg.vision) return error.NoVisionSession,
            .audio, .pcm => if (!cfg.audio) return error.NoAudioSession,
        }
    }
    const effective = control orelse Control{};
    try effective.check();
    // Inspect under a shared host-only permit before reserving model/device
    // execution. Temporary metadata is freed before that permit is returned.
    var device_bytes: usize = 0;
    var command_slots: u64 = 0;
    if (options.video_ahead) |ahead| {
        device_bytes = if (session.backend() == .metal) try ahead.plan.deviceBytes() else 0;
        command_slots = ahead.plan.commands;
    } else if (has_video) {
        var preflight = session.admitHostPreprocess(512 * 1024 * 1024) catch |err| return if (options.video_ahead != null and videoSpeculationPressure(err)) error.SpeculativeVideoAdmissionDenied else err;
        defer preflight.deinit();
        var host_budget = media_admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        defer std.debug.assert(std.meta.eql(host_budget.snapshot(), media_admission.Resources{}));
        for (group.content) |part| switch (part) {
            inline .video, .sampled_video => |payload| {
                const bytes = if (@TypeOf(payload) == SampledVideo) payload.bytes else payload;
                const sampling: @import("embedding_gemma2_video.zig").Sampling = if (@TypeOf(payload) == SampledVideo) payload.sampling else .reference;
                const packet_reader = if (@TypeOf(payload) == SampledVideo) payload.reader else null;
                const selection = if (@TypeOf(payload) == SampledVideo) payload.selection else null;
                if (selection != null and packet_reader == null) return error.InvalidVideoReaderSelection;
                const plan = try @import("embedding_gemma2_video.zig").inspectConfiguredPlan(a, bytes, packet_reader, effective, .{ .sampling = sampling, .pool = &host_budget, .selection = selection, .webm = if (@TypeOf(payload) == SampledVideo) payload.webm else null, .decoder = options.video_decoder }, session.backend() == .metal);
                device_bytes = @max(device_bytes, try plan.deviceBytes());
                command_slots = @max(command_slots, plan.commands);
            },
            else => {},
        };
    }
    const workspace = if (session.backend() == .metal)
        try std.math.add(usize, (if (has_video) @as(usize, 1536) else 768) * 1024 * 1024, device_bytes)
    else
        512 * 1024 * 1024;
    var permit = admitWithVisionCacheReclaim(session, .{ .batch = 1, .sequence = 8192, .input_bytes = input_bytes, .host_preprocess_bytes = 512 * 1024 * 1024, .workspace_bytes = workspace, .output_bytes = 8192 * 768 * @sizeOf(f32) }, effective) catch |err| return if (options.video_ahead != null and videoSpeculationPressure(err)) error.SpeculativeVideoAdmissionDenied else err;
    defer permit.deinit();
    // Video parts execute sequentially; their transient preparation shares use
    // a maximum, while projected tokens stay in the model workspace.
    var video_budget = media_admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024, .device_bytes = if (session.backend() == .metal) device_bytes else 0, .commands = if (session.backend() == .metal) command_slots else 0 } };
    defer std.debug.assert(std.meta.eql(video_budget.snapshot(), media_admission.Resources{}));
    var preparation_guard = if (has_video) try effective.enterUninterruptible(session.interruption()) else @import("../execution_control.zig").UninterruptibleGuard{};
    defer preparation_guard.deinit();
    var prefetched: ?*@import("embedding_gemma2_video.zig").Prefetch = null;
    defer if (options.video_ahead == null) {
        if (prefetched) |producer| producer.deinit();
    };
    if (options.video_ahead) |ahead| prefetched = ahead.producer;
    if (has_video and prefetched == null) {
        const context = try factory.videoPreparationContext(session);
        if (effective.io orelse context.io) |io| {
            for (group.content) |part| switch (part) {
                inline .video, .sampled_video => |payload| {
                    const bytes = if (@TypeOf(payload) == SampledVideo) payload.bytes else payload;
                    const sampling: @import("embedding_gemma2_video.zig").Sampling = if (@TypeOf(payload) == SampledVideo) payload.sampling else .reference;
                    const packet_reader = if (@TypeOf(payload) == SampledVideo) payload.reader else null;
                    const selection = if (@TypeOf(payload) == SampledVideo) payload.selection else null;
                    prefetched = try @import("embedding_gemma2_video.zig").Prefetch.startConfigured(a, io, context.device, bytes, packet_reader, effective, .{ .sampling = sampling, .pool = &video_budget, .selection = selection, .webm = if (@TypeOf(payload) == SampledVideo) payload.webm else null, .cache = cache, .decoder = options.video_decoder });
                    break;
                },
                else => {},
            };
        }
    }
    if (lock) |mutex| try effective.lock(mutex);
    defer if (lock) |mutex| mutex.unlock();
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    const cb = &managed.backend;
    const prepared = prepareGroupWithBudget(a, cb, tokenizer, group, options, effective, &video_budget, prefetched, cache) catch |err| return if (options.video_ahead != null and videoSpeculationPressure(err)) error.SpeculativeVideoAdmissionDenied else err;
    defer prepared.deinit(a);
    // Projection consumption has joined the current decoder. A new producer
    // may now run while this call executes the language backbone.
    if (options.video_ahead) |ahead| ahead.release();
    if (options.video_prepared) |hook| try hook.ready(hook.context);
    const count = prepared.ids.len;
    const mask = prepared.mask;
    if (prepared.embeddings == null and prepared.tensor == null) {
        const vector = try preparedText(cb, a, cfg, prepared.ids, mask, options.dimensions);
        errdefer a.free(vector);
        try effective.check();
        return .{ .vector = vector, .input_tokens = count };
    }
    const input = prepared.tensor orelse try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(prepared.embeddings.?, &.{ @intCast(count), 512 }));
    defer if (prepared.tensor == null) cb.free(input);
    const output = try encoder.forwardEmbeddingsCT(cb, a, cfg, input, mask, 1, count);
    defer cb.free(output);
    if (cb.kind() == .metal) {
        const op = cb.vtable.maskedMeanNormalizedRows orelse return error.EmbeddingGemma2OperationUnavailable;
        const pooled = (try op(cb.ptr, output, mask, 768, options.dimensions)) orelse return error.EmbeddingGemma2OperationUnavailable;
        defer cb.free(pooled);
        const vector = try readNormalizedVector(cb, a, pooled, options.dimensions);
        errdefer a.free(vector);
        try effective.check();
        return .{ .vector = vector, .input_tokens = count };
    }
    const values = try cb.toFloat32(output, a);
    defer a.free(values);
    const vectors = try poolBatch(a, values, mask, 1, count, options.dimensions);
    defer a.free(vectors);
    errdefer a.free(vectors[0]);
    try effective.check();
    return .{ .vector = vectors[0], .input_tokens = count };
}

const PreparedGroup = struct {
    ids: []i64,
    mask: []i64,
    embeddings: ?[]f32,
    tensor: ?CT = null,
    backend: ?*const ComputeBackend = null,

    fn deinit(self: PreparedGroup, a: std.mem.Allocator) void {
        a.free(self.ids);
        a.free(self.mask);
        if (self.embeddings) |embeddings| a.free(embeddings);
        if (self.tensor) |tensor| self.backend.?.free(tensor);
    }
};

fn prepareGroup(a: std.mem.Allocator, cb: *const @import("../ops/ops.zig").ComputeBackend, tokenizer: Tokenizer, group: Group, options: Options, effective: Control) !PreparedGroup {
    return prepareGroupWithBudget(a, cb, tokenizer, group, options, effective, null, null, null);
}
fn prepareGroupWithBudget(a: std.mem.Allocator, cb: *const @import("../ops/ops.zig").ComputeBackend, tokenizer: Tokenizer, group: Group, options: Options, effective: Control, video_budget: ?*media_admission.Pool, prefetched: ?*@import("embedding_gemma2_video.zig").Prefetch, cache: ?*FrameCache) !PreparedGroup {
    return prepareGroupCombined(a, cb, tokenizer, group, options, effective, video_budget, prefetched, cache, null);
}

fn prepareGroupWithImage(a: std.mem.Allocator, cb: *const ComputeBackend, tokenizer: Tokenizer, group: Group, options: Options, effective: Control, preprojected: ?SoftTokens) !PreparedGroup {
    return prepareGroupCombined(a, cb, tokenizer, group, options, effective, null, null, null, preprojected);
}

fn prepareGroupCombined(a: std.mem.Allocator, cb: *const ComputeBackend, tokenizer: Tokenizer, group: Group, options: Options, effective: Control, video_budget: ?*media_admission.Pool, prefetched: ?*@import("embedding_gemma2_video.zig").Prefetch, cache: ?*FrameCache, preprojected: ?SoftTokens) !PreparedGroup {
    const image_scope = @import("antfly_image").work_control.Scope.enter(effective.imageWorkControl());
    defer image_scope.deinit();
    const prefix = try encoder.taskPrefix(options.task_type);
    var has_text = false;
    for (group.content) |part| if (part == .text) {
        has_text = true;
    };
    var rendered = std.ArrayListUnmanaged(u8).empty;
    defer rendered.deinit(a);
    if (has_text) {
        if (group.title) |title| {
            const titled = try std.fmt.allocPrint(a, "title: {s} | text: ", .{title});
            defer a.free(titled);
            try rendered.appendSlice(a, titled);
        } else try rendered.appendSlice(a, prefix);
    }
    var media = std.ArrayListUnmanaged(SoftTokens).empty;
    defer {
        for (media.items) |item| {
            a.free(item.embeddings);
            if (item.tensor) |tensor| cb.free(tensor);
        }
        media.deinit(a);
    }
    var media_tokens: usize = 0;
    var used_prefetch = false;
    for (group.content, 0..) |part, index| {
        try effective.check();
        if (index > 0) try rendered.append(a, '\n');
        switch (part) {
            .text => |text| try rendered.appendSlice(a, text),
            inline .video, .sampled_video => |payload| {
                const bytes = if (@TypeOf(payload) == SampledVideo) payload.bytes else payload;
                const sampling: @import("embedding_gemma2_video.zig").Sampling = if (@TypeOf(payload) == SampledVideo) payload.sampling else .reference;
                const encoded = if (!used_prefetch and prefetched != null) blk: {
                    used_prefetch = true;
                    break :blk try prefetched.?.encodeAlloc(a, cb);
                } else try @import("embedding_gemma2_video.zig").encodeConfigured(a, cb, bytes, if (@TypeOf(payload) == SampledVideo) payload.reader else null, effective, .{ .sampling = sampling, .pool = video_budget, .selection = if (@TypeOf(payload) == SampledVideo) payload.selection else null, .webm = if (@TypeOf(payload) == SampledVideo) payload.webm else null, .cache = cache, .decoder = options.video_decoder });
                defer a.free(encoded.frame_tokens);
                var owned = true;
                errdefer if (owned) {
                    a.free(encoded.embeddings);
                    if (encoded.tensor) |tensor| cb.free(tensor);
                };
                var count_video: usize = 0;
                for (encoded.frame_tokens) |frame_count| count_video += frame_count;
                media_tokens = std.math.add(usize, media_tokens, count_video) catch return error.EmbeddingInputTooLong;
                if (media_tokens > 8192) return error.EmbeddingInputTooLong;
                // Tokenize with the validated image placeholder, then rewrite its
                // video rows to 258884. Upstream adds the video special token at
                // processor construction; original tokenizer sidecars lack it.
                for (encoded.frame_tokens) |count_frame| {
                    try rendered.appendSlice(a, "<|image>");
                    for (0..count_frame) |_| try rendered.appendSlice(a, "<|image|>");
                    try rendered.appendSlice(a, "<image|>");
                }
                try media.append(a, .{ .id = 258884, .embeddings = encoded.embeddings, .tokens = count_video, .tensor = encoded.tensor });
                owned = false;
            },
            .image, .audio, .raster, .pcm => {
                const bytes = partBytes(part);
                const is_image = part == .image or part == .raster;
                if (part == .audio and preprojected == null) {
                    var decoded = try @import("audio.zig").decodeBounded(a, bytes, .{}, 64 * 1024 * 1024);
                    defer decoded.deinit();
                    if (decoded.sample_rate == 0 or @as(u64, decoded.samples.len) * 16000 > @as(u64, decoded.sample_rate) * 480000) return error.AudioTooLong;
                }
                const projected = if (is_image) blk: {
                    if (preprojected) |p| {
                        if (group.content.len != 1 or (part != .raster and part != .image)) return error.InvalidInputShape;
                        break :blk SoftTokens{ .id = p.id, .embeddings = try a.dupe(f32, p.embeddings), .tokens = p.tokens };
                    }
                    const p = if (part == .raster) try projector.encodeEmbeddingGemma2Raster(cb, a, part.raster) else try projector.encodeEmbeddingGemma2Image(cb, a, bytes);
                    break :blk SoftTokens{ .id = 258880, .embeddings = p.embeddings, .tokens = p.tokens };
                } else blk: {
                    if (preprojected) |p| {
                        if (group.content.len != 1 or part != .audio or p.id != 258881) return error.InvalidInputShape;
                        break :blk SoftTokens{ .id = p.id, .embeddings = try a.dupe(f32, p.embeddings), .tokens = p.tokens };
                    }
                    const p = if (part == .pcm) try projector.encodeEmbeddingGemma2Pcm(cb, a, part.pcm) else try projector.encodeEmbeddingGemma2Audio(cb, a, bytes);
                    break :blk SoftTokens{ .id = 258881, .embeddings = p.embeddings, .tokens = p.tokens };
                };
                errdefer a.free(projected.embeddings);
                if (projected.tokens == 0 or projected.embeddings.len != projected.tokens * 512) return error.InvalidTensorShape;
                media_tokens = std.math.add(usize, media_tokens, projected.tokens) catch return error.EmbeddingInputTooLong;
                if (media_tokens > 8192) return error.EmbeddingInputTooLong;
                try rendered.appendSlice(a, if (is_image) "<|image>" else "<|audio>");
                for (0..projected.tokens) |_| try rendered.appendSlice(a, if (is_image) "<|image|>" else "<|audio|>");
                try rendered.appendSlice(a, if (is_image) "<image|>" else "<audio|>");
                try media.append(a, projected);
            },
        }
    }
    var tokens = try tokenizer.encodeForModel(a, rendered.items, 8193);
    defer tokens.deinit();
    var count: usize = 0;
    for (tokens.attention_mask) |m| count += @intFromBool(m != 0);
    if (count == 0 or count > 8192) return error.EmbeddingInputTooLong;
    const ids = try a.alloc(i64, count);
    errdefer a.free(ids);
    const mask = try a.alloc(i64, count);
    errdefer a.free(mask);
    for (ids, tokens.ids[0..count]) |*dest, id| dest.* = id;
    for (mask, tokens.attention_mask[0..count]) |*dest, m| dest.* = m;
    if (media.items.len == 0) return .{ .ids = ids, .mask = mask, .embeddings = null };
    const ew = try cb.getWeight("language_model.embed_tokens.weight");
    defer cb.free(ew);
    const raw = try cb.embeddingLookup(ew, ids, count, 512);
    defer cb.free(raw);
    // Raster batching retains its existing host packing contract. Groups with
    // resident video tokens compose the entire ordered sequence on the device.
    for (media.items) |item| if (item.tensor != null) {
        const composed = try composeDeviceTokens(a, cb, raw, ids, media.items);
        return .{ .ids = ids, .mask = mask, .embeddings = null, .tensor = composed, .backend = cb };
    };
    const embeddings = try cb.toFloat32(raw, a);
    errdefer a.free(embeddings);
    for (embeddings) |*value| value.* *= @sqrt(@as(f32, 512));
    try overlayMedia(ids, embeddings, media.items);
    return .{ .ids = ids, .mask = mask, .embeddings = embeddings };
}

/// Validates marker ordering and builds rows into [text; media], rewriting video
/// IDs only after the original tokenizer IDs have been looked up.
fn compositionRows(a: std.mem.Allocator, ids: []i64, media: []const SoftTokens) ![]i32 {
    const rows = try a.alloc(i32, ids.len);
    errdefer a.free(rows);
    var media_index: usize = 0;
    var soft_index: usize = 0;
    var offset = ids.len;
    for (ids, 0..) |id, row| {
        rows[row] = std.math.cast(i32, row) orelse return error.InvalidTensorShape;
        if (id != 258880 and id != 258881) continue;
        if (media_index >= media.len) return error.InvalidEmbeddingGroup;
        const item = media[media_index];
        if (item.tokens == 0 or id != (if (item.id == 258884) @as(i64, 258880) else item.id)) return error.InvalidEmbeddingGroup;
        rows[row] = std.math.cast(i32, offset + soft_index) orelse return error.InvalidTensorShape;
        if (item.id == 258884) ids[row] = 258884;
        soft_index += 1;
        if (soft_index == item.tokens) {
            offset += item.tokens;
            soft_index = 0;
            media_index += 1;
        }
    }
    if (media_index != media.len or soft_index != 0) return error.InvalidEmbeddingGroup;
    return rows;
}

fn composeDeviceTokens(a: std.mem.Allocator, cb: *const ComputeBackend, text: CT, ids: []i64, media: []const SoftTokens) !CT {
    const rows = try compositionRows(a, ids, media);
    defer a.free(rows);
    var table = (try cb.multiplyScalar(text, @sqrt(@as(f32, 512)))) orelse return error.EmbeddingGemma2OperationUnavailable;
    defer cb.free(table);
    var count = ids.len;
    for (media) |item| {
        const tensor = item.tensor orelse try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(item.embeddings, &.{ @intCast(item.tokens), 512 }));
        defer if (item.tensor == null) cb.free(tensor);
        const next = try cb.primConcatPrim(table, tensor, 0, &.{ @intCast(count), 512 }, &.{ @intCast(item.tokens), 512 });
        cb.free(table);
        table = next;
        count += item.tokens;
    }
    const indices = (try cb.fromInt32Shape(rows, &.{@intCast(rows.len)})) orelse return error.EmbeddingGemma2OperationUnavailable;
    defer cb.free(indices);
    return cb.primGather(table, indices, 0, &.{ @intCast(count), 512 });
}

fn overlayMedia(ids: []i64, embeddings: []f32, media: []const SoftTokens) !void {
    if (embeddings.len != ids.len * 512) return error.InvalidTensorShape;
    var media_index: usize = 0;
    var soft_index: usize = 0;
    for (ids, 0..) |id, row| {
        if (id != 258880 and id != 258881) continue;
        if (media_index >= media.len) return error.InvalidEmbeddingGroup;
        const item = media[media_index];
        if (id != (if (item.id == 258884) @as(i64, 258880) else item.id)) return error.InvalidEmbeddingGroup;
        if (item.id == 258884) ids[row] = 258884;
        @memcpy(embeddings[row * 512 ..][0..512], item.embeddings[soft_index * 512 ..][0..512]);
        soft_index += 1;
        if (soft_index == item.tokens) {
            soft_index = 0;
            media_index += 1;
        }
    }
    if (media_index != media.len or soft_index != 0) return error.InvalidEmbeddingGroup;
}

const Raster = @import("antfly_image").BorrowedRasterAttachment;
const ComputeBackend = @import("../ops/ops.zig").ComputeBackend;
const BatchExecution = @import("batch_execution.zig");
pub const RasterBatchResult = struct { vectors: [][]f32, execution: BatchExecution.Execution };
const raster_max_batch = 4;
const raster_max_sequence = 512;
const raster_max_padded_tokens = raster_max_batch * raster_max_sequence;

/// Bound live rows and padding, preserving input order. Avoid singleton tails
/// when neighboring sequence lengths permit another balanced cohort.
fn rasterBatchCount(lengths: []const usize, remaining: usize) usize {
    var count: usize = 0;
    var maximum: usize = 0;
    var total: usize = 0;
    for (lengths[0..@min(lengths.len, raster_max_batch)]) |length| {
        const next_max = @max(maximum, length);
        const padded = next_max * (count + 1);
        if (next_max > raster_max_sequence or padded > raster_max_padded_tokens or padded * 4 > (total + length) * 5) break;
        maximum = next_max;
        total += length;
        count += 1;
    }
    if (count == 4 and remaining == 5) count = 3;
    return @max(@as(usize, 1), count);
}

pub fn embedRasters(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, rasters: []const Raster, options: Options, lock: ?*std.atomic.Mutex, control: ?Control) !RasterBatchResult {
    const effective = control orelse Control{};
    try effective.check();
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.UnsupportedEmbeddingGemma2Backend;
    if (!cfg.vision) return error.NoVisionSession;
    if (!encoder.validDimension(options.dimensions)) return error.InvalidEmbeddingDimensions;
    _ = try encoder.taskPrefix(options.task_type);
    if (options.prepare_ahead and (session.backend() == .metal or session.backend() == .cuda))
        return embedPreparedMedia(out, session, tokenizer, cfg, .{ .rasters = rasters }, options, lock, effective, null);
    const vectors = try out.alloc([]f32, rasters.len);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| out.free(vector);
        out.free(vectors);
    }
    var observation = BatchExecution.Observation{};
    while (initialized < rasters.len) {
        try effective.check();
        var lengths: [raster_max_batch + 1]usize = undefined;
        const candidates = @min(rasters.len - initialized, lengths.len);
        for (rasters[initialized..][0..candidates], lengths[0..candidates]) |raster, *length| length.* = try projector.embeddingGemma2RasterSequenceLength(raster);
        const count = rasterBatchCount(lengths[0..candidates], rasters.len - initialized);
        var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = workspaceBackingAllocator(session.backend()), .limit = 512 * 1024 * 1024 };
        defer std.debug.assert(bounded.live == 0);
        const temporary = embedRasterCohort(bounded.allocator(), session, tokenizer, cfg, rasters[initialized..][0..count], options, lock, effective) catch |err| return if (err == error.OutOfMemory and bounded.denied) error.MemoryBudgetExceeded else err;
        defer {
            for (temporary) |vector| bounded.allocator().free(vector);
            bounded.allocator().free(temporary);
        }
        for (temporary) |vector| {
            vectors[initialized] = try out.dupe(f32, vector);
            initialized += 1;
        }
        observation.record(count);
    }
    try effective.check();
    return .{ .vectors = vectors, .execution = observation.execution(rasters.len) };
}

/// Encoded image rows use the same admitted vision/encoder cohorts as PDFs.
/// Invalid codecs may occupy indexed error slots; backend failures abort.
pub fn embedImages(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, images: []const []const u8, options: Options, lock: ?*std.atomic.Mutex, control: ?Control, item_errors: ?[]?anyerror) !RasterBatchResult {
    const effective = control orelse Control{};
    try effective.check();
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.UnsupportedEmbeddingGemma2Backend;
    if (!cfg.vision) return error.NoVisionSession;
    if (!encoder.validDimension(options.dimensions)) return error.InvalidEmbeddingDimensions;
    _ = try encoder.taskPrefix(options.task_type);
    if (item_errors) |errors| {
        if (errors.len != images.len) return error.InvalidInputShape;
        @memset(errors, null);
    }
    if (options.prepare_ahead and (session.backend() == .metal or session.backend() == .cuda))
        return embedPreparedMedia(out, session, tokenizer, cfg, .{ .images = images }, options, lock, effective, item_errors);
    const vectors = try out.alloc([]f32, images.len);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| out.free(vector);
        out.free(vectors);
    }
    var observation = BatchExecution.Observation{};
    while (initialized < images.len) {
        try effective.check();
        const count = @min(raster_max_batch, images.len - initialized);
        var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = workspaceBackingAllocator(session.backend()), .limit = 512 * 1024 * 1024 };
        defer std.debug.assert(bounded.live == 0);
        const errors = if (item_errors) |e| e[initialized..][0..count] else null;
        const temporary = embedImageCohort(bounded.allocator(), session, tokenizer, cfg, images[initialized..][0..count], options, lock, effective, errors) catch |err| return if (err == error.OutOfMemory and bounded.denied) error.MemoryBudgetExceeded else err;
        defer {
            for (temporary) |vector| bounded.allocator().free(vector);
            bounded.allocator().free(temporary);
        }
        var valid: usize = 0;
        for (temporary) |vector| {
            vectors[initialized] = try out.dupe(f32, vector);
            initialized += 1;
            if (vector.len != 0) valid += 1;
        }
        if (valid > 0) observation.record(valid);
    }
    try effective.check();
    return .{ .vectors = vectors, .execution = observation.execution(observation.native_items + observation.serial_items) };
}

pub const AudioBatchResult = struct { vectors: [][]f32, input_tokens: []usize, execution: BatchExecution.Execution };
pub fn audioRunRequest(backend: @import("../backends/backends.zig").BackendType, input_bytes: usize, retained_bytes: usize, batch: usize) @import("../backends/session.zig").RunRequest {
    return .{ .batch = batch, .sequence = 1024, .input_bytes = input_bytes, .pre_admitted_host_bytes = retained_bytes, .host_preprocess_bytes = 512 * 1024 * 1024, .workspace_bytes = if (backend == .metal) 768 * 1024 * 1024 else 512 * 1024 * 1024, .output_bytes = batch * 1024 * 768 * 4 };
}

/// Audio-only clips: conformer context stays independent per clip; length-
/// qualified cohorts share the downstream embedding encoder and final pooling.
pub fn embedAudios(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, clips: []const @import("audio_preparation.zig").Clip, options: Options, lock: ?*std.atomic.Mutex, supplied: ?Control, errors: ?[]?anyerror) !AudioBatchResult {
    const prep = @import("audio_preparation.zig");
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.UnsupportedEmbeddingGemma2Backend;
    if (!cfg.audio) return error.NoAudioSession;
    if (!encoder.validDimension(options.dimensions)) return error.InvalidEmbeddingDimensions;
    _ = try encoder.taskPrefix(options.task_type);
    const control = supplied orelse Control{};
    try control.check();
    if (errors) |items| {
        if (items.len != clips.len) return error.InvalidInputShape;
        @memset(items, null);
    }
    const vectors = try out.alloc([]f32, clips.len);
    @memset(vectors, &.{});
    errdefer {
        for (vectors) |vector| out.free(vector);
        out.free(vectors);
    }
    const token_counts = try out.alloc(usize, clips.len);
    @memset(token_counts, 0);
    errdefer out.free(token_counts);
    if (!options.resident_audio or (session.backend() != .metal and session.backend() != .cuda)) {
        for (clips, 0..) |clip, index| {
            const part: Part = if (clip.pcm) |pcm| .{ .pcm = pcm } else .{ .audio = clip.bytes };
            const result = embed(out, session, tokenizer, .{ .content = &.{part} }, options, lock, control) catch |err| {
                if (errors) |items| {
                    if (prep.abort(err)) return err;
                    items[index] = err;
                    continue;
                }
                return err;
            };
            vectors[index] = result.vector;
            token_counts[index] = result.input_tokens;
        }
        return .{ .vectors = vectors, .input_tokens = token_counts, .execution = .serial };
    }
    if (clips.len == 0) return .{ .vectors = vectors, .input_tokens = token_counts, .execution = .serial };
    if (lock) |gate| try control.lock(gate);
    defer if (lock) |gate| gate.unlock();
    var slots: [2]prep.Wave = undefined;
    var live: [2]bool = @splat(false);
    defer for (&slots, live) |*slot, exists| if (exists) slot.deinit();
    var current: usize = 0;
    var offset: usize = 0;
    const first = @min(@as(usize, 4), clips.len);
    slots[0] = prep.Wave.init(session, clips[0..first], if (errors) |e| e[0..first] else null, control, .gemma4, 64 * 1024 * 1024);
    live[0] = true;
    slots[0].run();
    var observation = BatchExecution.Observation{};
    while (offset < clips.len) {
        try control.check();
        const slot = &slots[current];
        slot.join();
        if (slot.failure) |err| return err;
        if (options.timings) |t| t.preprocess_ns += slot.prepare_ns;
        const next = 1 - current;
        const next_offset = offset + slot.consumed;
        const next_count = @min(@as(usize, 4), clips.len - next_offset);
        if (next_count > 0) {
            slots[next] = prep.Wave.init(session, clips[next_offset..][0..next_count], if (errors) |e| e[next_offset..][0..next_count] else null, control, .gemma4, 64 * 1024 * 1024);
            live[next] = true;
            if (options.prepare_ahead) {
                const spawned = slots[next].start();
                if (options.timings) |t| t.preparation_jobs += @intFromBool(spawned);
            }
        }
        if (options.timings) |t| t.max_prepared_audio = @max(t.max_prepared_audio, slot.clips.len + if (options.prepare_ahead) next_count else 0);
        try consumePreparedAudios(out, slot, tokenizer, cfg, options, vectors[offset..][0..slot.consumed], token_counts[offset..][0..slot.consumed], &observation, if (next_count > 0 and options.prepare_ahead) &slots[next] else null);
        slot.deinit();
        live[current] = false;
        offset = next_offset;
        if (next_count > 0) {
            if (!options.prepare_ahead) slots[next].run();
            slots[next].join();
            if (slots[next].failure) |err| if (prep.pressure(err)) {
                slots[next].deinit();
                slots[next] = prep.Wave.init(session, clips[next_offset..][0..next_count], if (errors) |e| e[next_offset..][0..next_count] else null, control, .gemma4, 64 * 1024 * 1024);
                if (options.timings) |t| t.preparation_backpressure_retries += 1;
                slots[next].run();
            };
            current = next;
        }
    }
    try control.check();
    return .{ .vectors = vectors, .input_tokens = token_counts, .execution = observation.execution(observation.native_items + observation.serial_items) };
}

fn consumePreparedAudios(out: std.mem.Allocator, slot: *@import("audio_preparation.zig").Wave, tokenizer: Tokenizer, cfg: encoder.Config, options: Options, vectors: [][]f32, token_counts: []usize, observation: *BatchExecution.Observation, future: ?*@import("audio_preparation.zig").Wave) !void {
    if (slot.retained == 0) return;
    // CPU preparation has its own 64-MiB ceiling. The admitted current GPU
    // phase may use the existing 512-MiB host workspace; the future CPU slot
    // keeps its smaller preparation ceiling and owns no device reservation.
    slot.bounded.limit = 512 * 1024 * 1024;
    const a = slot.allocator();
    const request = audioRunRequest(slot.session.backend(), slot.input_bytes, slot.retained_bytes, slot.retained);
    var permit = admitWithVisionCacheReclaim(slot.session, request, slot.control) catch |err| blk: {
        const next = future orelse return err;
        if (!@import("audio_preparation.zig").pressure(err)) return err;
        next.deinit();
        next.failure = err;
        break :blk try admitWithVisionCacheReclaim(slot.session, request, slot.control);
    };
    defer permit.deinit();
    var managed = try factory.getManagedComputeBackend(slot.session, a, null, slot.control);
    defer managed.deinit();
    const cb = &managed.backend;
    const before = cb.debugTimingSnapshot().provider;
    defer observeResidentTransfers(cb, before, options);
    var order: [4]usize = undefined;
    for (order[0..slot.retained], 0..) |*index, n| index.* = n;
    // Small length buckets reduce padding; original indexes restore output order.
    const Less = struct {
        fn less(wave: *@import("audio_preparation.zig").Wave, lhs: usize, rhs: usize) bool {
            return wave.gemma[lhs].tokenCount() < wave.gemma[rhs].tokenCount();
        }
    };
    std.mem.sort(usize, order[0..slot.retained], slot, Less.less);
    var cursor: usize = 0;
    while (cursor < slot.retained) {
        var count: usize = 0;
        var total: usize = 0;
        var longest: usize = 0;
        for (order[cursor..slot.retained]) |index| {
            const length = slot.gemma[index].tokenCount() + 4;
            const next_max = @max(longest, length);
            if (length > 1024 or (count > 0 and next_max * (count + 1) * 4 > (total + length) * 5)) break;
            longest = next_max;
            total += length;
            count += 1;
        }
        if (count == 0) return error.EmbeddingInputTooLong;
        const lengths = try a.alloc(usize, count);
        defer a.free(lengths);
        var joined: ?@import("../ops/ops.zig").CT = null;
        defer if (joined) |tensor| cb.free(tensor);
        var rows: usize = 0;
        const audio_started = mediaNow();
        for (order[cursor..][0..count], 0..) |index, n| {
            try slot.control.check();
            const projected = try projector.encodePreparedEmbeddingGemma2AudioResident(cb, a, &slot.gemma[index]);
            errdefer cb.free(projected.embeddings);
            lengths[n] = projected.tokens;
            if (joined) |previous| {
                const tensor = try cb.concatRows2D(a, previous, projected.embeddings, rows, projected.tokens, 512);
                cb.free(previous);
                cb.free(projected.embeddings);
                joined = tensor;
            } else joined = projected.embeddings;
            rows += projected.tokens;
        }
        if (options.timings) |t| {
            t.audio_ns += mediaNow() - audio_started;
            t.resident_cohorts += 1;
        }
        var projected = projector.ResidentProjectedImages{ .allocator = a, .backend = cb, .embeddings = joined.?, .tokens_per_image = lengths, .hidden_size = 512, .batches_executed = count, .max_batch_executed = 1 };
        const result = try runResidentMediaBatch(a, cb, cfg, tokenizer, &projected, options, true);
        defer {
            for (result) |vector| a.free(vector);
            a.free(result);
        }
        for (order[cursor..][0..count], result, lengths) |index, vector, tokens| {
            const dst = slot.indexes[index];
            vectors[dst] = try out.dupe(f32, vector);
            token_counts[dst] = tokens + 4;
        }
        observation.record(count);
        cursor += count;
    }
}

const MediaSource = union(enum) {
    images: []const []const u8,
    rasters: []const Raster,
    fn len(self: MediaSource) usize {
        return switch (self) {
            inline else => |items| items.len,
        };
    }
    fn slice(self: MediaSource, offset: usize, span: usize) MediaSource {
        return switch (self) {
            .images => |items| .{ .images = items[offset..][0..span] },
            .rasters => |items| .{ .rasters = items[offset..][0..span] },
        };
    }
    fn part(self: MediaSource, index: usize) Part {
        return switch (self) {
            .images => |items| .{ .image = items[index] },
            .rasters => |items| .{ .raster = items[index] },
        };
    }
    fn count(self: MediaSource, offset: usize) !usize {
        const remaining = self.len() - offset;
        if (self == .images) return @min(raster_max_batch, remaining);
        var lengths: [raster_max_batch + 1]usize = undefined;
        const candidates = @min(remaining, lengths.len);
        for (self.rasters[offset..][0..candidates], lengths[0..candidates]) |raster, *length| length.* = try projector.embeddingGemma2RasterSequenceLength(raster);
        return rasterBatchCount(lengths[0..candidates], remaining);
    }
};

const PreparationSlot = struct {
    session: Session,
    source: MediaSource,
    options: Options,
    control: Control,
    errors: ?[]?anyerror,
    bounded: @import("../runtime/bounded_allocator.zig").BoundedAllocator,
    permit: ?@import("../backends/session.zig").RunPermit = null,
    pixels: [raster_max_batch]projector.PreparedImage = undefined,
    indexes: [raster_max_batch]usize = undefined,
    retained: usize = 0,
    input_bytes: usize = 0,
    retained_bytes: usize = 0,
    prepare_ns: u64 = 0,
    failure: ?anyerror = null,
    thread: ?std.Thread = null,

    fn init(session: Session, source: MediaSource, options: Options, control: Control, errors: ?[]?anyerror) PreparationSlot {
        return .{ .session = session, .source = source, .options = options, .control = control, .errors = errors, .bounded = .{ .backing = workspaceBackingAllocator(session.backend()), .limit = 512 * 1024 * 1024 } };
    }
    fn allocator(self: *PreparationSlot) std.mem.Allocator {
        return self.bounded.allocator();
    }
    fn run(self: *PreparationSlot) void {
        self.prepare() catch |err| {
            self.failure = if (err == error.OutOfMemory and self.bounded.denied) error.MemoryBudgetExceeded else err;
        };
    }
    fn start(self: *PreparationSlot) bool {
        if (comptime @import("builtin").single_threaded or @import("builtin").cpu.arch.isWasm() or @import("builtin").os.tag == .freestanding) {
            self.run();
            return false;
        } else {
            self.thread = std.Thread.spawn(.{}, PreparationSlot.run, .{self}) catch {
                self.run();
                return false;
            };
            return true;
        }
    }
    fn join(self: *PreparationSlot) void {
        if (comptime !@import("builtin").single_threaded and !@import("builtin").cpu.arch.isWasm() and @import("builtin").os.tag != .freestanding) {
            if (self.thread) |thread| {
                thread.join();
                self.thread = null;
            }
        }
    }
    fn deinit(self: *PreparationSlot) void {
        self.join();
        self.releasePrepared();
    }
    fn releasePrepared(self: *PreparationSlot) void {
        for (self.pixels[0..self.retained]) |image| image.deinit();
        self.retained = 0;
        if (self.permit) |*permit| {
            permit.deinit();
            self.permit = null;
        }
        std.debug.assert(self.bounded.live == 0);
    }
    fn prepare(self: *PreparationSlot) !void {
        try self.control.check();
        const started = mediaNow();
        errdefer self.releasePrepared();
        for (0..self.source.len()) |index| {
            const part = self.source.part(index);
            validateGroup(.{ .content = &.{part} }, self.options) catch |err| {
                if (self.errors) |errors| errors[index] = err else return err;
            };
            self.input_bytes = std.math.add(usize, self.input_bytes, partBytes(part).len) catch return error.ResourceLimitExceeded;
        }
        self.permit = try self.session.admitHostPreprocess(std.math.add(usize, self.input_bytes, 512 * 1024 * 1024) catch return error.ResourceLimitExceeded);
        const image_scope = @import("antfly_image").work_control.Scope.enter(self.control.imageWorkControl());
        defer image_scope.deinit();
        for (0..self.source.len()) |index| {
            try self.control.check();
            if (self.errors) |errors| if (errors[index] != null) continue;
            self.pixels[self.retained] = switch (self.source.part(index)) {
                .image => |bytes| projector.prepareEmbeddingGemma2Image(self.allocator(), bytes),
                .raster => |raster| projector.prepareEmbeddingGemma2Raster(self.allocator(), raster),
                else => unreachable,
            } catch |err| {
                if (self.errors) |errors| {
                    if (err == error.OutOfMemory or err == error.Cancelled or err == error.Canceled or err == error.Timeout or err == error.DeadlineExceeded or err == error.MemoryBudgetExceeded) return err;
                    errors[index] = err;
                    continue;
                }
                return err;
            };
            self.indexes[self.retained] = index;
            self.retained += 1;
        }
        self.retained_bytes = self.input_bytes;
        for (self.pixels[0..self.retained]) |image| self.retained_bytes = std.math.add(usize, self.retained_bytes, image.pixels.len * 4) catch return error.ResourceLimitExceeded;
        try self.permit.?.retainHostBytes(self.retained_bytes);
        self.prepare_ns = mediaNow() - started;
    }
};

/// Two stable slots: one consumer and at most one CPU producer. Every exit
/// drains the producer before releasing borrowed sources or slot allocators.
fn embedPreparedMedia(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, cfg: encoder.Config, source: MediaSource, options: Options, lock: ?*std.atomic.Mutex, control: Control, errors: ?[]?anyerror) !RasterBatchResult {
    const vectors = try out.alloc([]f32, source.len());
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| out.free(vector);
        out.free(vectors);
    }
    if (source.len() == 0) return .{ .vectors = vectors, .execution = .serial };
    // Only the active model caller owns preprocessing leases. Queued callers
    // wait without reserving host memory; the active caller may still prepare
    // its one future CPU wave while this gate protects current GPU execution.
    if (lock) |mutex| try control.lock(mutex);
    defer if (lock) |mutex| mutex.unlock();
    var slots: [2]PreparationSlot = undefined;
    var live: [2]bool = @splat(false);
    defer for (&slots, live) |*slot, exists| if (exists) slot.deinit();
    var current: usize = 0;
    var count = try source.count(0);
    slots[current] = PreparationSlot.init(session, source.slice(0, count), options, control, if (errors) |e| e[0..count] else null);
    live[current] = true;
    slots[current].run();
    var observation = BatchExecution.Observation{};
    while (initialized < source.len()) {
        try control.check();
        const slot = &slots[current];
        slot.join();
        if (slot.failure) |failure| return failure;
        if (options.timings) |t| t.preprocess_ns += slot.prepare_ns;
        const next = 1 - current;
        const next_offset = initialized + count;
        var next_count: usize = 0;
        if (next_offset < source.len()) {
            next_count = try source.count(next_offset);
            slots[next] = PreparationSlot.init(session, source.slice(next_offset, next_count), options, control, if (errors) |e| e[next_offset..][0..next_count] else null);
            live[next] = true;
            const spawned = slots[next].start();
            if (options.timings) |t| {
                t.preparation_jobs += @intFromBool(spawned);
                t.max_prepared_images = @max(t.max_prepared_images, count + next_count);
            }
        } else if (options.timings) |t| t.max_prepared_images = @max(t.max_prepared_images, count);
        {
            const temporary = try consumePreparedMedia(slot, tokenizer, cfg, if (next_count != 0) &slots[next] else null);
            defer {
                for (temporary) |vector| slot.allocator().free(vector);
                slot.allocator().free(temporary);
            }
            var valid: usize = 0;
            for (temporary) |vector| {
                vectors[initialized] = try out.dupe(f32, vector);
                initialized += 1;
                valid += @intFromBool(vector.len != 0);
            }
            if (valid != 0) observation.record(valid);
        }
        slot.deinit();
        live[current] = false;
        if (next_count != 0) {
            slots[next].join();
            // Speculative host admission must not turn an otherwise-fitting
            // sequential request into an error. Retry once after draining the
            // previous wave; codec/control/runtime failures never retry.
            if (slots[next].failure) |failure| if (isPreparationPressure(failure)) {
                slots[next].deinit();
                slots[next] = PreparationSlot.init(session, source.slice(next_offset, next_count), options, control, if (errors) |e| e[next_offset..][0..next_count] else null);
                if (options.timings) |t| t.preparation_backpressure_retries += 1;
                slots[next].run();
            };
            current = next;
            count = next_count;
        }
    }
    try control.check();
    return .{ .vectors = vectors, .execution = observation.execution(observation.native_items + observation.serial_items) };
}

/// Vision intermediates drain before the embedding encoder starts. Projected
/// rows stay resident across that boundary and are covered by output storage.
/// Reserve the larger phase workspace rather than adding disjoint lifetimes.
pub fn imageRunRequest(backend: @import("../backends/backends.zig").BackendType, input_bytes: usize, retained_bytes: usize, batch: usize, vision_scratch: usize) @import("../backends/session.zig").RunRequest {
    return .{ .batch = batch, .sequence = raster_max_sequence, .input_bytes = input_bytes, .pre_admitted_host_bytes = retained_bytes, .host_preprocess_bytes = 512 * 1024 * 1024, .workspace_bytes = @max(vision_scratch, if (backend == .metal) @as(usize, 768 * 1024 * 1024) else @as(usize, 512 * 1024 * 1024)), .output_bytes = batch * raster_max_sequence * 768 * 4 };
}

fn isPreparationPressure(failure: anyerror) bool {
    return failure == error.MemoryBudgetExceeded or failure == error.ResourceLimitExceeded or failure == error.ResourceTemporarilyUnavailable;
}

fn consumePreparedMedia(slot: *PreparationSlot, tokenizer: Tokenizer, cfg: encoder.Config, lookahead: ?*PreparationSlot) ![][]f32 {
    const a = slot.allocator();
    const session = slot.session;
    const options = slot.options;
    const control = slot.control;
    const vectors = try a.alloc([]f32, slot.source.len());
    @memset(vectors, &.{});
    errdefer {
        for (vectors) |vector| a.free(vector);
        a.free(vectors);
    }
    if (slot.retained == 0) return vectors;
    const vision_limit: usize = 2 * 1024 * 1024 * 1024;
    const vision_scratch = try projector.embeddingGemma2PreparedScratch(slot.pixels[0..slot.retained], vision_limit);
    const request = imageRunRequest(session.backend(), slot.input_bytes, slot.retained_bytes, slot.source.len(), vision_scratch);
    var permit = admitWithVisionCacheReclaim(session, request, control) catch |err| blk: {
        const future = lookahead orelse return err;
        if (!isPreparationPressure(err)) return err;
        // Speculation must not occupy the budget needed by the current wave.
        // Drain/free it before retrying GPU admission; the owner will prepare
        // that wave again after the current one has released its permit.
        future.deinit();
        future.failure = err;
        break :blk try admitWithVisionCacheReclaim(session, request, control);
    };
    defer permit.deinit();
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    const transfer_before = managed.backend.debugTimingSnapshot().provider;
    defer observeResidentTransfers(&managed.backend, transfer_before, options);
    const vision_started = mediaNow();
    if (residentImagesEnabled(&managed.backend, options)) {
        if (options.timings) |t| t.resident_cohorts += 1;
        var resident = try projector.encodePreparedEmbeddingGemma2ImagesResident(&managed.backend, a, slot.pixels[0..slot.retained], .{ .max_scratch_bytes = vision_scratch });
        defer resident.deinit();
        if (options.timings) |t| t.vision_ns += mediaNow() - vision_started;
        const compact = try runResidentImageBatch(a, &managed.backend, cfg, tokenizer, &resident, options);
        defer a.free(compact);
        for (slot.indexes[0..slot.retained], compact) |index, vector| vectors[index] = vector;
        try control.check();
        return vectors;
    }
    if (options.timings) |t| t.host_cohorts += 1;
    var projected = try projector.encodePreparedEmbeddingGemma2Images(&managed.backend, a, slot.pixels[0..slot.retained], .{ .max_scratch_bytes = vision_scratch });
    defer projected.deinit();
    const pack_started = mediaNow();
    var groups: [raster_max_batch]PreparedGroup = undefined;
    var initialized: usize = 0;
    defer for (groups[0..initialized]) |group| group.deinit(a);
    var offset: usize = 0;
    for (slot.indexes[0..slot.retained], projected.tokens_per_image) |index, tokens| {
        const count = tokens * projected.hidden_size;
        groups[initialized] = try prepareGroupWithImage(a, &managed.backend, tokenizer, .{ .content = &.{slot.source.part(index)} }, options, control, .{ .id = 258880, .embeddings = projected.embeddings[offset..][0..count], .tokens = tokens });
        offset += count;
        initialized += 1;
    }
    if (options.timings) |t| {
        t.vision_ns += pack_started - vision_started;
        t.pack_ns += mediaNow() - pack_started;
    }
    const compact = try runPreparedBatchTimed(a, &managed.backend, cfg, groups[0..initialized], options);
    defer a.free(compact);
    for (slot.indexes[0..slot.retained], compact) |index, vector| vectors[index] = vector;
    try control.check();
    return vectors;
}

fn embedImageCohort(a: std.mem.Allocator, session: Session, tokenizer: Tokenizer, cfg: encoder.Config, images: []const []const u8, options: Options, lock: ?*std.atomic.Mutex, control: Control, item_errors: ?[]?anyerror) ![][]f32 {
    var input_bytes: usize = 0;
    for (images, 0..) |bytes, index| {
        validateGroup(.{ .content = &.{.{ .image = bytes }} }, options) catch |err| {
            if (item_errors) |errors| errors[index] = err else return err;
        };
        input_bytes = std.math.add(usize, input_bytes, bytes.len) catch return error.ResourceLimitExceeded;
    }
    const vision_scratch: usize = if (session.backend() == .metal or session.backend() == .cuda) 2 * 1024 * 1024 * 1024 else 512 * 1024 * 1024;
    // Encoded dimensions are not known until decoding. Reserve bounded host
    // preprocessing before codecs; reserve GPU scratch under the model lock.
    const preprocessing_bytes = std.math.add(usize, input_bytes, 512 * 1024 * 1024) catch return error.ResourceLimitExceeded;
    var host_permit = try session.admitHostPreprocess(preprocessing_bytes);
    defer host_permit.deinit();
    const image_scope = @import("antfly_image").work_control.Scope.enter(control.imageWorkControl());
    defer image_scope.deinit();
    const preprocess_started = mediaNow();
    var pixels: [raster_max_batch]projector.PreparedImage = undefined;
    var indexes: [raster_max_batch]usize = undefined;
    var retained: usize = 0;
    defer for (pixels[0..retained]) |item| item.deinit();
    for (images, 0..) |bytes, index| {
        try control.check();
        if (item_errors) |errors| if (errors[index] != null) continue;
        pixels[retained] = projector.prepareEmbeddingGemma2Image(a, bytes) catch |err| {
            if (item_errors) |errors| {
                if (err == error.OutOfMemory or err == error.Cancelled or err == error.Canceled or err == error.Timeout or err == error.DeadlineExceeded or err == error.MemoryBudgetExceeded) return err;
                errors[index] = err;
                continue;
            }
            return err;
        };
        indexes[retained] = index;
        retained += 1;
    }
    const vectors = try a.alloc([]f32, images.len);
    @memset(vectors, &.{});
    errdefer {
        for (vectors) |vector| a.free(vector);
        a.free(vectors);
    }
    if (options.timings) |t| t.preprocess_ns += mediaNow() - preprocess_started;
    if (retained == 0) return vectors;
    var retained_bytes = input_bytes;
    for (pixels[0..retained]) |item| retained_bytes = std.math.add(usize, retained_bytes, item.pixels.len * 4) catch return error.ResourceLimitExceeded;
    try host_permit.retainHostBytes(retained_bytes);
    if (lock) |mutex| try control.lock(mutex);
    defer if (lock) |mutex| mutex.unlock();
    var permit = try admitWithVisionCacheReclaim(session, .{ .batch = images.len, .sequence = raster_max_sequence, .input_bytes = input_bytes, .pre_admitted_host_bytes = retained_bytes, .host_preprocess_bytes = 512 * 1024 * 1024, .workspace_bytes = vision_scratch + (if (session.backend() == .metal) @as(usize, 768 * 1024 * 1024) else @as(usize, 512 * 1024 * 1024)), .output_bytes = images.len * raster_max_sequence * 768 * 4 }, control);
    defer permit.deinit();
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    const transfer_before = managed.backend.debugTimingSnapshot().provider;
    defer observeResidentTransfers(&managed.backend, transfer_before, options);
    if (residentImagesEnabled(&managed.backend, options)) {
        if (options.timings) |t| t.resident_cohorts += 1;
        const started = mediaNow();
        var resident = try projector.encodePreparedEmbeddingGemma2ImagesResident(&managed.backend, a, pixels[0..retained], .{ .max_scratch_bytes = vision_scratch });
        defer resident.deinit();
        if (options.timings) |t| t.vision_ns += mediaNow() - started;
        const compact = try runResidentImageBatch(a, &managed.backend, cfg, tokenizer, &resident, options);
        defer a.free(compact);
        for (indexes[0..retained], compact) |index, vector| vectors[index] = vector;
        try control.check();
        return vectors;
    }
    if (options.timings) |t| t.host_cohorts += 1;
    const vision_started = mediaNow();
    var projected = try projector.encodePreparedEmbeddingGemma2Images(&managed.backend, a, pixels[0..retained], .{ .max_scratch_bytes = vision_scratch });
    defer projected.deinit();
    const pack_started = mediaNow();
    var prepared: [raster_max_batch]PreparedGroup = undefined;
    var initialized: usize = 0;
    defer for (prepared[0..initialized]) |row| row.deinit(a);
    var offset: usize = 0;
    for (indexes[0..retained], projected.tokens_per_image) |index, tokens| {
        const count = tokens * projected.hidden_size;
        prepared[initialized] = try prepareGroupWithImage(a, &managed.backend, tokenizer, .{ .content = &.{.{ .image = images[index] }} }, options, control, .{ .id = 258880, .embeddings = projected.embeddings[offset..][0..count], .tokens = tokens });
        offset += count;
        initialized += 1;
    }
    if (options.timings) |t| {
        t.vision_ns += pack_started - vision_started;
        t.pack_ns += mediaNow() - pack_started;
    }
    const compact = try runPreparedBatchTimed(a, &managed.backend, cfg, prepared[0..initialized], options);
    defer a.free(compact);
    for (indexes[0..retained], compact) |index, vector| vectors[index] = vector;
    try control.check();
    return vectors;
}

fn embedRasterCohort(a: std.mem.Allocator, session: Session, tokenizer: Tokenizer, cfg: encoder.Config, rasters: []const Raster, options: Options, lock: ?*std.atomic.Mutex, control: Control) ![][]f32 {
    var input_bytes: usize = 0;
    for (rasters) |raster| {
        try validateGroup(.{ .content = &.{.{ .raster = raster }} }, options);
        input_bytes = std.math.add(usize, input_bytes, raster.bytes.len) catch return error.ResourceLimitExceeded;
    }
    // Image soft tokens are capped at 280. Reserve a conservative 512-token
    // row before projection; a cohort occupies at most 2048 padded slots.
    const vision_limit: usize = if (session.backend() == .metal or session.backend() == .cuda) 2 * 1024 * 1024 * 1024 else 512 * 1024 * 1024;
    const vision_scratch = try projector.embeddingGemma2RasterScratch(rasters, vision_limit);
    var host_permit = try session.admitHostPreprocess(input_bytes);
    defer host_permit.deinit();
    if (lock) |mutex| try control.lock(mutex);
    defer if (lock) |mutex| mutex.unlock();
    var permit = try admitWithVisionCacheReclaim(session, .{ .batch = rasters.len, .sequence = raster_max_sequence, .input_bytes = input_bytes, .pre_admitted_host_bytes = input_bytes, .host_preprocess_bytes = 512 * 1024 * 1024, .workspace_bytes = vision_scratch + (if (session.backend() == .metal) @as(usize, 768 * 1024 * 1024) else @as(usize, 512 * 1024 * 1024)), .output_bytes = rasters.len * raster_max_sequence * 768 * @sizeOf(f32) }, control);
    defer permit.deinit();
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    const transfer_before = managed.backend.debugTimingSnapshot().provider;
    defer observeResidentTransfers(&managed.backend, transfer_before, options);
    const image_scope = @import("antfly_image").work_control.Scope.enter(control.imageWorkControl());
    defer image_scope.deinit();
    const preprocess_started = mediaNow();
    var pixels: [raster_max_batch]projector.PreparedImage = undefined;
    var pixel_count: usize = 0;
    defer for (pixels[0..pixel_count]) |item| item.deinit();
    for (rasters) |raster| {
        try control.check();
        pixels[pixel_count] = try projector.prepareEmbeddingGemma2Raster(a, raster);
        pixel_count += 1;
    }
    if (options.timings) |t| t.preprocess_ns += mediaNow() - preprocess_started;
    if (residentImagesEnabled(&managed.backend, options)) {
        if (options.timings) |t| t.resident_cohorts += 1;
        const started = mediaNow();
        var resident = try projector.encodePreparedEmbeddingGemma2ImagesResident(&managed.backend, a, pixels[0..pixel_count], .{ .max_scratch_bytes = vision_scratch });
        defer resident.deinit();
        if (options.timings) |t| t.vision_ns += mediaNow() - started;
        const vectors = try runResidentImageBatch(a, &managed.backend, cfg, tokenizer, &resident, options);
        errdefer {
            for (vectors) |vector| a.free(vector);
            a.free(vectors);
        }
        try control.check();
        return vectors;
    }
    if (options.timings) |t| t.host_cohorts += 1;
    const vision_started = mediaNow();
    var projected = try projector.encodePreparedEmbeddingGemma2Images(&managed.backend, a, pixels[0..pixel_count], .{ .max_scratch_bytes = vision_scratch });
    defer projected.deinit();
    const pack_started = mediaNow();
    var prepared: [raster_max_batch]PreparedGroup = undefined;
    var initialized: usize = 0;
    defer for (prepared[0..initialized]) |row| row.deinit(a);
    var projected_offset: usize = 0;
    for (rasters, projected.tokens_per_image) |raster, tokens| {
        try control.check();
        const count = tokens * projected.hidden_size;
        prepared[initialized] = try prepareGroupWithImage(a, &managed.backend, tokenizer, .{ .content = &.{.{ .raster = raster }} }, options, control, .{ .id = 258880, .embeddings = projected.embeddings[projected_offset..][0..count], .tokens = tokens });
        projected_offset += count;
        initialized += 1;
    }
    if (options.timings) |t| {
        t.vision_ns += pack_started - vision_started;
        t.pack_ns += mediaNow() - pack_started;
    }
    const vectors = try runPreparedBatchTimed(a, &managed.backend, cfg, prepared[0..initialized], options);
    errdefer {
        for (vectors) |vector| a.free(vector);
        a.free(vectors);
    }
    try control.check();
    return vectors;
}

fn observeResidentTransfers(cb: *const ComputeBackend, before: @import("../ops/ops.zig").NativeQuantTimingStats, options: Options) void {
    if (options.timings) |t| {
        const after = cb.debugTimingSnapshot().provider;
        if (cb.kind() == .metal) {
            t.gpu_frame_ns += after.decoder_runtime_frame_gpu_nanos -| before.decoder_runtime_frame_gpu_nanos;
            t.frame_submits += after.decoder_runtime_frame_submits -| before.decoder_runtime_frame_submits;
            t.frame_wait_ns += after.decoder_runtime_frame_wait_nanos -| before.decoder_runtime_frame_wait_nanos;
            t.download_bytes = (t.download_bytes orelse 0) + after.metal_tensor_host_mirror_download_bytes - before.metal_tensor_host_mirror_download_bytes;
            t.readbacks = (t.readbacks orelse 0) + after.metal_tensor_to_host_device_calls - before.metal_tensor_to_host_device_calls;
            t.device_owned_peak_bytes = @max(t.device_owned_peak_bytes orelse 0, after.metal_tensor_device_owned_peak_live_bytes);
        }
    }
}

/// Tokenization stays on CPU; projected pixels and prompt embedding rows stay
/// on their creating backend through the encoder. Padding never pools into a row.
fn runResidentImageBatch(a: std.mem.Allocator, cb: *const ComputeBackend, cfg: encoder.Config, tokenizer: Tokenizer, projected: *const projector.ResidentProjectedImages, options: Options) ![][]f32 {
    return runResidentMediaBatch(a, cb, cfg, tokenizer, projected, options, false);
}
fn runResidentMediaBatch(a: std.mem.Allocator, cb: *const ComputeBackend, cfg: encoder.Config, tokenizer: Tokenizer, projected: *const projector.ResidentProjectedImages, options: Options, is_audio: bool) ![][]f32 {
    const max_sequence: usize = if (is_audio) 1024 else raster_max_sequence;
    const marker: i64 = if (is_audio) 258881 else 258880;
    const started = mediaNow();
    const batch = projected.tokens_per_image.len;
    if (batch == 0 or batch > raster_max_batch or projected.hidden_size != 512) return error.InvalidInputShape;
    var tokens: [raster_max_batch]PreparedGroup = undefined;
    var initialized: usize = 0;
    defer for (tokens[0..initialized]) |row| row.deinit(a);
    for (projected.tokens_per_image) |count| {
        if (count == 0 or count > (if (is_audio) @as(usize, 1020) else 280)) return error.InvalidTensorShape;
        var rendered = std.ArrayList(u8).empty;
        defer rendered.deinit(a);
        try rendered.appendSlice(a, if (is_audio) "<|audio>" else "<|image>");
        for (0..count) |_| try rendered.appendSlice(a, if (is_audio) "<|audio|>" else "<|image|>");
        try rendered.appendSlice(a, if (is_audio) "<audio|>" else "<image|>");
        var encoded = try tokenizer.encodeForModel(a, rendered.items, max_sequence + 1);
        defer encoded.deinit();
        var length: usize = 0;
        for (encoded.attention_mask) |m| length += @intFromBool(m != 0);
        if (length == 0 or length > max_sequence) return error.EmbeddingInputTooLong;
        const ids = try a.alloc(i64, length);
        errdefer a.free(ids);
        const mask = try a.alloc(i64, length);
        for (ids, encoded.ids[0..length]) |*dest, id| dest.* = id;
        for (mask, encoded.attention_mask[0..length]) |*dest, m| dest.* = m;
        tokens[initialized] = .{ .ids = ids, .mask = mask, .embeddings = null };
        initialized += 1;
    }
    const batch_inputs = try packResidentMediaRows(a, cb, tokens[0..initialized], projected.embeddings, projected.tokens_per_image, marker, max_sequence);
    defer batch_inputs.deinit(a, cb);
    if (options.timings) |t| t.pack_ns += mediaNow() - started;
    const encoder_started = mediaNow();
    const output = try encoder.forwardEmbeddingsCT(cb, a, cfg, batch_inputs.embeddings, batch_inputs.mask, batch, batch_inputs.sequence);
    defer cb.free(output);
    if (options.timings) |t| t.encoder_ns += mediaNow() - encoder_started;
    const pool_started = mediaNow();
    const vectors = try poolResidentBatch(a, cb, output, batch_inputs.mask, batch, batch_inputs.sequence, options.dimensions);
    if (options.timings) |t| t.pool_readback_ns += mediaNow() - pool_started;
    return vectors;
}

const ResidentPackedGroups = struct {
    embeddings: @import("../ops/ops.zig").CT,
    mask: []i64,
    sequence: usize,
    fn deinit(self: @This(), a: std.mem.Allocator, cb: *const ComputeBackend) void {
        cb.free(self.embeddings);
        a.free(self.mask);
    }
};

fn packResidentImageRows(a: std.mem.Allocator, cb: *const ComputeBackend, rows: []const PreparedGroup, projected: @import("../ops/ops.zig").CT, lengths: []const usize) !ResidentPackedGroups {
    return packResidentMediaRows(a, cb, rows, projected, lengths, 258880, raster_max_sequence);
}
fn packResidentMediaRows(a: std.mem.Allocator, cb: *const ComputeBackend, rows: []const PreparedGroup, projected: @import("../ops/ops.zig").CT, lengths: []const usize, marker: i64, max_sequence: usize) !ResidentPackedGroups {
    if (rows.len == 0 or rows.len > raster_max_batch or rows.len != lengths.len or !cb.supportsCopyRows2D()) return error.InvalidInputShape;
    var sequence: usize = 0;
    for (rows) |row| {
        if (row.ids.len == 0 or row.mask.len != row.ids.len) return error.InvalidInputShape;
        sequence = @max(sequence, row.ids.len);
    }
    if (sequence > max_sequence) return error.EmbeddingInputTooLong;
    const ids = try a.alloc(i64, rows.len * sequence);
    defer a.free(ids);
    @memset(ids, 0);
    const mask = try a.alloc(i64, ids.len);
    errdefer a.free(mask);
    @memset(mask, 0);
    for (rows, 0..) |row, index| {
        @memcpy(ids[index * sequence ..][0..row.ids.len], row.ids);
        @memcpy(mask[index * sequence ..][0..row.mask.len], row.mask);
    }
    const weight = try cb.getWeight("language_model.embed_tokens.weight");
    defer cb.free(weight);
    const raw = try cb.embeddingLookup(weight, ids, ids.len, 512);
    defer cb.free(raw);
    const scaled = (try cb.multiplyScalar(raw, @sqrt(@as(f32, 512)))) orelse return error.ResidentImageOperationUnavailable;
    const input = try cb.ensureDeviceResidentOwned(scaled);
    errdefer cb.free(input);
    var source_offset: usize = 0;
    for (rows, lengths, 0..) |row, count, index| {
        var first: ?usize = null;
        var found: usize = 0;
        for (row.ids, 0..) |id, r| {
            if (id == marker) {
                if (first == null) first = r;
                if (r != first.? + found or row.mask[r] == 0) return error.InvalidEmbeddingGroup;
                found += 1;
            }
        }
        if (found != count or count == 0) return error.InvalidEmbeddingGroup;
        if (!try cb.copyRows2D(a, input, index * sequence + first.?, projected, source_offset, count, 512)) return error.ResidentImageOperationUnavailable;
        source_offset += count;
    }
    return .{ .embeddings = input, .mask = mask, .sequence = sequence };
}

fn poolResidentBatch(a: std.mem.Allocator, cb: *const ComputeBackend, output: @import("../ops/ops.zig").CT, mask: []const i64, batch: usize, sequence: usize, dimensions: usize) ![][]f32 {
    if (!encoder.validDimension(dimensions) or mask.len != batch * sequence or cb.vtable.sliceRows2D == null) return error.InvalidInputShape;
    const vectors = try a.alloc([]f32, batch);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| a.free(vector);
        a.free(vectors);
    }
    for (0..batch) |index| {
        try cb.checkExecutionControl();
        const row_mask = mask[index * sequence ..][0..sequence];
        const row = try cb.sliceRows2D(a, output, index * sequence, sequence, 768);
        defer cb.free(row);
        const pooled = if (cb.vtable.maskedMeanRows) |op|
            (try op(cb.ptr, row, row_mask, 768, dimensions)) orelse return error.ResidentImageOperationUnavailable
        else blk: {
            const indexes = try a.alloc(u32, sequence);
            defer a.free(indexes);
            var count: usize = 0;
            for (row_mask, 0..) |active, r| if (active != 0) {
                indexes[count] = @intCast(r);
                count += 1;
            };
            if (count == 0) return error.EmptyEmbeddingInput;
            const active = (try cb.takeRows(row, indexes[0..count], count, 768)) orelse return error.ResidentImageOperationUnavailable;
            defer cb.free(active);
            break :blk try cb.primReduceMean(active, &.{0}, &.{ @intCast(count), 768 });
        };
        defer cb.free(pooled);
        const values = try cb.toFloat32(pooled, a);
        defer a.free(values);
        if (values.len < dimensions) return error.InvalidTensorShape;
        vectors[index] = try scoring.centroid(a, &.{values[0..dimensions]}, dimensions);
        initialized += 1;
    }
    return vectors;
}

const PackedGroups = struct {
    embeddings: []f32,
    mask: []i64,
    sequence: usize,

    fn deinit(self: PackedGroups, a: std.mem.Allocator) void {
        a.free(self.embeddings);
        a.free(self.mask);
    }
};

fn packPreparedGroups(a: std.mem.Allocator, rows: []const PreparedGroup) !PackedGroups {
    if (rows.len == 0 or rows.len > raster_max_batch) return error.InvalidInputShape;
    var sequence: usize = 0;
    for (rows) |row| {
        if (row.ids.len == 0 or row.mask.len != row.ids.len or row.embeddings == null or row.embeddings.?.len != row.ids.len * 512) return error.InvalidInputShape;
        sequence = @max(sequence, row.ids.len);
    }
    if (sequence > raster_max_sequence or sequence * rows.len > raster_max_padded_tokens) return error.EmbeddingInputTooLong;
    const embeddings = try a.alloc(f32, rows.len * sequence * 512);
    errdefer a.free(embeddings);
    @memset(embeddings, 0);
    const mask = try a.alloc(i64, rows.len * sequence);
    errdefer a.free(mask);
    @memset(mask, 0);
    for (rows, 0..) |row, index| {
        @memcpy(embeddings[index * sequence * 512 ..][0..row.embeddings.?.len], row.embeddings.?);
        @memcpy(mask[index * sequence ..][0..row.mask.len], row.mask);
    }
    return .{ .embeddings = embeddings, .mask = mask, .sequence = sequence };
}

fn runPreparedBatchTimed(a: std.mem.Allocator, cb: *const ComputeBackend, cfg: encoder.Config, rows: []const PreparedGroup, options: Options) ![][]f32 {
    const started = mediaNow();
    const batch_inputs = try packPreparedGroups(a, rows);
    defer batch_inputs.deinit(a);
    const host = try cb.fromFloat32Shape(batch_inputs.embeddings, &.{ @intCast(rows.len * batch_inputs.sequence), 512 });
    const input = try cb.ensureDeviceResidentOwned(host);
    defer cb.free(input);
    if (options.timings) |t| t.pack_ns += mediaNow() - started;
    const encoder_started = mediaNow();
    const output = try encoder.forwardEmbeddingsCT(cb, a, cfg, input, batch_inputs.mask, rows.len, batch_inputs.sequence);
    defer cb.free(output);
    if (options.timings) |t| t.encoder_ns += mediaNow() - encoder_started;
    const pool_started = mediaNow();
    if (cb.kind() == .metal) {
        const operation = cb.vtable.maskedMeanNormalizedRows orelse return error.EmbeddingGemma2OperationUnavailable;
        const vectors = try a.alloc([]f32, rows.len);
        var initialized: usize = 0;
        errdefer {
            for (vectors[0..initialized]) |vector| a.free(vector);
            a.free(vectors);
        }
        for (vectors, 0..) |*vector, index| {
            const view = try cb.sliceRows2D(a, output, index * batch_inputs.sequence, batch_inputs.sequence, 768);
            defer cb.free(view);
            const pooled = (try operation(cb.ptr, view, batch_inputs.mask[index * batch_inputs.sequence ..][0..batch_inputs.sequence], 768, options.dimensions)) orelse return error.EmbeddingGemma2OperationUnavailable;
            defer cb.free(pooled);
            vector.* = try readNormalizedVector(cb, a, pooled, options.dimensions);
            initialized += 1;
        }
        if (options.timings) |t| t.pool_readback_ns += mediaNow() - pool_started;
        return vectors;
    }
    const values = try cb.toFloat32(output, a);
    defer a.free(values);
    const vectors = try poolBatch(a, values, batch_inputs.mask, rows.len, batch_inputs.sequence, options.dimensions);
    if (options.timings) |t| t.pool_readback_ns += mediaNow() - pool_started;
    return vectors;
}

fn poolBatch(a: std.mem.Allocator, values: []const f32, mask: []const i64, batch: usize, sequence: usize, dimensions: usize) ![][]f32 {
    if (!encoder.validDimension(dimensions) or values.len != batch * sequence * 768 or mask.len != batch * sequence) return error.InvalidInputShape;
    for (0..batch) |index| {
        for (mask[index * sequence ..][0..sequence]) |active| {
            if (active != 0) break;
        } else return error.EmptyEmbeddingInput;
    }
    const vectors = try a.alloc([]f32, batch);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| a.free(vector);
        a.free(vectors);
    }
    for (vectors, 0..) |*vector, index| {
        var sum: [768]f32 = @splat(0);
        var count: usize = 0;
        for (mask[index * sequence ..][0..sequence], 0..) |active, row| {
            if (active == 0) continue;
            count += 1;
            for (sum[0..dimensions], values[(index * sequence + row) * 768 ..][0..dimensions]) |*dest, value| dest.* += value;
        }
        if (count == 0) return error.EmptyEmbeddingInput;
        for (sum[0..dimensions]) |*value| value.* /= @as(f32, @floatFromInt(count));
        vector.* = try scoring.centroid(a, &.{sum[0..dimensions]}, dimensions);
        initialized += 1;
    }
    return vectors;
}

/// Validation independent of model loading, shared by HTTP and linked APIs.
pub fn validateGroup(group: Group, options: Options) !void {
    _ = try encoder.taskPrefix(options.task_type);
    if (!encoder.validDimension(options.dimensions)) return error.InvalidEmbeddingDimensions;
    if (group.content.len == 0 or group.content.len > 64) return error.InvalidEmbeddingGroup;
    var has_text = false;
    var bytes: usize = 0;
    var raster_bytes: usize = 0;
    for (group.content) |part| {
        const value = partBytes(part);
        if (part == .sampled_video) {
            switch (part.sampled_video.sampling) {
                .timestamps, .source_timestamps => |policy| try policy.validate(),
                .reference => {},
            }
            if (part.sampled_video.webm) |input| {
                if (part.sampled_video.reader != null or part.sampled_video.selection != null or part.sampled_video.bytes.len != 0 or input.index.reader != input.reader or input.index.pictures.len == 0) return error.InvalidEmbeddingGroup;
                if (input.continuation) |owner| if (owner.reader != input.reader) return error.InvalidEmbeddingGroup;
                if (input.apple_continuation) |owner| if (owner.reader != input.reader) return error.InvalidEmbeddingGroup;
            }
            if (part.sampled_video.reader) |reader| {
                if (part.sampled_video.bytes.len != 0) return error.InvalidEmbeddingGroup;
                _ = try @import("embedding_gemma2_video.zig").packetBytes(reader);
            }
        }
        if (part == .pcm) {
            const pcm = part.pcm;
            if (pcm.sample_rate == 0 or pcm.sample_rate > 192000 or pcm.samples.len == 0) return error.UnsupportedAudioFormat;
            if (@as(u128, pcm.samples.len) * 16000 > @as(u128, pcm.sample_rate) * 480000) return error.AudioTooLong;
            for (pcm.samples) |sample| if (!std.math.isFinite(sample)) return error.UnsupportedAudioFormat;
        }
        if (part == .raster) {
            try part.raster.validate();
            try @import("image.zig").DecodeLimits.inference_default.validate(part.raster.width, part.raster.height);
        }
        if (value.len == 0 and !(part == .sampled_video and (part.sampled_video.webm != null or part.sampled_video.reader != null))) return error.EmptyEmbeddingInput;
        if (part == .raster) raster_bytes = std.math.add(usize, raster_bytes, value.len) catch return error.ResourceLimitExceeded else bytes = std.math.add(usize, bytes, value.len) catch return error.ResourceLimitExceeded;
        if (part == .text) {
            has_text = true;
            if (!std.unicode.utf8ValidateSlice(value)) return error.InvalidUtf8;
            for ([_][]const u8{ "<|image", "<image|>", "<|audio", "<audio|>", "<|video|>" }) |marker| if (std.mem.indexOf(u8, value, marker) != null) return error.InvalidEmbeddingGroup;
        }
    }
    if (bytes > 64 * 1024 * 1024 or raster_bytes > 512 * 1024 * 1024) return error.ResourceLimitExceeded;
    if (group.title) |title| if (!has_text or !std.mem.eql(u8, options.task_type, "RETRIEVAL_DOCUMENT") or !std.unicode.utf8ValidateSlice(title) or title.len > 8192 or std.mem.indexOf(u8, title, "<") != null) return error.InvalidEmbeddingTitle;
}

/// Encoder benchmark and serving share this boundary: prepared IDs/mask through
/// synchronized final vector readback. Loading, tokenization and HTTP stay out.
pub fn preparedText(cb: *const @import("../ops/ops.zig").ComputeBackend, a: std.mem.Allocator, cfg: encoder.Config, ids: []const i64, mask: []const i64, dimensions: usize) ![]f32 {
    if (!encoder.validDimension(dimensions)) return error.InvalidEmbeddingDimensions;
    if (ids.len == 0 or ids.len > 8192 or mask.len != ids.len) return error.InvalidEmbeddingInputLength;
    var count: usize = 0;
    for (ids, mask) |id, m| {
        if (id < 0 or id >= cfg.vocab_size) return error.InvalidTokenId;
        count += @intFromBool(m != 0);
    }
    if (count == 0) return error.EmptyEmbeddingInput;
    const resident = cb.kind() == .metal and !@import("antfly_platform").env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_DISABLE_RESIDENT_TEXT");
    if (resident) {
        const output = try encoder.forwardCT(cb, a, cfg, ids, mask, 1, ids.len);
        defer cb.free(output);
        const pooled = (if (cb.vtable.maskedMeanNormalizedRows) |op| try op(cb.ptr, output, mask, 768, dimensions) else null) orelse return error.EmbeddingGemma2OperationUnavailable;
        defer cb.free(pooled);
        return readNormalizedVector(cb, a, pooled, dimensions);
    }
    const ew = try cb.getWeight("language_model.embed_tokens.weight");
    defer cb.free(ew);
    const raw = try cb.embeddingLookup(ew, ids, ids.len, 512);
    defer cb.free(raw);
    const embeddings = try cb.toFloat32(raw, a);
    defer a.free(embeddings);
    for (embeddings) |*value| value.* *= @sqrt(@as(f32, 512));
    const host = try cb.fromFloat32Shape(embeddings, &.{ @intCast(ids.len), 512 });
    const input = try cb.ensureDeviceResidentOwned(host);
    defer cb.free(input);
    const output = try encoder.forwardEmbeddingsCT(cb, a, cfg, input, mask, 1, ids.len);
    defer cb.free(output);
    const values = try cb.toFloat32(output, a);
    defer a.free(values);
    const vector = try a.alloc(f32, dimensions);
    defer a.free(vector);
    @memset(vector, 0);
    for (mask, 0..) |m, row| {
        if (m == 0) continue;
        for (vector, values[row * 768 ..][0..dimensions]) |*dest, value| dest.* += value / @as(f32, @floatFromInt(count));
    }
    return scoring.centroid(a, &.{vector}, dimensions);
}

test "embeddinggemma2 raster batches bound padding and avoid singleton tails" {
    try std.testing.expectEqual(@as(usize, 3), rasterBatchCount(&.{ 284, 284, 284, 284, 284 }, 5));
    try std.testing.expectEqual(@as(usize, 2), rasterBatchCount(&.{ 284, 284 }, 2));
    try std.testing.expectEqual(@as(usize, 4), rasterBatchCount(&.{ 280, 284, 272, 284, 280, 284 }, 6));
    try std.testing.expectEqual(@as(usize, 1), rasterBatchCount(&.{ 8, 284, 284 }, 3));
    try std.testing.expectEqual(@as(usize, 1), rasterBatchCount(&.{ 284, 8, 284 }, 3));
}

test "embeddinggemma2 raster batch packing and pooling isolate rows padding and allocation failures" {
    const Runner = struct {
        fn run(a: std.mem.Allocator) !void {
            var first_ids = [_]i64{ 2, 1 };
            var second_ids = [_]i64{2};
            var first_mask = [_]i64{ 1, 1 };
            var second_mask = [_]i64{1};
            var first_embeddings: [2 * 512]f32 = @splat(1);
            var second_embeddings: [512]f32 = @splat(2);
            const rows = [_]PreparedGroup{
                .{ .ids = &first_ids, .mask = &first_mask, .embeddings = &first_embeddings },
                .{ .ids = &second_ids, .mask = &second_mask, .embeddings = &second_embeddings },
            };
            const inputs = try packPreparedGroups(a, &rows);
            defer inputs.deinit(a);
            try std.testing.expectEqual(@as(usize, 2), inputs.sequence);
            try std.testing.expectEqualSlices(i64, &.{ 1, 1, 1, 0 }, inputs.mask);
            try std.testing.expectEqualSlices(f32, &second_embeddings, inputs.embeddings[2 * 512 ..][0..512]);
            for (inputs.embeddings[3 * 512 ..]) |value| try std.testing.expectEqual(@as(f32, 0), value);
            var values: [4 * 768]f32 = @splat(0);
            values[0] = 0.3;
            values[1] = 0.4;
            values[768] = 0.3;
            values[769] = 0.4;
            values[2 * 768] = -0.3;
            values[2 * 768 + 1] = 0.4;
            @memset(values[3 * 768 ..], std.math.nan(f32));
            const vectors = try poolBatch(a, &values, inputs.mask, 2, 2, 128);
            defer {
                for (vectors) |vector| a.free(vector);
                a.free(vectors);
            }
            try std.testing.expectApproxEqAbs(@as(f32, 0.6), vectors[0][0], 1e-6);
            try std.testing.expectApproxEqAbs(@as(f32, -0.6), vectors[1][0], 1e-6);
            try std.testing.expectApproxEqAbs(@as(f32, 0.8), vectors[1][1], 1e-6);
            // An empty row must reject the entire batch without leaking an
            // already-produced vector from the preceding row.
            try std.testing.expectError(error.EmptyEmbeddingInput, poolBatch(a, &values, &.{ 1, 1, 0, 0 }, 2, 2, 128));
        }
    };
    try Runner.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "embeddinggemma2 raster batch cancellation precedes model access" {
    try std.testing.expectError(error.Timeout, embedRasters(std.testing.allocator, undefined, undefined, &.{}, .{}, null, .{ .deadline_ns = 0 }));
}

test "embeddinggemma2 video rows preserve image audio and frame ordering" {
    const a = std.testing.allocator;
    var ids = [_]i64{ 2, 255999, 258880, 258882, 255999, 258880, 258880, 258882, 255999, 258880, 258882, 256000, 258881, 258883, 1 };
    const data = try a.alloc(f32, ids.len * 512);
    defer a.free(data);
    @memset(data, -1);
    var image: [512]f32 = @splat(10);
    var frames: [3 * 512]f32 = undefined;
    for (&frames, 0..) |*v, i| v.* = @floatFromInt(20 + i / 512);
    var audio: [512]f32 = @splat(30);
    try overlayMedia(&ids, data, &.{ .{ .id = 258880, .tokens = 1, .embeddings = &image }, .{ .id = 258884, .tokens = 3, .embeddings = &frames }, .{ .id = 258881, .tokens = 1, .embeddings = &audio } });
    try std.testing.expectEqualSlices(i64, &.{ 2, 255999, 258880, 258882, 255999, 258884, 258884, 258882, 255999, 258884, 258882, 256000, 258881, 258883, 1 }, &ids);
    for ([_]usize{ 2, 5, 6, 9, 12 }, [_]f32{ 10, 20, 21, 22, 30 }) |row, value| for (data[row * 512 ..][0..512]) |got| try std.testing.expectEqual(value, got);
    try std.testing.expectEqual(@as(f32, -1), data[0]);
    var orphan = [_]i64{258880};
    try std.testing.expectError(error.InvalidEmbeddingGroup, overlayMedia(&orphan, data[0..512], &.{}));
    try validateGroup(.{ .content = &.{ .{ .video = "mp4" }, .{ .text = "caption" } } }, .{});
}

test {
    _ = @import("embedding_gemma2_video.zig");
}

test "embeddinggemma2 device composition rows preserve mixed media and reject partial markers" {
    const a = std.testing.allocator;
    var ids = [_]i64{ 2, 258880, 258880, 3, 258881, 258880, 258880 };
    const media = [_]SoftTokens{
        .{ .id = 258880, .embeddings = &.{}, .tokens = 2 },
        .{ .id = 258881, .embeddings = &.{}, .tokens = 1 },
        .{ .id = 258884, .embeddings = &.{}, .tokens = 2 },
    };
    const rows = try compositionRows(a, &ids, &media);
    defer a.free(rows);
    try std.testing.expectEqualSlices(i32, &.{ 0, 7, 8, 3, 9, 10, 11 }, rows);
    try std.testing.expectEqualSlices(i64, &.{ 2, 258880, 258880, 3, 258881, 258884, 258884 }, &ids);
    var partial = [_]i64{258880};
    try std.testing.expectError(error.InvalidEmbeddingGroup, compositionRows(a, &partial, media[0..1]));
    var wrong = [_]i64{258881};
    try std.testing.expectError(error.InvalidEmbeddingGroup, compositionRows(a, &wrong, media[0..1]));
    try std.testing.checkAllAllocationFailures(a, struct {
        fn run(allocator: std.mem.Allocator) !void {
            var sequence = [_]i64{ 2, 258880 };
            const mapped = try compositionRows(allocator, &sequence, &.{.{ .id = 258884, .tokens = 1, .embeddings = &.{} }});
            defer allocator.free(mapped);
        }
    }.run, .{});
}

test "embeddinggemma2 Metal device composition preserves ordered tokens without downloads" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    const hosted = @import("../ops/gpu_hosted_store.zig");
    const metal = @import("../ops/metal_compute.zig");
    const a = std.testing.allocator;
    var store = hosted.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    defer {
        metal.deinitPrefetchQueue(&store);
        store.lazy_weights.deinit(a);
    }
    metal.initPrefetchQueue(&store, a);
    var compute = try metal.MetalCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;
    const text_values = try a.alloc(f32, 6 * 512);
    defer a.free(text_values);
    for (text_values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(i % 29)) * 0.01;
    var image: [512]f32 = @splat(0.25);
    var audio: [512]f32 = @splat(-0.5);
    var video: [2 * 512]f32 = undefined;
    @memset(video[0..512], 0.75);
    @memset(video[512..], -0.125);
    const text = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(text_values, &.{ 6, 512 }));
    defer cb.free(text);
    const projected = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&video, &.{ 2, 512 }));
    defer cb.free(projected);
    const media = [_]SoftTokens{
        .{ .id = 258880, .tokens = 1, .embeddings = &image },
        .{ .id = 258884, .tokens = 2, .embeddings = &video, .tensor = projected },
        .{ .id = 258881, .tokens = 1, .embeddings = &audio },
    };
    var ids = [_]i64{ 2, 258880, 258880, 258880, 258881, 3 };
    const expected = try a.dupe(f32, text_values);
    defer a.free(expected);
    for (expected) |*value| value.* *= @sqrt(@as(f32, 512));
    var expected_ids = ids;
    try overlayMedia(&expected_ids, expected, &media);
    const before = cb.debugTimingSnapshot().provider.metal_tensor_host_mirror_download_bytes;
    const result = try composeDeviceTokens(a, &cb, text, &ids, &media);
    defer cb.free(result);
    try std.testing.expectEqual(before, cb.debugTimingSnapshot().provider.metal_tensor_host_mirror_download_bytes);
    try std.testing.expectEqualSlices(i64, &expected_ids, &ids);
    const actual = try cb.toFloat32(result, a);
    defer a.free(actual);
    for (actual, expected) |got, want| try std.testing.expectApproxEqAbs(want, got, 1e-6);
}

test "embeddinggemma2 video live window validates sampling before entering a model session" {
    const session: Session = undefined;
    const tokenizer: Tokenizer = undefined;
    try std.testing.expectError(error.InvalidSamplingPolicy, embedLiveWindow(std.testing.allocator, session, tokenizer, &.{}, .{ .step_ns = 0 }, null, .{}, null, null, null));
}

test "embeddinggemma2 video live pump checks cancellation before entering model admission" {
    const Dummy = struct {
        pub fn next(_: *@This()) !?@import("antfly_media").live_mp4.Segment {
            return null;
        }
        pub fn ended(_: *const @This()) bool {
            return true;
        }
    };
    var reader = Dummy{};
    var coordinator: @import("antfly_video").live_coordinator.Coordinator = undefined;
    try std.testing.expectError(error.Timeout, embedNextLive(std.testing.allocator, @as(Session, undefined), @as(Tokenizer, undefined), &coordinator, &reader, null, .{}, null, .{ .deadline_ns = 0 }));
}

fn populateResidentImageRows(store: *@import("../ops/native_compute.zig").WeightStore) !void {
    try @import("../ops/media_residency_test.zig").putWeight(store, "language_model.embed_tokens.weight", &.{ 1, 512 }, 0.25);
}
fn checkResidentImageRows(cb: ComputeBackend, _: ComputeBackend) !void {
    if (!cb.supportsCopyRows2D()) return error.SkipZigTest;
    const Probe = struct {
        var base: ComputeBackend = undefined;
        fn lookup(_: *anyopaque, weight: @import("../ops/ops.zig").CT, ids: []const i64, total: usize, dim: usize) anyerror!@import("../ops/ops.zig").CT {
            const remapped = try std.testing.allocator.alloc(i64, ids.len);
            defer std.testing.allocator.free(remapped);
            @memset(remapped, 0);
            return base.embeddingLookup(weight, remapped, total, dim);
        }
        fn reject(_: *anyopaque, _: @import("../ops/ops.zig").CT, _: std.mem.Allocator) anyerror![]f32 {
            return error.UnexpectedActivationReadback;
        }
    };
    const a = std.testing.allocator;
    Probe.base = cb;
    var vtable = cb.vtable.*;
    vtable.embeddingLookup = Probe.lookup;
    vtable.toFloat32 = Probe.reject;
    const guarded = ComputeBackend{ .ptr = cb.ptr, .vtable = &vtable };
    var pixels: [3 * 512]f32 = undefined;
    @memset(pixels[0..512], 1);
    @memset(pixels[512..1024], 2);
    @memset(pixels[1024..], 3);
    const source = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&pixels, &.{ 3, 512 }));
    defer cb.free(source);
    var ids0 = [_]i64{ 11, 258880, 258880, 12 };
    var ids1 = [_]i64{ 11, 258880, 12 };
    var mask0 = [_]i64{ 1, 1, 1, 1 };
    var mask1 = [_]i64{ 1, 1, 1 };
    const rows = [_]PreparedGroup{ .{ .ids = &ids0, .mask = &mask0, .embeddings = null }, .{ .ids = &ids1, .mask = &mask1, .embeddings = null } };
    const inputs = try packResidentImageRows(a, &guarded, &rows, source, &.{ 2, 1 });
    defer inputs.deinit(a, &cb);
    try std.testing.expectEqualSlices(i64, &.{ 1, 1, 1, 1, 1, 1, 1, 0 }, inputs.mask);
    const actual = try cb.toFloat32(inputs.embeddings, a);
    defer a.free(actual);
    for ([_]usize{ 1, 2, 5 }, [_]f32{ 1, 2, 3 }) |r, value| for (actual[r * 512 ..][0..512]) |v| try std.testing.expectEqual(value, v);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25) * @sqrt(@as(f32, 512)), actual[0], 1e-6);
    ids0[2] = 12;
    try std.testing.expectError(error.InvalidEmbeddingGroup, packResidentImageRows(a, &guarded, &rows, source, &.{ 2, 1 }));
}
test "embeddinggemma2 resident image packing preserves rows padding and rejects readbacks on Metal" {
    try @import("../ops/media_residency_test.zig").onMetal(populateResidentImageRows, checkResidentImageRows);
}
test "embeddinggemma2 resident image packing preserves rows padding and rejects readbacks on CUDA" {
    try @import("../ops/media_residency_test.zig").onCuda(populateResidentImageRows, checkResidentImageRows);
}
fn checkResidentPooling(cb: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    var values: [6 * 768]f32 = @splat(0);
    for ([_]usize{ 0, 1, 3 }) |r| {
        values[r * 768] = if (r == 3) -0.3 else 0.3;
        values[r * 768 + 1] = 0.4;
    }
    @memset(values[2 * 768 ..][0..768], std.math.nan(f32));
    @memset(values[4 * 768 ..], std.math.nan(f32));
    const tensor = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&values, &.{ 6, 768 }));
    defer cb.free(tensor);
    const before = @import("../ops/media_residency_test.zig").Transfers.snapshot(cb);
    const vectors = try poolResidentBatch(a, &cb, tensor, &.{ 1, 1, 0, 1, 0, 0 }, 2, 3, 128);
    defer {
        for (vectors) |v| a.free(v);
        a.free(vectors);
    }
    const after = @import("../ops/media_residency_test.zig").Transfers.snapshot(cb);
    if (cb.kind() == .metal) try std.testing.expectEqual(@as(u64, 2 * 128 * 4), after.downloads - before.downloads);
    try std.testing.expectApproxEqAbs(@as(f32, 0.6), vectors[0][0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, -0.6), vectors[1][0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), vectors[1][1], 1e-6);
    try std.testing.expectError(error.EmptyEmbeddingInput, poolResidentBatch(a, &cb, tensor, &.{ 1, 1, 0, 0, 0, 0 }, 2, 3, 128));
}
test "embeddinggemma2 resident pooling excludes NaN padding on native" {
    try @import("../ops/media_residency_test.zig").onNative(@import("../ops/media_residency_test.zig").noWeights, checkResidentPooling);
}
test "embeddinggemma2 resident pooling downloads only final vectors on Metal" {
    try @import("../ops/media_residency_test.zig").onMetal(@import("../ops/media_residency_test.zig").noWeights, checkResidentPooling);
}
test "embeddinggemma2 resident pooling excludes NaN padding on CUDA" {
    try @import("../ops/media_residency_test.zig").onCuda(@import("../ops/media_residency_test.zig").noWeights, checkResidentPooling);
}

test "gemma4 preparation producer drains cancellation and allocation failures" {
    const tensor = @import("../backends/tensor.zig");
    const Mock = struct {
        fn run(_: *anyopaque, _: []const tensor.Tensor, _: std.mem.Allocator) anyerror![]tensor.Tensor {
            return error.Unused;
        }
        fn info(_: *anyopaque) []const tensor.TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
        fn canceled(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    var dummy: u8 = 0;
    const session = Session{ .ptr = &dummy, .vtable = &.{ .run = Mock.run, .inputInfo = Mock.info, .outputInfo = Mock.info, .backend = Mock.backend, .close = Mock.close } };
    const images = [_][]const u8{"bad"};
    var canceled = PreparationSlot.init(session, .{ .images = &images }, .{}, .{ .check_fn = Mock.canceled }, null);
    canceled.bounded.backing = std.testing.allocator;
    _ = canceled.start();
    canceled.join();
    try std.testing.expectEqual(error.Cancelled, canceled.failure.?);
    canceled.deinit();
    try std.testing.expectEqual(@as(usize, 0), canceled.bounded.live);
    try std.testing.expect(canceled.permit == null);

    const bytes: [16]u8 = @splat(255);
    const raster = Raster{ .bytes = &bytes, .width = 2, .height = 2, .stride_bytes = 8 };
    var denied = PreparationSlot.init(session, .{ .rasters = &.{raster} }, .{}, .{}, null);
    denied.bounded.backing = std.testing.allocator;
    denied.bounded.limit = 4;
    _ = denied.start();
    denied.join();
    try std.testing.expectEqual(error.MemoryBudgetExceeded, denied.failure.?);
    denied.deinit();
    try std.testing.expectEqual(@as(usize, 0), denied.bounded.live);
    try std.testing.expect(denied.permit == null);

    // A caller waiting behind another GPU wave must not begin decoding or
    // consume a preprocessing lease, and its deadline must stay observable.
    var gate: std.atomic.Mutex = .unlocked;
    try std.testing.expect(gate.tryLock());
    defer gate.unlock();
    try std.testing.expectError(error.Timeout, embedPreparedMedia(std.testing.allocator, session, undefined, .{ .vision = true }, .{ .images = &images }, .{}, &gate, .{ .deadline_ns = 0 }, null));
    try std.testing.expect(!gate.tryLock());
}

fn checkResidentAudioRows(cb: ComputeBackend, _: ComputeBackend) !void {
    if (!cb.supportsCopyRows2D()) return error.SkipZigTest;
    const Probe = struct {
        var base: ComputeBackend = undefined;
        fn lookup(_: *anyopaque, weight: @import("../ops/ops.zig").CT, ids: []const i64, total: usize, dim: usize) anyerror!@import("../ops/ops.zig").CT {
            const remapped = try std.testing.allocator.alloc(i64, ids.len);
            defer std.testing.allocator.free(remapped);
            @memset(remapped, 0);
            return base.embeddingLookup(weight, remapped, total, dim);
        }
        fn reject(_: *anyopaque, _: @import("../ops/ops.zig").CT, _: std.mem.Allocator) anyerror![]f32 {
            return error.UnexpectedActivationReadback;
        }
    };
    const a = std.testing.allocator;
    Probe.base = cb;
    var vtable = cb.vtable.*;
    vtable.embeddingLookup = Probe.lookup;
    vtable.toFloat32 = Probe.reject;
    const guarded = ComputeBackend{ .ptr = cb.ptr, .vtable = &vtable };
    var pixels: [3 * 512]f32 = undefined;
    @memset(pixels[0..512], 1);
    @memset(pixels[512..1024], 2);
    @memset(pixels[1024..], 3);
    const source = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&pixels, &.{ 3, 512 }));
    defer cb.free(source);
    var ids0 = [_]i64{ 11, 258881, 258881, 12 };
    var ids1 = [_]i64{ 11, 258881, 12 };
    var mask0 = [_]i64{ 1, 1, 1, 1 };
    var mask1 = [_]i64{ 1, 1, 1 };
    const rows = [_]PreparedGroup{ .{ .ids = &ids0, .mask = &mask0, .embeddings = null }, .{ .ids = &ids1, .mask = &mask1, .embeddings = null } };
    const inputs = try packResidentMediaRows(a, &guarded, &rows, source, &.{ 2, 1 }, 258881, 1024);
    defer inputs.deinit(a, &cb);
    try std.testing.expectEqualSlices(i64, &.{ 1, 1, 1, 1, 1, 1, 1, 0 }, inputs.mask);
    const actual = try cb.toFloat32(inputs.embeddings, a);
    defer a.free(actual);
    for ([_]usize{ 1, 2, 5 }, [_]f32{ 1, 2, 3 }) |r, value| for (actual[r * 512 ..][0..512]) |v| try std.testing.expectEqual(value, v);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25) * @sqrt(@as(f32, 512)), actual[0], 1e-6);
    ids0[2] = 12;
    try std.testing.expectError(error.InvalidEmbeddingGroup, packResidentMediaRows(a, &guarded, &rows, source, &.{ 2, 1 }, 258881, 1024));
    // Audio can exceed the image sequence capacity without losing valid rows.
    const long_values = try a.alloc(f32, 600 * 512);
    defer a.free(long_values);
    @memset(long_values, 0.5);
    const long_source = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(long_values, &.{ 600, 512 }));
    defer cb.free(long_source);
    var long_ids: [604]i64 = @splat(258881);
    long_ids[0] = 11;
    @memset(long_ids[601..], 12);
    var long_mask: [604]i64 = @splat(1);
    const long_rows = [_]PreparedGroup{.{ .ids = &long_ids, .mask = &long_mask, .embeddings = null }};
    const long_inputs = try packResidentMediaRows(a, &guarded, &long_rows, long_source, &.{600}, 258881, 1024);
    defer long_inputs.deinit(a, &cb);
    try std.testing.expectEqual(@as(usize, 604), long_inputs.sequence);
    try std.testing.expectError(error.EmbeddingInputTooLong, packResidentMediaRows(a, &guarded, &long_rows, long_source, &.{600}, 258881, 512));
}

test "embeddinggemma2 resident audio packing preserves clip isolation and rejects readbacks on Metal" {
    try @import("../ops/media_residency_test.zig").onMetal(populateResidentImageRows, checkResidentAudioRows);
}

test "embeddinggemma2 video streaming output copies retain independent normalized vectors" {
    const a = std.testing.allocator;
    var vector = [_]f32{ 0.6, 0.8 };
    var delivery = LiveStreamDelivery{ .value = .{ .result = .{ .vector = &vector, .input_tokens = 7 }, .source_start_ns = 0, .source_end_ns = 1, .policy = .{ .start_ns = 0, .end_ns = 1, .step_ns = 1, .max_frames = 1 }, .ordinal = 0, .epoch = 0, .dash_timing = null, .hls_timing = null } };
    const copy = try delivery.copy(a);
    defer a.free(copy.result.?.vector);
    try std.testing.expectEqualSlices(f32, &vector, copy.result.?.vector);
    copy.result.?.vector[0] = -1;
    try std.testing.expectEqual(@as(f32, 0.6), vector[0]);
    delivery.acknowledge();
    try std.testing.expect(delivery.acknowledged);
}
test "embeddinggemma2 video streaming cancellation precedes model and Reader access" {
    const FakeReader = struct {
        pub fn streamNext(_: *@This(), _: *anyopaque, _: *const fn (*anyopaque, *@import("antfly_media").http_stream.FragmentDelivery) anyerror!void) !?@import("antfly_media").http_stream.ResumeStats {
            return error.UnexpectedReaderAccess;
        }
        pub fn ended(_: *const @This()) bool {
            return false;
        }
    };
    const Cancel = struct {
        fn check(_: ?*const anyopaque) !void {
            return error.Cancelled;
        }
        fn accept(_: *anyopaque, _: *LiveStreamDelivery) !void {
            return error.UnexpectedOutput;
        }
    };
    var flow: @import("antfly_video").live_coordinator.Coordinator = undefined;
    const control = Control{ .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, LiveStream.init(std.testing.allocator, undefined, undefined, &flow, null, .{}, null, control, null));
    var stream: LiveStream = undefined;
    stream.control = control;
    var reader = FakeReader{};
    try std.testing.expectError(error.Cancelled, stream.process(&reader, &reader, Cancel.accept));
}

test {
    _ = @import("embedding_gemma2_live_stream.zig");
}

test "embeddinggemma2 video cache admission reclaims ordered classes without weakening bounds cancellation or forced denial" {
    const memory = @import("../runtime/tier/memory.zig");
    const tensor = @import("../backends/tensor.zig");
    const Mode = enum { release, second_class, second_no_vision, busy, cancel, cancel_second, forced, permanent, insufficient, unrelated };
    const Probe = struct {
        held: ?memory.AdmissionLease = null,
        mode: Mode,
        calls: usize = 0,
        language_calls: usize = 0,
        cancelled: bool = false,
        fn reclaim(self: *@This(), _: Control) !usize {
            self.calls += 1;
            if (self.mode == .busy or self.mode == .second_no_vision) return 0;
            if (self.mode == .insufficient or self.mode == .second_class or self.mode == .cancel_second) {
                try self.held.?.retain(.{ .backend_weight_bytes = 3 });
                return 1;
            }
            self.held.?.release();
            self.held = null;
            if (self.mode == .cancel) self.cancelled = true;
            return 4;
        }
        fn reclaimLanguage(self: *@This(), _: Control) !usize {
            self.language_calls += 1;
            std.debug.assert(self.calls == 1);
            if (self.mode != .second_class and self.mode != .second_no_vision and self.mode != .cancel_second) return 0;
            const bytes: usize = if (self.mode == .second_no_vision) 4 else 3;
            self.held.?.release();
            self.held = null;
            if (self.mode == .cancel_second) self.cancelled = true;
            return bytes;
        }
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.cancelled) return error.Cancelled;
        }
        fn run(_: *anyopaque, _: []const tensor.Tensor, _: std.mem.Allocator) anyerror![]tensor.Tensor {
            return error.UnexpectedModelRun;
        }
        fn info(_: *anyopaque) []const tensor.TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
        const vtable = Session.VTable{ .run = run, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close };
    };
    for (std.enums.values(Mode)) |mode| {
        var controller = memory.AdmissionController{};
        defer controller.deinit();
        const limits = memory.Limits{ .backend_limit_bytes = if (mode == .unrelated) 16 else 8, .scratch_limit_bytes = if (mode == .unrelated) 8 else 0 };
        var probe = Probe{ .mode = mode };
        probe.held = try controller.tryAcquire(.gpu, limits, .{ .backend_weight_bytes = 4 }, false);
        defer if (probe.held) |*held| held.release();
        var other: ?memory.AdmissionLease = if (mode == .unrelated) try controller.tryAcquire(.gpu, limits, .{ .backend_scratch_bytes = 4 }, false) else null;
        defer if (other) |*held| held.release();
        if (mode == .forced) controller.configureForcedRunDenialsForTesting(1);
        const session = Session{ .ptr = &probe, .vtable = &Probe.vtable, .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = limits, .static_workspace_bytes = 0, .check_live_memory = false } };
        const request = @import("../backends/session.zig").RunRequest{ .batch = 1, .sequence = 1, .workspace_bytes = if (mode == .permanent) 9 else 6 };
        const control = Control{ .ptr = &probe, .check_fn = Probe.check };
        switch (mode) {
            .release, .second_class, .second_no_vision => {
                var permit = try admitReclaimingVisionCache(session, request, control, &probe, Probe.reclaim, Probe.reclaimLanguage);
                try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_weight_bytes);
                try std.testing.expectEqual(@as(usize, 6), controller.snapshot().backend_scratch_bytes);
                permit.deinit();
                try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
            },
            .permanent => try std.testing.expectError(error.ResourceLimitExceeded, admitReclaimingVisionCache(session, request, control, &probe, Probe.reclaim, Probe.reclaimLanguage)),
            .cancel, .cancel_second => {
                try std.testing.expectError(error.Cancelled, admitReclaimingVisionCache(session, request, control, &probe, Probe.reclaim, Probe.reclaimLanguage));
                try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
            },
            else => try std.testing.expectError(error.ResourceTemporarilyUnavailable, admitReclaimingVisionCache(session, request, control, &probe, Probe.reclaim, Probe.reclaimLanguage)),
        }
        const attempted = mode != .forced and mode != .permanent and mode != .unrelated;
        try std.testing.expectEqual(@as(usize, @intFromBool(attempted)), probe.calls);
        const language_attempted = mode == .second_class or mode == .second_no_vision or mode == .busy or mode == .cancel_second or mode == .insufficient;
        try std.testing.expectEqual(@as(usize, @intFromBool(language_attempted)), probe.language_calls);
    }
    // A vision-weight lease cannot relieve host-only, KV or scratch quotas.
    for ([_]memory.AdmissionPressure{ .shared_host, .{ .domain_host = .gpu }, .{ .domain_backend = .cpu }, .{ .domain_combined = .cpu }, .{ .domain_kv = .gpu }, .{ .domain_scratch = .gpu } }) |pressure|
        try std.testing.expect(!visionCacheCanRelieve(pressure));
}

test "embeddinggemma2 video lookahead cancellation precedes model admission and source access" {
    var pool = media_admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
    try std.testing.expectError(error.Timeout, VideoAhead.start(undefined, .{ .sampling = .reference }, .{ .deadline_ns = 0 }, null, &pool, .{}));
    try std.testing.expectEqual(@as(u64, 0), pool.snapshot().host_bytes);
}
