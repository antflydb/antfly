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
const platform = @import("antfly_platform");
const factory = @import("../architectures/session_factory.zig");
const encoder = @import("../architectures/embedding_gemma2.zig");
const projector = @import("../architectures/gemma4_projector.zig");
const Session = @import("../backends/session.zig").Session;
const RunPermit = @import("../backends/session.zig").RunPermit;
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const media_admission = @import("antfly_media").admission;
const scoring = @import("antfly_decisions").scoring;

const Scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler;
fn enterModel(session: Session, lock: ?*std.atomic.Mutex, control: Control, waiter: *Scheduler.Waiter) !void {
    try session.enterExecutionCohort(lock, control, waiter);
}
fn leaveModel(lock: ?*std.atomic.Mutex, waiter: *Scheduler.Waiter) void {
    if (waiter.owner != null) Scheduler.leave(waiter) else if (lock) |gate| gate.unlock();
}

/// One stable compute wrapper, but only ready GPU work owns the model/provider.
/// Suspending retires all request-local caches; outputs crossing stages must be
/// copied into independent buffers. CPU/CUDA retain established lease policy.
const StageLane = struct {
    session: Session,
    gate: ?*std.atomic.Mutex,
    waiter: *Scheduler.Waiter,
    cb: *ComputeBackend,
    control: Control,
    active: bool = true,
    permit: *RunPermit,
    timings: ?*MediaTimings = null,
    idle_backend: usize,
    extra: ?RunPermit = null,
    video_phases: bool = false,
    phase_provider_suspended: bool = false,
    owns_unstaged_provider: bool = false,
    composition_request: ?@import("../backends/session.zig").RunRequest = null,
    hook: @import("../ops/ops.zig").EmbeddingStageControl = undefined,
    fn attach(self: *StageLane) void {
        if (self.cb.kind() != .metal or self.gate == null or platform.env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_DISABLE_STAGE_SCHEDULING")) return;
        self.hook = .{ .ptr = self, .pause_fn = pause, .resume_fn = acquire, .cleanup_fn = cleanup, .workspace_fn = if (self.video_phases) workspace else null, .copy_fn = if (self.timings != null) copied else null };
        self.cb.embedding_stage = &self.hook;
    }
    fn pause(raw: *anyopaque) !void {
        const self: *StageLane = @ptrCast(@alignCast(raw));
        if (!self.active) return;
        const retired_at = if (self.timings != null) mediaNow() else 0;
        try @import("../ops/metal_compute.zig").MetalCompute.embeddingStageLease(self.cb, false);
        if (self.timings) |t| t.stage_retire_ns += mediaNow() - retired_at;
        if (self.extra) |*extra| extra.deinit();
        self.extra = null;
        if (self.permit.lease) |*lease| {
            var idle = lease.amounts;
            idle.backend_scratch_bytes = @min(idle.backend_scratch_bytes, self.idle_backend);
            // Retain is a reduction; if a process owner rejects it, keep the
            // conservative reservation and still hand the model gate onward.
            lease.retain(idle) catch {};
        }
        leaveModel(self.gate, self.waiter);
        self.active = false;
        if (self.timings) |t| t.stage_yields += 1;
    }
    fn enter(self: *StageLane, control: Control, execution: bool) !void {
        if (self.active) return;
        try enterModel(self.session, self.gate, control, self.waiter);
        errdefer leaveModel(self.gate, self.waiter);
        if (execution) try self.reserve(if (self.video_phases) 0 else null);
        errdefer {
            if (self.extra) |*extra| extra.deinit();
            self.extra = null;
        }
        const acquired_at = if (self.timings != null) mediaNow() else 0;
        try @import("../ops/metal_compute.zig").MetalCompute.embeddingStageLease(self.cb, true);
        if (self.timings) |t| t.stage_rebind_ns += mediaNow() - acquired_at;
        self.active = true;
        if (self.timings) |t| t.stage_acquisitions += 1;
    }
    fn reserve(self: *StageLane, bytes: ?usize) !void {
        // Every caller has fenced its previous GPU phase. Releasing an old
        // supplement before acquiring the replacement never borrows stale credit.
        if (self.extra) |*extra| extra.deinit();
        self.extra = null;
        var request = self.composition_request orelse self.permit.request orelse return error.InvalidAdmissionLeaseReduction;
        if (bytes) |work| {
            request.workspace_bytes = try std.math.add(usize, self.idle_backend, work);
            request.backend_input_bytes = 0;
            request.batch = 1;
            request.sequence = 1;
        }
        if (self.session.run_admission) |admission| self.extra = try self.admitPhase(try admission.estimateRequest(request, self.session.outputInfo()), null);
    }
    fn prepareReclaim(self: *StageLane, _: Control) !void {
        if (self.timings) |t| t.phase_reclaim_attempts += 1;
        // Keep the FIFO turn, but expose an idle provider to cache reclamation.
        // Inputs at these boundaries own independent buffers. No active frame
        // or request-local weight binding may survive retirement.
        if (!self.active or (self.gate == null and !self.owns_unstaged_provider)) return;
        const started = if (self.timings != null) mediaNow() else 0;
        try @import("../ops/metal_compute.zig").MetalCompute.embeddingStageLease(self.cb, false);
        self.phase_provider_suspended = true;
        if (self.timings) |t| t.stage_retire_ns += mediaNow() - started;
    }
    fn reclaimMedia(self: *StageLane, control: Control) !usize {
        return reclaimMediaWeights(self.session, control);
    }
    fn reclaimLanguage(self: *StageLane, control: Control) !usize {
        return factory.reclaimEmbeddingGemma2LanguageWeights(self.session, control);
    }
    fn admitPhase(self: *StageLane, target: @import("../runtime/tier/memory.zig").AdmissionAmounts, other: ?*const RunPermit) !RunPermit {
        defer if (self.phase_provider_suspended) {
            const started = if (self.timings != null) mediaNow() else 0;
            // Ownership restoration is cleanup, including after cancellation or
            // denial. It needs no compute supplement and cannot abandon tensors.
            while (true) {
                @import("../ops/metal_compute.zig").MetalCompute.embeddingStageLease(self.cb, true) catch {
                    platform.time.yieldNow();
                    continue;
                };
                break;
            }
            self.phase_provider_suspended = false;
            if (self.timings) |t| t.stage_rebind_ns += mediaNow() - started;
        };
        return admitSupplementReclaimingCaches(self.session, self.permit, other, target, self.control, self, prepareReclaim, reclaimMedia, reclaimLanguage);
    }
    fn copied(raw: *anyopaque, bytes: usize, nanos: u64) void {
        const self: *StageLane = @ptrCast(@alignCast(raw));
        if (self.timings) |t| {
            t.stage_copy_ns += nanos;
            t.stage_copy_bytes += bytes;
        }
    }
    fn workspace(raw: *anyopaque, bytes: ?usize) !void {
        const self: *StageLane = @ptrCast(@alignCast(raw));
        if (!self.active or self.cb.decoderRuntimeHasActiveFrame() or self.cb.decoderRuntimeHasSubmittedFrame()) return error.EmbeddingStageBusy;
        try self.control.check();
        try self.reserve(bytes);
    }
    fn acquire(raw: *anyopaque) !void {
        const self: *StageLane = @ptrCast(@alignCast(raw));
        try self.enter(self.control, true);
    }
    fn cleanup(raw: *anyopaque) void {
        const self: *StageLane = @ptrCast(@alignCast(raw));
        while (!self.active) self.enter(.{}, false) catch {
            platform.time.yieldNow();
        };
    }
    fn deinit(self: *StageLane) void {
        if (self.cb.embedding_stage == null) return;
        cleanup(self);
        self.cb.embedding_stage = null;
        if (self.extra) |*extra| extra.deinit();
        self.extra = null;
    }
    fn credit(self: *StageLane) ?*const RunPermit {
        return if (self.extra) |*extra| extra else null;
    }
    fn retain(self: *StageLane, request: @import("../backends/session.zig").RunRequest) !void {
        if (self.video_phases) {
            // The tower is fenced and only independent inputs remain. Admit
            // the exact backbone phase instead of restoring its tower peak.
            if (self.extra) |*extra| extra.deinit();
            self.extra = null;
            if (self.session.run_admission) |admission| self.extra = try self.admitPhase(try admission.estimateRequest(request, self.session.outputInfo()), null);
        }
        if (self.extra) |*extra| {
            try self.permit.retainForRunWithSupplement(extra, request);
        } else try self.permit.retainForRun(request);
        self.composition_request = null;
    }
};

fn videoPhaseAdmission(backend: @import("../backends/backends.zig").BackendType, lock: ?*std.atomic.Mutex) bool {
    return backend == .metal and lock != null and !platform.env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_DISABLE_STAGE_SCHEDULING");
}
fn queuedVideoRequest(request: @import("../backends/session.zig").RunRequest, retained_backend: usize) @import("../backends/session.zig").RunRequest {
    var queued = request;
    queued.batch = 1;
    queued.sequence = 1;
    queued.backend_input_bytes = 0;
    queued.workspace_bytes = retained_backend;
    queued.workspace_planned = true;
    return queued;
}

fn compositionRequest(request: @import("../backends/session.zig").RunRequest, retained: usize, rows: usize) !@import("../backends/session.zig").RunRequest {
    if (rows == 0 or rows > 4 * 8192) return error.EmbeddingInputTooLong;
    // Retained projections plus text lookup, composed output and one independent
    // handoff copy. No transformer layer or attention score matrix runs here.
    return queuedVideoRequest(request, try std.math.add(usize, retained, try std.math.mul(usize, rows, 3 * 512 * @sizeOf(f32))));
}
fn videoBackboneBounds(metal: bool, tokens: usize, legacy: usize) !ExecutionBounds {
    if (!metal) return executionBounds(true, tokens, 0, 0, 0, legacy);
    if (tokens == 0 or tokens > 8192) return error.EmbeddingInputTooLong;
    // Metal attention requires its bounded device operation; unsupported
    // capabilities report OperationUnavailable, without dense/host fallback.
    const activations = try std.math.mul(usize, tokens, 192 * 1024);
    return .{ .sequence = tokens, .host = @min(@as(usize, 512 * 1024 * 1024), 128 * 1024 * 1024 + activations), .workspace = 256 * 1024 * 1024 + activations };
}

/// A standalone image helper also owns a fenced Gemma 2 provider, without a
/// stage scheduler hook. Its temporary admission context restores that same
/// provider before returning and never takes or releases the caller's gate.
fn admitFencedPhase(cb: *const ComputeBackend, base: *RunPermit, other: ?*const RunPermit, target: @import("../runtime/tier/memory.zig").AdmissionAmounts, control: Control, timings: ?*MediaTimings) !RunPermit {
    var owner = StageLane{ .session = base.session, .gate = null, .waiter = undefined, .cb = @constCast(cb), .control = control, .permit = base, .idle_backend = 0, .owns_unstaged_provider = cb.kind() == .metal, .timings = timings };
    return owner.admitPhase(target, other);
}

/// Called at a fenced boundary. A relevant denial can retire provider bindings
/// once, reclaim optional classes in order, and retry the exact supplemental
/// deficit. Live retained leases remain independently owned throughout.
fn admitSupplementReclaimingCaches(session: Session, base: *const RunPermit, other: ?*const RunPermit, target: @import("../runtime/tier/memory.zig").AdmissionAmounts, control: Control, context: anytype, comptime prepare: anytype, comptime reclaim_media: anytype, comptime reclaim_language: anytype) !RunPermit {
    try control.check();
    var pressure: ?@import("../runtime/tier/memory.zig").AdmissionPressure = null;
    return session.admitSupplementPairWithPressure(base, other, target, &pressure) catch |err| blk: {
        var denied: anyerror = err;
        var prepared = false;
        inline for (.{ reclaim_media, reclaim_language }) |reclaim| {
            const reason = pressure orelse return denied;
            if (!visionCacheCanRelieve(reason)) return denied;
            try control.check();
            if (!prepared) {
                try prepare(context, control);
                prepared = true;
                try control.check();
                // Retirement itself can return admitted native bindings. Retry
                // before evicting model caches, and recheck the denial class.
                pressure = null;
                if (session.admitSupplementPairWithPressure(base, other, target, &pressure)) |permit| break :blk permit else |failure| denied = failure;
                if (!visionCacheCanRelieve(pressure orelse return denied)) return denied;
            }
            const bytes = try reclaim(context, control);
            try control.check();
            if (bytes != 0) {
                pressure = null;
                if (session.admitSupplementPairWithPressure(base, other, target, &pressure)) |permit| break :blk permit else |failure| denied = failure;
            }
        }
        return denied;
    };
}

fn visionCacheCanRelieve(pressure: @import("../runtime/tier/memory.zig").AdmissionPressure) bool {
    return switch (pressure) {
        .shared_unified, .external_budget, .live_host => true,
        .domain_backend, .domain_combined => |backend| backend == .gpu,
        else => false,
    };
}

fn reclaimMediaWeights(session: Session, control: Control) !usize {
    const vision = try factory.reclaimEmbeddingGemma2VisionWeights(session, control);
    return vision + try factory.reclaimEmbeddingGemma2AudioWeights(session, control);
}

fn admitWithVisionCacheReclaim(session: Session, request: @import("../backends/session.zig").RunRequest, control: Control) !@import("../backends/session.zig").RunPermit {
    return admitReclaimingVisionCache(session, request, control, session, reclaimMediaWeights, factory.reclaimEmbeddingGemma2LanguageWeights);
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
            const native_pressure = session.backend() == .native and switch (reason) {
                .shared_host => true,
                .domain_host, .domain_combined => |backend| backend == .cpu,
                else => false,
            };
            if (!visionCacheCanRelieve(reason) and !native_pressure) return denied;
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
    phase_reclaim_attempts: usize = 0,
    stage_retire_ns: u64 = 0,
    stage_rebind_ns: u64 = 0,
    stage_copy_ns: u64 = 0,
    stage_copy_bytes: usize = 0,
    stage_yields: usize = 0,
    stage_acquisitions: usize = 0,
    prepared_vision_attempts: usize = 0,
    vision_plan_fallbacks: usize = 0,
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
    /// Expanded input limit, including prompts, wrappers, and media tokens.
    max_tokens: usize = encoder.max_tokens,
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
    return options.resident_images and (cb.kind() == .metal or cb.kind() == .cuda) and cb.supportsCopyRows2D() and cb.vtable.allocUninitF32Shape != null and cb.vtable.sliceRows2D != null;
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
    return err == error.ResourceTemporarilyUnavailable or err == error.QueueFull or err == error.OutOfMemory or err == error.SharedAdmissionExceeded or err == error.MemoryBudgetExceeded or err == error.InferenceAdmissionQueueFull or err == error.ConcurrencyUnavailable;
}
pub fn embedCached(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, group: Group, options: Options, lock: ?*std.atomic.Mutex, control: ?Control, cache: ?*FrameCache) !Result {
    if (cache) |owner| {
        if (owner.session.ptr != session.ptr or owner.session.vtable != session.vtable) return error.InvalidVideoCacheSession;
        if (options.video_ahead == null) owner.control = control orelse .{};
    }
    const video_group = for (group.content) |part| {
        if (part == .video or part == .sampled_video) break true;
    } else false;
    if (session.backend() == .metal and !video_group and options.video_ahead == null and options.prepare_ahead and options.resident_images and options.resident_audio) {
        var errors: [1]?anyerror = .{null};
        const result = try embedGroupCohort(out, session, tokenizer, &.{group}, options, lock, control orelse .{}, &errors);
        defer out.free(result.vectors);
        defer out.free(result.input_tokens);
        if (errors[0]) |err| {
            out.free(result.vectors[0]);
            return err;
        }
        return .{ .vector = result.vectors[0], .input_tokens = result.input_tokens[0] };
    }
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = workspaceBackingAllocator(session.backend()), .limit = 512 * 1024 * 1024 };
    defer std.debug.assert(bounded.live == 0);
    const result = embedScoped(&bounded, session, tokenizer, group, options, lock, control, cache) catch |err| blk: {
        if (err == error.SpeculativeVideoAdmissionDenied) {
            options.video_ahead.?.abandon();
            var serial = options;
            serial.video_ahead = null;
            break :blk try embedScoped(&bounded, session, tokenizer, group, serial, lock, control, cache);
        }
        return if ((err == error.OutOfMemory and bounded.denied) or err == error.SharedAdmissionExceeded) error.MemoryBudgetExceeded else err;
    };
    defer bounded.allocator().free(result.vector);
    return .{ .vector = try out.dupe(f32, result.vector), .input_tokens = result.input_tokens };
}

/// Indexed readers need packet/picture metadata and one bounded probe payload.
/// Unknown container inputs retain the full conservative parsing ceiling.
fn preflightHostBytes(group: Group) !usize {
    var count: usize = 0;
    for (group.content) |part| switch (part) {
        .video => return 512 * 1024 * 1024,
        .sampled_video => |movie| {
            // An unbound AVC reader may reconstruct pictures to establish its
            // logical index (fields/partitions). Only a bound selection proves
            // that inspection needs metadata rather than decoder references.
            if (movie.reader != null and movie.selection == null) return 512 * 1024 * 1024;
            const n = if (movie.reader) |reader| reader.packets.len else if (movie.webm) |webm| if (webm.timeline != null and movie.sampling != .reference) @as(usize, 32) else webm.index.pictures.len else return 512 * 1024 * 1024;
            count = try std.math.add(usize, count, n);
        },
        else => {},
    };
    return @min(@as(usize, 512 * 1024 * 1024), try std.math.add(usize, 64 * 1024 * 1024, try std.math.mul(usize, count, 512)));
}

const ExecutionBounds = struct { sequence: usize, host: usize, workspace: usize };
/// Preserve the conservative path for uninspected media. Known video/text
/// shapes cover retained PLE/frame activations, dense attention, projections,
/// and a fixed runtime/weight-conversion allowance. Allocations enforce host.
fn executionBounds(known: bool, tokens: usize, preparation: usize, vision: usize, device: usize, legacy: usize) !ExecutionBounds {
    if (!known) return .{ .sequence = 8192, .host = 512 * 1024 * 1024, .workspace = legacy };
    const sequence = @min(@as(usize, 8192), @max(@as(usize, 1), tokens));
    const activations = try std.math.mul(usize, sequence, 192 * 1024);
    const attention = try std.math.mul(usize, try std.math.mul(usize, sequence, sequence), 48);
    const language = try std.math.add(usize, 64 * 1024 * 1024, try std.math.add(usize, activations, attention));
    const host = @min(@as(usize, 512 * 1024 * 1024), try std.math.add(usize, 64 * 1024 * 1024, try std.math.add(usize, language, try std.math.add(usize, preparation, vision))));
    const work = try std.math.add(usize, 256 * 1024 * 1024, try std.math.add(usize, @max(language, vision), device));
    return .{ .sequence = sequence, .host = host, .workspace = @min(legacy, work) };
}

const GroupInspection = struct { bounds: ExecutionBounds, input_bytes: usize, has_video: bool, device_bytes: usize, commands: u64 };
fn inspectGroup(bounded: *@import("../runtime/bounded_allocator.zig").BoundedAllocator, session: Session, cfg: encoder.Config, group: Group, options: Options, effective: Control) !GroupInspection {
    const a = bounded.allocator();
    if (session.backend() == .cuda) {
        for (group.content) |part| switch (part) {
            .image => if (!cfg.vision) return error.NoVisionSession,
            .audio => if (!cfg.audio) return error.NoAudioSession,
            else => {},
        };
        const request = try cudaGroupRequest(group, options);
        return .{ .bounds = .{ .sequence = request.sequence, .host = request.host_preprocess_bytes, .workspace = request.workspace_bytes }, .input_bytes = request.input_bytes, .has_video = false, .device_bytes = 0, .commands = 0 };
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
    try effective.check();
    // Inspect under a shared host-only permit before reserving model/device
    // execution. Temporary metadata is freed before that permit is returned.
    var device_bytes: usize = 0;
    var command_slots: u64 = 0;
    var shape_known = true;
    var sequence_bound: usize = 128 + (if (group.title) |title| title.len else 0);
    var preparation_bound: usize = 0;
    var vision_bound: usize = 0;
    for (group.content) |part| switch (part) {
        .text => |bytes| sequence_bound = std.math.add(usize, sequence_bound, bytes.len) catch return error.ResourceLimitExceeded,
        .video, .sampled_video => {},
        else => shape_known = false,
    };
    if (options.video_ahead) |ahead| {
        device_bytes = if (session.backend() == .metal) try ahead.plan.deviceBytes() else 0;
        command_slots = ahead.plan.commands;
        sequence_bound = try std.math.add(usize, sequence_bound, ahead.plan.sequence_tokens);
        preparation_bound = try ahead.plan.hostBytes();
        vision_bound = ahead.plan.vision_workspace_bytes;
    } else if (has_video) {
        const preflight_bytes = try preflightHostBytes(group);
        bounded.limit = preflight_bytes;
        var preflight = session.admitHostPreprocess(preflight_bytes) catch |err| return if (options.video_ahead != null and videoSpeculationPressure(err)) error.SpeculativeVideoAdmissionDenied else err;
        defer preflight.deinit();
        var host_budget = media_admission.Pool{ .limits = .{ .host_bytes = preflight_bytes } };
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
                sequence_bound = try std.math.add(usize, sequence_bound, plan.sequence_tokens);
                preparation_bound = @max(preparation_bound, try plan.hostBytes());
                vision_bound = @max(vision_bound, plan.vision_workspace_bytes);
            },
            else => {},
        };
    }
    const legacy_workspace = if (session.backend() == .metal)
        try std.math.add(usize, (if (has_video) @as(usize, 1536) else 768) * 1024 * 1024, device_bytes)
    else
        512 * 1024 * 1024;
    const bounds = try executionBounds(shape_known, sequence_bound, preparation_bound, vision_bound, device_bytes, legacy_workspace);
    return .{ .bounds = bounds, .input_bytes = input_bytes, .has_video = has_video, .device_bytes = device_bytes, .commands = command_slots };
}

fn embedScoped(bounded: *@import("../runtime/bounded_allocator.zig").BoundedAllocator, session: Session, tokenizer: Tokenizer, group: Group, options: Options, lock: ?*std.atomic.Mutex, control: ?Control, cache: ?*FrameCache) !Result {
    bounded.limit = 512 * 1024 * 1024;
    const a = bounded.allocator();
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.UnsupportedEmbeddingGemma2Backend;
    try validateGroupContent(group, options, session.backend() == .cuda);
    if (session.backend() == .cuda) {
        try validateCudaGroup(group, options);
        if (group.content.len == 1 and group.content[0] == .video) return embedVideo(a, session, tokenizer, group.content[0].video, options, lock, control);
        if (!singleImageGroup(group) or !options.resident_images) return embedCudaGroup(a, session, tokenizer, group, options, lock, control);
    }
    if (options.video_ahead) |ahead| {
        if (group.content.len != 1 or group.content[0] != .sampled_video or !ahead.matches(group.content[0].sampled_video, options.video_decoder, session, cache) or ahead.released) return error.InvalidVideoReaderSelection;
    }
    const effective = control orelse Control{};
    const inspection = try inspectGroup(bounded, session, cfg, group, options, effective);
    const bounds = inspection.bounds;
    const input_bytes = inspection.input_bytes;
    const has_video = inspection.has_video;
    const device_bytes = inspection.device_bytes;
    const command_slots = inspection.commands;
    // The allocator enforces the same host bound advertised to admission.
    // A failed speculative call restores the conservative ceiling on retry.
    if (bounded.live > bounds.host) return error.MemoryBudgetExceeded;
    bounded.limit = bounds.host;
    const video_phases = has_video and videoPhaseAdmission(session.backend(), lock);
    const idle_backend = device_bytes + bounds.sequence * 512 * 4 + 64 * 1024 * 1024;
    const execution_request: @import("../backends/session.zig").RunRequest = .{ .batch = 1, .sequence = bounds.sequence, .input_bytes = input_bytes, .host_preprocess_bytes = bounds.host, .workspace_bytes = bounds.workspace, .output_bytes = options.dimensions * @sizeOf(f32) };
    var permit = admitWithVisionCacheReclaim(session, if (video_phases) queuedVideoRequest(execution_request, idle_backend) else execution_request, effective) catch |err| return if (options.video_ahead != null and videoSpeculationPressure(err)) error.SpeculativeVideoAdmissionDenied else err;
    defer permit.deinit();
    // Video parts execute sequentially; their transient preparation shares use
    // a maximum, while projected tokens stay in the model workspace.
    var video_budget = media_admission.Pool{ .limits = .{ .host_bytes = bounds.host, .device_bytes = if (session.backend() == .metal) device_bytes else 0, .commands = if (session.backend() == .metal) command_slots else 0 } };
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
    var execution_waiter = Scheduler.Waiter{};
    try enterModel(session, lock, effective, &execution_waiter);
    defer leaveModel(lock, &execution_waiter);
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    var lane = StageLane{ .session = session, .gate = lock, .waiter = &execution_waiter, .permit = &permit, .timings = options.timings, .idle_backend = idle_backend, .cb = &managed.backend, .control = effective, .video_phases = video_phases, .composition_request = if (video_phases) try compositionRequest(execution_request, idle_backend, bounds.sequence) else null };
    lane.attach();
    defer lane.deinit();
    if (video_phases) try lane.reserve(null);
    const cb = &managed.backend;
    if (cb.kind() == .cuda and singleImageGroup(group) and residentImagesEnabled(cb, options)) {
        const before = MediaTransferSnapshot.init(cb);
        defer observeResidentTransfers(cb, before, options);
        const image_scope = @import("antfly_image").work_control.Scope.enter(effective.imageWorkControl());
        defer image_scope.deinit();
        const preprocess_started = mediaNow();
        var image = switch (group.content[0]) {
            .image => |bytes| try projector.prepareEmbeddingGemma2ImageForBackend(a, bytes, session.backend()),
            .raster => |raster| try projector.prepareEmbeddingGemma2RasterForBackend(a, raster, session.backend()),
            else => unreachable,
        };
        defer image.deinit();
        if (options.timings) |t| t.preprocess_ns += mediaNow() - preprocess_started;
        const vision_started = mediaNow();
        var projected = try projector.encodePreparedEmbeddingGemma2ImagesResident(cb, a, &.{image}, .{});
        defer projected.deinit();
        if (options.timings) |t| {
            t.vision_ns += mediaNow() - vision_started;
            t.resident_cohorts += 1;
        }
        var counts: [1]usize = undefined;
        const vectors = try runResidentMediaBatchObserved(a, cb, cfg, tokenizer, &projected, options, false, &permit, &counts);
        defer a.free(vectors);
        errdefer a.free(vectors[0]);
        try effective.check();
        return .{ .vector = vectors[0], .input_tokens = counts[0] };
    }
    var prepared = prepareGroupWithBudget(a, cb, tokenizer, group, options, effective, &video_budget, prefetched, cache) catch |err| return if (options.video_ahead != null and videoSpeculationPressure(err)) error.SpeculativeVideoAdmissionDenied else err;
    defer prepared.deinit(a);
    defer if (cb.embedding_stage) |stage| stage.cleanup();
    if (cb.embedding_stage) |stage| {
        if (prepared.tensor) |tensor| {
            prepared.tensor = try @import("../ops/metal_compute.zig").MetalCompute.escapeEmbeddingStage(cb, tensor);
            cb.free(tensor);
        }
        try stage.pause();
        try stage.acquire();
    }

    const count = prepared.ids.len;
    if (count > bounds.sequence) return error.MemoryBudgetExceeded;
    // The rendered sequence is now known for every media type. Projection has
    // drained preparation; keep its live host owners and projected input while
    // returning idle vision/decode headroom before admitting the next window.
    const phase = try videoBackboneBounds(session.backend() == .metal, count, bounds.workspace);
    const phase_host = @min(bounds.host, @max(bounded.live, phase.host));
    try lane.retain(.{ .batch = 1, .sequence = count, .input_bytes = input_bytes, .backend_input_bytes = if (session.backend() == .metal) count * 512 * 4 else null, .preprocessing_complete = session.backend() == .metal, .workspace_planned = session.backend() == .metal, .host_preprocess_bytes = phase_host, .workspace_bytes = phase.workspace, .output_bytes = options.dimensions * @sizeOf(f32) });
    bounded.limit = phase_host;
    if (options.video_ahead) |ahead| ahead.release();
    if (options.video_prepared) |hook| try hook.ready(hook.context);
    const mask = prepared.mask;
    if (prepared.embeddings == null and prepared.tensor == null) {
        const vector = try preparedText(cb, a, cfg, prepared.ids, mask, options.dimensions);
        errdefer a.free(vector);
        try effective.check();
        return .{ .vector = vector, .input_tokens = count };
    }
    const input = prepared.tensor orelse try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(prepared.embeddings.?, &.{ @intCast(count), 512 }));
    defer if (prepared.tensor == null) cb.free(input);
    const output = try encoder.forwardPooledEmbeddingsCT(cb, a, cfg, input, mask, 1, count, options.dimensions);
    defer cb.free(output);
    const vector = try readNormalizedVector(cb, a, output, options.dimensions);
    errdefer a.free(vector);
    try effective.check();
    return .{ .vector = vector, .input_tokens = count };
}

/// Independent windows share backbone execution, never attention or pooling.
/// Decode remains ordered so retained codec continuations can advance safely.
/// No queue fill delay: callers pass only already available windows (max four).
pub const VideoBatchResult = struct { vectors: [][]f32, input_tokens: []usize, execution: BatchExecution.Execution };
pub const VideoBatchItem = struct { movie: SampledVideo, plan: ?*const @import("embedding_gemma2_video.zig").Plan = null, ahead: ?*VideoAhead = null };
pub fn embedVideos(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, items: []const VideoBatchItem, options: Options, lock: ?*std.atomic.Mutex, control: Control, cache: ?*FrameCache) !VideoBatchResult {
    if (items.len == 0 or items.len > 4 or options.video_ahead != null) return error.InvalidInputShape;
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.UnsupportedEmbeddingGemma2Backend;
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = workspaceBackingAllocator(session.backend()), .limit = 64 * 1024 * 1024 };
    const a = bounded.allocator();
    defer std.debug.assert(bounded.live == 0);
    var preflight = try session.admitHostPreprocess(64 * 1024 * 1024);
    var preflight_live = true;
    defer if (preflight_live) preflight.deinit();
    const running_ahead = for (items) |item| {
        if (item.ahead != null) break true;
    } else false;
    if (running_ahead) for (items) |item| {
        if (item.ahead == null and item.plan == null) return error.BatchIncompatible;
    };
    var longest: usize = 0;
    var total: usize = 0;
    var raw_longest: usize = 0;
    var device: usize = 0;
    var commands: u64 = 0;
    var host: usize = 64 * 1024 * 1024;
    for (items) |item| {
        const movie = item.movie;
        try control.check();
        if (item.ahead) |ahead| if (ahead.released or !ahead.matches(movie, options.video_decoder, session, cache)) return error.InvalidVideoReaderSelection;
        const plan = if (item.ahead) |ahead| ahead.plan else if (item.plan) |plan| plan.* else try @import("embedding_gemma2_video.zig").inspectConfiguredPlan(a, movie.bytes, movie.reader, control, .{ .sampling = movie.sampling, .selection = movie.selection, .webm = movie.webm, .decoder = options.video_decoder }, session.backend() == .metal);
        const sequence = try std.math.add(usize, plan.sequence_tokens, 128);
        longest = @max(longest, sequence);
        raw_longest = @max(raw_longest, plan.sequence_tokens);
        total = try std.math.add(usize, total, plan.sequence_tokens);
        device = @max(device, try plan.deviceBytes());
        commands = @max(commands, plan.commands);
        host = @max(host, try plan.hostBytes());
    }
    // Avoid padding-heavy cohorts. The runner falls back to independent calls.
    const compact_policy = encoder.compactRowsEnabled(session.backend() == .metal);
    if (compact_policy) {
        if (total + items.len * 128 > 8192) return error.BatchIncompatible;
    } else {
        if (longest > 2048 or raw_longest * items.len * 4 > total * 5) return error.BatchIncompatible;
        if (session.backend() == .metal and encoder.metalBatchCapacity(raw_longest, items.len) < items.len) return error.BatchIncompatible;
    }
    preflight.deinit();
    preflight_live = false;
    bounded.limit = 512 * 1024 * 1024;
    if (host > bounded.limit) return error.MemoryBudgetExceeded;
    const workspace = try std.math.add(usize, (if (session.backend() == .metal) @as(usize, 1536) else 512) * 1024 * 1024, if (session.backend() == .metal) device else 0);
    const video_phases = videoPhaseAdmission(session.backend(), lock);
    const idle_backend = device + total * 512 * 4 + 64 * 1024 * 1024;
    const execution_request: @import("../backends/session.zig").RunRequest = .{ .batch = if (compact_policy) 1 else items.len, .sequence = if (compact_policy) total + items.len * 128 else longest, .host_preprocess_bytes = bounded.limit, .workspace_bytes = workspace, .output_bytes = items.len * options.dimensions * 4 };
    var permit = try admitWithVisionCacheReclaim(session, if (video_phases) queuedVideoRequest(execution_request, idle_backend) else execution_request, control);
    defer permit.deinit();
    var pool = media_admission.Pool{ .limits = .{ .host_bytes = host, .device_bytes = if (session.backend() == .metal) device else 0, .commands = if (session.backend() == .metal) commands else 0 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media_admission.Resources{}));
    var execution_waiter = Scheduler.Waiter{};
    try enterModel(session, lock, control, &execution_waiter);
    defer leaveModel(lock, &execution_waiter);
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    var lane = StageLane{ .session = session, .gate = lock, .waiter = &execution_waiter, .permit = &permit, .timings = options.timings, .idle_backend = idle_backend, .cb = &managed.backend, .control = control, .video_phases = video_phases, .composition_request = if (video_phases) try compositionRequest(execution_request, idle_backend, total + items.len * 128) else null };
    lane.attach();
    defer lane.deinit();
    if (video_phases) try lane.reserve(null);
    const cb = &managed.backend;
    var rows: [4]PreparedGroup = undefined;
    var initialized: usize = 0;
    defer for (rows[0..initialized]) |row| row.deinit(a);
    defer if (cb.embedding_stage) |stage| stage.cleanup();
    for (items) |item| {
        const movie = item.movie;
        try validateGroup(.{ .content = &.{.{ .sampled_video = movie }} }, options);
        rows[initialized] = try prepareGroupWithBudget(a, cb, tokenizer, .{ .content = &.{.{ .sampled_video = movie }} }, options, control, &pool, if (item.ahead) |ahead| ahead.producer else null, cache);
        if (cb.embedding_stage != null) if (rows[initialized].tensor) |tensor| {
            rows[initialized].tensor = @import("../ops/metal_compute.zig").MetalCompute.escapeEmbeddingStage(cb, tensor) catch |err| {
                rows[initialized].deinit(a);
                return err;
            };
            cb.free(tensor);
        };
        if (item.ahead) |ahead| ahead.release();
        initialized += 1;
        if (cb.embedding_stage) |stage| {
            try stage.pause();
            try stage.acquire();
        }
    }
    for (rows[0..initialized]) |row| if (row.ids.len > longest) return error.MemoryBudgetExceeded;
    const batch_inputs = try packVideoRows(a, cb, rows[0..initialized]);
    defer batch_inputs.deinit(a, cb);
    // Use the established activation/attention envelope over all padded rows.
    // Treating rows as one sequence deliberately overestimates cross-row
    // attention; it can never under-admit a batch with independent masks.
    var encoder_rows = items.len * batch_inputs.sequence;
    if (batch_inputs.compact_input) {
        encoder_rows = 0;
        for (batch_inputs.mask) |value| {
            encoder_rows += @intFromBool(value != 0);
        }
    }
    const phase = try videoBackboneBounds(session.backend() == .metal, encoder_rows, workspace);
    const phase_host = @min(bounded.limit, @max(bounded.live, phase.host));
    try lane.retain(.{ .batch = if (batch_inputs.compact_input) 1 else items.len, .sequence = if (batch_inputs.compact_input) encoder_rows else batch_inputs.sequence, .backend_input_bytes = if (session.backend() == .metal) encoder_rows * 512 * 4 else null, .preprocessing_complete = session.backend() == .metal, .workspace_planned = session.backend() == .metal, .host_preprocess_bytes = phase_host, .workspace_bytes = phase.workspace, .output_bytes = items.len * options.dimensions * 4 });
    bounded.limit = phase_host;
    if (options.video_prepared) |hook| try hook.ready(hook.context);
    const output = try encoder.forwardPackedPooledEmbeddingsCT(cb, a, cfg, batch_inputs.embeddings, batch_inputs.mask, items.len, batch_inputs.sequence, options.dimensions, batch_inputs.compact_input);
    defer cb.free(output);
    const vectors = try readPooledBatch(a, cb, output, items.len, options.dimensions);
    defer {
        for (vectors) |vector| a.free(vector);
        a.free(vectors);
    }
    const result_vectors = try out.alloc([]f32, items.len);
    errdefer out.free(result_vectors);
    const counts = try out.alloc(usize, items.len);
    errdefer out.free(counts);
    var copied: usize = 0;
    errdefer for (result_vectors[0..copied]) |vector| out.free(vector);
    for (result_vectors, counts, vectors, rows[0..initialized]) |*dst, *count, vector, row| {
        try control.check();
        dst.* = try out.dupe(f32, vector);
        copied += 1;
        count.* = row.ids.len;
    }
    return .{ .vectors = result_vectors, .input_tokens = counts, .execution = if (items.len > 1) .native_batch else .serial };
}

pub fn preparedCohortCount(lengths: []const usize, maximum: usize) usize {
    return preparedCohortCountForBackend(lengths, maximum, false);
}
pub fn preparedCohortCountForBackend(lengths: []const usize, maximum: usize, compact: bool) usize {
    var count: usize = 0;
    var tokens: usize = 0;
    var longest: usize = 0;
    for (lengths[0..@min(lengths.len, @min(maximum, 4))]) |length| {
        if (length == 0 or length > 8192) break;
        const next = @max(longest, length);
        if (if (compact) tokens + length > 8192 else next * (count + 1) > 8192 or (count > 0 and next * (count + 1) * 4 > (tokens + length) * 5)) break;
        longest = next;
        tokens += length;
        count += 1;
    }
    return count;
}

/// Cross-request prepared stages share one backbone call after length bucketing.
/// Decode/tower admission is independent of padded encoder admission. Nothing
/// owned by a submitter escapes this synchronous cohort; slots retain identity.
pub fn embedGroupCohort(out: std.mem.Allocator, session: Session, tokenizer: Tokenizer, groups: []const Group, options: Options, lock: ?*std.atomic.Mutex, control: Control, errors: []?anyerror) !AudioBatchResult {
    if (groups.len == 0 or groups.len > 4 or errors.len != groups.len or options.video_ahead != null) return error.BatchIncompatible;
    if (session.backend() == .cuda) {
        const policy = factory.getEmbeddingGemma2CudaPolicy(session) orelse return error.BatchIncompatible;
        if (!policy.media_batch) return error.BatchIncompatible;
    }
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.UnsupportedEmbeddingGemma2Backend;
    var bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator{ .backing = workspaceBackingAllocator(session.backend()), .limit = 512 * 1024 * 1024 };
    const a = bounded.allocator();
    defer std.debug.assert(bounded.live == 0);
    const vectors = try out.alloc([]f32, groups.len);
    @memset(vectors, &.{});
    errdefer {
        for (vectors) |vector| out.free(vector);
        out.free(vectors);
    }
    const counts = try out.alloc(usize, groups.len);
    @memset(counts, 0);
    errdefer out.free(counts);
    @memset(errors, null);
    var indexes: [4]usize = undefined;
    var inspected: usize = 0;
    var host: usize = 64 * 1024 * 1024;
    var workspace: usize = 0;
    var device: usize = 0;
    var commands: u64 = 0;
    var inputs: usize = 0;
    var sequence: usize = 1;
    var has_video = false;
    for (groups, 0..) |group, index| {
        try control.check();
        validateGroup(group, options) catch |err| {
            errors[index] = err;
            continue;
        };
        const view = inspectGroup(&bounded, session, cfg, group, options, control) catch |err| {
            if (@import("audio_preparation.zig").abort(err)) return err;
            errors[index] = err;
            continue;
        };
        indexes[inspected] = index;
        inspected += 1;
        host = @max(host, view.bounds.host);
        workspace = @max(workspace, view.bounds.workspace);
        device = @max(device, view.device_bytes);
        commands = @max(commands, view.commands);
        inputs = try std.math.add(usize, inputs, view.input_bytes);
        sequence = @max(sequence, view.bounds.sequence);
        has_video = has_video or view.has_video;
    }
    if (inspected == 0) return .{ .vectors = vectors, .input_tokens = counts, .execution = .serial };
    var execution_waiter = Scheduler.Waiter{};
    try enterModel(session, lock, control, &execution_waiter);
    defer leaveModel(lock, &execution_waiter);
    // Completed projections from earlier stages remain admitted while the
    // next tower executes. At most four 8192x512 F32 inputs can be retained.
    // Composition may retain projected rows beside composed backbone rows;
    // the fixed margin also covers one independent handoff copy.
    const retained = inspected * 8192 * 512 * @sizeOf(f32);
    workspace = try std.math.add(usize, workspace, retained);
    bounded.limit = host;
    const video_phases = has_video and videoPhaseAdmission(session.backend(), lock);
    const idle_backend = device + 2 * retained + 64 * 1024 * 1024;
    const execution_request: @import("../backends/session.zig").RunRequest = .{ .batch = 1, .sequence = if (boundedVision(session.backend())) 1 else sequence, .input_bytes = inputs, .backend_input_bytes = if (boundedVision(session.backend())) 0 else null, .host_preprocess_bytes = host, .workspace_bytes = if (boundedVision(session.backend()) and !has_video) 2 * retained + 64 * 1024 * 1024 else workspace, .output_bytes = inspected * options.dimensions * 4 };
    var permit = try admitWithVisionCacheReclaim(session, if (video_phases) queuedVideoRequest(execution_request, idle_backend) else execution_request, control);
    defer permit.deinit();
    var pool = media_admission.Pool{ .limits = .{ .host_bytes = host, .device_bytes = if (session.backend() == .metal) device else 0, .commands = if (session.backend() == .metal) commands else 0 } };
    defer std.debug.assert(std.meta.eql(pool.snapshot(), media_admission.Resources{}));
    var guard = if (has_video) try control.enterUninterruptible(session.interruption()) else @import("../execution_control.zig").UninterruptibleGuard{};
    defer guard.deinit();
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    var lane = StageLane{ .session = session, .gate = lock, .waiter = &execution_waiter, .permit = &permit, .timings = options.timings, .idle_backend = if (video_phases) idle_backend else device + retained, .cb = &managed.backend, .control = control, .video_phases = video_phases, .composition_request = if (video_phases) try compositionRequest(execution_request, idle_backend, inspected * @min(options.max_tokens, 8192)) else null };
    lane.attach();
    defer lane.deinit();
    const cb = &managed.backend;
    var cache = FrameCache{ .allocator = workspaceBackingAllocator(session.backend()), .session = session, .control = control };
    defer cache.deinit();
    var scheduler = CohortReady{ .a = a, .out = out, .session = session, .cb = cb, .lane = &lane, .tokenizer = tokenizer, .groups = groups, .indexes = indexes[0..inspected], .options = options, .control = control, .errors = errors, .permit = &permit, .pool = &pool, .cache = &cache, .vectors = vectors, .counts = counts, .inputs = inputs, .host = host, .retained = retained };
    var media_parts = try projectCohortParts(a, session, cb, groups, indexes[0..inspected], options, control, &permit, errors, &scheduler);
    defer media_parts.deinit(a, cb);
    return .{ .vectors = vectors, .input_tokens = counts, .execution = scheduler.observation.execution(scheduler.observation.native_items + scheduler.observation.serial_items) };
}

fn cohortGroupReady(group: Group, parts: []const ?SoftTokens, options: Options) bool {
    for (group.content, 0..) |part, p| {
        const eligible = ((part == .image or part == .raster) and options.resident_images) or ((part == .audio or part == .pcm) and options.resident_audio);
        if (eligible and parts[p] == null) return false;
    }
    return true;
}

test "embeddinggemma2 cohort readiness respects every required modality and execution policy" {
    const parts = [_]Part{ .{ .text = "query" }, .{ .image = "image" }, .{ .audio = "audio" } };
    const group = Group{ .content = &parts };
    var projected: [3]?SoftTokens = @splat(null);
    try std.testing.expect(!cohortGroupReady(group, &projected, .{}));
    projected[1] = .{ .id = 258880, .embeddings = &.{}, .tokens = 1 };
    try std.testing.expect(!cohortGroupReady(group, &projected, .{}));
    projected[2] = .{ .id = 258881, .embeddings = &.{}, .tokens = 1 };
    try std.testing.expect(cohortGroupReady(group, &projected, .{}));
    projected = @splat(null);
    try std.testing.expect(cohortGroupReady(group, &projected, .{ .resident_images = false, .resident_audio = false }));
    try std.testing.expect(cohortGroupReady(.{ .content = parts[0..1] }, projected[0..1], .{}));
}

/// Execute ready groups while the next CPU preparation wave is running. GPU
/// work stays on the existing single leased backend; this is pipeline overlap,
/// not concurrent encoders. Result ownership remains with original indexes.
const CohortReady = struct {
    a: std.mem.Allocator,
    out: std.mem.Allocator,
    session: Session,
    cb: *const ComputeBackend,
    lane: *StageLane,
    tokenizer: Tokenizer,
    groups: []const Group,
    indexes: []const usize,
    options: Options,
    control: Control,
    errors: []?anyerror,
    permit: *RunPermit,
    pool: *media_admission.Pool,
    cache: *FrameCache,
    vectors: [][]f32,
    counts: []usize,
    inputs: usize,
    host: usize,
    retained: usize,
    deferred: [4]bool = @splat(false),
    done: [4]bool = @splat(false),
    observation: BatchExecution.Observation = .{},

    fn run(self: *CohortReady, parts: *CohortParts, force: bool) !void {
        var entered = false;
        defer if (entered) if (self.cb.embedding_stage) |stage| stage.pause() catch {};
        const a = self.a;
        const out = self.out;
        const session = self.session;
        const cb = self.cb;
        const tokenizer = self.tokenizer;
        const groups = self.groups;
        const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.UnsupportedEmbeddingGemma2Backend;
        const options = self.options;
        const control = self.control;
        const errors = self.errors;
        const permit = self.permit;
        const pool = self.pool;
        const cache = self.cache;
        const vectors = self.vectors;
        const counts = self.counts;
        const inputs = self.inputs;
        const host = self.host;
        const retained = self.retained;
        var ready: [4]usize = undefined;
        var ready_count: usize = 0;
        var pending: usize = 0;
        for (self.indexes) |index| {
            if (self.done[index]) continue;
            if (errors[index] != null) {
                self.done[index] = true;
                if (!entered) if (cb.embedding_stage) |stage| {
                    try stage.acquire();
                    entered = true;
                };
                parts.releaseGroup(a, cb, index);
                continue;
            }
            pending += 1;
            if (!cohortGroupReady(groups[index], parts.parts[index], options)) continue;
            ready[ready_count] = index;
            ready_count += 1;
        }
        if (ready_count == 0) return;
        // Coalesce a singleton for at most one preparation wave. This keeps
        // small mixed requests from degenerating into one backbone per item,
        // while a slow remaining group cannot indefinitely delay ready work.
        if (!force and ready_count == 1 and pending > 1 and !self.deferred[ready[0]]) {
            self.deferred[ready[0]] = true;
            return;
        }
        if (!entered) if (cb.embedding_stage) |stage| {
            try stage.acquire();
            entered = true;
        };
        for (ready[0..ready_count]) |index| self.done[index] = true;
        if (@import("antfly_platform").env.getenvBool("ANTFLY_GEMMA4_GROUP_BROKER_TRACE")) std.debug.print("gemma4-group-ready items={d} indexes={any}\n", .{ ready_count, ready[0..ready_count] });
        var rows: [4]PreparedGroup = undefined;
        var owners: [4]usize = undefined;
        var prepared: usize = 0;
        defer for (rows[0..prepared]) |row| row.deinit(a);
        for (ready[0..ready_count]) |index| {
            try control.check();
            if (errors[index] != null) continue;
            if (cb.embedding_stage) |stage| try stage.workspace(null);
            rows[prepared] = (if (cb.kind() == .cuda) prepareCudaProjectedGroup(a, cb, tokenizer, groups[index], options, control, parts.parts[index]) else prepareGroupCombined(a, cb, tokenizer, groups[index], options, control, pool, null, cache, parts.parts[index])) catch |err| {
                if (@import("audio_preparation.zig").abort(err)) return err;
                errors[index] = err;
                continue;
            };
            if (cb.embedding_stage != null) if (rows[prepared].tensor) |tensor| {
                rows[prepared].tensor = @import("../ops/metal_compute.zig").MetalCompute.escapeEmbeddingStage(cb, tensor) catch |err| {
                    rows[prepared].deinit(a);
                    return err;
                };
                cb.free(tensor);
            };
            counts[index] = rows[prepared].ids.len;
            owners[prepared] = index;
            prepared += 1;
        }
        // Stable insertion sort over a tiny cohort; public results retain input order.
        for (0..prepared) |i| {
            var j = i;
            while (j > 0 and rows[j].ids.len < rows[j - 1].ids.len) : (j -= 1) {
                std.mem.swap(PreparedGroup, &rows[j], &rows[j - 1]);
                std.mem.swap(usize, &owners[j], &owners[j - 1]);
            }
        }
        const observation = &self.observation;
        const Phase = struct { offset: usize, count: usize };
        var phases: [4]Phase = undefined;
        var phase_count: usize = 0;
        var peak = @import("../runtime/tier/memory.zig").AdmissionAmounts{};
        var offset: usize = 0;
        while (offset < prepared) {
            var lengths: [4]usize = undefined;
            for (rows[offset..prepared], lengths[0 .. prepared - offset]) |row, *length| length.* = row.ids.len;
            var take = preparedCohortCountForBackend(lengths[0 .. prepared - offset], 4, encoder.compactRowsEnabled(cb.kind() == .metal));
            var longest: usize = 0;
            if (take == 0) return error.EmbeddingInputTooLong;
            // Model intermediates are bounded by the same per-row envelope as
            // segmented Metal frames; there is no cross-picture dense score matrix.
            var request: @import("../backends/session.zig").RunRequest = undefined;
            while (true) {
                longest = rows[offset + take - 1].ids.len;
                var active: usize = 0;
                for (rows[offset..][0..take]) |row| for (row.mask) |value| {
                    active += @intFromBool(value != 0);
                };
                const packed_geometry = encoder.compactRowsEnabled(cb.kind() == .metal);
                request = .{ .batch = if (packed_geometry) 1 else take, .sequence = if (packed_geometry) active else longest, .input_bytes = inputs, .workspace_planned = cb.kind() == .metal, .backend_input_bytes = (if (packed_geometry) active else take * longest) * 512 * 4, .host_preprocess_bytes = if (cb.kind() == .cuda and force) 1024 * 1024 + prepared * options.max_tokens * 16 else host, .workspace_bytes = (if (cb.kind() == .cuda) try cudaEncoderWorkspace(longest, take) else 256 * 1024 * 1024 + (if (packed_geometry) active else take * longest) * 192 * 1024) + retained, .preprocessing_complete = cb.kind() == .cuda, .output_bytes = take * options.dimensions * @sizeOf(f32) };
                if (take == 1 or try session.fitsRun(request)) break;
                take -= 1;
            }
            if (session.run_admission) |admission| {
                const needed = try admission.estimateRequest(request, session.outputInfo());
                inline for (@typeInfo(@TypeOf(peak)).@"struct".field_names) |name| @field(peak, name) = @max(@field(peak, name), @field(needed, name));
            }
            phases[phase_count] = .{ .offset = offset, .count = take };
            phase_count += 1;
            offset += take;
        }
        if (cb.kind() == .cuda and force) {
            // No producer/tower remains. Release the completed preparation
            // peak before acquiring any batch supplement. Reductions never
            // borrow capacity; the supplement covers dimensions that grow.
            if (permit.lease) |*lease| {
                var remaining = peak;
                inline for (@typeInfo(@TypeOf(peak)).@"struct".field_names) |name|
                    @field(remaining, name) = @min(@field(remaining, name), @field(lease.amounts, name));
                try lease.retain(remaining);
            }
        }
        if (self.lane.video_phases) try self.lane.reserve(0);
        var supplement = try self.lane.admitPhase(peak, self.lane.credit());
        defer supplement.deinit();
        for (phases[0..phase_count]) |phase| {
            offset = phase.offset;
            const take = phase.count;
            const packed_rows = try packVideoRows(a, cb, rows[offset..][0..take]);
            defer packed_rows.deinit(a, cb);
            const compact = if (cb.kind() == .cuda) try runCudaPackedGroups(a, cb, cfg, packed_rows, take, options.dimensions, control) else read: {
                const output = try encoder.forwardPackedPooledEmbeddingsCT(cb, a, cfg, packed_rows.embeddings, packed_rows.mask, take, packed_rows.sequence, options.dimensions, packed_rows.compact_input);
                defer cb.free(output);
                break :read try readPooledBatch(a, cb, output, take, options.dimensions);
            };
            defer {
                for (compact) |vector| a.free(vector);
                a.free(compact);
            }
            for (compact, owners[offset..][0..take]) |vector, index| vectors[index] = try out.dupe(f32, vector);
            observation.record(take);
        }
        for (ready[0..ready_count]) |index| parts.releaseGroup(a, cb, index);
    }
};

const CohortParts = struct {
    parts: [4][]?SoftTokens = @splat(&.{}),
    fn releaseGroup(self: *CohortParts, a: std.mem.Allocator, cb: *const ComputeBackend, index: usize) void {
        for (self.parts[index]) |*part| if (part.*) |value| {
            a.free(value.embeddings);
            if (value.tensor) |tensor| cb.free(tensor);
            part.* = null;
        };
    }
    fn deinit(self: *CohortParts, a: std.mem.Allocator, cb: *const ComputeBackend) void {
        for (self.parts) |parts| {
            for (parts) |part| if (part) |value| {
                a.free(value.embeddings);
                if (value.tensor) |tensor| cb.free(tensor);
            };
            a.free(parts);
        }
    }
};
const PartRef = struct { group: usize, part: usize, image: bool };
fn mixedWaveEnd(refs: []const PartRef, start: usize) usize {
    var end = start + 1;
    while (end < refs.len and end - start < 4 and refs[end].image == refs[start].image) : (end += 1) {}
    return end;
}

test "embeddinggemma2 preparation waves cross modality boundaries without mixing tower batches" {
    const refs = [_]PartRef{
        .{ .group = 0, .part = 0, .image = true },
        .{ .group = 1, .part = 0, .image = true },
        .{ .group = 2, .part = 0, .image = false },
        .{ .group = 3, .part = 0, .image = false },
    };
    try std.testing.expectEqual(@as(usize, 2), mixedWaveEnd(&refs, 0));
    try std.testing.expectEqual(@as(usize, 4), mixedWaveEnd(&refs, 2));
}
/// CPU-only producer. No backend/model state enters the worker. Its own shared
/// lease and allocator bound the one future wave until join and destruction.
const MixedWave = struct {
    session: Session,
    groups: []const Group,
    refs: []const PartRef,
    control: Control,
    image: bool,
    budget: @import("../runtime/bounded_allocator.zig").BoundedAllocator = .{ .backing = std.heap.smp_allocator, .limit = 64 * 1024 * 1024 },
    allocation_exceeded_budget: bool = false,
    permit: ?RunPermit = null,
    thread: ?std.Thread = null,
    failure: ?anyerror = null,
    errors: [4]?anyerror = @splat(null),
    images: [4]projector.PreparedImage = undefined,
    audio: [4]projector.PreparedAudio = undefined,
    indexes: [4]usize = undefined,
    count: usize = 0,
    fn allocator(self: *MixedWave) std.mem.Allocator {
        self.budget.failure_context = self;
        self.budget.allocation_failed = allocationFailed;
        return self.budget.allocator();
    }
    fn allocationFailed(raw: ?*anyopaque, failure: @import("../runtime/bounded_allocator.zig").BoundedAllocator.AllocationFailure) void {
        const self: *MixedWave = @ptrCast(@alignCast(raw.?));
        self.allocation_exceeded_budget = failure.kind == .declared_limit;
    }
    fn run(self: *MixedWave) void {
        self.prepare() catch |err| {
            self.failure = if (err == error.OutOfMemory and self.allocation_exceeded_budget) error.MemoryBudgetExceeded else err;
            return;
        };
        self.groupGeometry();
    }
    fn prepare(self: *MixedWave) !void {
        try self.control.check();
        if (self.image) {
            var decoded: usize = 0;
            for (self.refs, 0..) |ref, index| {
                const part = self.groups[ref.group].content[ref.part];
                if (part == .image) {
                    const header = @import("image.zig").inspectEncodedForInference(part.image, null) catch |err| {
                        if (@import("audio_preparation.zig").abort(err)) return err;
                        self.errors[index] = err;
                        continue;
                    };
                    decoded = try std.math.add(usize, decoded, try std.math.mul(usize, try std.math.mul(usize, header.width, header.height), 3));
                }
            }
            if (self.budget.limit == 64 * 1024 * 1024) self.budget.limit = @min(@as(usize, 512 * 1024 * 1024), try std.math.add(usize, 64 * 1024 * 1024, decoded));
        }
        self.permit = try self.session.admitHostPreprocess(self.budget.limit);
        const a = self.allocator();
        const scope = @import("antfly_image").work_control.Scope.enter(self.control.imageWorkControl());
        defer scope.deinit();
        for (self.refs, 0..) |ref, index| {
            try self.control.check();
            if (self.errors[index] != null) continue;
            const part = self.groups[ref.group].content[ref.part];
            if (self.image) {
                self.images[self.count] = (if (part == .raster) projector.prepareEmbeddingGemma2RasterForBackend(a, part.raster, self.session.backend()) else projector.prepareEmbeddingGemma2ImageForBackend(a, part.image, self.session.backend())) catch |err| {
                    if (@import("audio_preparation.zig").abort(err)) return err;
                    self.errors[index] = err;
                    continue;
                };
            } else {
                self.audio[self.count] = (if (part == .pcm) projector.prepareEmbeddingGemma2AudioPcmWithPacking(a, part.pcm, self.session.backend() != .native) else projector.prepareEmbeddingGemma2AudioForBackend(a, part.audio, .{}, self.session.backend())) catch |err| {
                    if (@import("audio_preparation.zig").abort(err)) return err;
                    self.errors[index] = err;
                    continue;
                };
            }
            self.indexes[self.count] = index;
            self.count += 1;
        }
    }
    fn groupGeometry(self: *MixedWave) void {
        // Stable in-wave bucketing leaves source/error slots untouched and
        // never extends the bounded preparation window or waits for more work.
        if (!self.image) return;
        var i: usize = 1;
        while (i < self.count) : (i += 1) {
            const image = self.images[i];
            const slot = self.indexes[i];
            const key = projector.embeddingGemma2GeometryKey(image.geometry);
            var j = i;
            while (j > 0 and projector.embeddingGemma2GeometryKey(self.images[j - 1].geometry) > key) : (j -= 1) {
                self.images[j] = self.images[j - 1];
                self.indexes[j] = self.indexes[j - 1];
            }
            self.images[j] = image;
            self.indexes[j] = slot;
        }
    }
    fn start(self: *MixedWave) bool {
        if (comptime @import("builtin").single_threaded or @import("builtin").cpu.arch.isWasm()) {
            self.run();
            return false;
        }
        self.thread = std.Thread.spawn(.{}, run, .{self}) catch {
            self.run();
            return false;
        };
        return true;
    }
    fn join(self: *MixedWave) void {
        if (comptime !@import("builtin").single_threaded and !@import("builtin").cpu.arch.isWasm()) if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        };
    }
    fn deinit(self: *MixedWave) void {
        self.join();
        if (self.image) {
            for (self.images[0..self.count]) |*image| image.deinit();
        } else {
            for (self.audio[0..self.count]) |*audio| audio.deinit();
        }
        self.count = 0;
        if (self.permit) |*permit| permit.deinit();
        self.permit = null;
        std.debug.assert(self.budget.live == 0);
    }
};
fn projectCohortParts(a: std.mem.Allocator, session: Session, cb: *const ComputeBackend, groups: []const Group, indexes: []const usize, options: Options, control: Control, base: *RunPermit, errors: []?anyerror, scheduler: *CohortReady) !CohortParts {
    var result = CohortParts{};
    errdefer result.deinit(a, cb);
    defer if (cb.embedding_stage) |stage| stage.cleanup();
    for (indexes) |index| {
        result.parts[index] = try a.alloc(?SoftTokens, groups[index].content.len);
        @memset(result.parts[index], null);
    }
    var refs: [256]PartRef = undefined;
    var count: usize = 0;
    for ([_]bool{ true, false }) |image| {
        for (indexes) |index| for (groups[index].content, 0..) |part, p| {
            if (errors[index] != null) continue;
            const eligible = if (image) (part == .image or part == .raster) else (part == .audio or part == .pcm);
            if (eligible and (if (image) options.resident_images else options.resident_audio)) {
                refs[count] = .{ .group = index, .part = p, .image = image };
                count += 1;
            }
        };
    }
    var waves: [2]MixedWave = undefined;
    var live: [2]bool = @splat(false);
    defer for (&waves, live) |*wave, active| if (active) wave.deinit();
    var current: usize = 0;
    var offset: usize = 0;
    if (count == 0) {
        try scheduler.run(&result, true);
        return result;
    }
    waves[0] = .{ .session = session, .groups = groups, .refs = refs[0..mixedWaveEnd(refs[0..count], 0)], .control = control, .image = refs[0].image };
    live[0] = true;
    if (options.prepare_ahead) {
        _ = waves[0].start();
    } else waves[0].run();
    // Text-only or non-resident groups can run while the first CPU wave
    // prepares, rather than delaying that producer behind the backbone.
    try scheduler.run(&result, false);
    while (offset < count) {
        const wave = &waves[current];
        const image = wave.image;
        if (cb.embedding_stage) |stage| try stage.pause();
        wave.join();
        if (wave.failure) |err| return err;
        for (wave.refs, wave.errors[0..wave.refs.len]) |ref, err| if (err) |failure| {
            errors[ref.group] = failure;
        };
        const next = 1 - current;
        const end = offset + wave.refs.len;
        if (end < count) {
            waves[next] = .{ .session = session, .groups = groups, .refs = refs[end..mixedWaveEnd(refs[0..count], end)], .control = control, .image = refs[end].image };
            live[next] = true;
            const spawned = if (options.prepare_ahead) waves[next].start() else blk: {
                waves[next].run();
                break :blk false;
            };
            if (@import("antfly_platform").env.getenvBool("ANTFLY_GEMMA4_GROUP_BROKER_TRACE")) std.debug.print("gemma4-group-prepare overlap={}\n", .{spawned});
        }
        if (wave.count != 0) {
            if (cb.embedding_stage) |stage| try stage.acquire();
            var request = base.request.?;
            const scratch = if (image) (if (boundedVision(session.backend())) try projector.embeddingGemma2PreparedPlanScratch(wave.images[0..wave.count]) else try projector.embeddingGemma2PreparedScratch(wave.images[0..wave.count], 2 * 1024 * 1024 * 1024)) else audioRunRequest(session.backend(), 0, 0, wave.count).workspace_bytes;
            request.workspace_bytes = @max(request.workspace_bytes, scratch + groups.len * 8192 * 512 * 4);
            var supplement = if (!image and session.run_admission != null) try scheduler.lane.admitPhase(try session.run_admission.?.estimateRequest(request, session.outputInfo()), scheduler.lane.credit()) else RunPermit{ .session = session, .lease = null };
            defer supplement.deinit();
            var audio: [4]*const projector.PreparedAudio = undefined;
            if (!image) {
                for (wave.audio[0..wave.count], audio[0..wave.count]) |*clip, *dest| dest.* = clip;
            }
            var projected = if (image) try projectPreparedImages(true, cb, a, wave.images[0..wave.count], scratch, base, control, options.timings, null, scheduler.retained, scheduler.lane.credit()) else try projector.encodePreparedEmbeddingGemma2AudioBatchResident(cb, a, audio[0..wave.count]);
            defer projected.deinit();
            if (@import("antfly_platform").env.getenvBool("ANTFLY_GEMMA4_GROUP_BROKER_TRACE")) std.debug.print("gemma4-group-tower kind={s} items={d}\n", .{ if (image) "image" else "audio", projected.max_batch_executed });
            var first: usize = 0;
            for (projected.tokens_per_image, wave.indexes[0..wave.count]) |tokens, slot| {
                const ref = wave.refs[slot];
                if (errors[ref.group] != null) {
                    first += tokens;
                    continue;
                }
                var total: usize = tokens;
                for (result.parts[ref.group]) |part| if (part) |value| {
                    total += value.tokens;
                };
                if (total > 8192) {
                    errors[ref.group] = error.EmbeddingInputTooLong;
                    first += tokens;
                    continue;
                }
                const view = try cb.sliceRows2D(a, projected.embeddings, first, tokens, 512);
                const escaped = if (cb.embedding_stage != null) @import("../ops/metal_compute.zig").MetalCompute.escapeEmbeddingStage(cb, view) catch |err| {
                    cb.free(view);
                    return err;
                } else view;
                if (cb.embedding_stage != null) cb.free(view);
                result.parts[ref.group][ref.part] = .{ .id = if (image) 258880 else 258881, .embeddings = &.{}, .tensor = escaped, .tokens = tokens };
                first += tokens;
            }
        }
        if (cb.embedding_stage) |stage| try stage.pause();
        // Projection owns its device output. Release CPU preparation and its
        // admission before the backbone competes with the next producer wave.
        wave.deinit();
        live[current] = false;
        try scheduler.run(&result, end == count);
        offset = end;
        current = next;
    }
    return result;
}

fn prepareCudaProjectedGroup(a: std.mem.Allocator, cb: *const ComputeBackend, tokenizer: Tokenizer, group: Group, options: Options, control: Control, projected: []const ?SoftTokens) !PreparedGroup {
    const content = @import("embedding_gemma2_content.zig");
    const parts = try a.alloc(content.Part, group.content.len);
    defer a.free(parts);
    const rows = try a.alloc(?content.Projected, parts.len);
    defer a.free(rows);
    for (group.content, parts, projected, rows) |part, *dest, soft, *row| {
        dest.* = switch (part) {
            .text => |text| .{ .text = text },
            .image => |image| .{ .image = image },
            .audio => |audio| .{ .audio = .{ .bytes = audio } },
            else => return error.BatchIncompatible,
        };
        row.* = if (soft) |value| .{ .tensor = value.tensor orelse return error.BatchIncompatible, .tokens = value.tokens } else null;
    }
    const prefix = if (group.title) |title| try std.fmt.allocPrint(a, "title: {s} | text: ", .{title}) else try a.dupe(u8, try encoder.taskPrefix(options.task_type));
    defer a.free(prefix);
    const prepared = try content.prepareResidentProjected(cb, a, tokenizer, prefix, parts, options.max_tokens, control, rows);
    // Transfer all owned fields; no tower output is downloaded for composition.
    return .{ .ids = prepared.input_ids, .mask = prepared.attention_mask, .embeddings = prepared.input_embeddings, .tensor = prepared.resident_embeddings, .backend = cb };
}
fn runCudaPackedGroups(a: std.mem.Allocator, cb: *const ComputeBackend, cfg: encoder.Config, rows: ResidentPackedGroups, batch: usize, dimensions: usize, control: Control) ![][]f32 {
    const compute: *@import("../ops/cuda/cuda_compute.zig").CudaCompute = @ptrCast(@alignCast(cb.ptr));
    if (batch > 1) compute.embedding_gemma2_media_batches += 1;
    const values = try compute.runEmbeddingGemma2(a, cfg.resident orelse return error.UnsupportedEmbeddingGemma2Backend, .{ .resident_embeddings = rows.embeddings }, rows.mask, batch, rows.sequence, dimensions, control);
    defer a.free(values);
    const output = try a.alloc([]f32, batch);
    var initialized: usize = 0;
    errdefer {
        for (output[0..initialized]) |row| a.free(row);
        a.free(output);
    }
    for (output, 0..) |*row, index| {
        row.* = try a.dupe(f32, values[index * dimensions ..][0..dimensions]);
        initialized += 1;
    }
    return output;
}

fn packVideoRows(a: std.mem.Allocator, cb: *const ComputeBackend, rows: []const PreparedGroup) !ResidentPackedGroups {
    if (encoder.compactRowsEnabled(cb.kind() == .metal)) return packCompactPreparedRows(a, cb, rows);
    var sequence: usize = 0;
    for (rows) |row| sequence = @max(sequence, row.ids.len);
    const mask = try a.alloc(i64, sequence * rows.len);
    errdefer a.free(mask);
    @memset(mask, 0);
    const input = if (cb.kind() == .cuda) cuda: {
        const compute: *@import("../ops/cuda/cuda_compute.zig").CudaCompute = @ptrCast(@alignCast(cb.ptr));
        break :cuda try compute.embeddingGemma2Composition(sequence * rows.len);
    } else (try cb.allocUninitF32Shape(&.{ @intCast(sequence * rows.len), 512 })) orelse return error.ResidentImageOperationUnavailable;
    errdefer cb.free(input);
    var padding: usize = 0;
    for (rows) |row| padding = @max(padding, sequence - row.ids.len);
    var zero_tensor: ?CT = null;
    defer if (zero_tensor) |tensor| cb.free(tensor);
    if (padding != 0 and cb.kind() != .cuda) {
        const zeros = try a.alloc(f32, padding * 512);
        defer a.free(zeros);
        @memset(zeros, 0);
        zero_tensor = try cb.fromFloat32Shape(zeros, &.{ @intCast(padding), 512 });
    }
    for (rows, 0..) |row, index| {
        if (row.mask.len != row.ids.len) return error.InvalidInputShape;
        @memcpy(mask[index * sequence ..][0..row.mask.len], row.mask);
        if (sequence > row.ids.len and cb.kind() != .cuda) {
            if (!try cb.copyRows2D(a, input, index * sequence + row.ids.len, zero_tensor.?, 0, sequence - row.ids.len, 512)) return error.UnsupportedPrimitiveOp;
        }
        const source = row.tensor orelse if (row.embeddings) |values|
            try cb.fromFloat32Shape(values, &.{ @intCast(row.ids.len), 512 })
        else blk: {
            // Text-only members have IDs rather than preprojected media rows.
            const weight = try cb.getWeight("language_model.embed_tokens.weight");
            defer cb.free(weight);
            const raw = try cb.embeddingLookup(weight, row.ids, row.ids.len, 512);
            defer cb.free(raw);
            break :blk (try cb.multiplyScalar(raw, @sqrt(@as(f32, 512)))) orelse return error.EmbeddingGemma2OperationUnavailable;
        };
        defer if (row.tensor == null) cb.free(source);
        if (!try cb.copyRows2D(a, input, index * sequence, source, 0, row.ids.len, 512)) return error.UnsupportedPrimitiveOp;
    }
    return .{ .embeddings = input, .mask = mask, .sequence = sequence };
}

fn packCompactPreparedRows(a: std.mem.Allocator, cb: *const ComputeBackend, rows: []const PreparedGroup) !ResidentPackedGroups {
    if (rows.len == 0 or rows.len > 4) return error.InvalidInputShape;
    var sequence: usize = 0;
    var active: usize = 0;
    for (rows) |row| {
        if (row.mask.len != row.ids.len or row.ids.len == 0 or row.ids.len > 8192) return error.InvalidInputShape;
        sequence = @max(sequence, row.ids.len);
        var count: usize = 0;
        for (row.mask) |value| {
            if (value != 0 and value != 1) return error.InvalidAttentionMask;
            count += @intFromBool(value != 0);
        }
        if (count == 0) return error.EmptyEmbeddingInput;
        active += count;
    }
    if (active > 8192) return error.EmbeddingInputTooLong;
    const mask = try a.alloc(i64, rows.len * sequence);
    errdefer a.free(mask);
    @memset(mask, 0);
    const output = (try cb.allocUninitF32Shape(&.{ @intCast(active), 512 })) orelse return error.ResidentImageOperationUnavailable;
    errdefer cb.free(output);
    var destination: usize = 0;
    for (rows, 0..) |row, item| {
        @memcpy(mask[item * sequence ..][0..row.mask.len], row.mask);
        const source = row.tensor orelse if (row.embeddings) |values| try cb.fromFloat32Shape(values, &.{ @intCast(row.ids.len), 512 }) else {
            var count: usize = 0;
            for (row.mask) |value| count += @intFromBool(value != 0);
            const ids = try a.alloc(i64, count);
            defer a.free(ids);
            var at: usize = 0;
            for (row.ids, row.mask) |id, valid| if (valid != 0) {
                ids[at] = id;
                at += 1;
            };
            const weight = try cb.getWeight("language_model.embed_tokens.weight");
            defer cb.free(weight);
            const raw = try cb.embeddingLookup(weight, ids, count, 512);
            defer cb.free(raw);
            const scaled = (try cb.multiplyScalar(raw, @sqrt(@as(f32, 512)))) orelse return error.ResidentImageOperationUnavailable;
            defer cb.free(scaled);
            if (!try cb.copyRows2D(a, output, destination, scaled, 0, count, 512)) return error.UnsupportedPrimitiveOp;
            destination += count;
            continue;
        };
        defer if (row.tensor == null) cb.free(source);
        var cursor: usize = 0;
        while (cursor < row.mask.len) {
            if (row.mask[cursor] == 0) {
                cursor += 1;
                continue;
            }
            const first = cursor;
            while (cursor < row.mask.len and row.mask[cursor] != 0) : (cursor += 1) {}
            if (!try cb.copyRows2D(a, output, destination, source, first, cursor - first, 512)) return error.UnsupportedPrimitiveOp;
            destination += cursor - first;
        }
    }
    return .{ .embeddings = output, .mask = mask, .sequence = sequence, .compact_input = true };
}

/// One clip produces one pooled vector. All decoded pixels, projected frame rows,
/// and encoder inputs remain on CUDA; only the final requested vector is read back.
fn embedVideo(a: std.mem.Allocator, session: Session, tokenizer: Tokenizer, bytes: []const u8, options: Options, lock: ?*std.atomic.Mutex, control: ?Control) !Result {
    if (comptime !@import("build_options").enable_cuda or @import("builtin").os.tag != .linux) return error.UnsupportedVideoBackend;
    if (session.backend() != .cuda) return error.UnsupportedVideoBackend;
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.UnsupportedEmbeddingGemma2Backend;
    if (!cfg.vision) return error.NoVisionSession;
    const effective = control orelse Control{};
    try effective.check();
    var execution_waiter = Scheduler.Waiter{};
    try enterModel(session, lock, effective, &execution_waiter);
    defer leaveModel(lock, &execution_waiter);
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    const compute: *@import("../ops/cuda/cuda_compute.zig").CudaCompute = @ptrCast(@alignCast(managed.backend.ptr));
    const request: @import("../backends/session.zig").RunRequest = .{ .batch = 1, .sequence = @min(options.max_tokens, 4546), .input_bytes = bytes.len, .host_preprocess_bytes = 128 * 1024 * 1024, .workspace_bytes = 1536 * 1024 * 1024, .output_bytes = options.dimensions * @sizeOf(f32) };
    var permit = session.admit(request) catch |err| reclaim: {
        if (err != error.ResourceTemporarilyUnavailable and err != error.ResourceLimitExceeded) return err;
        try compute.evictEmbeddingGemma2Caches();
        break :reclaim try session.admit(request);
    };
    defer permit.deinit();
    const cb = &managed.backend;
    const scope = @import("antfly_image").work_control.Scope.enter(effective.imageWorkControl());
    defer scope.deinit();
    const before = MediaTransferSnapshot.init(cb);
    defer observeResidentTransfers(cb, before, options);
    const started = mediaNow();
    var projected = projector.encodeEmbeddingGemma2VideoResident(cb, a, bytes, options.max_tokens) catch |err| return videoInputError(err);
    defer projected.deinit();
    if (options.timings) |t| {
        t.vision_ns += mediaNow() - started;
        t.resident_cohorts += 1;
    }
    const pack_started = mediaNow();
    var rendered = std.ArrayList(u8).empty;
    defer rendered.deinit(a);
    var total: usize = 0;
    for (projected.images.tokens_per_image) |count| {
        if (count == 0 or count > 140) return error.InvalidTensorShape;
        try rendered.appendSlice(a, "<|image>");
        for (0..count) |_| try rendered.appendSlice(a, "<|video|>");
        try rendered.appendSlice(a, "<image|>");
        total += count;
    }
    var encoded = try tokenizer.encodeForModel(a, rendered.items, options.max_tokens + 1);
    defer encoded.deinit();
    var length: usize = 0;
    for (encoded.attention_mask) |m| length += @intFromBool(m != 0);
    // BOS/EOS once for the whole clip; two wrapper tokens per frame.
    if (length != total + projected.images.tokens_per_image.len * 2 + 2 or length > options.max_tokens) return error.EmbeddingInputTooLong;
    const ids = try a.alloc(i64, length);
    defer a.free(ids);
    const mask = try a.alloc(i64, length);
    defer a.free(mask);
    for (ids, encoded.ids[0..length]) |*dest, id| dest.* = id;
    for (mask, encoded.attention_mask[0..length]) |*dest, m| dest.* = m;
    const row = PreparedGroup{ .ids = ids, .mask = mask, .embeddings = null };
    const video_input = try packResidentMediaRows(a, cb, &.{row}, projected.images.embeddings, &.{total}, 258884, options.max_tokens);
    defer video_input.deinit(a, cb);
    if (options.timings) |t| t.pack_ns += mediaNow() - pack_started;
    const encoder_started = mediaNow();
    try permit.retainForRun(.{ .batch = 1, .sequence = length, .input_bytes = bytes.len, .host_preprocess_bytes = length * 32, .workspace_bytes = try cudaEncoderWorkspace(length, 1), .preprocessing_complete = true, .output_bytes = options.dimensions * @sizeOf(f32) });
    const vector = try compute.runEmbeddingGemma2(a, cfg.resident orelse return error.UnsupportedEmbeddingGemma2Backend, .{ .resident_embeddings = video_input.embeddings }, video_input.mask, 1, length, options.dimensions, cb.execution_control);
    errdefer a.free(vector);
    if (options.timings) |t| t.encoder_ns += mediaNow() - encoder_started;
    try effective.check();
    return .{ .vector = vector, .input_tokens = length };
}

fn videoInputError(err: anyerror) anyerror {
    return switch (err) {
        error.UnexpectedEndOfSource, error.EmptyVideo, error.EmptyVideoTrack, error.MalformedMedia, error.MissingMovieMetadata, error.UnsupportedContainer, error.UnsupportedDataReference, error.UnsupportedDisplayGeometry, error.UnsupportedEncryptedMedia, error.UnsupportedFragmentedMp4, error.UnsupportedHybridMp4, error.UnsupportedInterlacedVideo, error.UnsupportedSampleDescription, error.UnsupportedSampleSize, error.UnsupportedTimeline, error.UnsupportedVideoCodec, error.InvalidTimebase, error.TimestampOverflow, error.NoRandomAccessPoint, error.InvalidVideoTimestamp, error.NonMonotonicVideoPresentation, error.UnsupportedVideoFormat, error.UnsupportedVideoContainerColor, error.InvalidVideoGeometry, error.InvalidVideoConfiguration, error.InvalidVideoPacket, error.VideoDecodeFailed => error.InvalidVideoInput,
        else => err,
    };
}

fn singleImageGroup(group: Group) bool {
    return group.content.len == 1 and (group.content[0] == .image or group.content[0] == .raster);
}

fn cudaEncoderWorkspace(sequence: usize, batch: usize) !usize {
    return std.math.add(usize, 128 * 1024 * 1024, try std.math.mul(usize, try std.math.mul(usize, sequence, batch), 32 * 1024));
}

/// Header-only bounds before towers acquire device/host storage. Images are
/// inspected without decoding. Unknown audio retains its bounded decode peak.
fn cudaGroupRequest(group: Group, options: Options) !@import("../backends/session.zig").RunRequest {
    var tokens: usize = 128;
    if (group.title) |title| tokens = try std.math.add(usize, tokens, title.len);
    var input: usize = 0;
    var host: usize = 64 * 1024 * 1024;
    var tower: usize = 0;
    for (group.content) |part| {
        input = try std.math.add(usize, input, partBytes(part).len);
        switch (part) {
            .text => |text| tokens = try std.math.add(usize, tokens, text.len),
            .image => |bytes| {
                const info = try @import("image.zig").inspectEncodedForInference(bytes, null);
                const g = try projector.embeddingGemma2ImageGeometry(info.width, info.height);
                tokens = try std.math.add(usize, tokens, g.tokenCount() + 4);
                const pixels = try std.math.mul(usize, info.width, info.height);
                // Decoded RGB, decoder/intermediate storage, and source staging.
                host = @max(host, try std.math.add(usize, 64 * 1024 * 1024, try std.math.mul(usize, pixels, 12)));
                tower = @max(tower, try projector.embeddingGemma2ImageScratch(info.width, info.height, 2 * 1024 * 1024 * 1024));
            },
            .audio => |bytes| {
                const audio_tokens = try projector.embeddingGemma2EncodedAudioSequenceBound(bytes);
                tokens = try std.math.add(usize, tokens, audio_tokens);
                host = @max(host, 64 * 1024 * 1024 + audio_tokens * 32 * 1024);
                tower = @max(tower, 512 * 1024 * 1024);
            },
            else => return error.BatchIncompatible,
        }
    }
    const sequence = @min(tokens, options.max_tokens);
    const retained = try std.math.mul(usize, sequence, 512 * @sizeOf(f32));
    return .{ .batch = 1, .sequence = sequence, .input_bytes = input, .host_preprocess_bytes = @min(host, 512 * 1024 * 1024), .workspace_bytes = @max(try cudaEncoderWorkspace(sequence, 1), try std.math.add(usize, tower, retained)), .output_bytes = options.dimensions * @sizeOf(f32) };
}

fn embedCudaGroup(a: std.mem.Allocator, session: Session, tokenizer: Tokenizer, group: Group, options: Options, lock: ?*std.atomic.Mutex, control: ?Control) !Result {
    const content = @import("embedding_gemma2_content.zig");
    const parts = try a.alloc(content.Part, group.content.len);
    defer a.free(parts);
    for (group.content, parts) |part, *dest| dest.* = switch (part) {
        .text => |text| .{ .text = text },
        .image => |image| .{ .image = image },
        .audio => |audio| .{ .audio = .{ .bytes = audio } },
        .raster, .video, .sampled_video, .pcm => return error.UnsupportedInput,
    };
    const prefix = if (group.title) |title| try std.fmt.allocPrint(a, "title: {s} | text: ", .{title}) else try a.dupe(u8, try encoder.taskPrefix(options.task_type));
    defer a.free(prefix);
    const effective = control orelse Control{};
    try effective.check();
    var input_bytes: usize = 0;
    for (group.content) |part| input_bytes = try std.math.add(usize, input_bytes, partBytes(part).len);
    var execution_waiter = Scheduler.Waiter{};
    try enterModel(session, lock, effective, &execution_waiter);
    defer leaveModel(lock, &execution_waiter);
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    const compute: *@import("../ops/cuda/cuda_compute.zig").CudaCompute = @ptrCast(@alignCast(managed.backend.ptr));
    const request = try cudaGroupRequest(group, options);
    var permit = session.admit(request) catch |err| retry: {
        if (err != error.ResourceTemporarilyUnavailable and err != error.ResourceLimitExceeded) return err;
        try compute.evictEmbeddingGemma2Caches();
        break :retry try session.admit(request);
    };
    defer permit.deinit();
    const before = MediaTransferSnapshot.init(&managed.backend);
    defer observeResidentTransfers(&managed.backend, before, options);
    var prepared = try @import("embedding_gemma2_content.zig").prepareResident(&managed.backend, a, tokenizer, prefix, parts, options.max_tokens, 64 * 1024 * 1024, control);
    defer prepared.deinit();
    try permit.retainForRun(.{ .batch = 1, .sequence = prepared.sequence, .input_bytes = input_bytes, .host_preprocess_bytes = prepared.sequence * 2 * @sizeOf(i64), .workspace_bytes = try cudaEncoderWorkspace(prepared.sequence, 1), .preprocessing_complete = true, .output_bytes = options.dimensions * @sizeOf(f32) });
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.UnsupportedEmbeddingGemma2Backend;
    const output = try compute.runEmbeddingGemma2(a, cfg.resident orelse return error.UnsupportedEmbeddingGemma2Backend, .{ .resident_embeddings = prepared.resident_embeddings.? }, prepared.attention_mask, 1, prepared.sequence, options.dimensions, control);
    return .{ .vector = output, .input_tokens = prepared.sequence };
}

const PreparedGroup = struct {
    ids: []i64,
    mask: []i64,
    embeddings: ?[]f32,
    tensor: ?CT = null,
    backend: ?*const ComputeBackend = null,
    borrowed_tokens: bool = false,

    fn deinit(self: PreparedGroup, a: std.mem.Allocator) void {
        if (!self.borrowed_tokens) {
            a.free(self.ids);
            a.free(self.mask);
        }
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
    const parts = [_]?SoftTokens{preprojected};
    return prepareGroupCombined(a, cb, tokenizer, group, options, effective, null, null, null, &parts);
}

fn prepareGroupCombined(a: std.mem.Allocator, cb: *const ComputeBackend, tokenizer: Tokenizer, group: Group, options: Options, effective: Control, video_budget: ?*media_admission.Pool, prefetched: ?*@import("embedding_gemma2_video.zig").Prefetch, cache: ?*FrameCache, preprojected: ?[]const ?SoftTokens) !PreparedGroup {
    if (preprojected) |parts| if (parts.len != group.content.len) return error.InvalidInputShape;
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
                    break :blk try prefetched.?.encodeAllocStaged(a, cb, cb.embedding_stage);
                } else try @import("embedding_gemma2_video.zig").encodeConfigured(a, cb, bytes, if (@TypeOf(payload) == SampledVideo) payload.reader else null, effective, .{ .stage = cb.embedding_stage, .sampling = sampling, .pool = video_budget, .selection = if (@TypeOf(payload) == SampledVideo) payload.selection else null, .webm = if (@TypeOf(payload) == SampledVideo) payload.webm else null, .cache = cache, .decoder = options.video_decoder });
                defer a.free(encoded.frame_tokens);
                var owned = true;
                errdefer if (owned) {
                    a.free(encoded.embeddings);
                    if (encoded.tensor) |tensor| cb.free(tensor);
                };
                var count_video: usize = 0;
                for (encoded.frame_tokens) |frame_count| count_video += frame_count;
                media_tokens = std.math.add(usize, media_tokens, count_video) catch return error.EmbeddingInputTooLong;
                if (media_tokens > options.max_tokens) return error.EmbeddingInputTooLong;
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
                const inline_tower = if (preprojected) |parts| parts[index] == null else true;
                if (inline_tower) if (cb.embedding_stage) |stage| try stage.workspace(1536 * 1024 * 1024);
                var projected = if (if (preprojected) |parts| parts[index] else null) |p| blk: {
                    if (p.id != (if (is_image) @as(i64, 258880) else @as(i64, 258881))) return error.InvalidInputShape;
                    if (p.tensor) |tensor| {
                        const view = try cb.sliceRows2D(a, tensor, 0, p.tokens, 512);
                        break :blk SoftTokens{ .id = p.id, .embeddings = &.{}, .tokens = p.tokens, .tensor = view };
                    }
                    break :blk SoftTokens{ .id = p.id, .embeddings = try a.dupe(f32, p.embeddings), .tokens = p.tokens };
                } else if (is_image) blk: {
                    if (residentImagesEnabled(cb, options)) {
                        var pixels = if (part == .raster) try projector.prepareEmbeddingGemma2Raster(a, part.raster) else try projector.prepareEmbeddingGemma2Image(a, bytes);
                        defer pixels.deinit();
                        var resident = try projector.encodePreparedEmbeddingGemma2ImagesResident(cb, a, &.{pixels}, .{});
                        defer resident.deinit();
                        const count = resident.tokens_per_image[0];
                        const tensor = try cb.sliceRows2D(a, resident.embeddings, 0, count, 512);
                        break :blk SoftTokens{ .id = 258880, .embeddings = &.{}, .tokens = count, .tensor = tensor };
                    }
                    const p = if (part == .raster) try projector.encodeEmbeddingGemma2Raster(cb, a, part.raster) else try projector.encodeEmbeddingGemma2Image(cb, a, bytes);
                    break :blk SoftTokens{ .id = 258880, .embeddings = p.embeddings, .tokens = p.tokens };
                } else blk: {
                    const device = options.resident_audio and (cb.kind() == .metal or cb.kind() == .cuda);
                    // Strict preparation validates duration on the same decode
                    // subsequently consumed by the tower. No validation decode.
                    var prepared = if (part == .pcm)
                        try projector.prepareEmbeddingGemma2AudioPcmWithPacking(a, part.pcm, device)
                    else
                        try projector.prepareEmbeddingGemma2AudioWithPacking(a, bytes, .{}, device);
                    defer prepared.deinit();
                    const p = try projector.encodePreparedEmbeddingGemma2AudioResident(cb, a, &prepared);
                    if (device) break :blk SoftTokens{ .id = 258881, .embeddings = &.{}, .tokens = p.tokens, .tensor = p.embeddings };
                    defer cb.free(p.embeddings);
                    break :blk SoftTokens{ .id = 258881, .embeddings = try cb.toFloat32(p.embeddings, a), .tokens = p.tokens };
                };
                errdefer {
                    a.free(projected.embeddings);
                    if (projected.tensor) |tensor| cb.free(tensor);
                }
                if (inline_tower) if (cb.embedding_stage) |stage| {
                    if (stage.workspace_fn != null) if (projected.tensor) |old| {
                        projected.tensor = try @import("../ops/metal_compute.zig").MetalCompute.escapeEmbeddingStage(cb, old);
                        cb.free(old);
                    };
                    try stage.workspace(null);
                };
                if (projected.tokens == 0 or (projected.tensor == null and projected.embeddings.len != projected.tokens * 512)) return error.InvalidTensorShape;
                media_tokens = std.math.add(usize, media_tokens, projected.tokens) catch return error.EmbeddingInputTooLong;
                if (media_tokens > options.max_tokens) return error.EmbeddingInputTooLong;
                try rendered.appendSlice(a, if (is_image) "<|image>" else "<|audio>");
                for (0..projected.tokens) |_| try rendered.appendSlice(a, if (is_image) "<|image|>" else "<|audio|>");
                try rendered.appendSlice(a, if (is_image) "<image|>" else "<audio|>");
                try media.append(a, projected);
            },
        }
    }
    var tokens = try tokenizer.encodeForModel(a, rendered.items, options.max_tokens + 1);
    defer tokens.deinit();
    var count: usize = 0;
    for (tokens.attention_mask) |m| count += @intFromBool(m != 0);
    if (count == 0 or count > options.max_tokens) return error.EmbeddingInputTooLong;
    const ids = try a.alloc(i64, count);
    errdefer a.free(ids);
    const mask = try a.alloc(i64, count);
    errdefer a.free(mask);
    for (ids, tokens.ids[0..count]) |*dest, id| dest.* = id;
    for (mask, tokens.attention_mask[0..count]) |*dest, m| dest.* = m;
    if (media.items.len == 0) return .{ .ids = ids, .mask = mask, .embeddings = null };
    if (try composeCompactMedia(a, cb, ids, media.items)) |tensor| return .{ .ids = ids, .mask = mask, .embeddings = null, .tensor = tensor, .backend = cb };
    const ew = try cb.getWeight("language_model.embed_tokens.weight");
    defer cb.free(ew);
    const raw = try cb.embeddingLookup(ew, ids, count, 512);
    defer cb.free(raw);
    // Every resident modality participates in one ordered device composition.
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

/// Compact wrappers and media share the same indexed compositor for single,
/// mixed and video groups. Multiple media owners are packed once, not by a
/// sequence of growing concatenations. Preserve tokenizer IDs until success.
fn composeCompactMedia(a: std.mem.Allocator, cb: *const ComputeBackend, ids: []i64, media: []const SoftTokens) !?CT {
    if (cb.kind() != .metal or platform.env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_METAL_DISABLE_FOLLOWUP") or platform.env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_METAL_DISABLE_MEDIA_TEMPLATES")) return null;
    const compose = cb.vtable.embeddingComposeRows orelse return null;
    const rewritten = try a.dupe(i64, ids);
    defer a.free(rewritten);
    const sources = try compositionRows(a, rewritten, media);
    defer a.free(sources);
    const map = try a.alloc(i32, ids.len);
    defer a.free(map);
    var wrapper_count: usize = 0;
    var media_count: usize = 0;
    for (media) |item| media_count = try std.math.add(usize, media_count, item.tokens);
    for (sources, map, 0..) |source, *dest, i| {
        if (source == i) {
            dest.* = std.math.cast(i32, wrapper_count) orelse return error.InvalidTensorShape;
            wrapper_count += 1;
        } else dest.* = -(std.math.cast(i32, @as(usize, @intCast(source)) - ids.len + 1) orelse return error.InvalidTensorShape);
    }
    const selected = try a.alloc(i64, wrapper_count);
    defer a.free(selected);
    for (sources, ids, 0..) |source, id, i| if (source == i) {
        selected[@intCast(map[i])] = id;
    };
    const weight = try cb.getWeight("language_model.embed_tokens.weight");
    defer cb.free(weight);
    const raw: ?CT = if (selected.len > 0) try cb.embeddingLookup(weight, selected, selected.len, 512) else null;
    defer if (raw) |tensor| cb.free(tensor);
    const scaled: ?CT = if (raw) |tensor| (try cb.multiplyScalar(tensor, @sqrt(@as(f32, 512)))) orelse return error.ResidentImageOperationUnavailable else null;
    defer if (scaled) |tensor| cb.free(tensor);
    const direct = media.len == 1 and media[0].tensor != null;
    const pixels = if (direct) media[0].tensor.? else (try cb.allocUninitF32Shape(&.{ @intCast(media_count), 512 })) orelse return null;
    defer if (!direct) cb.free(pixels);
    if (!direct) {
        var offset: usize = 0;
        for (media) |item| {
            const tensor = item.tensor orelse try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(item.embeddings, &.{ @intCast(item.tokens), 512 }));
            defer if (item.tensor == null) cb.free(tensor);
            if (!try cb.copyRows2D(a, pixels, offset, tensor, 0, item.tokens, 512)) return error.ResidentImageOperationUnavailable;
            offset += item.tokens;
        }
    }
    const result = (try compose(cb.ptr, scaled, pixels, map, 512)) orelse return null;
    @memcpy(ids, rewritten);
    return result;
}

fn composeDeviceTokens(a: std.mem.Allocator, cb: *const ComputeBackend, text: CT, ids: []i64, media: []const SoftTokens) !CT {
    const rows = try compositionRows(a, ids, media);
    defer a.free(rows);
    if (!cb.supportsCopyRows2D() or cb.vtable.allocUninitF32Shape == null) return composeDeviceTokensGather(cb, text, ids, media, rows);
    const output = (try cb.allocUninitF32Shape(&.{ @intCast(ids.len), 512 })) orelse return composeDeviceTokensGather(cb, text, ids, media, rows);
    errdefer cb.free(output);
    const scaled = (try cb.multiplyScalar(text, @sqrt(@as(f32, 512)))) orelse return error.EmbeddingGemma2OperationUnavailable;
    defer cb.free(scaled);
    if (!try cb.copyRows2D(a, output, 0, scaled, 0, ids.len, 512)) return error.EmbeddingGemma2OperationUnavailable;
    // One destination allocation. Copy runs across video frame markers without
    // repeatedly concatenating and copying the whole growing token table.
    var base = ids.len;
    var cursor: usize = 0;
    for (media) |item| {
        const tensor = item.tensor orelse try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(item.embeddings, &.{ @intCast(item.tokens), 512 }));
        defer if (item.tensor == null) cb.free(tensor);
        var consumed: usize = 0;
        while (consumed < item.tokens) {
            while (cursor < rows.len and rows[cursor] != @as(i32, @intCast(base + consumed))) : (cursor += 1) {}
            if (cursor == rows.len) return error.InvalidEmbeddingGroup;
            var run: usize = 1;
            while (run < item.tokens - consumed and cursor + run < rows.len and rows[cursor + run] == @as(i32, @intCast(base + consumed + run))) : (run += 1) {}
            if (!try cb.copyRows2D(a, output, cursor, tensor, consumed, run, 512)) return error.EmbeddingGemma2OperationUnavailable;
            consumed += run;
            cursor += run;
        }
        base += item.tokens;
    }
    return output;
}

fn composeDeviceTokensGather(cb: *const ComputeBackend, text: CT, ids: []const i64, media: []const SoftTokens, rows: []const i32) !CT {
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

pub fn imageCohortLimit(session: Session) usize {
    // Larger CUDA GEMMs alter BF16 row arithmetic. Preserve qualified
    // singleton outputs unless a caller explicitly opts into coalescing.
    if (session.backend() == .cuda) {
        const policy = factory.getEmbeddingGemma2CudaPolicy(session) orelse return 1;
        if (!policy.image_batch) return 1;
    }
    return raster_max_batch;
}

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
    if (options.max_tokens == 0 or options.max_tokens > encoder.max_tokens) return error.InvalidEmbeddingTokenLimit;
    _ = try encoder.taskPrefix(options.task_type);
    if (options.prepare_ahead and imageCohortLimit(session) > 1 and (session.backend() == .metal or session.backend() == .cuda))
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
        const count = @min(imageCohortLimit(session), rasterBatchCount(lengths[0..candidates], rasters.len - initialized));
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
    if (options.prepare_ahead and imageCohortLimit(session) > 1 and (session.backend() == .metal or session.backend() == .cuda))
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
        const count = @min(imageCohortLimit(session), images.len - initialized);
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
    return .{ .batch = batch, .sequence = 1024, .input_bytes = input_bytes, .pre_admitted_host_bytes = retained_bytes, .host_preprocess_bytes = 512 * 1024 * 1024, .workspace_bytes = if (backend == .metal) 768 * 1024 * 1024 else 512 * 1024 * 1024, .output_bytes = batch * 768 * 4 };
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
    if (!options.resident_audio) {
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
        if (slot.retained > 0) {
            var execution_waiter = Scheduler.Waiter{};
            try enterModel(session, lock, control, &execution_waiter);
            defer leaveModel(lock, &execution_waiter);
            try consumePreparedAudios(out, slot, tokenizer, cfg, options, vectors[offset..][0..slot.consumed], token_counts[offset..][0..slot.consumed], &observation, if (next_count > 0 and options.prepare_ahead) &slots[next] else null);
        }
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
    var sequence: usize = 1;
    for (slot.gemma[0..slot.retained]) |*prepared| sequence = @max(sequence, prepared.tokenCount() + 4);
    const request = try preparedRunRequest(slot.session.backend(), slot.input_bytes, slot.retained_bytes, slot.retained, sequence, if (slot.session.backend() == .metal) 768 * 1024 * 1024 else 512 * 1024 * 1024);
    if (slot.bounded.live > request.host_preprocess_bytes) return error.MemoryBudgetExceeded;
    slot.bounded.limit = request.host_preprocess_bytes;
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
    const before = MediaTransferSnapshot.init(cb);
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
        var inputs: [4]*const projector.PreparedAudio = undefined;
        for (order[cursor..][0..count], inputs[0..count]) |index, *input| input.* = &slot.gemma[index];
        const audio_started = mediaNow();
        var projected = try projector.encodePreparedEmbeddingGemma2AudioBatchResident(cb, a, inputs[0..count]);
        defer projected.deinit();
        const lengths = projected.tokens_per_image;
        for (lengths, order[cursor..][0..count]) |length, index| if (length != slot.gemma[index].tokenCount()) return error.InvalidTensorShape;
        if (options.timings) |t| {
            t.audio_ns += mediaNow() - audio_started;
            t.resident_cohorts += 1;
        }
        const result = try runResidentMediaBatch(a, cb, cfg, tokenizer, &projected, options, true, if (cursor + count == slot.retained) &permit else null);
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
        self.permit = try self.session.admitHostPreprocess(std.math.add(usize, self.input_bytes, if (self.source == .rasters) try rasterPreparationBytes(self.session.backend(), self.source.rasters) else 512 * 1024 * 1024) catch return error.ResourceLimitExceeded);
        const image_scope = @import("antfly_image").work_control.Scope.enter(self.control.imageWorkControl());
        defer image_scope.deinit();
        for (0..self.source.len()) |index| {
            try self.control.check();
            if (self.errors) |errors| if (errors[index] != null) continue;
            self.pixels[self.retained] = switch (self.source.part(index)) {
                .image => |bytes| projector.prepareEmbeddingGemma2ImageForBackend(self.allocator(), bytes, self.session.backend()),
                .raster => |raster| projector.prepareEmbeddingGemma2RasterForBackend(self.allocator(), raster, self.session.backend()),
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
        for (self.pixels[0..self.retained]) |image| self.retained_bytes = std.math.add(usize, self.retained_bytes, image.retainedBytes()) catch return error.ResourceLimitExceeded;
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
    // Each ready wave takes one FIFO turn. CPU preparation and queue waiting
    // retain bounded host leases, never the execution gate/provider lease.
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
            var execution_waiter = Scheduler.Waiter{};
            try enterModel(session, lock, control, &execution_waiter);
            defer leaveModel(lock, &execution_waiter);
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
fn retainPackedPhase(permit: *RunPermit, batch: usize, packed_rows: ResidentPackedGroups) !void {
    if (!packed_rows.compact_input) return retainPreparedPhase(permit, batch, packed_rows.sequence);
    const old = permit.request orelse return;
    var active: usize = 0;
    for (packed_rows.mask) |value| active += @intFromBool(value != 0);
    try permit.retainForRun(.{ .batch = 1, .sequence = active, .input_bytes = old.input_bytes, .backend_input_bytes = active * 512 * 4, .pre_admitted_host_bytes = old.pre_admitted_host_bytes, .host_preprocess_bytes = @min(old.host_preprocess_bytes, 128 * 1024 * 1024 + active * 192 * 1024), .preprocessing_complete = true, .workspace_bytes = @min(old.workspace_bytes, 256 * 1024 * 1024 + active * 192 * 1024), .output_bytes = batch * 768 * 4 });
}
fn retainPreparedPhase(permit: *RunPermit, batch: usize, sequence: usize) !void {
    const old = permit.request orelse return;
    var phase = try preparedRunRequest(permit.session.backend(), old.input_bytes, old.pre_admitted_host_bytes, batch, sequence, 0);
    // Keep any still-owned features and packing allocations within the previous
    // host ceiling while retiring the drained projector workspace. Image callers
    // have already released their owned preparation and its matching credit.
    phase.host_preprocess_bytes = @min(old.host_preprocess_bytes, phase.host_preprocess_bytes);
    phase.preprocessing_complete = true;
    try permit.retainForRun(phase);
}

fn preparedRunRequest(backend: @import("../backends/backends.zig").BackendType, input_bytes: usize, retained_bytes: usize, batch: usize, sequence: usize, media_workspace: usize) !@import("../backends/session.zig").RunRequest {
    if (batch == 0 or sequence == 0 or sequence > 1024) return error.EmbeddingInputTooLong;
    const per_row = try std.math.add(usize, try std.math.mul(usize, sequence, 192 * 1024), try std.math.mul(usize, try std.math.mul(usize, sequence, sequence), 48));
    const language = try std.math.add(usize, 64 * 1024 * 1024, try std.math.mul(usize, batch, per_row));
    // Retained preparation is additive so it can be released independently
    // without increasing the execution lease or borrowing its host credit.
    const prepared = retained_bytes -| input_bytes;
    const host = try std.math.add(usize, prepared, @min(@as(usize, 512 * 1024 * 1024), try std.math.add(usize, 64 * 1024 * 1024, language)));
    const legacy: usize = if (backend == .metal) 768 * 1024 * 1024 else 512 * 1024 * 1024;
    return .{ .batch = batch, .sequence = sequence, .workspace_planned = boundedVision(backend), .input_bytes = input_bytes, .backend_input_bytes = try std.math.mul(usize, try std.math.mul(usize, batch, sequence), 512 * 4), .pre_admitted_host_bytes = retained_bytes, .host_preprocess_bytes = host, .workspace_bytes = @max(media_workspace, @min(legacy, try std.math.add(usize, 256 * 1024 * 1024, language))), .output_bytes = try std.math.mul(usize, batch, 768 * 4) };
}

/// Raster geometry is known before execution. Metal borrows its source;
/// CPU resize and CUDA RGB packing reserve their actual bounded preparation.
fn rasterPreparationBytes(backend: @import("../backends/backends.zig").BackendType, rasters: []const Raster) !usize {
    if (backend == .metal) return 0;
    var bytes: usize = 64 * 1024 * 1024;
    for (rasters) |raster| {
        const g = try projector.embeddingGemma2ImageGeometry(raster.width, raster.height);
        const area = if (backend == .cuda) try std.math.mul(usize, raster.width, raster.height) else try std.math.mul(usize, g.width, g.height);
        bytes = try std.math.add(usize, bytes, try std.math.mul(usize, area, if (backend == .cuda) 3 else 12));
    }
    return bytes;
}
fn preparedImageRunRequest(backend: @import("../backends/backends.zig").BackendType, input: usize, retained: usize, pixels: []const projector.PreparedImage, max_tokens: usize, scratch: usize) !@import("../backends/session.zig").RunRequest {
    var sequence: usize = 1;
    for (pixels) |item| sequence = @max(sequence, item.geometry.tokenCount() + 4);
    return preparedRunRequest(backend, input, retained, pixels.len, @min(sequence, max_tokens), scratch);
}
/// Projection has fenced all reads of these sources. Release owned pixels and
/// reduce both leases together: no stale credit survives in the run request.
fn releaseImagePreparation(pixels: []projector.PreparedImage, host: *RunPermit, run: *RunPermit) !void {
    const old = run.request orelse return;
    const released = old.pre_admitted_host_bytes -| old.input_bytes;
    for (pixels) |*item| {
        item.deinit();
        item.pixels = &.{};
        item.source = null;
    }
    var phase = old;
    phase.pre_admitted_host_bytes = old.input_bytes;
    phase.host_preprocess_bytes -= released;
    try run.retainForRun(phase);
    try host.retainHostBytes(old.input_bytes);
}

/// Queue planning and execution use the same physical-input/vision policy.
/// This estimate grants no preprocessing credit: the queue owns no host lease.
pub fn imageHeaderRunRequest(backend: @import("../backends/backends.zig").BackendType, input_bytes: usize, decoded_bytes: usize, batch: usize, sequence: usize, patches: usize) !@import("../backends/session.zig").RunRequest {
    const scratch = try projector.embeddingGemma2GeometryPlanScratch(patches, sequence -| 4, batch);
    var request = try preparedRunRequest(backend, input_bytes, input_bytes, batch, sequence, scratch);
    request.pre_admitted_host_bytes = 0;
    request.host_preprocess_bytes = try std.math.add(usize, request.host_preprocess_bytes, decoded_bytes);
    return request;
}

pub fn imageRunRequest(backend: @import("../backends/backends.zig").BackendType, input_bytes: usize, retained_bytes: usize, batch: usize, vision_scratch: usize) @import("../backends/session.zig").RunRequest {
    return .{ .batch = batch, .sequence = raster_max_sequence, .input_bytes = input_bytes, .pre_admitted_host_bytes = retained_bytes, .host_preprocess_bytes = 512 * 1024 * 1024, .workspace_bytes = @max(vision_scratch, if (backend == .metal) @as(usize, 768 * 1024 * 1024) else @as(usize, 512 * 1024 * 1024)), .output_bytes = batch * 768 * 4 };
}

fn isPreparationPressure(failure: anyerror) bool {
    return failure == error.MemoryBudgetExceeded or failure == error.ResourceLimitExceeded or failure == error.ResourceTemporarilyUnavailable;
}

/// The prepared contract prevents unreserved dense attention/FFN fallback.
/// Any declined plan drains its frames and partial results before this catch.
/// An independent supplement owns the legacy peak until projection completes;
/// never release/reacquire the base or reclaim model caches under its provider.
fn projectPreparedImages(comptime resident: bool, cb: *const ComputeBackend, a: std.mem.Allocator, pixels: []const projector.PreparedImage, scratch: usize, permit: *RunPermit, control: Control, timings: ?*MediaTimings, bounded: ?*@import("../runtime/bounded_allocator.zig").BoundedAllocator, retained_workspace: usize, credit: ?*const RunPermit) !(if (resident) projector.ResidentProjectedImages else projector.ProjectedImages) {
    const Run = struct {
        fn call(backend: *const ComputeBackend, allocator: std.mem.Allocator, images: []const projector.PreparedImage, options: projector.ImageBatchOptions) !(if (resident) projector.ResidentProjectedImages else projector.ProjectedImages) {
            if (resident) return projector.encodePreparedEmbeddingGemma2ImagesResident(backend, allocator, images, options);
            return projector.encodePreparedEmbeddingGemma2Images(backend, allocator, images, options);
        }
    };
    const previous_limit = if (bounded) |allocator| allocator.limit else 0;
    defer if (bounded) |allocator| {
        allocator.limit = previous_limit;
    };
    const prepared = boundedVision(permit.session.backend());
    if (prepared) if (timings) |t| {
        t.prepared_vision_attempts += 1;
    };
    var request = permit.request orelse return error.InvalidAdmissionLeaseReduction;
    request.workspace_bytes = @max(request.workspace_bytes, try std.math.add(usize, scratch, retained_workspace));
    var phase = if (permit.session.run_admission) |admission| try admitFencedPhase(cb, permit, credit, try admission.estimateRequest(request, permit.session.outputInfo()), control, timings) else RunPermit{ .session = permit.session, .lease = null };
    defer phase.deinit();
    return Run.call(cb, a, pixels, .{ .max_scratch_bytes = scratch, .require_prepared_vision = prepared }) catch |err| {
        if (err != error.PreparedVisionUnavailable) return err;
        if (cb.decoderRuntimeHasActiveFrame() or cb.decoderRuntimeHasSubmittedFrame()) return error.PreparedVisionFallbackBusy;
        if (timings) |t| t.vision_plan_fallbacks += 1;
        try control.check();
        const legacy = try projector.embeddingGemma2PreparedScratch(pixels, 2 * 1024 * 1024 * 1024);
        phase.deinit();
        request.workspace_bytes = @max(request.workspace_bytes, try std.math.add(usize, legacy, retained_workspace));
        request.host_preprocess_bytes = @max(request.host_preprocess_bytes, 512 * 1024 * 1024);
        var supplement = if (permit.session.run_admission) |admission| try admitFencedPhase(cb, permit, credit, try admission.estimateRequest(request, permit.session.outputInfo()), control, timings) else @import("../backends/session.zig").RunPermit{ .session = permit.session, .lease = null };
        defer supplement.deinit();
        if (bounded) |allocator| allocator.limit = @max(allocator.limit, @as(usize, 512 * 1024 * 1024));
        return Run.call(cb, a, pixels, .{ .max_scratch_bytes = legacy });
    };
}

pub fn boundedVision(backend: @import("../backends/backends.zig").BackendType) bool {
    return backend == .metal and !platform.env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_DISABLE_VISION_PLAN");
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
    const vision_scratch = if (boundedVision(session.backend())) try projector.embeddingGemma2PreparedPlanScratch(slot.pixels[0..slot.retained]) else try projector.embeddingGemma2PreparedScratch(slot.pixels[0..slot.retained], vision_limit);
    var sequence: usize = 1;
    for (slot.pixels[0..slot.retained]) |pixels| sequence = @max(sequence, pixels.geometry.tokenCount() + 4);
    const request = try preparedRunRequest(session.backend(), slot.input_bytes, slot.retained_bytes, slot.retained, sequence, vision_scratch);
    if (slot.bounded.live > request.host_preprocess_bytes) return error.MemoryBudgetExceeded;
    slot.bounded.limit = @min(@as(usize, 512 * 1024 * 1024), request.host_preprocess_bytes);
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
    const transfer_before = MediaTransferSnapshot.init(&managed.backend);
    defer observeResidentTransfers(&managed.backend, transfer_before, options);
    const vision_started = mediaNow();
    if (residentImagesEnabled(&managed.backend, options)) {
        if (options.timings) |t| t.resident_cohorts += 1;
        var resident = try projectPreparedImages(true, &managed.backend, a, slot.pixels[0..slot.retained], vision_scratch, &permit, control, options.timings, &slot.bounded, 0, null);
        defer resident.deinit();
        if (options.timings) |t| t.vision_ns += mediaNow() - vision_started;
        try releaseImagePreparation(slot.pixels[0..slot.retained], &slot.permit.?, &permit);
        slot.retained_bytes = slot.input_bytes;
        const compact = try runResidentImageBatch(a, &managed.backend, cfg, tokenizer, &resident, options, &permit);
        defer a.free(compact);
        for (slot.indexes[0..slot.retained], compact) |index, vector| vectors[index] = vector;
        try control.check();
        return vectors;
    }
    if (options.timings) |t| t.host_cohorts += 1;
    var projected = try projectPreparedImages(false, &managed.backend, a, slot.pixels[0..slot.retained], vision_scratch, &permit, control, options.timings, &slot.bounded, 0, null);
    defer projected.deinit();
    try releaseImagePreparation(slot.pixels[0..slot.retained], &slot.permit.?, &permit);
    slot.retained_bytes = slot.input_bytes;
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
    const compact = try runPreparedBatchTimed(a, &managed.backend, cfg, groups[0..initialized], options, &permit);
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
    const vision_limit: usize = if (session.backend() == .metal or session.backend() == .cuda) 2 * 1024 * 1024 * 1024 else 512 * 1024 * 1024;
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
        pixels[retained] = projector.prepareEmbeddingGemma2ImageForBackend(a, bytes, session.backend()) catch |err| {
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
    for (pixels[0..retained]) |item| retained_bytes = std.math.add(usize, retained_bytes, item.retainedBytes()) catch return error.ResourceLimitExceeded;
    try host_permit.retainHostBytes(retained_bytes);
    const vision_scratch = if (boundedVision(session.backend())) try projector.embeddingGemma2PreparedPlanScratch(pixels[0..retained]) else try projector.embeddingGemma2PreparedScratch(pixels[0..retained], vision_limit);
    const request = try preparedImageRunRequest(session.backend(), input_bytes, retained_bytes, pixels[0..retained], options.max_tokens, vision_scratch);
    var execution_waiter = Scheduler.Waiter{};
    try enterModel(session, lock, control, &execution_waiter);
    defer leaveModel(lock, &execution_waiter);
    var permit = try admitWithVisionCacheReclaim(session, request, control);
    defer permit.deinit();
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    const transfer_before = MediaTransferSnapshot.init(&managed.backend);
    defer observeResidentTransfers(&managed.backend, transfer_before, options);
    if (residentImagesEnabled(&managed.backend, options)) {
        if (options.timings) |t| t.resident_cohorts += 1;
        const started = mediaNow();
        var resident = try projectPreparedImages(true, &managed.backend, a, pixels[0..retained], vision_scratch, &permit, control, options.timings, null, 0, null);
        defer resident.deinit();
        if (options.timings) |t| t.vision_ns += mediaNow() - started;
        try releaseImagePreparation(pixels[0..retained], &host_permit, &permit);
        const compact = try runResidentImageBatch(a, &managed.backend, cfg, tokenizer, &resident, options, &permit);
        defer a.free(compact);
        for (indexes[0..retained], compact) |index, vector| vectors[index] = vector;
        try control.check();
        return vectors;
    }
    if (options.timings) |t| t.host_cohorts += 1;
    const vision_started = mediaNow();
    var projected = try projectPreparedImages(false, &managed.backend, a, pixels[0..retained], vision_scratch, &permit, control, options.timings, null, 0, null);
    defer projected.deinit();
    try releaseImagePreparation(pixels[0..retained], &host_permit, &permit);
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
    const compact = try runPreparedBatchTimed(a, &managed.backend, cfg, prepared[0..initialized], options, &permit);
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
    const vision_limit: usize = if (session.backend() == .metal or session.backend() == .cuda) 2 * 1024 * 1024 * 1024 else 512 * 1024 * 1024;
    _ = try projector.embeddingGemma2RasterScratch(rasters, vision_limit);
    var host_permit = try session.admitHostPreprocess(try std.math.add(usize, input_bytes, try rasterPreparationBytes(session.backend(), rasters)));
    defer host_permit.deinit();
    const image_scope = @import("antfly_image").work_control.Scope.enter(control.imageWorkControl());
    defer image_scope.deinit();
    const preprocess_started = mediaNow();
    var pixels: [raster_max_batch]projector.PreparedImage = undefined;
    var pixel_count: usize = 0;
    defer for (pixels[0..pixel_count]) |item| item.deinit();
    for (rasters) |raster| {
        try control.check();
        pixels[pixel_count] = try projector.prepareEmbeddingGemma2RasterForBackend(a, raster, session.backend());
        pixel_count += 1;
    }
    if (options.timings) |t| t.preprocess_ns += mediaNow() - preprocess_started;
    var retained_bytes = input_bytes;
    for (pixels[0..pixel_count]) |item| retained_bytes = try std.math.add(usize, retained_bytes, item.retainedBytes());
    try host_permit.retainHostBytes(retained_bytes);
    const vision_scratch = if (boundedVision(session.backend())) try projector.embeddingGemma2PreparedPlanScratch(pixels[0..pixel_count]) else try projector.embeddingGemma2PreparedScratch(pixels[0..pixel_count], vision_limit);
    const request = try preparedImageRunRequest(session.backend(), input_bytes, retained_bytes, pixels[0..pixel_count], options.max_tokens, vision_scratch);
    var execution_waiter = Scheduler.Waiter{};
    try enterModel(session, lock, control, &execution_waiter);
    defer leaveModel(lock, &execution_waiter);
    var permit = try admitWithVisionCacheReclaim(session, request, control);
    defer permit.deinit();
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    const transfer_before = MediaTransferSnapshot.init(&managed.backend);
    defer observeResidentTransfers(&managed.backend, transfer_before, options);
    if (residentImagesEnabled(&managed.backend, options)) {
        if (options.timings) |t| t.resident_cohorts += 1;
        const started = mediaNow();
        var resident = try projectPreparedImages(true, &managed.backend, a, pixels[0..pixel_count], vision_scratch, &permit, control, options.timings, null, 0, null);
        defer resident.deinit();
        if (options.timings) |t| t.vision_ns += mediaNow() - started;
        try releaseImagePreparation(pixels[0..pixel_count], &host_permit, &permit);
        const vectors = try runResidentImageBatch(a, &managed.backend, cfg, tokenizer, &resident, options, &permit);
        errdefer {
            for (vectors) |vector| a.free(vector);
            a.free(vectors);
        }
        try control.check();
        return vectors;
    }
    if (options.timings) |t| t.host_cohorts += 1;
    const vision_started = mediaNow();
    var projected = try projectPreparedImages(false, &managed.backend, a, pixels[0..pixel_count], vision_scratch, &permit, control, options.timings, null, 0, null);
    defer projected.deinit();
    try releaseImagePreparation(pixels[0..pixel_count], &host_permit, &permit);
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
    const vectors = try runPreparedBatchTimed(a, &managed.backend, cfg, prepared[0..initialized], options, &permit);
    errdefer {
        for (vectors) |vector| a.free(vector);
        a.free(vectors);
    }
    try control.check();
    return vectors;
}

const MediaTransferSnapshot = struct {
    metal: @import("../ops/ops.zig").NativeQuantTimingStats,
    cuda: @import("../ops/ops.zig").TrainingRuntimeStats,

    fn init(cb: *const ComputeBackend) MediaTransferSnapshot {
        return .{ .metal = cb.debugTimingSnapshot().provider, .cuda = cb.trainingRuntimeStats() };
    }
};

fn observeResidentTransfers(cb: *const ComputeBackend, before: MediaTransferSnapshot, options: Options) void {
    if (options.timings) |t| {
        const after = cb.debugTimingSnapshot().provider;
        if (cb.kind() == .metal) {
            t.gpu_frame_ns += after.decoder_runtime_frame_gpu_nanos -| before.metal.decoder_runtime_frame_gpu_nanos;
            t.frame_submits += after.decoder_runtime_frame_submits -| before.metal.decoder_runtime_frame_submits;
            t.frame_wait_ns += after.decoder_runtime_frame_wait_nanos -| before.metal.decoder_runtime_frame_wait_nanos;
            t.download_bytes = (t.download_bytes orelse 0) + after.metal_tensor_host_mirror_download_bytes - before.metal.metal_tensor_host_mirror_download_bytes;
            t.readbacks = (t.readbacks orelse 0) + after.metal_tensor_to_host_device_calls - before.metal.metal_tensor_to_host_device_calls;
            t.device_owned_peak_bytes = @max(t.device_owned_peak_bytes orelse 0, after.metal_tensor_device_owned_peak_live_bytes);
        } else if (cb.kind() == .cuda) {
            const runtime = cb.trainingRuntimeStats();
            t.download_bytes = (t.download_bytes orelse 0) + runtime.d2h_bytes - before.cuda.d2h_bytes;
            t.readbacks = (t.readbacks orelse 0) + runtime.download_events - before.cuda.download_events;
        }
    }
}

/// Tokenization stays on CPU; projected pixels and prompt embedding rows stay
/// on their creating backend through the encoder. Padding never pools into a row.
fn runResidentImageBatch(a: std.mem.Allocator, cb: *const ComputeBackend, cfg: encoder.Config, tokenizer: Tokenizer, projected: *const projector.ResidentProjectedImages, options: Options, permit: *RunPermit) ![][]f32 {
    return runResidentMediaBatch(a, cb, cfg, tokenizer, projected, options, false, permit);
}
fn runResidentMediaBatch(a: std.mem.Allocator, cb: *const ComputeBackend, cfg: encoder.Config, tokenizer: Tokenizer, projected: *const projector.ResidentProjectedImages, options: Options, is_audio: bool, permit: ?*RunPermit) ![][]f32 {
    return runResidentMediaBatchObserved(a, cb, cfg, tokenizer, projected, options, is_audio, permit, null);
}
fn mediaTemplateKey(tokenizer: Tokenizer, count: usize, max_sequence: usize, is_audio: bool) ?u64 {
    const generation = tokenizer.cache_identity orelse return null;
    var hash = std.hash.Wyhash.init(0x6d65646961746f6b);
    const identity = .{ @intFromPtr(tokenizer.ptr), @intFromPtr(tokenizer.vtable), generation };
    inline for (identity) |value| hash.update(std.mem.asBytes(&value));
    hash.update(std.mem.asBytes(&count));
    hash.update(std.mem.asBytes(&max_sequence));
    hash.update(std.mem.asBytes(&is_audio));
    return hash.final();
}
fn runResidentMediaBatchObserved(a: std.mem.Allocator, cb: *const ComputeBackend, cfg: encoder.Config, tokenizer: Tokenizer, projected: *const projector.ResidentProjectedImages, options: Options, is_audio: bool, permit: ?*RunPermit, input_tokens: ?[]usize) ![][]f32 {
    const max_sequence: usize = @min(options.max_tokens, if (is_audio) @as(usize, 1024) else raster_max_sequence);
    const marker: i64 = if (is_audio) 258881 else 258880;
    const started = mediaNow();
    const batch = projected.tokens_per_image.len;
    if (batch == 0 or batch > raster_max_batch or projected.hidden_size != 512) return error.InvalidInputShape;
    if (input_tokens) |counts| if (counts.len != batch) return error.InvalidInputShape;
    if (cb.vtable.embeddingModelPrepare) |prepare| try prepare(cb.ptr, cfg.num_hidden_layers);
    var tokens: [raster_max_batch]PreparedGroup = undefined;
    var initialized: usize = 0;
    defer for (tokens[0..initialized]) |row| row.deinit(a);
    for (projected.tokens_per_image) |count| {
        if (count == 0 or count > (if (is_audio) @as(usize, 1020) else 280)) return error.InvalidTensorShape;
        const template_key = mediaTemplateKey(tokenizer, count, max_sequence, is_audio);
        const template_cache = if (template_key != null) cb.vtable.embeddingMediaTemplate else null;
        if (template_cache) |cache| if (try cache(cb.ptr, template_key.?, null, null)) |template| {
            tokens[initialized] = .{ .ids = template.ids, .mask = template.mask, .embeddings = null, .borrowed_tokens = true };
            if (input_tokens) |counts| counts[initialized] = template.ids.len;
            initialized += 1;
            continue;
        };
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
        if (template_cache) |cache| {
            if (try cache(cb.ptr, template_key.?, ids, mask)) |template| {
                a.free(ids);
                a.free(mask);
                tokens[initialized] = .{ .ids = template.ids, .mask = template.mask, .embeddings = null, .borrowed_tokens = true };
            } else tokens[initialized] = .{ .ids = ids, .mask = mask, .embeddings = null };
        } else tokens[initialized] = .{ .ids = ids, .mask = mask, .embeddings = null };
        if (input_tokens) |counts| counts[initialized] = length;
        initialized += 1;
    }
    const batch_inputs = try packResidentMediaRows(a, cb, tokens[0..initialized], projected.embeddings, projected.tokens_per_image, marker, max_sequence);
    defer batch_inputs.deinit(a, cb);
    if (options.timings) |t| t.pack_ns += mediaNow() - started;
    const encoder_started = mediaNow();
    if (permit) |phase| try retainPackedPhase(phase, batch, batch_inputs);
    if (comptime @import("build_options").enable_cuda) {
        if (cb.kind() == .cuda) {
            const compute: *@import("../ops/cuda/cuda_compute.zig").CudaCompute = @ptrCast(@alignCast(cb.ptr));
            const flat = try compute.runEmbeddingGemma2(a, cfg.resident orelse return error.UnsupportedEmbeddingGemma2Backend, .{ .resident_embeddings = batch_inputs.embeddings }, batch_inputs.mask, batch, batch_inputs.sequence, options.dimensions, cb.execution_control);
            defer a.free(flat);
            const vectors = try a.alloc([]f32, batch);
            var copied_vectors: usize = 0;
            errdefer {
                for (vectors[0..copied_vectors]) |vector| a.free(vector);
                a.free(vectors);
            }
            for (vectors, 0..) |*vector, index| {
                vector.* = try a.dupe(f32, flat[index * options.dimensions ..][0..options.dimensions]);
                copied_vectors += 1;
            }
            if (options.timings) |t| t.encoder_ns += mediaNow() - encoder_started;
            return vectors;
        }
    }
    const output = try encoder.forwardPackedPooledEmbeddingsCT(cb, a, cfg, batch_inputs.embeddings, batch_inputs.mask, batch, batch_inputs.sequence, options.dimensions, batch_inputs.compact_input);
    defer cb.free(output);
    if (options.timings) |t| t.encoder_ns += mediaNow() - encoder_started;
    const pool_started = mediaNow();
    const vectors = try readPooledBatch(a, cb, output, batch, options.dimensions);
    if (options.timings) |t| t.pool_readback_ns += mediaNow() - pool_started;
    return vectors;
}

const ResidentPackedGroups = struct {
    embeddings: @import("../ops/ops.zig").CT,
    mask: []i64,
    sequence: usize,
    compact_input: bool = false,
    fn deinit(self: @This(), a: std.mem.Allocator, cb: *const ComputeBackend) void {
        cb.free(self.embeddings);
        a.free(self.mask);
    }
};

fn packResidentImageRows(a: std.mem.Allocator, cb: *const ComputeBackend, rows: []const PreparedGroup, projected: @import("../ops/ops.zig").CT, lengths: []const usize) !ResidentPackedGroups {
    return packResidentMediaRows(a, cb, rows, projected, lengths, 258880, raster_max_sequence);
}
fn packResidentMediaRows(a: std.mem.Allocator, cb: *const ComputeBackend, rows: []const PreparedGroup, projected: @import("../ops/ops.zig").CT, lengths: []const usize, marker: i64, max_sequence: usize) !ResidentPackedGroups {
    return packResidentMediaRowsPolicy(a, cb, rows, projected, lengths, marker, max_sequence, true);
}
fn packResidentMediaRowsPolicy(a: std.mem.Allocator, cb: *const ComputeBackend, rows: []const PreparedGroup, projected: CT, lengths: []const usize, marker: i64, max_sequence: usize, allow_compact: bool) !ResidentPackedGroups {
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
    const compact_lookup = cb.kind() == .metal and !platform.env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_METAL_DISABLE_FOLLOWUP") and !platform.env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_METAL_DISABLE_MEDIA_TEMPLATES");
    const compact_input = allow_compact and compact_lookup and encoder.compactRowsEnabled(cb.kind() == .metal) and cb.vtable.embeddingComposeRows != null;
    var active_rows: usize = 0;
    for (mask) |value| active_rows += @intFromBool(value != 0);
    const compact_ids = if (compact_lookup) blk: {
        var count: usize = 0;
        for (ids, mask) |id, valid| count += @intFromBool(id != marker and (!compact_input or valid != 0));
        const selected = try a.alloc(i64, count);
        var cursor: usize = 0;
        for (ids, mask) |id, valid| if (id != marker and (!compact_input or valid != 0)) {
            selected[cursor] = id;
            cursor += 1;
        };
        break :blk selected;
    } else ids;
    defer if (compact_lookup) a.free(compact_ids);
    const raw: ?CT = if (compact_ids.len != 0) try cb.embeddingLookup(weight, compact_ids, compact_ids.len, 512) else null;
    defer if (raw) |tensor| cb.free(tensor);
    const scaled: ?CT = if (raw) |tensor| (try cb.multiplyScalar(tensor, if (cb.kind() == .cuda) @as(f32, 22.625) else @sqrt(@as(f32, 512)))) orelse return error.ResidentImageOperationUnavailable else null;
    defer if (compact_lookup) if (scaled) |tensor| cb.free(tensor);
    if (compact_lookup) if (cb.vtable.embeddingComposeRows) |compose| {
        const map = try a.alloc(i32, if (compact_input) active_rows else ids.len);
        defer a.free(map);
        var output_row: usize = 0;
        var wrapper_index: usize = 0;
        var media_offset: usize = 0;
        for (rows, lengths, 0..) |row, count, item| {
            var found: usize = 0;
            for (0..sequence) |pos| {
                const original = item * sequence + pos;
                if (compact_input and mask[original] == 0) continue;
                const destination = if (compact_input) output_row else original;
                output_row += 1;
                if (ids[original] == marker) {
                    if (pos >= row.ids.len or row.mask[pos] == 0 or found >= count) return error.InvalidEmbeddingGroup;
                    map[destination] = -@as(i32, @intCast(media_offset + found + 1));
                    found += 1;
                } else {
                    map[destination] = @intCast(wrapper_index);
                    wrapper_index += 1;
                }
            }
            if (found != count or count == 0) return error.InvalidEmbeddingGroup;
            media_offset += count;
        }
        if (try compose(cb.ptr, scaled, projected, map, 512)) |input| return .{ .embeddings = input, .mask = mask, .sequence = sequence, .compact_input = compact_input };
        if (compact_input) {
            const fallback = try packResidentMediaRowsPolicy(a, cb, rows, projected, lengths, marker, max_sequence, false);
            a.free(mask);
            return fallback;
        }
    };
    // Resident media supplies every marker row; retain original lookup values
    // for all other positions, including masked padding, with exact rounding.
    const input = if (compact_lookup) (try cb.allocUninitF32Shape(&.{ @intCast(ids.len), 512 })) orelse return error.ResidentImageOperationUnavailable else try cb.ensureDeviceResidentOwned(scaled.?);
    errdefer cb.free(input);
    if (compact_lookup) {
        var source: usize = 0;
        var cursor: usize = 0;
        while (cursor < ids.len) {
            if (ids[cursor] == marker) {
                cursor += 1;
                continue;
            }
            const first = cursor;
            while (cursor < ids.len and ids[cursor] != marker) : (cursor += 1) {}
            const count = cursor - first;
            if (!try cb.copyRows2D(a, input, first, scaled.?, source, count, 512)) return error.ResidentImageOperationUnavailable;
            source += count;
        }
    }
    var source_offset: usize = 0;
    for (rows, lengths, 0..) |row, count, index| {
        var found: usize = 0;
        var r: usize = 0;
        while (r < row.ids.len) {
            if (row.ids[r] != marker) {
                r += 1;
                continue;
            }
            const first = r;
            while (r < row.ids.len and row.ids[r] == marker) : (r += 1) {
                if (row.mask[r] == 0) return error.InvalidEmbeddingGroup;
            }
            const run = r - first;
            if (found + run > count) return error.InvalidEmbeddingGroup;
            if (!try cb.copyRows2D(a, input, index * sequence + first, projected, source_offset + found, run, 512)) return error.ResidentImageOperationUnavailable;
            found += run;
        }
        if (found != count or count == 0) return error.InvalidEmbeddingGroup;
        source_offset += count;
    }
    return .{ .embeddings = input, .mask = mask, .sequence = sequence };
}

fn readPooledBatch(a: std.mem.Allocator, cb: *const ComputeBackend, output: CT, batch: usize, dimensions: usize) ![][]f32 {
    const data = try cb.toFloat32(output, a);
    defer a.free(data);
    if (data.len != batch * dimensions) return error.InvalidTensorShape;
    const vectors = try a.alloc([]f32, batch);
    var initialized: usize = 0;
    errdefer {
        for (vectors[0..initialized]) |vector| a.free(vector);
        a.free(vectors);
    }
    for (vectors, 0..) |*vector, item| {
        var nonzero = false;
        for (data[item * dimensions ..][0..dimensions]) |value| {
            if (!std.math.isFinite(value)) return error.InvalidEmbeddingDecisionOutput;
            nonzero = nonzero or value != 0;
        }
        if (!nonzero) return error.InvalidEmbeddingDecisionOutput;
        vector.* = try a.dupe(f32, data[item * dimensions ..][0..dimensions]);
        initialized += 1;
    }
    return vectors;
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
            const mean = try cb.primReduceMean(active, &.{0}, &.{ @intCast(count), 768 });
            if (dimensions == 768) break :blk mean;
            defer cb.free(mean);
            // Apply Matryoshka truncation before the download, matching the
            // fused masked-mean path's final-vector-only transfer contract.
            break :blk try cb.sliceLastDim(mean, 0, dimensions);
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

fn runPreparedBatchTimed(a: std.mem.Allocator, cb: *const ComputeBackend, cfg: encoder.Config, rows: []const PreparedGroup, options: Options, permit: *RunPermit) ![][]f32 {
    if (encoder.compactRowsEnabled(cb.kind() == .metal)) {
        const packed_rows = try packCompactPreparedRows(a, cb, rows);
        defer packed_rows.deinit(a, cb);
        try retainPackedPhase(permit, rows.len, packed_rows);
        const output = try encoder.forwardPackedPooledEmbeddingsCT(cb, a, cfg, packed_rows.embeddings, packed_rows.mask, rows.len, packed_rows.sequence, options.dimensions, true);
        defer cb.free(output);
        return readPooledBatch(a, cb, output, rows.len, options.dimensions);
    }
    const started = mediaNow();
    const batch_inputs = try packPreparedGroups(a, rows);
    defer batch_inputs.deinit(a);
    const host = try cb.fromFloat32Shape(batch_inputs.embeddings, &.{ @intCast(rows.len * batch_inputs.sequence), 512 });
    const input = try cb.ensureDeviceResidentOwned(host);
    defer cb.free(input);
    if (options.timings) |t| t.pack_ns += mediaNow() - started;
    const encoder_started = mediaNow();
    try retainPreparedPhase(permit, rows.len, batch_inputs.sequence);
    const output = try encoder.forwardPooledEmbeddingsCT(cb, a, cfg, input, batch_inputs.mask, rows.len, batch_inputs.sequence, options.dimensions);
    defer cb.free(output);
    if (options.timings) |t| t.encoder_ns += mediaNow() - encoder_started;
    const pool_started = mediaNow();
    const vectors = try readPooledBatch(a, cb, output, rows.len, options.dimensions);
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
    // Preflight validates the layout before a session is acquired. The
    // execution path additionally checks the active backend's capabilities.
    return validateGroupContent(group, options, true);
}

fn validateCudaGroup(group: Group, options: Options) !void {
    try validateGroupContent(group, options, true);
    for (group.content) |part| switch (part) {
        .video => if (group.content.len != 1 or group.title != null) return error.InvalidEmbeddingGroup,
        .sampled_video, .pcm => return error.UnsupportedInput,
        else => {},
    };
}

fn validateGroupContent(group: Group, options: Options, allow_manual_markers: bool) !void {
    _ = try encoder.taskPrefix(options.task_type);
    if (!encoder.validDimension(options.dimensions)) return error.InvalidEmbeddingDimensions;
    if (options.max_tokens == 0 or options.max_tokens > encoder.max_tokens) return error.InvalidEmbeddingTokenLimit;
    if (group.content.len == 0 or group.content.len > 64) return error.InvalidEmbeddingGroup;
    var has_text = false;
    var bytes: usize = 0;
    var raster_bytes: usize = 0;
    var images: usize = 0;
    var audios: usize = 0;
    var image_markers: usize = 0;
    var audio_markers: usize = 0;
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
        switch (part) {
            .image, .raster => images += 1,
            .audio, .pcm => audios += 1,
            else => {},
        }
        if (value.len == 0 and !(part == .sampled_video and (part.sampled_video.webm != null or part.sampled_video.reader != null))) return error.EmptyEmbeddingInput;
        if (part == .raster) raster_bytes = std.math.add(usize, raster_bytes, value.len) catch return error.ResourceLimitExceeded else bytes = std.math.add(usize, bytes, value.len) catch return error.ResourceLimitExceeded;
        if (part == .text) {
            has_text = true;
            if (!std.unicode.utf8ValidateSlice(value)) return error.InvalidUtf8;
            var remaining = value;
            while (std.mem.indexOfScalar(u8, remaining, '<')) |at| {
                const tail = remaining[at..];
                if (allow_manual_markers and std.mem.startsWith(u8, tail, "<|image|>")) {
                    image_markers += 1;
                    remaining = tail["<|image|>".len..];
                    continue;
                }
                if (allow_manual_markers and std.mem.startsWith(u8, tail, "<|audio|>")) {
                    audio_markers += 1;
                    remaining = tail["<|audio|>".len..];
                    continue;
                }
                for ([_][]const u8{ "<|image", "<image|>", "<|audio", "<audio|>", "<|video|>" }) |marker| if (std.mem.startsWith(u8, tail, marker)) return error.InvalidEmbeddingGroup;
                remaining = tail[1..];
            }
        }
    }
    // The resident CUDA frontend expands explicit placeholders itself. Once
    // any is present, every supplied media item must be referenced exactly
    // once; generated wrapper tokens remain forbidden in caller text.
    if (image_markers + audio_markers > 0 and (image_markers != images or audio_markers != audios)) return error.MediaPlaceholderMismatch;
    if (bytes > (if (group.content.len == 1 and group.content[0] == .video) @as(usize, 256 * 1024 * 1024) else 64 * 1024 * 1024) or raster_bytes > 512 * 1024 * 1024) return error.ResourceLimitExceeded;
    if (group.title) |title| if (!has_text or !std.mem.eql(u8, options.task_type, "RETRIEVAL_DOCUMENT") or !std.unicode.utf8ValidateSlice(title) or title.len > 8192 or std.mem.indexOf(u8, title, "<") != null) return error.InvalidEmbeddingTitle;
}

test "embeddinggemma2 CUDA groups admit complete manual media layouts and reject malformed ones" {
    try validateGroupContent(.{ .content = &.{ .{ .text = "before <|image|> after" }, .{ .image = "image" } } }, .{}, true);
    try validateGroupContent(.{ .content = &.{ .{ .text = "<|audio|> then <|image|>" }, .{ .image = "image" }, .{ .audio = "audio" } } }, .{}, true);
    try std.testing.expectError(error.MediaPlaceholderMismatch, validateGroupContent(.{ .content = &.{.{ .text = "<|image|>" }} }, .{}, true));
    try std.testing.expectError(error.MediaPlaceholderMismatch, validateGroupContent(.{ .content = &.{ .{ .text = "<|image|><|image|>" }, .{ .image = "image" } } }, .{}, true));
    try std.testing.expectError(error.MediaPlaceholderMismatch, validateGroupContent(.{ .content = &.{ .{ .text = "<|image|>" }, .{ .image = "image" }, .{ .audio = "audio" } } }, .{}, true));
    for ([_][]const u8{ "<|image>", "<image|>", "<|audio>", "<audio|>", "<|video|>" }) |marker|
        try std.testing.expectError(error.InvalidEmbeddingGroup, validateGroupContent(.{ .content = &.{ .{ .text = marker }, .{ .image = "image" } } }, .{}, true));
    try validateGroup(.{ .content = &.{ .{ .text = "<|image|>" }, .{ .image = "image" } } }, .{});
    try std.testing.expectError(error.InvalidEmbeddingGroup, validateGroupContent(.{ .content = &.{ .{ .text = "<|image|>" }, .{ .image = "image" } } }, .{}, false));
}

/// Encoder benchmark and serving share this boundary: prepared IDs/mask through
/// synchronized final vector readback. Loading, tokenization and HTTP stay out.
pub fn preparedText(cb: *const ComputeBackend, a: std.mem.Allocator, cfg: encoder.Config, ids: []const i64, mask: []const i64, dimensions: usize) ![]f32 {
    const vectors = try preparedTexts(cb, a, cfg, ids, mask, 1, ids.len, dimensions);
    defer a.free(vectors);
    return vectors[0];
}

/// Shared compact output path for flat text and ordered media groups. Caller
/// admission covers the physical batch; large padded inputs split upstream.
pub fn preparedTexts(cb: *const ComputeBackend, a: std.mem.Allocator, cfg: encoder.Config, ids: []const i64, mask: []const i64, batch: usize, sequence: usize, dimensions: usize) ![][]f32 {
    if (!encoder.validDimension(dimensions)) return error.InvalidEmbeddingDimensions;
    const rows = std.math.mul(usize, batch, sequence) catch return error.InvalidEmbeddingInputLength;
    var compact_input = encoder.compactRowsEnabled(cb.kind() == .metal);
    if (batch == 0 or (compact_input and batch > 64) or sequence == 0 or sequence > 8192 or (!compact_input and rows > 8192) or ids.len != rows or mask.len != rows) return error.InvalidEmbeddingInputLength;
    var active: usize = 0;
    for (mask) |value| {
        if (value != 0 and value != 1) return error.InvalidAttentionMask;
        active += @intFromBool(value != 0);
    }
    if (active == 0) return error.EmptyEmbeddingInput;
    if (compact_input and active > 8192) return error.InvalidEmbeddingInputLength;
    if (batch == 1 and active == rows) compact_input = false;
    const selected = if (compact_input) try a.alloc(i64, active) else null;
    defer if (selected) |values| a.free(values);
    if (selected) |values| {
        var at: usize = 0;
        for (ids, mask) |id, valid| if (valid != 0) {
            values[at] = id;
            at += 1;
        };
    }
    for (ids) |id| if (id < 0 or id >= cfg.vocab_size) return error.InvalidTokenId;
    const ew = try cb.getWeight("language_model.embed_tokens.weight");
    defer cb.free(ew);
    const raw = try cb.embeddingLookup(ew, selected orelse ids, if (compact_input) active else rows, 512);
    defer cb.free(raw);
    const input = (try cb.multiplyScalar(raw, @sqrt(@as(f32, 512)))) orelse return error.EmbeddingGemma2OperationUnavailable;
    defer cb.free(input);
    const output = try encoder.forwardPackedPooledEmbeddingsCT(cb, a, cfg, input, mask, batch, sequence, dimensions, compact_input);
    defer cb.free(output);
    return readPooledBatch(a, cb, output, batch, dimensions);
}

test "embeddinggemma2 grouped tokenization rejects overflow without truncating" {
    const Mock = struct {
        tokens: usize,
        fn encode(ptr: *anyopaque, a: std.mem.Allocator, _: []const u8, max_length: usize) !@import("inference_tokenizer").EncodeResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const count = @min(self.tokens, max_length);
            const ids = try a.alloc(i32, count);
            errdefer a.free(ids);
            const mask = try a.alloc(i32, count);
            @memset(ids, 3);
            @memset(mask, 1);
            return .{ .ids = ids, .attention_mask = mask, .allocator = a };
        }
    };
    var mock = Mock{ .tokens = 512 };
    var vtable: Tokenizer.VTable = undefined;
    vtable.encodeForModel = Mock.encode;
    const tokenizer = Tokenizer{ .ptr = &mock, .vtable = &vtable };
    const cb: ComputeBackend = undefined; // Text-only preparation does not invoke compute.
    const group = Group{ .content = &.{.{ .text = "document" }} };
    const options = Options{ .max_tokens = 512 };
    const prepared = try prepareGroup(std.testing.allocator, &cb, tokenizer, group, options, .{});
    defer prepared.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 512), prepared.ids.len);
    for ([_]usize{ 513, 8192, 16384 }) |count| {
        mock.tokens = count;
        try std.testing.expectError(error.EmbeddingInputTooLong, prepareGroup(std.testing.allocator, &cb, tokenizer, group, options, .{}));
    }
    try std.testing.expectError(error.InvalidEmbeddingTokenLimit, validateGroup(group, .{ .max_tokens = 0 }));
    try std.testing.expectError(error.InvalidEmbeddingTokenLimit, validateGroup(group, .{ .max_tokens = encoder.max_tokens + 1 }));
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
    try std.testing.expectApproxEqAbs(@as(f32, 0.25) * (if (cb.kind() == .cuda) @as(f32, 22.625) else @sqrt(@as(f32, 512))), actual[0], 1e-6);
    // A clip's marker runs are separated by per-frame image wrappers.
    var video_ids = [_]i64{ 2, 255999, 258884, 258884, 258882, 255999, 258884, 258882, 1 };
    var video_mask: [video_ids.len]i64 = @splat(1);
    const video_rows = [_]PreparedGroup{.{ .ids = &video_ids, .mask = &video_mask, .embeddings = null }};
    const video_inputs = try packResidentMediaRows(a, &guarded, &video_rows, source, &.{3}, 258884, 8192);
    defer video_inputs.deinit(a, &cb);
    const video_actual = try cb.toFloat32(video_inputs.embeddings, a);
    defer a.free(video_actual);
    for ([_]usize{ 2, 3, 6 }, [_]f32{ 1, 2, 3 }) |r, value| for (video_actual[r * 512 ..][0..512]) |v| try std.testing.expectEqual(value, v);
    for ([_]usize{ 0, 1, 4, 5, 7, 8 }) |r| try std.testing.expectEqual(actual[0], video_actual[r * 512]);
    try std.testing.expectError(error.EmbeddingInputTooLong, packResidentMediaRows(a, &guarded, &video_rows, source, &.{3}, 258884, 8));
    video_mask[6] = 0;
    try std.testing.expectError(error.InvalidEmbeddingGroup, packResidentMediaRows(a, &guarded, &video_rows, source, &.{3}, 258884, 8192));
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
    if (cb.kind() == .cuda) try std.testing.expectEqual(@as(u64, 2 * 128 * 4), after.cuda_bytes - before.cuda_bytes);
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
    try std.testing.expectApproxEqAbs(@as(f32, 0.25) * (if (cb.kind() == .cuda) @as(f32, 22.625) else @sqrt(@as(f32, 512))), actual[0], 1e-6);
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

test "embeddinggemma2 video rejects mixed groups and invalid limits before decoding" {
    try validateCudaGroup(.{ .content = &.{.{ .video = "clip" }} }, .{});
    try std.testing.expectError(error.InvalidEmbeddingGroup, validateCudaGroup(.{ .content = &.{ .{ .text = "caption" }, .{ .video = "clip" } } }, .{}));
    try std.testing.expectError(error.InvalidEmbeddingTitle, validateCudaGroup(.{ .title = "clip", .content = &.{.{ .video = "clip" }} }, .{}));
    try std.testing.expectError(error.InvalidEmbeddingTokenLimit, validateCudaGroup(.{ .content = &.{.{ .video = "clip" }} }, .{ .max_tokens = 0 }));
}

test "embeddinggemma2 truncated video headers are input errors without masking runtime failures" {
    const media = @import("antfly_media");
    const a = std.testing.allocator;
    for ([_][]const u8{ &.{1}, @embedFile("../testdata/video/closed.mp4")[0..7], &.{ 0, 0, 0, 1, 'f', 'r', 'e', 'e' } }) |bytes| {
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "truncated" };
        if (media.mp4.Reader.init(a, &source, .{})) |parsed| {
            var reader = parsed;
            reader.deinit();
            return error.TestUnexpectedResult;
        } else |err| try std.testing.expectEqual(error.InvalidVideoInput, videoInputError(err));
        try std.testing.expectEqual(@as(usize, 0), source.retained_bytes);
    }
    for ([_]anyerror{ error.Cancelled, error.Timeout, error.OutOfMemory, error.CudaDriverError, error.NvdecUnavailable }) |err|
        try std.testing.expectEqual(err, videoInputError(err));
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

test "embeddinggemma2 video execution bounds scale with inspected shape and retain unknown media ceiling" {
    var unbound_reader: @import("antfly_media").mp4.Reader = undefined;
    const unbound_parts = [_]Part{.{ .sampled_video = .{ .reader = &unbound_reader, .sampling = .reference } }};
    try std.testing.expectEqual(@as(usize, 512 * 1024 * 1024), try preflightHostBytes(.{ .content = &unbound_parts }));
    const small = try executionBounds(true, 128, 16 * 1024 * 1024, 128 * 1024 * 1024, 0, 1536 * 1024 * 1024);
    const large = try executionBounds(true, 4096, 16 * 1024 * 1024, 128 * 1024 * 1024, 0, 1536 * 1024 * 1024);
    try std.testing.expect(small.host < large.host);
    try std.testing.expect(small.workspace < large.workspace);
    try std.testing.expectEqual(@as(usize, 128), small.sequence);
    const unknown = try executionBounds(false, 1, 0, 0, 0, 1536 * 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 8192), unknown.sequence);
    try std.testing.expectEqual(@as(usize, 512 * 1024 * 1024), unknown.host);
    try std.testing.expectEqual(@as(usize, 1536 * 1024 * 1024), unknown.workspace);
}

test "embeddinggemma2 video prepared media admission uses actual audio and image rows" {
    try std.testing.expect(videoSpeculationPressure(error.QueueFull));
    try std.testing.expect(!videoSpeculationPressure(error.MalformedVideoPacket));
    const small = try preparedRunRequest(.metal, 1024, 2048, 2, 32, 16 * 1024 * 1024);
    const large = try preparedRunRequest(.metal, 1024, 2048, 2, 1024, 16 * 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 32), small.sequence);
    try std.testing.expect(small.host_preprocess_bytes < large.host_preprocess_bytes);
    try std.testing.expect(small.workspace_bytes < large.workspace_bytes);
    try std.testing.expectEqual(@as(usize, 2 * 768 * 4), small.output_bytes.?);
    try std.testing.expectEqual(small.output_bytes.?, large.output_bytes.?);
    try std.testing.expect(small.host_preprocess_bytes + small.input_bytes >= small.pre_admitted_host_bytes);
    try std.testing.expectError(error.EmbeddingInputTooLong, preparedRunRequest(.native, 0, 0, 1, 1025, 0));
}

test "embeddinggemma2 video packed windows retain independent masks padding and resident rows" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var first_ids = [_]i64{ 2, 3 };
    var second_ids = [_]i64{ 2, 4, 3 };
    var first_mask = [_]i64{ 1, 1 };
    var second_mask = [_]i64{ 1, 1, 1 };
    var first: [2 * 512]f32 = @splat(0.25);
    var second: [3 * 512]f32 = @splat(0.75);
    const resident = try cb.fromFloat32Shape(&second, &.{ 3, 512 });
    defer cb.free(resident);
    const inputs = [_]PreparedGroup{
        .{ .ids = &first_ids, .mask = &first_mask, .embeddings = &first },
        .{ .ids = &second_ids, .mask = &second_mask, .embeddings = null, .tensor = resident, .backend = &cb },
    };
    const result = try packVideoRows(a, &cb, &inputs);
    defer result.deinit(a, &cb);
    try std.testing.expectEqualSlices(i64, &.{ 1, 1, 0, 1, 1, 1 }, result.mask);
    const values = try cb.toFloat32(result.embeddings, a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &first, values[0..first.len]);
    for (values[first.len .. 3 * 512]) |value| try std.testing.expectEqual(@as(f32, 0), value);
    try std.testing.expectEqualSlices(f32, &second, values[3 * 512 ..]);
}

test "embeddinggemma2 prepared cohorts bound padding total rows and invalid shapes" {
    try std.testing.expectEqual(@as(usize, 2), preparedCohortCount(&.{ 16, 17, 8192 }, 4));
    try std.testing.expectEqual(@as(usize, 2), preparedCohortCount(&.{ 4096, 4096, 4096 }, 4));
    try std.testing.expectEqual(@as(usize, 1), preparedCohortCount(&.{ 8192, 8192 }, 4));
    try std.testing.expectEqual(@as(usize, 0), preparedCohortCount(&.{8193}, 4));
    try std.testing.expectEqual(@as(usize, 0), preparedCohortCount(&.{0}, 4));
    try std.testing.expectEqual(@as(usize, 0), preparedCohortCount(&.{}, 4));
    try std.testing.expectEqual(@as(usize, 1), preparedCohortCount(&.{ 32, 32 }, 1));
}

test "embeddinggemma2 direct composition spans video markers and unwinds allocation failures" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            const native = @import("../ops/native_compute.zig");
            var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
            var compute = native.NativeCompute.init(a, &store, null);
            defer compute.deinit();
            const cb = compute.computeBackend();
            var ids = [_]i64{ 2, 258880, 5, 258880, 3 };
            const text_values: [5 * 512]f32 = @splat(1);
            var video: [2 * 512]f32 = undefined;
            @memset(video[0..512], 0.25);
            @memset(video[512..], -0.75);
            const text = try cb.fromFloat32Shape(&text_values, &.{ 5, 512 });
            defer cb.free(text);
            const source = try cb.fromFloat32Shape(&video, &.{ 2, 512 });
            defer cb.free(source);
            const composed = try composeDeviceTokens(a, &cb, text, &ids, &.{.{ .id = 258884, .tokens = 2, .embeddings = &.{}, .tensor = source }});
            defer cb.free(composed);
            const result = try cb.toFloat32(composed, a);
            defer a.free(result);
            for (0..5) |r| for (result[r * 512 ..][0..512]) |x| try std.testing.expectEqual(if (r == 1) @as(f32, 0.25) else if (r == 3) @as(f32, -0.75) else @sqrt(@as(f32, 512)), x);
            try std.testing.expectEqualSlices(i64, &.{ 2, 258884, 5, 258884, 3 }, &ids);
        }
    };
    try Case.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "embeddinggemma2 prepared packing embeds text IDs beside projected media without aliasing" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            const native = @import("../ops/native_compute.zig");
            var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
            defer store.deinitOwned();
            try @import("../ops/media_residency_test.zig").putWeight(&store, "language_model.embed_tokens.weight", &.{ 3, 512 }, 0.125);
            var compute = native.NativeCompute.init(a, &store, null);
            defer compute.deinit();
            const cb = compute.computeBackend();
            var text_ids = [_]i64{ 1, 2 };
            var text_mask = [_]i64{ 1, 1 };
            var media_ids = [_]i64{258880};
            var media_mask = [_]i64{1};
            var media: [512]f32 = @splat(0.5);
            const rows = [_]PreparedGroup{
                .{ .ids = &text_ids, .mask = &text_mask, .embeddings = null },
                .{ .ids = &media_ids, .mask = &media_mask, .embeddings = &media },
            };
            const packed_rows = try packVideoRows(a, &cb, &rows);
            defer packed_rows.deinit(a, &cb);
            const values = try cb.toFloat32(packed_rows.embeddings, a);
            defer a.free(values);
            for (values[0 .. 2 * 512]) |x| try std.testing.expectEqual(@as(f32, 0.125) * @sqrt(@as(f32, 512)), x);
            for (values[2 * 512 ..][0..512]) |x| try std.testing.expectEqual(@as(f32, 0.5), x);
            for (values[3 * 512 ..]) |x| try std.testing.expectEqual(@as(f32, 0), x);
            try std.testing.expectEqualSlices(i64, &.{ 1, 1, 1, 0 }, packed_rows.mask);
        }
    };
    try Case.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "embeddinggemma2 mixed preparation drains cancellation partial errors and denied allocations" {
    const tensor = @import("../backends/tensor.zig");
    const Mock = struct {
        fn run(_: *anyopaque, _: []const tensor.Tensor, _: std.mem.Allocator) anyerror![]tensor.Tensor {
            return error.InvalidInputShape;
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
    const pixels: [16]u8 = @splat(255);
    const groups = [_]Group{ .{ .content = &.{.{ .image = "bad" }} }, .{ .content = &.{.{ .raster = .{ .bytes = &pixels, .width = 2, .height = 2, .stride_bytes = 8 } }} } };
    const refs = [_]PartRef{ .{ .group = 0, .part = 0, .image = true }, .{ .group = 1, .part = 0, .image = true } };
    var canceled = MixedWave{ .session = session, .groups = &groups, .refs = &refs, .control = .{ .check_fn = Mock.canceled }, .image = true };
    _ = canceled.start();
    canceled.join();
    try std.testing.expectEqual(error.Cancelled, canceled.failure.?);
    try std.testing.expect(canceled.permit == null);
    canceled.deinit();
    var partial = MixedWave{ .session = session, .groups = &groups, .refs = &refs, .control = .{}, .image = true };
    partial.budget.backing = std.testing.allocator;
    partial.run();
    try std.testing.expect(partial.failure == null);
    try std.testing.expect(partial.errors[0] != null);
    try std.testing.expectEqual(@as(usize, 1), partial.count);
    try std.testing.expectEqual(@as(usize, 1), partial.indexes[0]);
    partial.deinit();
    var denied = MixedWave{ .session = session, .groups = &groups, .refs = refs[1..], .control = .{}, .image = true };
    denied.budget.backing = std.testing.allocator;
    denied.budget.limit = 4;
    _ = denied.start();
    denied.join();
    try std.testing.expectEqual(error.MemoryBudgetExceeded, denied.failure.?);
    denied.deinit();
}

test "embeddinggemma2 NVDEC cache admission policy is linked on portable hosts" {
    _ = @import("../ops/cuda/video_cache_budget.zig");
    _ = @import("../ops/cuda/embedding_gemma2_execution.zig");
    _ = @import("../ops/cuda/embedding_gemma2_policy.zig");
    _ = @import("../ops/cuda/embedding_gemma2_resize.zig");
}

test "embeddinggemma2 CUDA header admission scales text instead of reserving max tokens" {
    const short = try cudaGroupRequest(.{ .content = &.{.{ .text = "hello" }} }, .{});
    const text: [2048]u8 = @splat('a');
    const long = try cudaGroupRequest(.{ .content = &.{.{ .text = &text }} }, .{});
    try std.testing.expect(short.sequence < long.sequence);
    try std.testing.expect(short.workspace_bytes < long.workspace_bytes);
    try std.testing.expectEqual(@as(usize, 64 * 1024 * 1024), short.host_preprocess_bytes);
    try std.testing.expect(short.workspace_bytes < 1536 * 1024 * 1024);
}

test "embeddinggemma2 CUDA prepared mixed cohorts preserve singleton vectors and retry after cancellation" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const env = @import("antfly_platform").env;
    const model = env.getenv("ANTFLY_EMBEDDINGGEMMA2_MODEL") orelse return error.SkipZigTest;
    const oracle = env.getenv("ANTFLY_EMBEDDINGGEMMA2_MEDIA_ORACLE") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const file = @import("../util/c_file.zig");
    const image_path = try std.fs.path.join(a, &.{ std.fs.path.dirname(oracle).?, "image.png" });
    defer a.free(image_path);
    const audio_path = try std.fs.path.join(a, &.{ std.fs.path.dirname(oracle).?, "audio.wav" });
    defer a.free(audio_path);
    const image_bytes = try file.readFile(a, image_path);
    defer a.free(image_bytes);
    const audio_bytes = try file.readFile(a, audio_path);
    defer a.free(audio_bytes);
    const tokenizer_path = try std.fs.path.join(a, &.{ model, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try file.readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    const hf = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer hf.deinitSelf();
    const session = try factory.createCudaSession(a, model);
    defer session.close();
    // Test-only policy setup precedes all use; serving policies are immutable.
    {
        var managed = try factory.getManagedComputeBackend(session, a, null, null);
        defer managed.deinit();
        const compute: *@import("../ops/cuda/cuda_compute.zig").CudaCompute = @ptrCast(@alignCast(managed.backend.ptr));
        compute.embedding_gemma2_policy.media_batch = true;
    }
    const groups = [_]Group{
        .{ .content = &.{ .{ .audio = audio_bytes }, .{ .text = "first <|image|> then <|audio|> last" }, .{ .image = image_bytes } } },
        .{ .content = &.{.{ .text = "A short query" }} },
        .{ .content = &.{.{ .audio = audio_bytes }} },
        .{ .content = &.{ .{ .text = "Describe these inputs." }, .{ .audio = audio_bytes }, .{ .image = image_bytes } } },
    };
    const Abort = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    var errors: [4]?anyerror = @splat(null);
    try std.testing.expectError(error.Cancelled, embedGroupCohort(a, session, hf.tokenizer(), &groups, .{}, null, .{ .check_fn = Abort.check }, &errors));
    const actual = try embedGroupCohort(a, session, hf.tokenizer(), &groups, .{}, null, .{}, &errors);
    defer {
        for (actual.vectors) |row| a.free(row);
        a.free(actual.vectors);
        a.free(actual.input_tokens);
    }
    {
        var managed = try factory.getManagedComputeBackend(session, a, null, null);
        defer managed.deinit();
        const compute: *@import("../ops/cuda/cuda_compute.zig").CudaCompute = @ptrCast(@alignCast(managed.backend.ptr));
        try std.testing.expect(compute.embedding_gemma2_media_batches > 0);
    }
    for (groups, actual.vectors, actual.input_tokens, errors) |group, vector, count, err| {
        try std.testing.expectEqual(@as(?anyerror, null), err);
        const expected = try embed(a, session, hf.tokenizer(), group, .{}, null, null);
        defer a.free(expected.vector);
        try std.testing.expectEqual(expected.input_tokens, count);
        var dot: f64 = 0;
        var x2: f64 = 0;
        var y2: f64 = 0;
        for (expected.vector, vector) |x, y| {
            try std.testing.expectApproxEqAbs(x, y, 1e-3);
            dot += @as(f64, x) * y;
            x2 += @as(f64, x) * x;
            y2 += @as(f64, y) * y;
        }
        try std.testing.expect(dot / @sqrt(x2 * y2) >= 0.9999);
    }
}

test "embeddinggemma2 Metal media composition writes compact rows and preserves rectangular fallback" {
    const Case = struct {
        fn populate(store: *@import("../ops/native_compute.zig").WeightStore) !void {
            // Marker 258880 is deliberately outside this vocabulary. The
            // projection supplies its rows; only IDs 0, 1 and 2 may be looked up.
            try @import("../ops/media_residency_test.zig").putWeight(store, "language_model.embed_tokens.weight", &.{ 3, 512 }, 0.125);
        }
        fn check(cb: ComputeBackend, _: ComputeBackend) !void {
            const a = std.testing.allocator;
            var first = [_]i64{ 1, 258880, 258880, 2 };
            var second = [_]i64{ 2, 258880, 1 };
            var first_mask = [_]i64{ 1, 1, 1, 1 };
            var second_mask = [_]i64{ 1, 1, 1 };
            const rows = [_]PreparedGroup{
                .{ .ids = &first, .mask = &first_mask, .embeddings = null },
                .{ .ids = &second, .mask = &second_mask, .embeddings = null },
            };
            var media: [3 * 512]f32 = undefined;
            for (&media, 0..) |*v, i| v.* = @as(f32, @floatFromInt(5 + i / 512)) / 10;
            const source = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&media, &.{ 3, 512 }));
            defer cb.free(source);
            const result = try packResidentMediaRows(a, &cb, &rows, source, &.{ 2, 1 }, 258880, 32);
            defer result.deinit(a, &cb);
            const values = try cb.toFloat32(result.embeddings, a);
            defer a.free(values);
            const expected = [_]f32{ 0.125 * @sqrt(@as(f32, 512)), 0.5, 0.6, 0.125 * @sqrt(@as(f32, 512)), 0.125 * @sqrt(@as(f32, 512)), 0.7, 0.125 * @sqrt(@as(f32, 512)), 0.125 * @sqrt(@as(f32, 512)) };
            try std.testing.expect(result.compact_input);
            try std.testing.expectEqual(@as(usize, 7 * 512), values.len);
            for (expected[0..7], 0..) |value, row| for (values[row * 512 ..][0..512]) |actual| try std.testing.expectApproxEqAbs(value, actual, 1e-6);
            const legacy = try packResidentMediaRowsPolicy(a, &cb, &rows, source, &.{ 2, 1 }, 258880, 32, false);
            defer legacy.deinit(a, &cb);
            const legacy_values = try cb.toFloat32(legacy.embeddings, a);
            defer a.free(legacy_values);
            for (expected, 0..) |value, row| for (legacy_values[row * 512 ..][0..512]) |actual| try std.testing.expectApproxEqAbs(value, actual, 1e-6);
            try std.testing.expectEqualSlices(i64, &.{ 1, 1, 1, 1, 1, 1, 1, 0 }, result.mask);
            var marker = [_]i64{258880};
            var active = [_]i64{1};
            const pure = try packResidentMediaRows(a, &cb, &.{.{ .ids = &marker, .mask = &active, .embeddings = null }}, source, &.{1}, 258880, 32);
            defer pure.deinit(a, &cb);
            const projected = try cb.toFloat32(pure.embeddings, a);
            defer a.free(projected);
            for (projected) |value| try std.testing.expectEqual(@as(f32, 0.5), value);
        }
    };
    try @import("../ops/media_residency_test.zig").onMetal(Case.populate, Case.check);
}

test "embeddinggemma2 media templates require immutable tokenizer generations" {
    var context: u8 = 0;
    const table: Tokenizer.VTable = undefined;
    var tokenizer = Tokenizer{ .ptr = &context, .vtable = &table };
    try std.testing.expect(mediaTemplateKey(tokenizer, 4, 32, false) == null);
    tokenizer.cache_identity = 42;
    const key = mediaTemplateKey(tokenizer, 4, 32, false).?;
    try std.testing.expectEqual(key, mediaTemplateKey(tokenizer, 4, 32, false).?);
    try std.testing.expect(key != mediaTemplateKey(tokenizer, 5, 32, false).?);
    try std.testing.expect(key != mediaTemplateKey(tokenizer, 4, 64, false).?);
    try std.testing.expect(key != mediaTemplateKey(tokenizer, 4, 32, true).?);
    tokenizer.cache_identity = 43;
    try std.testing.expect(key != mediaTemplateKey(tokenizer, 4, 32, false).?);
}

test "embeddinggemma2 compact cohort policy uses active rows while portable policy bounds padding" {
    try std.testing.expectEqual(@as(usize, 2), preparedCohortCountForBackend(&.{ 1024, 7168 }, 4, true));
    try std.testing.expectEqual(@as(usize, 1), preparedCohortCountForBackend(&.{ 1024, 7168 }, 4, false));
    try std.testing.expectEqual(@as(usize, 3), preparedCohortCountForBackend(&.{ 32, 1024, 4096, 4096 }, 4, true));
    try std.testing.expectEqual(@as(usize, 1), preparedCohortCountForBackend(&.{ 8192, 1 }, 4, true));
}

test "embeddinggemma2 Metal direct packed rows exclude NaN padding and preserve original positions" {
    const Case = struct {
        fn run(cb: ComputeBackend, _: ComputeBackend) !void {
            const a = std.testing.allocator;
            var data: [5 * 512]f32 = undefined;
            for (&data, 0..) |*value, i| value.* = @floatFromInt(i / 512 + 1);
            @memset(data[512..1024], std.math.nan(f32));
            @memset(data[1536..2048], std.math.nan(f32));
            const input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&data, &.{ 5, 512 }));
            defer cb.free(input);
            const first = try cb.sliceRows2D(a, input, 0, 3, 512);
            defer cb.free(first);
            const second = try cb.sliceRows2D(a, input, 3, 2, 512);
            defer cb.free(second);
            var ids_a = [_]i64{ 1, 2, 3 };
            var mask_a = [_]i64{ 1, 0, 1 };
            var ids_b = [_]i64{ 4, 5 };
            var mask_b = [_]i64{ 0, 1 };
            const rows = [_]PreparedGroup{ .{ .ids = &ids_a, .mask = &mask_a, .embeddings = null, .tensor = first }, .{ .ids = &ids_b, .mask = &mask_b, .embeddings = null, .tensor = second } };
            const packed_rows = try packCompactPreparedRows(a, &cb, &rows);
            defer packed_rows.deinit(a, &cb);
            const values = try cb.toFloat32(packed_rows.embeddings, a);
            defer a.free(values);
            try std.testing.expectEqualSlices(i64, &.{ 1, 0, 1, 0, 1, 0 }, packed_rows.mask);
            try std.testing.expectEqual(@as(usize, 3 * 512), values.len);
            for ([_]f32{ 1, 3, 5 }, 0..) |expected, row| for (values[row * 512 ..][0..512]) |value| try std.testing.expectEqual(expected, value);
        }
    };
    try @import("../ops/media_residency_test.zig").onMetal(@import("../ops/media_residency_test.zig").noWeights, Case.run);
}

test "embeddinggemma2 image phases free preparation and admit concurrent peers within shared budgets" {
    const memory = @import("../runtime/tier/memory.zig");
    const tensor = @import("../backends/tensor.zig");
    const Mock = struct {
        fn run(_: *anyopaque, _: []const tensor.Tensor, _: std.mem.Allocator) anyerror![]tensor.Tensor {
            return error.Unused;
        }
        fn info(_: *anyopaque) []const tensor.TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .metal;
        }
        fn close(_: *anyopaque) void {}
        const vtable = Session.VTable{ .run = run, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close };
    };
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var dummy: u8 = 0;
    const session = Session{ .ptr = &dummy, .vtable = &Mock.vtable, .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .backend_limit_bytes = 900 * 1024 * 1024, .host_limit_bytes = 1024 * 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
    const a = std.testing.allocator;
    const input: usize = 128;
    const geometry = try projector.embeddingGemma2ImageGeometry(48, 48);
    var pixels = [_]projector.PreparedImage{.{ .allocator = a, .pixels = try a.alloc(f32, geometry.width * geometry.height * 3), .geometry = geometry }};
    defer pixels[0].deinit();
    const retained = input + pixels[0].retainedBytes();
    var host = try session.admitHostPreprocess(retained);
    defer host.deinit();
    const request = try preparedImageRunRequest(.metal, input, retained, &pixels, 512, 512 * 1024 * 1024);
    try std.testing.expectEqual(geometry.tokenCount() + 4, request.sequence);
    try std.testing.expectEqual(@as(usize, 512 * 1024 * 1024), request.workspace_bytes);
    var run = try session.admit(request);
    defer run.deinit();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, session.admit(request));
    const before = controller.snapshot();
    try releaseImagePreparation(&pixels, &host, &run);
    try std.testing.expectEqual(@as(usize, 0), pixels[0].pixels.len);
    try std.testing.expectEqual(input, run.request.?.pre_admitted_host_bytes);
    try std.testing.expectEqual(before.host_scratch_bytes - (retained - input), controller.snapshot().host_scratch_bytes);
    try retainPreparedPhase(&run, 1, request.sequence);
    try std.testing.expect(controller.snapshot().backend_scratch_bytes < before.backend_scratch_bytes);
    var peer = try session.admit(request);
    peer.deinit();
    run.deinit();
    host.deinit();
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
}

test "embeddinggemma2 Metal raster preparation borrows strided bytes without host allocation" {
    const bytes: [5 * 63 + 13 * 4]u8 = @splat(31);
    const raster = Raster{ .bytes = &bytes, .width = 13, .height = 6, .stride_bytes = 63 };
    var prepared = try projector.prepareEmbeddingGemma2RasterForBackend(std.testing.failing_allocator, raster, .metal);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 0), prepared.pixels.len);
    try std.testing.expect(!prepared.source_owned);
    try std.testing.expectEqual(@intFromPtr(bytes[0..].ptr), @intFromPtr(prepared.source.?.data.ptr));
    try std.testing.expectEqual(@as(usize, 63), try prepared.source.?.rowStride());
    try std.testing.expectEqual(@as(usize, 0), prepared.retainedBytes());
    try std.testing.expectEqual(@as(usize, 0), try rasterPreparationBytes(.metal, &.{raster}));
}

test "embeddinggemma2 mixed geometry bucketing preserves sparse source slots and stable order" {
    const a = std.testing.allocator;
    var wave = MixedWave{ .session = undefined, .groups = &.{}, .refs = &.{}, .control = .{}, .image = true, .count = 4 };
    const small = try projector.embeddingGemma2ImageGeometry(48, 96);
    const large = try projector.embeddingGemma2ImageGeometry(480, 480);
    for ([_]projector.Geometry{ large, small, large, small }, 0..) |geometry, i| {
        wave.images[i] = .{ .allocator = a, .pixels = &.{}, .geometry = geometry };
    }
    wave.indexes = .{ 0, 2, 3, 1 };
    wave.groupGeometry();
    try std.testing.expectEqualSlices(usize, &.{ 2, 1, 0, 3 }, &wave.indexes);
    try std.testing.expectEqual(small, wave.images[0].geometry);
    try std.testing.expectEqual(small, wave.images[1].geometry);
    try std.testing.expectEqual(large, wave.images[2].geometry);
    try std.testing.expectEqual(large, wave.images[3].geometry);
    wave.image = false;
    wave.groupGeometry();
    try std.testing.expectEqualSlices(usize, &.{ 2, 1, 0, 3 }, &wave.indexes);
}

test "embeddinggemma2 queued video retains preparation without reserving execution scratch" {
    const memory = @import("../runtime/tier/memory.zig");
    const request: @import("../backends/session.zig").RunRequest = .{ .batch = 4, .sequence = 4096, .input_bytes = 32 * 1024 * 1024, .host_preprocess_bytes = 128 * 1024 * 1024, .workspace_bytes = 1536 * 1024 * 1024, .output_bytes = 4096 };
    const queued = queuedVideoRequest(request, 96 * 1024 * 1024);
    try std.testing.expectEqual(request.input_bytes, queued.input_bytes);
    try std.testing.expectEqual(request.host_preprocess_bytes, queued.host_preprocess_bytes);
    try std.testing.expectEqual(@as(?usize, 0), queued.backend_input_bytes);
    try std.testing.expectEqual(@as(usize, 1), queued.sequence);
    const tensor = @import("../backends/tensor.zig");
    const Mock = struct {
        fn run(_: *anyopaque, _: []const tensor.Tensor, _: std.mem.Allocator) anyerror![]tensor.Tensor {
            return error.Unused;
        }
        fn info(_: *anyopaque) []const tensor.TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .metal;
        }
        fn close(_: *anyopaque) void {}
    };
    var dummy: u8 = 0;
    var controller = memory.AdmissionController{};
    const session = Session{ .ptr = &dummy, .vtable = &.{ .run = Mock.run, .inputInfo = Mock.info, .outputInfo = Mock.info, .backend = Mock.backend, .close = Mock.close }, .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .backend_limit_bytes = 2048 * 1024 * 1024, .host_limit_bytes = 2048 * 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
    var first = try session.admit(queued);
    defer first.deinit();
    var second = try session.admit(queued);
    defer second.deinit();
    var lane = StageLane{ .session = session, .gate = null, .waiter = undefined, .cb = undefined, .control = .{}, .permit = &first, .idle_backend = queued.workspace_bytes, .video_phases = true, .composition_request = request };
    defer if (lane.extra) |*extra| extra.deinit();
    try lane.reserve(null);
    try std.testing.expectEqual(request.workspace_bytes + queued.workspace_bytes, controller.snapshot().backend_scratch_bytes);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, lane.reserve(1860 * 1024 * 1024));
    // A denied replacement leaves both queued owners intact and grants no work.
    try std.testing.expect(lane.extra == null);
    try std.testing.expectEqual(2 * queued.workspace_bytes, controller.snapshot().backend_scratch_bytes);
    try lane.reserve(256 * 1024 * 1024);
    try std.testing.expectEqual(2 * queued.workspace_bytes + 256 * 1024 * 1024, controller.snapshot().backend_scratch_bytes);
    try lane.reserve(0);
    try std.testing.expectEqual(2 * queued.workspace_bytes, controller.snapshot().backend_scratch_bytes);
    // A backbone may be larger than the idle lease; it obtains independent
    // capacity before reducing retained ownership to its exact next phase.
    var backbone = queued;
    backbone.workspace_bytes = 512 * 1024 * 1024;
    try lane.retain(backbone);
    try std.testing.expectEqual(backbone.workspace_bytes + queued.workspace_bytes, controller.snapshot().backend_scratch_bytes);
    if (lane.extra) |*extra| extra.deinit();
    lane.extra = null;
    first.deinit();
    second.deinit();
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
}

test "embeddinggemma2 composition and Metal backbone plans track physical stages" {
    const request: @import("../backends/session.zig").RunRequest = .{ .batch = 4, .sequence = 8192, .input_bytes = 64 * 1024 * 1024, .host_preprocess_bytes = 512 * 1024 * 1024, .workspace_bytes = 1536 * 1024 * 1024 };
    const composed = try compositionRequest(request, 64 * 1024 * 1024, 8192);
    try std.testing.expectEqual(@as(usize, 112 * 1024 * 1024), composed.workspace_bytes);
    try std.testing.expectEqual(@as(usize, 1), composed.sequence);
    try std.testing.expectEqual(@as(?usize, 0), composed.backend_input_bytes);
    try std.testing.expectEqual(request.input_bytes, composed.input_bytes);
    const encoder_phase = try videoBackboneBounds(true, 4096, 1536 * 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 1024 * 1024 * 1024), encoder_phase.workspace);
    const full = try videoBackboneBounds(true, 8192, 1536 * 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 1792 * 1024 * 1024), full.workspace);
    // Conservative portable policy remains unchanged, including its hard cap.
    try std.testing.expectEqual(try executionBounds(true, 4096, 0, 0, 0, 512 * 1024 * 1024), try videoBackboneBounds(false, 4096, 512 * 1024 * 1024));
    try std.testing.expectError(error.EmbeddingInputTooLong, videoBackboneBounds(true, 8193, 0));
}

test "embeddinggemma2 phase supplements reclaim idle caches preserve owners and respect denial constraints" {
    const memory = @import("../runtime/tier/memory.zig");
    const tensor = @import("../backends/tensor.zig");
    const Mode = enum { release, retired, language, busy, cancel, forced, permanent, unrelated };
    const Probe = struct {
        held: ?memory.AdmissionLease = null,
        mode: Mode,
        prepared: bool = false,
        media_calls: usize = 0,
        language_calls: usize = 0,
        cancelled: bool = false,
        fn prepare(self: *@This(), _: Control) !void {
            self.prepared = true;
            if (self.mode == .retired) {
                self.held.?.release();
                self.held = null;
            }
        }
        fn media(self: *@This(), _: Control) !usize {
            std.debug.assert(self.prepared);
            self.media_calls += 1;
            if (self.mode == .busy or self.mode == .language) return 0;
            self.held.?.release();
            self.held = null;
            if (self.mode == .cancel) self.cancelled = true;
            return 4;
        }
        fn language(self: *@This(), _: Control) !usize {
            self.language_calls += 1;
            if (self.mode != .language) return 0;
            self.held.?.release();
            self.held = null;
            return 4;
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
            return .metal;
        }
        fn close(_: *anyopaque) void {}
        const vtable = Session.VTable{ .run = run, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close };
    };
    for ([_]bool{ false, true }) |credit| {
        for (std.enums.values(Mode)) |mode| {
            var controller = memory.AdmissionController{};
            defer controller.deinit();
            const limits = memory.Limits{ .backend_limit_bytes = if (mode == .unrelated) 16 else 10, .scratch_limit_bytes = if (mode == .unrelated) 6 else 0 };
            var probe = Probe{ .mode = mode };
            probe.held = try controller.tryAcquire(.gpu, limits, .{ .backend_weight_bytes = 4 }, false);
            defer if (probe.held) |*held| held.release();

            const session = Session{ .ptr = &probe, .vtable = &Probe.vtable, .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = limits, .static_workspace_bytes = 0, .check_live_memory = false } };
            var peer = try session.admit(.{ .workspace_bytes = 2, .output_bytes = 0 });
            defer peer.deinit();
            var base = try session.admit(.{ .workspace_bytes = 2, .output_bytes = 0 });
            defer base.deinit();
            const original = base.lease.?.amounts;
            if (mode == .forced) controller.configureForcedRunDenialsForTesting(1);
            const target = memory.AdmissionAmounts{ .backend_scratch_bytes = if (mode == .permanent) 20 else if (credit) 8 else 6 };
            const control = Control{ .ptr = &probe, .check_fn = Probe.check };
            switch (mode) {
                .release, .retired, .language => {
                    var extra = try admitSupplementReclaimingCaches(session, &base, if (credit) &peer else null, target, control, &probe, Probe.prepare, Probe.media, Probe.language);
                    try std.testing.expectEqual(@as(usize, 8), controller.snapshot().backend_scratch_bytes);
                    try std.testing.expectEqual(original, base.lease.?.amounts);
                    extra.deinit();
                },
                .cancel => try std.testing.expectError(error.Cancelled, admitSupplementReclaimingCaches(session, &base, if (credit) &peer else null, target, control, &probe, Probe.prepare, Probe.media, Probe.language)),
                .permanent => try std.testing.expectError(error.ResourceLimitExceeded, admitSupplementReclaimingCaches(session, &base, if (credit) &peer else null, target, control, &probe, Probe.prepare, Probe.media, Probe.language)),
                else => try std.testing.expectError(error.ResourceTemporarilyUnavailable, admitSupplementReclaimingCaches(session, &base, if (credit) &peer else null, target, control, &probe, Probe.prepare, Probe.media, Probe.language)),
            }
            try std.testing.expectEqual(mode == .release or mode == .retired or mode == .language or mode == .busy or mode == .cancel, probe.prepared);
            if (mode == .retired) try std.testing.expectEqual(@as(usize, 0), probe.media_calls + probe.language_calls);
            try std.testing.expectEqual(original, base.lease.?.amounts);
            try std.testing.expectEqual(@as(usize, 2), peer.lease.?.amounts.backend_scratch_bytes);
        }
    }
}

/// Opt-in checkpoint qualification: a warmed optional cache must yield to a
/// phase supplement while the FIFO turn and independent input remain owned.
pub fn qualifyVideoPhaseReclaim(a: std.mem.Allocator, source_session: Session, lock: ?*std.atomic.Mutex, control: Control) !void {
    if (source_session.backend() != .metal) return error.SkipZigTest;
    var session = source_session;
    var admission = session.run_admission orelse return error.ExpectedRunAdmission;
    const before = admission.controller.snapshotBackend(.gpu);
    admission.limits.backend_limit_bytes = before.backendTotalBytes() + 64 * 1024 * 1024;
    session.run_admission = admission;
    var base = try session.admit(.{ .workspace_bytes = 64 * 1024 * 1024, .workspace_planned = true, .backend_input_bytes = 0, .output_bytes = 0 });
    defer base.deinit();
    var waiter = Scheduler.Waiter{};
    try enterModel(session, lock, control, &waiter);
    defer leaveModel(lock, &waiter);
    var managed = try factory.getManagedComputeBackend(session, a, null, control);
    defer managed.deinit();
    var timing = MediaTimings{};
    var lane = StageLane{ .session = session, .gate = lock, .waiter = &waiter, .cb = &managed.backend, .control = control, .permit = &base, .idle_backend = 64 * 1024 * 1024, .video_phases = true, .timings = &timing };
    lane.attach();
    defer lane.deinit();
    const cb = &managed.backend;
    const input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 1, 4 }));
    defer cb.free(input);
    try lane.reserve(1024 * 1024);
    try std.testing.expect(timing.phase_reclaim_attempts > 0);
    try std.testing.expect(!lane.phase_provider_suspended);
    const values = try cb.toFloat32(input, a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, values);
    try std.testing.expect(admission.controller.snapshotBackend(.gpu).backendTotalBytes() <= admission.limits.backend_limit_bytes);
    std.debug.print("embeddinggemma2 video_phase_reclaim warmed_cache=true retained_input_parity=true attempts={d}\n", .{timing.phase_reclaim_attempts});
}
