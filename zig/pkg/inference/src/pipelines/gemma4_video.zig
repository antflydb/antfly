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

//! Gemma 4 video input. CUDA decodes on the device; every other backend reuses
//! the qualified lib/video routes and projects frames as ordinary images.
const std = @import("std");
const media = @import("antfly_media");
const video = @import("antfly_video");
const projector_mod = @import("../architectures/gemma4_projector.zig");
const embedding_video = @import("embedding_gemma2_video.zig");
const embedding_gemma2 = @import("embedding_gemma2.zig");
const ComputeBackend = @import("../ops/ops.zig").ComputeBackend;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Session = @import("../backends/session.zig").Session;
const memory = @import("../runtime/tier/memory.zig");

// At most one token per ASCII timestamp byte, two separating spaces and two
// vision boundary tokens. Minutes can exceed two digits on portable decoders.
const max_minutes = std.math.maxInt(i64) / (60 * std.time.us_per_s);
const timestamp_bytes = std.fmt.comptimePrint("{d}:59", .{max_minutes}).len;
pub const token_allowance = video.packet_sampling.gemma4_max_frames * (projector_mod.video_tokens_per_frame + timestamp_bytes + 4);

const sampling: embedding_video.Sampling = .gemma4;

/// Decoder and indexing memory, shared through one pool per request.
const portable_pool_bytes: u64 = 512 * 1024 * 1024;
const inspection_bytes: usize = 64 * 1024 * 1024;
/// Prepared model-sized frames, plus one displayed frame at the source pixel limit.
const portable_frame_bytes: usize = 96 * 1024 * 1024 + 48 * 1024 * 1024;
/// Reserved through generation admission before any portable video work starts.
pub const portable_host_bytes: usize = portable_pool_bytes + portable_frame_bytes;

/// The run budget owns per-invocation limits, including offline CLI overrides.
/// The session additionally owns process-wide serving admission. Neither replaces
/// the other: a CLI session normally has no shared admission controller.
pub const Admission = struct {
    run_budget: ?*memory.RunBudget = null,
    session: ?Session = null,
    explicit_scratch_limit: usize = 0,

    fn acquire(self: Admission, additional_shared: bool, reserved: Reservation) !Permit {
        var permit: Permit = .{ .reservation = reserved };
        errdefer permit.deinit();
        if (self.run_budget) |budget| {
            permit.budget = budget;
            permit.previous_scratch_limit = budget.limits.scratch_limit_bytes;
            if (budget.limits.scratch_limit_bytes != 0) budget.limits = try memory.withAutomaticScratchFloor(budget.limits, self.explicit_scratch_limit, budget.scratchTotalBytes(), reserved.host_bytes +| reserved.surface_bytes);
            try budget.reserveEstimate(scratchEstimate(reserved.host_bytes, .host));
            permit.reserved_host = true;
            if (reserved.surface_bytes != 0) {
                try budget.reserveEstimate(scratchEstimate(reserved.surface_bytes, .backend));
                permit.reserved_surfaces = true;
            }
        }
        // Non-CUDA serving requests already reserve portable host memory during
        // generation admission. CUDA acquires it only after a decoder rejection.
        if (additional_shared) if (self.session) |session| {
            permit.shared = try admitPortableFallback(session, self.explicit_scratch_limit);
        };
        return permit;
    }
};

const Permit = struct {
    budget: ?*memory.RunBudget = null,
    previous_scratch_limit: usize = 0,
    reservation: Reservation = .{},
    reserved_host: bool = false,
    reserved_surfaces: bool = false,
    shared: ?@import("../backends/session.zig").RunPermit = null,
    surface_shared: ?@import("../backends/session.zig").RunPermit = null,

    /// Admit an in-band SPS's additional device envelope after activation,
    /// before opening a decoder. Both ledgers roll back on shared denial.
    fn ensureSurfaces(self: *Permit, admission: Admission, required: usize) !void {
        if (required <= self.reservation.surface_bytes) return;
        if (self.surface_shared != null) return error.InvalidVideoCacheReservation;
        const growth = required - self.reservation.surface_bytes;
        const previous_limit = if (self.budget) |budget| budget.limits.scratch_limit_bytes else 0;
        if (self.budget) |budget| {
            if (budget.limits.scratch_limit_bytes != 0) budget.limits = try memory.withAutomaticScratchFloor(budget.limits, admission.explicit_scratch_limit, budget.scratchTotalBytes(), growth);
            budget.reserveEstimate(scratchEstimate(growth, .backend)) catch |err| {
                budget.limits.scratch_limit_bytes = previous_limit;
                return err;
            };
        }
        errdefer if (self.budget) |budget| {
            budget.releaseEstimate(scratchEstimate(growth, .backend));
            budget.limits.scratch_limit_bytes = previous_limit;
        };
        if (admission.session) |original| {
            var session = original;
            if (session.run_admission) |*shared| {
                shared.limits = try memory.withAutomaticScratchFloor(shared.limits, admission.explicit_scratch_limit, shared.controller.snapshot().scratchTotalBytes(), growth);
            }
            self.surface_shared = try session.admitRetainedMedia(0, growth);
        }
        self.reservation.surface_bytes = required;
        self.reserved_surfaces = self.budget != null;
    }

    fn deinit(self: *Permit) void {
        if (self.surface_shared) |*held| held.deinit();
        if (self.shared) |*held| held.deinit();
        if (self.budget) |budget| {
            if (self.reserved_host) budget.releaseEstimate(scratchEstimate(self.reservation.host_bytes, .host));
            if (self.reserved_surfaces) budget.releaseEstimate(scratchEstimate(self.reservation.surface_bytes, .backend));
            budget.limits.scratch_limit_bytes = self.previous_scratch_limit;
        }
        self.* = .{};
    }
};

fn scratchEstimate(bytes: usize, tier: memory.ResidencyTier) memory.Estimate {
    return .{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = bytes, .scratch_tier = tier };
}

/// Retained inspection, one packet plus the native sample copy, and prepared
/// and displayed frames. VideoToolbox surfaces have a separate device budget.
const apple_hevc_host_bytes: usize = inspection_bytes + 32 * 1024 * 1024 + portable_frame_bytes;

pub const Reservation = struct {
    host_bytes: usize = portable_host_bytes,
    /// VideoToolbox surfaces, which cache hits and portable budgets don't cover.
    surface_bytes: usize = 0,
};

/// What this clip's route will hold on a host without NVDEC. Unreadable clips keep
/// the portable reservation; decoding reports what's wrong with them.
pub fn reservation(_: std.mem.Allocator, bytes: []const u8) Reservation {
    return reservationControlled(bytes, .{});
}

fn reservationControlled(bytes: []const u8, control: Control) Reservation {
    if (@import("builtin").os.tag != .macos) return .{};
    const adapter = control.imageWorkControl();
    const track = media.mp4.probeTrack(bytes, video.mp4_coded.limits, .{ .context = adapter.context, .check_fn = adapter.check_fn }) catch return .{};
    if (track.codec != .hevc) return .{};
    // Do not guess activation from hvcC array order or inspect packet data here.
    // The execution scope resolves the active SPS under this host reservation,
    // then acquires the exact reference/output envelope before decoder creation.
    return .{ .host_bytes = apple_hevc_host_bytes };
}

pub fn encode(cb: *const ComputeBackend, admission: Admission, a: std.mem.Allocator, projector: *projector_mod.ProjectorStore, bytes: []const u8) !projector_mod.ProjectedVideo {
    if (bytes.len > video.packet_sampling.gemma4_max_input_bytes) return error.VideoInputTooLarge;
    if (cb.kind() == .cuda) {
        // NVDEC takes a narrower set of clips than the catalog advertises, and
        // some rejections only surface mid-bitstream. Portable decoding decides
        // what's really invalid; either route projects on the device.
        if (projector_mod.encodeProjectedVideoResident(cb, a, projector, bytes, admission.explicit_scratch_limit == 0)) |projected| {
            return projected;
        } else |err| {
            // Decoder envelope rejections can retry under portable admission.
            if (err != error.NvdecUnavailable and err != error.VideoDurationExceeded and err != error.DecoderEnvelopeExceeded and inputError(err) != error.InvalidVideoInput) return err;
            std.log.info("gemma4 video falling back to portable decoding: {s}", .{@errorName(err)});
        }
        var permit = try admission.acquire(true, .{});
        defer permit.deinit();
        return encodePortable(cb, a, projector, bytes, admission, &permit);
    }
    try cb.checkExecutionControl();
    var permit = try admission.acquire(false, reservationControlled(bytes, cb.execution_control orelse .{}));
    defer permit.deinit();
    return encodePortable(cb, a, projector, bytes, admission, &permit);
}

/// Admission reserved nothing for this route, since most clips never take it, and
/// the run's own scratch can already fill the automatic cap. Grow that cap to fit,
/// as model scratch floors do; physical limits and an operator's cap still hold.
fn admitPortableFallback(session: Session, explicit_scratch_limit: usize) !@import("../backends/session.zig").RunPermit {
    var fallback = session;
    if (fallback.run_admission) |*admission| {
        admission.limits = try memory.withAutomaticScratchFloor(
            admission.limits,
            explicit_scratch_limit,
            admission.controller.snapshot().scratchTotalBytes(),
            portable_host_bytes,
        );
    }
    return fallback.admitHostPreprocess(portable_host_bytes);
}

test "gemma4 video portable fallback fits beside a run's scratch unless the operator capped it" {
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    const cap = portable_host_bytes + 1024;
    const session = Session{
        .ptr = undefined,
        .vtable = undefined,
        .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .scratch_limit_bytes = cap }, .static_workspace_bytes = 0 },
    };
    var run = try session.admitHostPreprocess(cap);
    defer run.deinit();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, session.admitHostPreprocess(portable_host_bytes));
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, admitPortableFallback(session, cap));
    var permit = try admitPortableFallback(session, 0);
    defer permit.deinit();
    try std.testing.expectEqual(cap + portable_host_bytes, controller.snapshot().host_scratch_bytes);
}

test "gemma4 video portable admission enforces CLI host combined and scratch limits without session admission" {
    const cases = [_]memory.Limits{
        .{ .host_limit_bytes = portable_host_bytes - 1 },
        .{ .combined_limit_bytes = portable_host_bytes - 1 },
        .{ .scratch_limit_bytes = portable_host_bytes - 1 },
    };
    const session = Session{ .ptr = undefined, .vtable = undefined };
    for (cases) |limits| {
        var budget = memory.RunBudget.init(limits);
        const admission = Admission{ .run_budget = &budget, .session = session, .explicit_scratch_limit = limits.scratch_limit_bytes };
        try std.testing.expectError(error.MemoryBudgetExceeded, admission.acquire(true, .{}));
        try std.testing.expectEqual(@as(usize, 0), budget.host_scratch_bytes);
        try std.testing.expectEqual(limits, budget.limits);
    }
}

test "gemma4 video portable admission refunds both owners and restores automatic scratch floors on failure" {
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    const session = Session{
        .ptr = undefined,
        .vtable = undefined,
        .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false },
    };
    var budget = memory.RunBudget.init(.{ .scratch_limit_bytes = 1024 });
    const base: memory.Estimate = .{ .prompt_tokens = 0, .retained_tokens = 0, .kv_bytes = 0, .kv_tier = .host, .scratch_bytes = 1024, .scratch_tier = .backend };
    try budget.reserveEstimate(base);
    const Probe = struct {
        fn decode(admission: Admission) !void {
            var permit = try admission.acquire(true, .{});
            defer permit.deinit();
            try std.testing.expectEqual(portable_host_bytes, admission.run_budget.?.host_scratch_bytes);
            try std.testing.expectEqual(portable_host_bytes, admission.session.?.run_admission.?.controller.snapshot().host_scratch_bytes);
            return error.InvalidVideoInput;
        }
    };
    try std.testing.expectError(error.InvalidVideoInput, Probe.decode(.{ .run_budget = &budget, .session = session }));
    try std.testing.expectEqual(@as(usize, 0), budget.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 1024), budget.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 1024), budget.limits.scratch_limit_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}

test "gemma4 video portable admission rolls back local capacity on shared denial and reuses prepaid serving memory" {
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    const session = Session{
        .ptr = undefined,
        .vtable = undefined,
        .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = portable_host_bytes }, .static_workspace_bytes = 0, .check_live_memory = false },
    };
    var existing = try session.admitHostPreprocess(portable_host_bytes);
    defer existing.deinit();
    var budget = memory.RunBudget.init(.{ .scratch_limit_bytes = 1024 });
    const admission = Admission{ .run_budget = &budget, .session = session };
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, admission.acquire(true, .{}));
    try std.testing.expectEqual(@as(usize, 0), budget.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 1024), budget.limits.scratch_limit_bytes);
    // Non-CUDA serving admission already holds the shared host reservation.
    var permit = try admission.acquire(false, .{});
    try std.testing.expectEqual(portable_host_bytes, budget.host_scratch_bytes);
    try std.testing.expectEqual(portable_host_bytes, controller.snapshot().host_scratch_bytes);
    permit.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 1024), budget.limits.scratch_limit_bytes);
}

test "gemma4 video portable encode admits before parsing and refunds malformed input" {
    const Probe = struct {
        fn kind(_: *anyopaque) @import("../ops/ops.zig").BackendKind {
            return .native;
        }
    };
    // Malformed media fails before accessing projector tensors or compute ops.
    var vtable: ComputeBackend.VTable = undefined;
    vtable.backendKind = Probe.kind;
    const cb = ComputeBackend{ .ptr = &vtable, .vtable = &vtable };
    var projector: projector_mod.ProjectorStore = undefined;
    var budget = memory.RunBudget.init(.{ .host_limit_bytes = portable_host_bytes - 1 });
    const admission = Admission{ .run_budget = &budget };
    try std.testing.expectError(error.MemoryBudgetExceeded, encode(&cb, admission, std.testing.allocator, &projector, "not a video"));
    budget.limits.host_limit_bytes = portable_host_bytes;
    try std.testing.expectError(error.InvalidVideoInput, encode(&cb, admission, std.testing.allocator, &projector, "not a video"));
    try std.testing.expectEqual(@as(usize, 0), budget.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), budget.limits.scratch_limit_bytes);
}

const Observer = struct {
    a: std.mem.Allocator,
    cb: *const ComputeBackend,
    frames: *projector_mod.PortableVideoFrames,
    fn accept(raw: *anyopaque, slot: usize, frame: video.preparation.BorrowedFrame, options: video.preparation.Options, control: media.source.Control) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.cb.checkExecutionControl();
        var displayed = try frame.display(self.a, options, control);
        defer displayed.deinit(self.a);
        try self.frames.prepare(slot, displayed.view(), options.presentation_width, options.presentation_height);
    }
};

fn encodePortable(cb: *const ComputeBackend, a: std.mem.Allocator, projector: *projector_mod.ProjectorStore, bytes: []const u8, admission: Admission, permit: *Permit) !projector_mod.ProjectedVideo {
    // Indexing and decoding run under the model execution lock, so they must honor
    // the request's cancellation and deadline between selected frames too.
    const control = cb.execution_control orelse Control{};
    // VideoToolbox surfaces are a separate budget from decoder heap memory.
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes - inspection_bytes, .device_bytes = video.apple_hevc.max_surface_bytes } };
    var inspection = embedding_video.InspectionScope{ .allocator = a, .limit = inspection_bytes };
    defer inspection.deinit();
    const configuration = embedding_video.Configuration{ .sampling = sampling, .pool = &pool, .inspection_scope = &inspection };
    const timestamps = embedding_video.selectedConfiguredPresentationUs(a, bytes, control, configuration) catch |err| return inputError(err);
    defer a.free(timestamps);
    const inspected = inspection.find(bytes, null, configuration).?;
    if (!inspected.webm and inspected.reader.track.codec == .hevc) {
        const adapter = control.imageWorkControl();
        const required = try video.apple_hevc.surfaceBytesControlled(inspected.reader.track, .{ .context = adapter.context, .check_fn = adapter.check_fn });
        if (required > video.apple_hevc.max_surface_bytes) return error.SharedAdmissionExceeded;
        try permit.ensureSurfaces(admission, std.math.cast(usize, required) orelse return error.SharedAdmissionExceeded);
    }
    var frames = try projector_mod.PortableVideoFrames.init(cb, a, projector, timestamps.len);
    defer frames.deinit();
    var observer: Observer = .{ .a = a, .cb = cb, .frames = &frames };
    embedding_video.observeConfigured(a, bytes, null, control, .{
        .observer = .{ .context = &observer, .accept = Observer.accept },
        .sampling = sampling,
        .pool = &pool,
        .inspection_scope = &inspection,
    }) catch |err| return inputError(err);
    return frames.project(timestamps);
}

/// Client media failures become one input error. Cancellation, deadlines and
/// capacity errors keep their identity through the shared embedding normalizer.
fn inputError(err: anyerror) anyerror {
    return embedding_gemma2.videoInputError(err);
}

test "gemma4 video timestamps follow presentation order on every portable container" {
    const a = std.testing.allocator;
    inline for (.{ "mjpeg.mov", "bframes.mp4", "closed.mp4", "vp9.webm", "av1.webm" }) |name| {
        const times = try embedding_video.selectedPresentationUs(a, @embedFile("../testdata/video/" ++ name), .{}, sampling, null);
        defer a.free(times);
        try std.testing.expect(times.len > 0 and times.len <= 32);
        try std.testing.expectEqual(@as(i64, 0), times[0]);
        for (times[1..], times[0 .. times.len - 1]) |next, previous| try std.testing.expect(next > previous);
    }
}

test "gemma4 video timestamps match the frames delivered to the observer" {
    const a = std.testing.allocator;
    inline for (.{ "mjpeg.mov", "bframes.mp4", "vp9.webm" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name);
        const times = try embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, null);
        defer a.free(times);
        const Count = struct {
            seen: []bool,
            fn accept(raw: *anyopaque, slot: usize, frame: video.preparation.BorrowedFrame, options: video.preparation.Options, control: media.source.Control) anyerror!void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                var displayed = try frame.display(std.testing.allocator, options, control);
                defer displayed.deinit(std.testing.allocator);
                try std.testing.expect(displayed.width > 0);
                try std.testing.expect(!self.seen[slot]);
                self.seen[slot] = true;
            }
        };
        const seen = try a.alloc(bool, times.len);
        defer a.free(seen);
        @memset(seen, false);
        var counter: Count = .{ .seen = seen };
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        try embedding_video.observeConfigured(a, bytes, null, .{}, .{ .observer = .{ .context = &counter, .accept = Count.accept }, .sampling = sampling, .pool = &pool });
        for (seen) |value| try std.testing.expect(value);
    }
}

test "gemma4 video timestamps honor request cancellation before indexing and decoding" {
    const a = std.testing.allocator;
    const Cancelled = struct {
        fn cancelled(_: ?*anyopaque) bool {
            return true;
        }
    };
    var unused: u8 = 0;
    const control: Control = .{ .cancellation = .{ .is_cancelled_fn = Cancelled.cancelled } };
    inline for (.{ "bframes.mp4", "vp9.webm" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name);
        try std.testing.expectError(error.Cancelled, embedding_video.selectedPresentationUs(a, bytes, control, sampling, null));
        const Refuse = struct {
            fn accept(_: *anyopaque, _: usize, _: video.preparation.BorrowedFrame, _: video.preparation.Options, _: media.source.Control) anyerror!void {
                return error.UnexpectedFrame;
            }
        };
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = 512 * 1024 * 1024 } };
        try std.testing.expectError(error.Cancelled, embedding_video.observeConfigured(a, bytes, null, control, .{ .observer = .{ .context = &unused, .accept = Refuse.accept }, .sampling = sampling, .pool = &pool }));
    }
}

test "gemma4 video corrupt input is a client error while cancellation and capacity keep their identity" {
    const a = std.testing.allocator;
    const truncated = @embedFile("../testdata/video/bframes.mp4")[0..64];
    inline for (.{ "not a video at all", truncated }) |bytes| {
        const failure = if (embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, null)) |times| blk: {
            a.free(times);
            break :blk error.TestExpectedError;
        } else |err| err;
        try std.testing.expectEqual(error.InvalidVideoInput, inputError(failure));
    }
    for ([_]anyerror{ error.Cancelled, error.Timeout, error.SharedAdmissionExceeded, error.OutOfMemory }) |err| {
        try std.testing.expectEqual(err, inputError(err));
    }
}

test "gemma4 video indexing and decoding share one bounded pool" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/av1.webm");
    var tiny = media.admission.Pool{ .limits = .{ .host_bytes = 1 } };
    try std.testing.expectError(error.SharedAdmissionExceeded, embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, &tiny));
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes } };
    const times = try embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, &pool);
    defer a.free(times);
    try std.testing.expectEqual(media.admission.Resources{}, pool.snapshot());
    try std.testing.expect(pool.high_water.host_bytes > 0);
}

test "gemma4 video reserves portable memory through generation admission" {
    const generation = @import("generation.zig");
    const messages = [_]generation.Message{.{ .role = "user", .content = "describe", .video_bytes = "clip" }};
    const portable = try generation.nativeGenerationMediaAdmissionForBackend(std.testing.allocator, "", &messages, .{ .family = .gemma }, .generic);
    try std.testing.expectEqual(portable_host_bytes, portable.host_scratch_bytes);
    try std.testing.expectEqual(token_allowance, portable.token_allowance);
    const cuda = try generation.nativeGenerationMediaAdmissionForBackend(std.testing.allocator, "", &messages, .{ .family = .gemma }, .cuda);
    try std.testing.expectEqual(@as(usize, 0), cuda.host_scratch_bytes);
    const text = [_]generation.Message{.{ .role = "user", .content = "describe" }};
    try std.testing.expectEqual(@as(usize, 0), (try generation.nativeGenerationMediaAdmissionForBackend(std.testing.allocator, "", &text, .{ .family = .gemma }, .generic)).host_scratch_bytes);
}

test "gemma4 video decodes AV1 in MP4 like its WebM route" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/codecs/av1.mp4");
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes } };
    const timestamps = try embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, &pool);
    defer a.free(timestamps);
    try std.testing.expect(timestamps.len != 0);
    const Count = struct {
        frames: usize = 0,
        width: u32 = 0,
        fn accept(raw: *anyopaque, _: usize, frame: video.preparation.BorrowedFrame, options: video.preparation.Options, control: media.source.Control) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var displayed = try frame.display(std.testing.allocator, options, control);
            defer displayed.deinit(std.testing.allocator);
            self.frames += 1;
            self.width = displayed.width;
        }
    };
    var count: Count = .{};
    try embedding_video.observeConfigured(a, bytes, null, .{}, .{ .observer = .{ .context = &count, .accept = Count.accept }, .sampling = sampling, .pool = &pool, .decoder = .{ .policy = .portable } });
    try std.testing.expectEqual(timestamps.len, count.frames);
    try std.testing.expectEqual(@as(u32, 192), count.width);
}

test "gemma4 video decodes HEVC in MP4 at FFmpeg's timestamps" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/codecs/hevc-hvc1.mp4");
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes, .device_bytes = video.apple_hevc.max_surface_bytes } };
    const timestamps = try embedding_video.selectedPresentationUs(a, bytes, .{}, sampling, &pool);
    defer a.free(timestamps);
    try std.testing.expect(timestamps.len != 0);
    const Count = struct {
        frames: usize = 0,
        width: u32 = 0,
        fn accept(raw: *anyopaque, _: usize, frame: video.preparation.BorrowedFrame, options: video.preparation.Options, control: media.source.Control) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var displayed = try frame.display(std.testing.allocator, options, control);
            defer displayed.deinit(std.testing.allocator);
            self.frames += 1;
            self.width = displayed.width;
        }
    };
    var count: Count = .{};
    try embedding_video.observeConfigured(a, bytes, null, .{}, .{ .observer = .{ .context = &count, .accept = Count.accept }, .sampling = sampling, .pool = &pool });
    try std.testing.expectEqual(timestamps.len, count.frames);
    try std.testing.expectEqual(@as(u32, 192), count.width);
}

test "gemma4 video prepays HEVC inspection and admits surfaces after activation" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/codecs/hevc-10bit.mp4");
    const reserved = reservation(a, bytes);
    try std.testing.expectEqual(apple_hevc_host_bytes, reserved.host_bytes);
    try std.testing.expectEqual(@as(usize, 0), reserved.surface_bytes);
    const generation = @import("generation.zig");
    const messages = [_]generation.Message{.{ .role = "user", .content = "describe", .video_bytes = bytes }};
    const admitted = try generation.nativeGenerationMediaAdmissionForBackend(a, "", &messages, .{ .family = .gemma }, .generic);
    try std.testing.expectEqual(reserved.host_bytes, admitted.host_scratch_bytes);
    try std.testing.expectEqual(reserved.surface_bytes, admitted.backend_scratch_bytes);
    // Surfaces at 4K Main 10 are the case the portable budget undercounted.
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "budget-config" };
    var reader = try media.mp4.Reader.init(a, &source, video.mp4_coded.limits);
    defer reader.deinit();
    var track = reader.track;
    track.width = 3840;
    track.height = 2160;
    const output = try video.apple.surfacePayloadBytesForDepth(track.width, track.height, 10);
    try std.testing.expect(try video.apple_hevc.surfaceBytes(track) > output);
    // Other codecs and unreadable clips keep the portable reservation.
    try std.testing.expectEqual(@as(usize, 0), reservation(a, @embedFile("../testdata/video/codecs/av1.mp4")).surface_bytes);
    try std.testing.expectEqual(portable_host_bytes, reservation(a, "not a video").host_bytes);
}

test "gemma4 video surface reservations honor an explicit scratch cap and release cleanly" {
    const reserved = Reservation{ .host_bytes = 1024, .surface_bytes = 4096 };
    var capped = memory.RunBudget.init(.{ .scratch_limit_bytes = 2048 });
    const refused = Admission{ .run_budget = &capped, .explicit_scratch_limit = 2048 };
    try std.testing.expectError(error.MemoryBudgetExceeded, refused.acquire(false, reserved));
    try std.testing.expectEqual(@as(usize, 0), capped.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), capped.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 2048), capped.limits.scratch_limit_bytes);
    var automatic = memory.RunBudget.init(.{ .scratch_limit_bytes = 2048 });
    const grows = Admission{ .run_budget = &automatic };
    var permit = try grows.acquire(false, reserved);
    try std.testing.expectEqual(@as(usize, 1024), automatic.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 4096), automatic.backend_scratch_bytes);
    permit.deinit();
    try std.testing.expectEqual(@as(usize, 0), automatic.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 2048), automatic.limits.scratch_limit_bytes);
}

test "gemma4 video decodes in-band HEVC parameter sets through VideoToolbox" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/codecs/hevc-hev1-inband.mp4");
    try std.testing.expectEqual(@as(usize, 0), reservation(a, bytes).surface_bytes);
    const Count = struct {
        frames: usize = 0,
        fn accept(raw: *anyopaque, _: usize, _: video.preparation.BorrowedFrame, _: video.preparation.Options, _: media.source.Control) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.frames += 1;
        }
    };
    var count: Count = .{};
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes, .device_bytes = video.apple_hevc.max_surface_bytes } };
    try embedding_video.observeConfigured(a, bytes, null, .{}, .{ .observer = .{ .context = &count, .accept = Count.accept }, .sampling = sampling, .pool = &pool });
    try std.testing.expect(count.frames != 0);
}

test "gemma4 video names the missing HEVC route instead of calling the clip corrupt" {
    const a = std.testing.allocator;
    const Reject = struct {
        fn accept(_: *anyopaque, _: usize, _: video.preparation.BorrowedFrame, _: video.preparation.Options, _: media.source.Control) anyerror!void {
            return error.TestUnexpectedResult;
        }
    };
    var context: u8 = 0;
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes } };
    inline for (.{ "hevc-hvc1.mp4", "hevc-422.mp4" }) |name| {
        const result = embedding_video.observeConfigured(a, @embedFile("../testdata/video/codecs/" ++ name), null, .{}, .{ .observer = .{ .context = &context, .accept = Reject.accept }, .sampling = sampling, .pool = &pool, .decoder = .{ .policy = .portable } });
        try std.testing.expectError(error.HevcDecoderUnavailable, result);
    }
    // The shared input normalizer must keep this distinct from corrupt media.
    try std.testing.expectEqual(error.HevcDecoderUnavailable, inputError(error.HevcDecoderUnavailable));
}

test "gemma4 video tone maps HEVC whose HDR signal is only in the bitstream" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    const Tone = struct {
        seen: usize = 0,
        fn accept(raw: *anyopaque, _: usize, _: video.preparation.BorrowedFrame, options: video.preparation.Options, _: media.source.Control) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(video.preparation.Matrix.bt2020_ncl, options.matrix);
            try std.testing.expectEqual(video.tone.Policy.hlg_reinhard_100, options.tone_mapping);
            self.seen += 1;
        }
    };
    var tone: Tone = .{};
    var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes, .device_bytes = video.apple_hevc.max_surface_bytes } };
    inline for (.{ "hevc-hlg-nocolr.mp4", "hevc-hlg-partial-inband.mp4" }) |name| {
        tone.seen = 0;
        const bytes = @embedFile("../testdata/video/codecs/" ++ name);
        try std.testing.expectEqual(@as(usize, 0), reservation(a, bytes).surface_bytes);
        try embedding_video.observeConfigured(a, bytes, null, .{}, .{ .observer = .{ .context = &tone, .accept = Tone.accept }, .sampling = sampling, .pool = &pool });
        try std.testing.expect(tone.seen != 0);
    }
}

test "gemma4 video reports presentation size for anamorphic containers" {
    const a = std.testing.allocator;
    inline for (.{ "anamorphic-2x1.mp4", "anamorphic-2x1.webm", "bframes.mp4" }, .{ true, true, false }) |name, anamorphic| {
        const Record = struct {
            width: u32 = 0,
            height: u32 = 0,
            raster_width: u32 = 0,
            raster_height: u32 = 0,
            fn accept(raw: *anyopaque, _: usize, frame: video.preparation.BorrowedFrame, options: video.preparation.Options, control: media.source.Control) anyerror!void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                var displayed = try frame.display(std.testing.allocator, options, control);
                defer displayed.deinit(std.testing.allocator);
                self.width = options.presentation_width;
                self.height = options.presentation_height;
                self.raster_width = displayed.width;
                self.raster_height = displayed.height;
            }
        };
        var record: Record = .{};
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes } };
        try embedding_video.observeConfigured(a, @embedFile("../testdata/video/" ++ name), null, .{}, .{ .observer = .{ .context = &record, .accept = Record.accept }, .sampling = sampling, .pool = &pool });
        try std.testing.expectEqual(record.raster_height, record.height);
        if (anamorphic) {
            try std.testing.expectEqual(@as(u32, 64), record.raster_width);
            try std.testing.expectEqual(@as(u32, 128), record.width);
        } else {
            try std.testing.expectEqual(record.raster_width, record.width);
        }
    }
}

test "HEVC inband SAR agrees across admission planning and decoded presentation" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    inline for (.{ "hevc-sar-inband.mp4", "hevc-multi-sps.mp4" }) |name| {
        const inband = @embedFile("../testdata/video/codecs/" ++ name);
        const original = @embedFile("../testdata/video/codecs/hevc-sar-nopasp.mp4");
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes, .device_bytes = video.apple_hevc.max_surface_bytes } };
        const expected = try embedding_video.inspectPlan(a, original, .{}, sampling, &pool, true);
        const actual = try embedding_video.inspectPlan(a, inband, .{}, sampling, &pool, true);
        try std.testing.expectEqual(expected.sequence_tokens, actual.sequence_tokens);
        try std.testing.expectEqual(expected.patch_output_bytes, actual.patch_output_bytes);
        try std.testing.expectEqual(reservation(a, original).host_bytes, reservation(a, inband).host_bytes);
        var budget_source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = inband }, .identity = "inband-budget" };
        var input = try embedding_video.PacketInput.init(a, &budget_source, inband);
        defer input.deinit();
        try std.testing.expect(actual.apple_surface_bytes > 0);
        try std.testing.expectEqual(actual.apple_surface_bytes, try video.apple_hevc.surfaceBytes(input.reader.track));
        const Record = struct {
            seen: usize = 0,
            fn accept(raw: *anyopaque, _: usize, frame: video.preparation.BorrowedFrame, options: video.preparation.Options, control: media.source.Control) anyerror!void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try std.testing.expectEqual(@as(u32, 128), options.presentation_width);
                try std.testing.expectEqual(@as(u32, 64), options.presentation_height);
                var displayed = try frame.display(std.testing.allocator, options, control);
                defer displayed.deinit(std.testing.allocator);
                try std.testing.expectEqual(@as(u32, 64), displayed.width);
                try std.testing.expectEqual(@as(u32, 64), displayed.height);
                self.seen += 1;
            }
        };
        var record: Record = .{};
        try embedding_video.observeConfigured(a, inband, null, .{}, .{ .observer = .{ .context = &record, .accept = Record.accept }, .sampling = sampling, .pool = &pool });
        try std.testing.expect(record.seen != 0);
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = inband }, .identity = "borrowed-sar" };
        var reader = try media.mp4.Reader.init(a, &source, video.mp4_coded.limits);
        defer reader.deinit();
        for ([_]bool{ false, true }) |scoped| {
            var scope = embedding_video.InspectionScope{ .allocator = a };
            defer scope.deinit();
            record.seen = 0;
            try embedding_video.observeConfigured(a, inband, &reader, .{}, .{ .observer = .{ .context = &record, .accept = Record.accept }, .sampling = sampling, .pool = &pool, .inspection_scope = if (scoped) &scope else null });
            try std.testing.expect(record.seen != 0);
        }
    }
}

test "HEVC admission probes metadata without allocations or sample inspection" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    inline for (.{ "hevc-10bit.mp4", "hevc-sar-inband.mp4", "hevc-multi-sps.mp4" }) |name| {
        const fixture = @embedFile("../testdata/video/codecs/" ++ name);
        const expected = reservation(a, fixture);
        try std.testing.expectEqual(apple_hevc_host_bytes, expected.host_bytes);
        try std.testing.expectEqual(@as(usize, 0), expected.surface_bytes);
        try std.testing.expectEqualDeep(expected, reservation(denied.allocator(), fixture));
        // Admission never reads samples, even for in-band configuration.
        const corrupted = try a.dupe(u8, fixture);
        defer a.free(corrupted);
        var cursor: usize = 0;
        while (cursor + 8 <= corrupted.len) {
            const size = std.mem.readInt(u32, corrupted[cursor..][0..4], .big);
            if (size < 8 or size > corrupted.len - cursor) break;
            if (std.mem.eql(u8, corrupted[cursor + 4 ..][0..4], "mdat")) @memset(corrupted[cursor + 8 ..][0 .. size - 8], 0xff);
            cursor += size;
        }
        try std.testing.expectEqualDeep(expected, reservation(denied.allocator(), corrupted));
    }
    try std.testing.expectEqual(@as(usize, 0), denied.allocated_bytes);
}

test "HEVC inband surface upgrades enforce both ledgers and roll back denial" {
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    const session = Session{ .ptr = undefined, .vtable = undefined, .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .backend_limit_bytes = 8192, .scratch_limit_bytes = 2048 }, .static_workspace_bytes = 0, .check_live_memory = false } };
    var prepaid = try session.admitRetainedMedia(1024, 1024);
    defer prepaid.deinit();
    var budget = memory.RunBudget.init(.{ .scratch_limit_bytes = 2048 });
    const admission = Admission{ .run_budget = &budget, .session = session };
    var permit = try admission.acquire(false, .{ .host_bytes = 1024, .surface_bytes = 1024 });
    try permit.ensureSurfaces(admission, 4096);
    try std.testing.expectEqual(@as(usize, 4096), budget.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 4096), controller.snapshot().backend_scratch_bytes);
    // Reusing a sufficient reservation never charges it again.
    try permit.ensureSurfaces(admission, 4096);
    permit.deinit();
    try std.testing.expectEqual(@as(usize, 0), budget.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 2048), budget.limits.scratch_limit_bytes);
    try std.testing.expectEqual(@as(usize, 1024), controller.snapshot().backend_scratch_bytes);
    var explicit = memory.RunBudget.init(.{ .scratch_limit_bytes = 2048 });
    const capped = Admission{ .run_budget = &explicit, .explicit_scratch_limit = 2048 };
    var refused = try capped.acquire(false, .{ .host_bytes = 1024 });
    defer refused.deinit();
    try std.testing.expectError(error.MemoryBudgetExceeded, refused.ensureSurfaces(capped, 4096));
    try std.testing.expectEqual(@as(usize, 0), explicit.backend_scratch_bytes);
    var peer_session = session;
    peer_session.run_admission.?.limits.scratch_limit_bytes = 0;
    var peer = try peer_session.admitRetainedMedia(0, 7168);
    defer peer.deinit();
    var rollback = try admission.acquire(false, .{ .host_bytes = 1024, .surface_bytes = 1024 });
    defer rollback.deinit();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, rollback.ensureSurfaces(admission, 4096));
    try std.testing.expectEqual(@as(usize, 1024), budget.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 2048), budget.limits.scratch_limit_bytes);
    try std.testing.expectEqual(@as(usize, 1024), rollback.reservation.surface_bytes);
}

test "HEVC cropped reference admission is coded sized before and after inband activation" {
    if (@import("builtin").os.tag != .macos) return error.SkipZigTest;
    const a = std.testing.allocator;
    inline for (.{ "hevc-cropped4k-8.mp4", "hevc-cropped4k-10.mp4", "hevc-cropped4k-inband.mp4" }) |name| {
        const bytes = @embedFile("../testdata/video/codecs/" ++ name);
        const initial = reservation(a, bytes);
        var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = bytes }, .identity = "cropped-reference-budget" };
        var input = try embedding_video.PacketInput.init(a, &source, bytes);
        defer input.deinit();
        const required = try video.apple_hevc.surfaceBytes(input.reader.track);
        try std.testing.expect(required > 400 * 1024 * 1024);
        var controller = memory.AdmissionController{};
        defer controller.deinit();
        const session = Session{ .ptr = undefined, .vtable = undefined, .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false } };
        var prepaid = try session.admitRetainedMedia(initial.host_bytes, initial.surface_bytes);
        defer prepaid.deinit();
        var budget = memory.RunBudget.init(.{});
        const admission = Admission{ .run_budget = &budget, .session = session };
        var permit = try admission.acquire(false, initial);
        try permit.ensureSurfaces(admission, @intCast(required));
        try std.testing.expectEqual(required, budget.backend_scratch_bytes);
        try std.testing.expect(controller.snapshot().backend_scratch_bytes >= required);
        try std.testing.expectEqual(@as(usize, 0), initial.surface_bytes);
        permit.deinit();
        try std.testing.expectEqual(@as(usize, 0), budget.backend_scratch_bytes);
        try std.testing.expectEqual(initial.surface_bytes, controller.snapshot().backend_scratch_bytes);
    }
}

test "gemma4 video plans multi-PPS AVC once and uses the current request control on cache hits" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/h264-two-pps.mp4");
    var inspection = embedding_video.InspectionScope{ .allocator = a };
    defer inspection.deinit();
    const config = embedding_video.Configuration{ .inspection_scope = &inspection, .sampling = .gemma4 };
    const times = try embedding_video.selectedConfiguredPresentationUs(a, bytes, .{}, config);
    defer a.free(times);
    const entry = inspection.find(bytes, null, config).?;
    try std.testing.expect(entry.reader.track.multiple_avc_parameter_sets);
    try std.testing.expect(entry.reader.track.edit.source_duration == null);
    try std.testing.expectEqual(@as(i64, 0), times[0]);
    try std.testing.expectError(error.Timeout, embedding_video.selectedConfiguredPresentationUs(a, bytes, .{ .deadline_ns = 0 }, config));
    const direct = try embedding_video.selectedPresentationUs(a, bytes, .{}, .gemma4, null);
    defer a.free(direct);
    try std.testing.expectEqualSlices(i64, times, direct);
}

test "gemma4 video split picture keeps its earliest presentation time" {
    const a = std.testing.allocator;
    var source = media.source.Source{ .allocator = a, .storage = .{ .borrowed = @embedFile("../testdata/video/h264-paff-adaptive-packets-8-1.mp4") }, .identity = "fields" };
    var reader = try media.mp4.Reader.init(a, &source, .{});
    defer reader.deinit();
    var logical = try video.h264_pictures.planning(a, &reader, .{}, null);
    defer logical.deinit();
    var selected_pair = false;
    for (logical.pictures) |picture| {
        const last = picture.last_packet orelse continue;
        if (last == picture.first_packet) continue;
        reader.track.timescale = 1000;
        reader.track.edit = .{ .source_start = 0, .source_duration = 1000 };
        for (reader.packets) |*packet| packet.pts = -100;
        reader.packets[picture.first_packet].pts = 500;
        reader.packets[last].pts = 200;
        const plan = try video.packet_sampling.gemma4PortablePlan(a, &reader, null);
        try std.testing.expectEqualSlices(usize, &.{picture.first_packet}, plan.ids());
        try std.testing.expectEqualSlices(i64, &.{200000}, plan.timestamps());
        selected_pair = true;
        break;
    }
    try std.testing.expect(selected_pair);
}

test "gemma4 video bicubic cache survives pixel and geometry changes" {
    const image = @import("antfly_image");
    const a = std.testing.allocator;
    var cache = image.processing.PillowResizeCache{};
    defer cache.deinit();
    var bytes: [7 * 5 * 3]u8 = undefined;
    const view = @import("image.zig").ImageU8{ .width = 7, .height = 5, .format = .rgb8, .data = &bytes };
    var previous: ?[*]u8 = null;
    for ([_]u32{ 4, 4, 8 }, 0..) |width, frame| {
        for (&bytes, 0..) |*v, i| v.* = @truncate(i * 41 + frame * 17);
        const expected = try image.processing.preprocessDecodedRectWithResample(a, view, width, 4, .{ 0, 0, 0 }, .{ 1, 1, 1 }, .torchvision_bicubic);
        defer a.free(expected);
        const actual = try image.processing.preprocessWithResizeCache(a, view, width, 4, .{ 0, 0, 0 }, .{ 1, 1, 1 }, 1.0 / 255.0, .torchvision_bicubic, &cache);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, expected, actual);
        if (frame == 1) try std.testing.expect(previous.? == cache.plan.?.horizontal.ptr);
        previous = cache.plan.?.horizontal.ptr;
    }
}

test "gemma4 video portable decoder delivers the planned slots across NVDEC limits" {
    const a = std.testing.allocator;
    const Counter = struct {
        slots: std.StaticBitSet(32) = .empty,
        fn accept(raw: *anyopaque, slot: usize, _: video.preparation.BorrowedFrame, _: video.preparation.Options, _: media.source.Control) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(slot < 32 and !self.slots.isSet(slot));
            self.slots.set(slot);
        }
    };
    inline for (.{ "h264-two-pps.mp4", "h264-18001.mp4" }) |name| {
        const bytes = @embedFile("../testdata/video/" ++ name);
        var scope = embedding_video.InspectionScope{ .allocator = a, .limit = inspection_bytes };
        defer scope.deinit();
        var pool = media.admission.Pool{ .limits = .{ .host_bytes = portable_pool_bytes } };
        var count = Counter{};
        const options = embedding_video.Configuration{ .sampling = .gemma4, .pool = &pool, .inspection_scope = &scope, .decoder = .{ .policy = .portable }, .observer = .{ .context = &count, .accept = Counter.accept } };
        const timestamps = try embedding_video.selectedConfiguredPresentationUs(a, bytes, .{}, options);
        defer a.free(timestamps);
        try embedding_video.observeConfigured(a, bytes, null, .{}, options);
        try std.testing.expectEqual(timestamps.len, count.slots.count());
        if (comptime std.mem.eql(u8, name, "h264-18001.mp4")) {
            try std.testing.expectEqual(@as(usize, 32), timestamps.len);
            try std.testing.expectEqual(@as(i64, 60000000), timestamps[31]);
        }
    }
}

test "gemma4 video AV1 MP4 preserves the edit across portable selection and decoder preroll" {
    const a = std.testing.allocator;
    const bytes = try a.dupe(u8, @embedFile("../testdata/video/codecs/av1.mp4"));
    defer a.free(bytes);
    const edit = std.mem.indexOf(u8, bytes, "elst").? + 4;
    // Movie clock 1000 Hz, track clock 16384 Hz, four pictures per second.
    // Start between frames at 0.375 s and retain exactly two seconds.
    std.mem.writeInt(u32, bytes[edit + 8 ..][0..4], 2000, .big);
    std.mem.writeInt(i32, bytes[edit + 12 ..][0..4], 6144, .big);
    var scope = embedding_video.InspectionScope{ .allocator = a, .limit = inspection_bytes };
    defer scope.deinit();
    const config = embedding_video.Configuration{ .sampling = .gemma4, .inspection_scope = &scope, .decoder = .{ .policy = .portable } };
    const times = try embedding_video.selectedConfiguredPresentationUs(a, bytes, .{}, config);
    defer a.free(times);
    try std.testing.expectEqualSlices(i64, &.{ 125000, 1125000 }, times);
    const direct = try embedding_video.selectedPresentationUs(a, bytes, .{}, .gemma4, null);
    defer a.free(direct);
    try std.testing.expectEqualSlices(i64, times, direct);
    const entry = scope.find(bytes, null, config).?;
    try std.testing.expectEqualSlices(usize, &.{ 2, 6 }, entry.selected.?);
    try std.testing.expectEqual(@as(usize, 16), entry.webm_reader.packets.len);
    const Counter = struct {
        seen: [2]bool = @splat(false),
        fn accept(raw: *anyopaque, slot: usize, _: video.preparation.BorrowedFrame, _: video.preparation.Options, _: media.source.Control) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(slot < 2 and !self.seen[slot]);
            self.seen[slot] = true;
        }
    };
    var counter = Counter{};
    var observed = config;
    observed.observer = .{ .context = &counter, .accept = Counter.accept };
    try embedding_video.observeConfigured(a, bytes, null, .{}, observed);
    try std.testing.expectEqual([2]bool{ true, true }, counter.seen);
}

test "gemma4 video multi-PPS supports embedding sampling and sparse long-clip decode" {
    const a = std.testing.allocator;
    const bytes = @embedFile("../testdata/video/h264-two-pps.mp4");
    var source = media.source.Source{ .allocator = a, .identity = "multi-PPS", .storage = .{ .borrowed = bytes } };
    var original = try media.mp4.Reader.init(a, &source, .{});
    defer original.deinit();
    const policies = [_]embedding_video.Sampling{ .reference, .{ .timestamps = .{} }, .{ .source_timestamps = .{ .end_ns = std.time.ns_per_s } } };
    for (policies) |policy| {
        const selected = try embedding_video.selectWithSampling(a, &original, policy);
        defer a.free(selected);
        try std.testing.expect(selected.len != 0);
    }
    // Repeat an independently coded GOP by borrowing its payloads. This spans
    // both previous limits without a second large checked-in bitstream.
    var reader = original;
    reader.track.timescale = 30;
    reader.packets = try a.alloc(media.mp4.Packet, 12_000);
    defer a.free(reader.packets);
    for (reader.packets, 0..) |*packet, i| {
        packet.* = original.packets[i % original.packets.len];
        packet.pts = @intCast(i);
        packet.duration = 1;
        packet.sync = false; // Prove restart decisions come from the active slice headers.
    }
    var scope = embedding_video.InspectionScope{ .allocator = a, .limit = inspection_bytes };
    defer scope.deinit();
    const config = embedding_video.Configuration{ .sampling = .gemma4 };
    _ = try scope.inspect(bytes, &reader, .{}, config, false);
    const entry = scope.find(bytes, &reader, config).?;
    const Counter = struct {
        seen: std.StaticBitSet(32) = .empty,
        fn publish(raw: *anyopaque, slot: usize, _: *const video.h264.Frame) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!self.seen.isSet(slot));
            self.seen.set(slot);
        }
    };
    var counter = Counter{};
    const before_reads = source.reads;
    const before_bytes = source.total_bytes;
    const stats = try entry.configurations.?.decodeSelectedBorrowed(a, &reader, entry.selected.?, &counter, Counter.publish);
    try std.testing.expectEqual(@as(usize, 32), counter.seen.count());
    try std.testing.expectEqual(stats.decoded_packets, source.reads - before_reads);
    try std.testing.expectEqual(stats.payload_bytes, source.total_bytes - before_bytes);
    try std.testing.expect(stats.decoded_packets <= 32 * original.packets.len);
}
